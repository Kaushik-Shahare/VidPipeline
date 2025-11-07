from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import json
from dotenv import load_dotenv
import logging
import os

load_dotenv()

logger = logging.getLogger(__name__)

kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
kafka_topic = os.getenv("KAFKA_VIDEO_TOPIC", "video-processing")


async def send_video_processing_message(video_hash: str, video_path: str):
    """
    Send a video processing message to Kafka.
    This will be consumed by the Kafka consumer in the lifespan.
    
    Args:
        video_hash: The hash of the video
        video_path: Location of the uploaded video (Azure blob name)
    """
    producer = None
    try:
        producer = AIOKafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        await producer.start()
        
        message = {
            'video_hash': video_hash,
            'video_path': video_path
        }
        
        logger.info(f"Sending video processing message to Kafka: {message}")
        await producer.send_and_wait(kafka_topic, message)
        logger.info(f"Successfully sent message to Kafka for video {video_hash}")
        
    except Exception as e:
        logger.error(f"Failed to send message to Kafka: {e}")
        raise
    finally:
        if producer:
            await producer.stop()


async def consume_video_processing_messages(celery_app):
    """
    Consume video processing messages from Kafka and send them to Celery.
    This runs in the background during the app lifespan.
    
    Args:
        celery_app: The Celery application instance
    """
    consumer = None
    try:
        consumer = AIOKafkaConsumer(
            kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='video-processing-group',
            auto_offset_reset='earliest'
        )
        await consumer.start()
        logger.info(f"Kafka consumer started, listening on topic: {kafka_topic}")
        
        async for message in consumer:
            try:
                message_data = message.value
                logger.info(f"Received message from Kafka: {message_data}")
                
                video_hash = message_data.get('video_hash')
                video_path = message_data.get('video_path')
                
                if not video_hash or not video_path:
                    logger.error(f"Invalid message format: {message_data}")
                    continue
                
                # Send to Celery for processing
                from celery_app import process_video
                result = process_video.delay(message_data)
                logger.info(f"Sent task to Celery for video {video_hash}, task_id: {result.id}")
                
            except Exception as e:
                logger.error(f"Error processing Kafka message: {e}")
                # Continue consuming other messages
                continue
                
    except Exception as e:
        logger.error(f"Kafka consumer error: {e}")
        raise
    finally:
        if consumer:
            await consumer.stop()
            logger.info("Kafka consumer stopped")

