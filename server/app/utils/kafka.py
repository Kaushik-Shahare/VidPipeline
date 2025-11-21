from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import json
from dotenv import load_dotenv
import logging
import os
import signal
import asyncio

load_dotenv()

logger = logging.getLogger(__name__)

kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
kafka_topic = os.getenv("KAFKA_VIDEO_TOPIC", "video-processing")


async def send_kafka_message(topic: str, message: dict):
    """
    Send a message to a specific Kafka topic.
    
    Args:
        topic: The Kafka topic to send to
        message: Dictionary containing the message data
    """
    producer = None
    try:
        producer = AIOKafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        await producer.start()
        
        logger.info(f"Sending message to Kafka topic '{topic}': {message}")
        await producer.send_and_wait(topic, message)
        logger.info(f"Successfully sent message to Kafka topic '{topic}'")
        
    except Exception as e:
        logger.error(f"Failed to send message to Kafka topic '{topic}': {e}")
        raise
    finally:
        if producer:
            await producer.stop()


async def send_video_processing_message(video_hash: str, video_path: str):
    """
    Send a video processing message to Kafka.
    This will be consumed by the Kafka consumer service.
    
    Args:
        video_hash: The hash of the video
        video_path: Location of the uploaded video (Azure blob name)
    """
    await send_kafka_message(
        topic=kafka_topic,
        message={
            'video_hash': video_hash,
            'video_path': video_path
        }
    )


async def consume_video_processing_messages(celery_app, group_id: str = None, profile: str = None):
    """
    Consume video processing messages from Kafka and send them to Celery.
    Runs as a standalone service with graceful shutdown support.
    
    Args:
        celery_app: The Celery application instance
    """
    consumer = None
    shutdown_event = asyncio.Event()
    
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, initiating graceful shutdown...")
        shutdown_event.set()
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        chosen_group = group_id or os.getenv("KAFKA_CONSUMER_GROUP") or f"video_processing_{profile or 'default'}"
        consumer = AIOKafkaConsumer(
            kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id=chosen_group,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
        )
        await consumer.start()
        logger.info(f"Kafka consumer started, listening on topic: {kafka_topic}")
        logger.info(f"Consumer group: {chosen_group}")
        
        while not shutdown_event.is_set():
            try:
                # Use get_many with timeout to allow checking shutdown_event
                data = await asyncio.wait_for(
                    consumer.getmany(timeout_ms=1000),
                    timeout=2.0
                )
                
                for tp, messages in data.items():
                    for message in messages:
                        try:
                            message_data = message.value
                            logger.info(f"Received message from Kafka: {message_data}")
                            
                            video_hash = message_data.get('video_hash')
                            video_path = message_data.get('video_path')
                            
                            if not video_hash or not video_path:
                                logger.error(f"Invalid message format: {message_data}")
                                continue
                            
                            # If a profile is provided to this consumer, send a profile-specific task
                            if profile:
                                logger.info(f"Dispatching profile '{profile}' task for video {video_hash}")
                                try:
                                    # Thumbnail gets its own task
                                    if profile == 'thumbnail':
                                        result = celery_app.send_task(
                                            'tasks.process_thumbnail',
                                            args=[message_data],
                                            queue='video_processing_thumbnail'
                                        )
                                    else:
                                        # Use a task dedicated to profile-based processing
                                        result = celery_app.send_task(
                                            'tasks.process_profile',
                                            args=[message_data, profile],
                                            queue=f"video_processing_{profile}"
                                        )
                                    logger.info(f"Sent profile task to Celery for video {video_hash}, task_id: {getattr(result, 'id', repr(result))}")
                                except Exception as e:
                                    logger.error(f"Failed to enqueue profile task: {e}")
                                    continue
                            else:
                                # Fallback: schedule the original, full processing task
                                from celery_app import process_video
                                result = process_video.delay(message_data)
                                logger.info(f"Sent task to Celery for video {video_hash}, task_id: {result.id}")
                            
                        except Exception as e:
                            logger.error(f"Error processing Kafka message: {e}", exc_info=True)
                            continue
                            
            except asyncio.TimeoutError:
                # Normal timeout, continue loop to check shutdown_event
                continue
            except Exception as e:
                logger.error(f"Error fetching messages: {e}", exc_info=True)
                await asyncio.sleep(5)  # Back off on errors
                
    except Exception as e:
        logger.error(f"Kafka consumer error: {e}", exc_info=True)
        raise
    finally:
        if consumer:
            await consumer.stop()
            logger.info("Kafka consumer stopped gracefully")

