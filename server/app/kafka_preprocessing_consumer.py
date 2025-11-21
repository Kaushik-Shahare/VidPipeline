"""
Kafka Consumer Service for Video Preprocessing
Consumes messages from video_preprocessing topic and triggers preprocessing Celery tasks.
"""

import asyncio
import logging
import os
import signal
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Main entry point for preprocessing consumer."""
    from utils.kafka import consume_video_processing_messages
    from celery_app import celery_app, preprocess_video
    
    # Get consumer configuration from environment
    consumer_group = os.getenv("KAFKA_CONSUMER_GROUP", "video_preprocessing_consumer")
    kafka_topic = os.getenv("KAFKA_PREPROCESSING_TOPIC", "video_preprocessing")
    
    logger.info(f"Starting preprocessing consumer...")
    logger.info(f"Consumer Group: {consumer_group}")
    logger.info(f"Kafka Topic: {kafka_topic}")
    logger.info(f"Bootstrap Servers: {os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')}")
    
    # Import and modify consume function to use preprocessing topic
    from aiokafka import AIOKafkaConsumer
    import json
    
    consumer = None
    shutdown_event = asyncio.Event()
    
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, initiating graceful shutdown...")
        shutdown_event.set()
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        consumer = AIOKafkaConsumer(
            kafka_topic,
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id=consumer_group,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
        )
        
        await consumer.start()
        logger.info(f"Preprocessing consumer started and listening on topic: {kafka_topic}")
        
        # Main consumption loop
        while not shutdown_event.is_set():
            try:
                # Use asyncio.wait_for to allow periodic shutdown checks
                msg_batch = await asyncio.wait_for(
                    consumer.getmany(timeout_ms=1000, max_records=10),
                    timeout=2.0
                )
                
                for topic_partition, messages in msg_batch.items():
                    for message in messages:
                        try:
                            logger.info(f"Received preprocessing message: {message.value}")
                            
                            # Send to Celery preprocessing task
                            task = preprocess_video.delay(message.value)
                            logger.info(f"Dispatched preprocessing task {task.id} for video {message.value.get('video_hash')}")
                            
                        except Exception as e:
                            logger.error(f"Error processing message: {e}", exc_info=True)
                            # Continue processing other messages
                            
            except asyncio.TimeoutError:
                # Timeout is expected, continue loop
                continue
            except Exception as e:
                logger.error(f"Error in consumption loop: {e}", exc_info=True)
                await asyncio.sleep(5)  # Back off on error
                
    except Exception as e:
        logger.error(f"Fatal error in preprocessing consumer: {e}", exc_info=True)
        raise
    finally:
        if consumer:
            logger.info("Stopping preprocessing consumer...")
            await consumer.stop()
            logger.info("Preprocessing consumer stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Preprocessing consumer interrupted by user")
    except Exception as e:
        logger.error(f"Preprocessing consumer failed: {e}", exc_info=True)
        exit(1)
