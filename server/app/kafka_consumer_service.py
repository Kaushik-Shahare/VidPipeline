"""
Standalone Kafka consumer service.
Deploy as a separate Kubernetes deployment with replicas=1
"""
import asyncio
import logging
import os
from utils.kafka import consume_video_processing_messages
from celery_app import celery_app

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("logs/kafka_consumer.log", mode='a'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

async def main():
    """Main entry point for Kafka consumer service"""
    logger.info("Starting Kafka consumer service...")
    logger.info(f"Connecting to Kafka and Redis...")
    
    try:
        # Read optional consumer group and profile from environment so this single
        # service can be deployed multiple times with different group/profile
        consumer_group = os.getenv("KAFKA_CONSUMER_GROUP")
        consumer_profile = os.getenv("CONSUMER_PROFILE")
        await consume_video_processing_messages(celery_app, group_id=consumer_group, profile=consumer_profile)
    except KeyboardInterrupt:
        logger.info("Kafka consumer service shutting down...")
    except Exception as e:
        logger.error(f"Fatal error in Kafka consumer: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
