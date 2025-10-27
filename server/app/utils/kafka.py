from aiokafka import AIOKafkaProducer
import json
from dotenv import load_dotenv
load_dotenv()
import os

kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
kafka_topic = os.getenv("KAFKA_VIDEO_TOPIC")

async def send_video_processing_message(video_hash, video_path):
    producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap_servers)
    await producer.start()
    try:
        message = json.dumps({'video_hash': video_hash, 'video_path': video_path}).encode('utf-8')
        await producer.send_and_wait(kafka_topic, message)
    finally:
        await producer.stop()
