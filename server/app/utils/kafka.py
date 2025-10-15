from aiokafka import AIOKafkaProducer
import json

kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'video_processing'

async def send_video_processing_message(video_hash, video_path):
    producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap_servers)
    await producer.start()
    try:
        message = json.dumps({'video_hash': video_hash, 'video_path': video_path}).encode('utf-8')
        await producer.send_and_wait(kafka_topic, message)
    finally:
        await producer.stop()
