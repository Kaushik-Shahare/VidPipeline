from aiokafka import AIOKafkaConsumer
from utils.ffmpeg_util import transcode_to_dash, transcode_to_hls
from crud.video import update_video_details
from core.database import AsyncSessionLocal
import logging
import json
import os

logging.basicConfig(filename='worker.log', level=logging.INFO)

kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'video_processing'

async def process_video_message(message):
    logging.info("Processing video message")

    try:
        payload = json.loads(message.value.decode('utf-8'))
    except Exception as e:
        logging.error(f"Failed to parse message: {e}")
        return

    video_hash = payload.get('video_hash')
    input_path = payload.get('video_path')
    if not video_hash or not input_path:
        logging.error("Message missing required fields 'video_hash' or 'video_path'")
        return

    # Derive output directory from input path to avoid relative path issues
    try:
        output_dir = os.path.dirname(input_path)
    except Exception:
        output_dir = f"media/uploads/{video_hash}"

    logging.info(f"Transcoding video {video_hash}")

    hls_path = transcode_to_hls(input_path, output_dir)
    logging.info(f"Transcoding to DASH for video {video_hash}")

    dash_path = transcode_to_dash(input_path, output_dir)
    logging.info(f"Transcoding completed for {video_hash}")

    logging.info(f"Updating video status to completed for {video_hash}")
    # Convert filesystem paths to web paths under /media
    app_dir = os.path.abspath(os.path.dirname(__file__))
    media_dir = os.path.join(app_dir, 'media')
    def to_web_path(fs_path: str) -> str:
        try:
            rel = os.path.relpath(fs_path, media_dir)
            return f"/media/{rel}"
        except Exception:
            # Fallback: detect '/media/' marker in fs path
            marker = f"{os.sep}media{os.sep}"
            idx = fs_path.rfind(marker)
            return f"/media/{fs_path[idx+len(marker):]}" if idx != -1 else fs_path

    hls_url = to_web_path(hls_path)
    dash_url = to_web_path(dash_path)
    async with AsyncSessionLocal() as db:
        await update_video_details(video_hash, {
            "status": "completed",
            "url": hls_url,  # primary playback URL defaults to HLS
            "hls_url": hls_url,
            "dash_url": dash_url 
        }, db)

async def consume_video_processing_message():
    consumer = AIOKafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_bootstrap_servers,
        group_id="video_processor_group",
        enable_auto_commit=False
    )
    await consumer.start()
    try:
        async for msg in consumer:
            logging.info(f"Consumed message: {msg.value.decode('utf-8')}")
            # Here you can add code to process the video messagek
            await process_video_message(msg)
            await consumer.commit()
    finally:
        await consumer.stop()

