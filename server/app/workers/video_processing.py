from aiokafka import AIOKafkaConsumer
from utils.ffmpeg_util import transcode_to_dash, transcode_to_hls, video_thumbnail
from crud.video import update_video_details
from core.database import AsyncSessionLocal
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import logging
import json
import os
import asyncio

load_dotenv()

logging.basicConfig(filename='logs/worker.log', level=logging.INFO)

kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
kafka_topic = os.getenv("KAFKA_VIDEO_TOPIC")

executor = ThreadPoolExecutor(max_workers=2)

async def run_ffmpeg_async(func, *args, **kwargs):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, lambda: func(*args, **kwargs))

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

    hls_path = await run_ffmpeg_async(transcode_to_hls, input_path, output_dir)
    logging.info(f"Transcoding to DASH for video {video_hash}")

    dash_path = await run_ffmpeg_async(transcode_to_dash, input_path, output_dir)
    logging.info(f"Transcoding completed for {video_hash}")

    thumbnail_path = await run_ffmpeg_async(video_thumbnail, input_path, output_dir)
    logging.info(f"Thumbnail Generation Completed for video_hash {video_hash}")

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
    thumbnail_url = to_web_path(thumbnail_path)
    async with AsyncSessionLocal() as db:
        await update_video_details(video_hash, {
            "status": "completed",
            "url": hls_url,  # primary playback URL defaults to HLS
            "hls_url": hls_url,
            "dash_url": dash_url,
            "thumbnail_url": thumbnail_url
        }, db)

async def consume_video_processing_message():
    """Resilient consumer loop that reconnects on errors and keeps running."""
    while True:
        consumer = AIOKafkaConsumer(
            kafka_topic,
            bootstrap_servers=kafka_bootstrap_servers,
            group_id="video_processor_group",
            enable_auto_commit=True,            # reduce UnknownMemberId/IllegalGeneration churn
            auto_offset_reset="earliest",
            heartbeat_interval_ms=3000,
            session_timeout_ms=10000,
        )
        try:
            logging.info("Starting Kafka consumer...")
            await consumer.start()
            async for msg in consumer:
                try:
                    logging.info(f"Consumed message: {msg.value.decode('utf-8')}")
                    await process_video_message(msg)
                except Exception as e:
                    logging.exception(f"Failed to process message: {e}")
        except Exception as e:
            logging.exception(f"Kafka consumer error, will retry: {e}")
            # backoff before retrying connection
            await asyncio.sleep(3)
        finally:
            try:
                await consumer.stop()
            except Exception:
                pass

