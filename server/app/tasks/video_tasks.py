from celery import Task
import json
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VideoProcessingTask(Task):
    """Video processing task for Celery"""
    name = 'tasks.process_video'

executor = ThreadPoolExecutor(max_workers=2)


async def run_ffmpeg_async(func, *args, **kwargs):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, lambda: func(*args, **kwargs))

def create_process_video_task(celery_app):
    """Create the process_video task"""
    
    @celery_app.task(base=VideoProcessingTask, bind=True, name='tasks.process_video')
    def process_video(self, message_data):
        """
        Process video task in Celery worker.
        Runs in the background without blocking the main FastAPI thread.
        """
        logger.info(f"Processing video task: {message_data}")
        
        # Extract message data
        video_hash = message_data.get('video_hash')
        input_path = message_data.get('video_path')
        
        if not video_hash or not input_path:
            logger.error("Message missing required fields 'video_hash' or 'video_path'")
            return {"status": "error", "message": "Missing required fields"}
        
        try:
            # Run the async video processing in a new event loop
            result = asyncio.run(_async_process_video(video_hash, input_path))
            return result
        except Exception as e:
            logger.exception(f"Error processing video: {e}")
            return {"status": "error", "message": str(e)}
    
    return process_video


async def _async_process_video(video_hash: str, input_path: str):
    """Internal async function to process video"""
    import sys
    import os
    
    # Get the app directory (parent of tasks/)
    app_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    
    # Add the app directory to the path for imports
    if app_dir not in sys.path:
        sys.path.insert(0, app_dir)
    
    from utils.azure_blob import download_video_blob, upload_directory
    from utils.ffmpeg_util import transcode_to_dash, transcode_to_hls, video_thumbnail
    from crud.video import update_video_details
    from core.database import AsyncSessionLocal
    import logging
    
    logger = logging.getLogger(__name__)
    logger.info(f"Transcoding video {video_hash}")
    
    # Ensure video is available locally for processing
    local_dir = os.path.join(app_dir, "media", "uploads", video_hash)
    os.makedirs(local_dir, exist_ok=True)

    local_input_path = os.path.join(local_dir, "source.mp4")

    if not os.path.exists(local_input_path) or os.path.getsize(local_input_path) == 0:
        await run_ffmpeg_async(download_video_blob, video_hash, local_input_path)

    # Derive output directory from local input path
    output_dir = os.path.dirname(local_input_path)
    
    # Process video
    hls_path = await run_ffmpeg_async(transcode_to_hls, local_input_path, output_dir)
    logger.info(f"Transcoding to DASH for video {video_hash}")
    
    dash_path = await run_ffmpeg_async(transcode_to_dash, local_input_path, output_dir)
    logger.info(f"Transcoding completed for {video_hash}")
    
    thumbnail_path = await run_ffmpeg_async(video_thumbnail, local_input_path, output_dir)
    logger.info(f"Thumbnail Generation Completed for video_hash {video_hash}")

    # Upload processed outputs back to Azure Blob under the same directory as source
    uploaded_assets = {}
    try:
        # Exclude the original source file to avoid overwriting
        uploaded_assets = upload_directory(video_hash, output_dir, dest_prefix="", exclude=["source.mp4"])
        logger.info(f"Uploaded processed assets to Azure Blob for {video_hash}")

        # After successful upload, remove local files to free disk space
        try:
            import shutil
            shutil.rmtree(output_dir, ignore_errors=False)
            logger.info(f"Deleted local processed directory for {video_hash}: {output_dir}")
        except Exception as cleanup_err:
            logger.warning(f"Could not delete local directory {output_dir} for {video_hash}: {cleanup_err}")

    except Exception as e:
        logger.exception(f"Failed to upload processed assets for {video_hash}: {e}")
    
    # Helper to normalize paths for lookup
    def to_rel(path: str) -> str:
        return os.path.relpath(path, output_dir).replace("\\", "/")

    hls_rel = to_rel(hls_path)
    dash_rel = to_rel(dash_path)
    thumbnail_rel = to_rel(thumbnail_path)

    # Update database
    media_dir = os.path.join(app_dir, 'media')
    
    def to_web_path(fs_path: str) -> str:
        try:
            # Simple approach: find /media/ in path and use everything after it
            if '/media/' in fs_path:
                parts = fs_path.split('/media/', 1)
                if len(parts) == 2:
                    return f"/media/{parts[1]}"
            # Fallback: try relative path
            rel = os.path.relpath(fs_path, media_dir)
            return f"/media/{rel}"
        except Exception as e:
            logger.error(f"Error converting path {fs_path}: {e}")
            return fs_path
    
    def lookup_uploaded_url(rel_path: str, fallback_path: str) -> str:
        info = uploaded_assets.get(rel_path, {})
        url = info.get("url") if isinstance(info, dict) else None
        if url:
            return url
        logger.warning(f"Falling back to local path for {rel_path} in video {video_hash}")
        return to_web_path(fallback_path)

    hls_url = lookup_uploaded_url(hls_rel, hls_path)
    dash_url = lookup_uploaded_url(dash_rel, dash_path)
    thumbnail_url = lookup_uploaded_url(thumbnail_rel, thumbnail_path)
    
    # Log the URLs for debugging
    logger.info(f"Generated URLs - HLS: {hls_url}, DASH: {dash_url}, Thumbnail: {thumbnail_url}")
    
    async with AsyncSessionLocal() as db:
        await update_video_details(video_hash, {
            "status": "completed",
            "url": hls_url,
            "hls_url": hls_url,
            "dash_url": dash_url,
            "thumbnail_url": thumbnail_url
        }, db)
    
    return {
        "status": "completed",
        "video_hash": video_hash,
        "hls_url": hls_url,
        "dash_url": dash_url,
        "thumbnail_url": thumbnail_url
    }

