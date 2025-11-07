"""Utility to send video processing tasks to Celery"""

from celery_app import celery_app, process_video
import logging

logger = logging.getLogger(__name__)


def send_video_processing_task(video_hash: str, video_path: str):
    """
    Send a video processing task to Celery.
    This runs asynchronously and doesn't block the main thread.
    
    Args:
        video_hash: The hash of the video
        video_path: Path to the video file
    
    Returns:
        Celery AsyncResult instance
    """
    try:
        message_data = {
            'video_hash': video_hash,
            'video_path': video_path
        }
        
        # Send task to Celery (non-blocking)
        result = process_video.delay(message_data)
        logger.info(f"Sent video processing task to Celery for {video_hash}, task_id: {result.id}")
        return result
    except Exception as e:
        logger.error(f"Failed to send video processing task to Celery: {e}")
        raise

