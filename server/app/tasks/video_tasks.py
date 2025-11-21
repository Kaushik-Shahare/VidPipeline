from celery import Task
import json
import logging
import asyncio
import tempfile
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VideoProcessingTask(Task):
    """Video processing task for Celery"""
    name = 'tasks.process_video'


class PreprocessingTask(Task):
    """Preprocessing task for compression and virus scanning"""
    name = 'tasks.preprocess_video'


async def run_ffmpeg_async(func, *args, **kwargs):
    """Run blocking FFmpeg function in thread pool to avoid blocking event loop."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

def create_process_video_task(celery_app):
    """Create the process_video task"""
    
    @celery_app.task(
        base=VideoProcessingTask,
        bind=True,
        name='tasks.process_video',
        max_retries=3,
        default_retry_delay=60,
        autoretry_for=(Exception,),
        retry_backoff=True,
        retry_backoff_max=600,
        retry_jitter=True
    )
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
            raise ValueError("Missing required fields 'video_hash' or 'video_path'")
        
        try:
            # Run the async video processing in a new event loop
            result = asyncio.run(_async_process_video(video_hash, input_path))
            return result
        except Exception as e:
            logger.exception(f"Error processing video: {e}")
            # Re-raise to trigger Celery retry
            raise
    
    return process_video


def create_preprocess_video_task(celery_app):
    """Create preprocessing task for compression and virus scanning."""
    
    @celery_app.task(
        base=PreprocessingTask,
        bind=True,
        name='tasks.preprocess_video',
        max_retries=2,
        default_retry_delay=120,
        autoretry_for=(Exception,),
        retry_backoff=True,
        retry_backoff_max=600,
        retry_jitter=True
    )
    def preprocess_video(self, message_data):
        """Preprocess video: compress and virus scan before transcoding.
        
        Steps:
        1. Download video from Azure
        2. Run virus scan (ClamAV)
        3. Compress video to reduce size
        4. Upload compressed version back to Azure
        5. Trigger video processing pipeline
        
        Args:
            message_data: dict with video_hash and video_path
        """
        logger.info(f"Preprocessing video task: {message_data}")
        
        video_hash = message_data.get('video_hash')
        input_path = message_data.get('video_path')
        
        if not video_hash or not input_path:
            logger.error("Missing required fields 'video_hash' or 'video_path'")
            raise ValueError("Missing required fields")
        
        try:
            # Run the async preprocessing in a new event loop (same pattern as process_video)
            result = asyncio.run(_async_preprocess_video(video_hash, input_path))
            return result
        except Exception as e:
            logger.exception(f"Preprocessing failed for {video_hash}: {e}")
            # Re-raise to trigger Celery retry
            raise
    
    return preprocess_video


async def _async_preprocess_video(video_hash: str, video_path: str) -> dict:
    """Async preprocessing workflow (matches pattern from _async_process_video)."""
    from utils.azure_blob import download_video_blob, upload_file
    from utils.ffmpeg_util import compress_video, extract_video_metadata
    from utils.kafka import send_kafka_message
    from utils.virus_scan import scan_file_for_viruses
    from crud.video import update_video_details
    from core.database import engine
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    import os
    
    logger.info(f"Starting preprocessing for video {video_hash}")
    
    # Create fresh engine and session for this event loop
    DATABASE_URL = os.getenv("DATABASE_URL")
    local_engine = create_async_engine(DATABASE_URL, echo=True)
    LocalSessionLocal = sessionmaker(
        bind=local_engine,
        class_=AsyncSession,
        expire_on_commit=False
    )
    
    temp_original = None
    temp_compressed = None
    
    try:
        # Update status to preprocessing
        async with LocalSessionLocal() as db:
            await update_video_details(video_hash, {"status": "preprocessing"}, db)
    
        # Step 1: Download video from Azure
        logger.info(f"Downloading video from Azure: {video_path}")
        temp_original = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4')
        temp_original.close()
        
        download_video_blob(video_hash, temp_original.name)
        logger.info(f"Downloaded to {temp_original.name}")
        
        # Step 2: Extract video metadata using ffprobe
        logger.info("Extracting video metadata...")
        
        try:
            metadata = extract_video_metadata(temp_original.name)
            logger.info(f"Extracted metadata: {metadata}")
            
            # Update database with metadata
            async with LocalSessionLocal() as db:
                await update_video_details(video_hash, {
                    "width": metadata.get('width'),
                    "height": metadata.get('height'),
                    "duration": int(metadata.get('duration', 0)),
                    "codec": metadata.get('codec'),
                    "actual_mime_type": metadata.get('mime_type')
                }, db)
            
        except Exception as e:
            # Log warning but continue - metadata extraction is not critical
            logger.warning(f"Failed to extract metadata for {video_hash}: {e}")
        
        # Step 3: Virus scan
        logger.info("Running virus scan...")
        scan_result = scan_file_for_viruses(temp_original.name)
        
        if not scan_result['clean']:
            error_msg = f"Virus detected: {scan_result['threat']}"
            logger.error(error_msg)
            # Update status to failed with specific virus scan error
            async with LocalSessionLocal() as db:
                await update_video_details(video_hash, {
                    "status": "failed",
                    "description": f"Virus scan failed: {error_msg}"
                }, db)
            # DO NOT send Kafka message - stop processing here
            raise RuntimeError(error_msg)
        
        logger.info(f"Virus scan passed: {scan_result['scan_result']}")
        
        # Step 4: Compress video
        logger.info("Compressing video...")
        temp_compressed = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4')
        temp_compressed.close()
        
        compression_stats = compress_video(
            temp_original.name,
            temp_compressed.name,
            target_bitrate='2500k'  # Adjust based on quality needs
        )
        
        logger.info(f"Compression complete: Saved {compression_stats['saved_mb']} MB "
                   f"({compression_stats['compression_percent']}% reduction)")
        
        # Step 5: Upload compressed version back to Azure
        logger.info("Uploading compressed video to Azure...")
        compressed_blob_path = f"{video_hash}/source.mp4"
        compressed_url = upload_file(video_hash, temp_compressed.name, compressed_blob_path)
        
        logger.info(f"Compressed video uploaded to {compressed_url}")
        
        # Step 6: Update video status to processing
        async with LocalSessionLocal() as db:
            await update_video_details(video_hash, {"status": "processing"}, db)
        
        # Step 7: Send Kafka message to video_processing topic for transcoding
        logger.info(f"Sending video {video_hash} to video_processing topic...")
        
        await send_kafka_message(
            topic='video_processing',
            message={
                'video_hash': video_hash,
                'video_path': compressed_blob_path
            }
        )
        
        logger.info(f"Successfully sent video {video_hash} to video_processing topic")
        
        return {
            "status": "success",
            "video_hash": video_hash,
            "virus_scan": scan_result['scan_result'],
            "compression": compression_stats,
            "compressed_url": compressed_url
        }
        
    except Exception as e:
        logger.error(f"Preprocessing failed: {e}")
        raise
    finally:
        # Cleanup temp files
        if temp_original and os.path.exists(temp_original.name):
            os.unlink(temp_original.name)
            logger.info(f"Cleaned up temp original: {temp_original.name}")
        
        if temp_compressed and os.path.exists(temp_compressed.name):
            os.unlink(temp_compressed.name)
            logger.info(f"Cleaned up temp compressed: {temp_compressed.name}")
        
        # Dispose engine
        await local_engine.dispose()


def create_process_profile_task(celery_app):
    @celery_app.task(
        bind=True,
        name='tasks.process_profile',
        max_retries=3,
        default_retry_delay=60,
        autoretry_for=(Exception,),
        retry_backoff=True,
        retry_backoff_max=600,
        retry_jitter=True
    )
    def process_profile(self, message_data, profile: str):
        """Process a single profile (e.g., 144p, 360p) — this is intended to be
        enqueued by Kafka consumers running in different consumer groups so work is
        distributed across workers.
        """
        logger.info(f"Processing profile task: profile={profile}, data={message_data}")

        video_hash = message_data.get('video_hash')
        input_path = message_data.get('video_path')

        if not video_hash or not input_path:
            logger.error("Message missing required fields 'video_hash' or 'video_path' for profile task")
            raise ValueError("Missing required fields 'video_hash' or 'video_path' for profile task")

        try:
            # Run profile transcode in its own event loop
            result = asyncio.run(_async_process_profile(video_hash, input_path, profile))
            return result
        except Exception as e:
            logger.exception(f"Error processing profile {profile} task: {e}")
            # Re-raise to trigger Celery retry
            raise

    return process_profile


def create_process_thumbnail_task(celery_app):
    @celery_app.task(
        bind=True,
        name='tasks.process_thumbnail',
        max_retries=3,
        default_retry_delay=60,
        autoretry_for=(Exception,),
        retry_backoff=True,
        retry_backoff_max=600,
        retry_jitter=True
    )
    def process_thumbnail(self, message_data):
        """Process thumbnail generation as a separate task for fanout."""
        logger.info(f"Processing thumbnail task: {message_data}")

        video_hash = message_data.get('video_hash')
        input_path = message_data.get('video_path')

        if not video_hash or not input_path:
            logger.error("Message missing required fields 'video_hash' or 'video_path' for thumbnail task")
            raise ValueError("Missing required fields 'video_hash' or 'video_path' for thumbnail task")

        try:
            result = asyncio.run(_async_process_thumbnail(video_hash, input_path))
            return result
        except Exception as e:
            logger.exception(f"Error processing thumbnail task: {e}")
            # Re-raise to trigger Celery retry
            raise

    return process_thumbnail


async def _async_process_thumbnail(video_hash: str, input_path: str):
    """Generate thumbnail for video."""
    import sys
    import os

    app_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    if app_dir not in sys.path:
        sys.path.insert(0, app_dir)

    from utils.azure_blob import download_video_blob, upload_directory
    from utils.ffmpeg_util import video_thumbnail
    from crud.video import mark_profile_complete, update_video_details
    from core.database import AsyncSessionLocal
    import logging

    logger = logging.getLogger(__name__)

    # Ensure local copy exists
    local_dir = os.path.join(app_dir, 'media', 'uploads', video_hash)
    os.makedirs(local_dir, exist_ok=True)
    local_input_path = os.path.join(local_dir, 'source.mp4')

    if not os.path.exists(local_input_path) or os.path.getsize(local_input_path) == 0:
        await run_ffmpeg_async(download_video_blob, video_hash, local_input_path)

    # Generate thumbnail
    try:
        thumbnail_path = await run_ffmpeg_async(video_thumbnail, local_input_path, local_dir)
        logger.info(f"Thumbnail generated for {video_hash}: {thumbnail_path}")
        
        # Upload thumbnail
        thumbnail_file = os.path.basename(thumbnail_path)
        uploaded = upload_directory(video_hash, local_dir, dest_prefix="", exclude=['source.mp4', 'hls', 'dash'])
        
        # Get thumbnail URL
        thumbnail_info = uploaded.get(thumbnail_file, {})
        thumbnail_url = thumbnail_info.get("url") if isinstance(thumbnail_info, dict) else None
        
        logger.info(f"Thumbnail uploaded for {video_hash}: {thumbnail_url}")
    except Exception as e:
        logger.exception(f"Failed to generate/upload thumbnail for {video_hash}: {e}")
        raise

    # Mark thumbnail as complete
    async with AsyncSessionLocal() as db:
        video, all_done = await mark_profile_complete(video_hash, 'thumbnail', db)
        
        # Update thumbnail URL
        await update_video_details(video_hash, {
            "thumbnail_url": thumbnail_url
        }, db)
        
        if all_done:
            logger.info(f"Thumbnail was the last task for {video_hash}, triggering master generation...")
            # Note: Master generation should happen in profile completion, but check here too
            from utils.ffmpeg_util import generate_hls_master_playlist
            try:
                master_path = await run_ffmpeg_async(
                    generate_hls_master_playlist,
                    local_dir,
                    ['144p', '360p', '480p', '720p', '1080p']
                )
                
                master_dir = os.path.join(local_dir, 'hls')
                master_uploaded = upload_directory(
                    video_hash,
                    master_dir,
                    dest_prefix="hls",
                    exclude=['144p', '360p', '480p', '720p', '1080p']
                )
                
                master_rel = "hls/master.m3u8"
                master_info = master_uploaded.get("master.m3u8", {})
                master_url = master_info.get("url") if isinstance(master_info, dict) else None
                
                await update_video_details(video_hash, {
                    "status": "completed",
                    "hls_url": master_url,
                    "hls_master_url": master_url
                }, db)
                
                logger.info(f"Master playlist generated via thumbnail completion for {video_hash}")
            except Exception as e:
                logger.warning(f"Master might already be generated for {video_hash}: {e}")

    return {"status": "completed", "video_hash": video_hash, "thumbnail_url": thumbnail_url}


async def _async_process_profile(video_hash: str, input_path: str, profile: str):
    """Async wrapper to process a specific HLS profile and upload results."""
    import sys
    import os

    app_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    if app_dir not in sys.path:
        sys.path.insert(0, app_dir)

    from utils.azure_blob import download_video_blob, upload_directory
    from utils.ffmpeg_util import transcode_hls_profile, generate_hls_master_playlist
    from crud.video import mark_profile_complete, update_video_details
    from core.database import AsyncSessionLocal
    import logging

    logger = logging.getLogger(__name__)

    # Ensure local copy exists
    local_dir = os.path.join(app_dir, 'media', 'uploads', video_hash)
    os.makedirs(local_dir, exist_ok=True)
    local_input_path = os.path.join(local_dir, 'source.mp4')

    if not os.path.exists(local_input_path) or os.path.getsize(local_input_path) == 0:
        # download the original source
        await run_ffmpeg_async(download_video_blob, video_hash, local_input_path)

    # Perform profile-specific transcode
    try:
        playlist_path = await run_ffmpeg_async(transcode_hls_profile, local_input_path, local_dir, profile)
        # Upload this profile directory to blob storage under video_hash/hls/{profile}
        uploaded = upload_directory(video_hash, os.path.join(local_dir, 'hls', profile), dest_prefix=f"hls/{profile}")
        logger.info(f"Uploaded profile {profile} assets for {video_hash}: {uploaded}")
    except Exception as e:
        logger.exception(f"Failed to transcode/upload profile {profile} for {video_hash}: {e}")
        raise

    # Mark this profile as complete in the database
    async with AsyncSessionLocal() as db:
        video, all_done = await mark_profile_complete(video_hash, profile, db)
        
        if all_done:
            logger.info(f"All profiles completed for {video_hash}. Generating master playlist...")
            try:
                # Generate master playlist
                master_path = await run_ffmpeg_async(
                    generate_hls_master_playlist,
                    local_dir,
                    ['144p', '360p', '480p', '720p', '1080p']
                )
                
                # Upload master playlist
                master_dir = os.path.join(local_dir, 'hls')
                master_uploaded = upload_directory(
                    video_hash,
                    master_dir,
                    dest_prefix="hls",
                    exclude=['144p', '360p', '480p', '720p', '1080p']  # Only upload master.m3u8
                )
                
                # Get master playlist URL
                master_rel = "hls/master.m3u8"
                master_info = master_uploaded.get("master.m3u8", {})
                master_url = master_info.get("url") if isinstance(master_info, dict) else None
                
                # Update video status to completed and set master URL
                await update_video_details(video_hash, {
                    "status": "completed",
                    "hls_url": master_url,
                    "hls_master_url": master_url
                }, db)
                
                logger.info(f"Master playlist generated and uploaded for {video_hash}: {master_url}")
                
                # Cleanup local files after everything is done
                try:
                    import shutil
                    shutil.rmtree(local_dir, ignore_errors=False)
                    logger.info(f"Deleted local directory for {video_hash}: {local_dir}")
                except Exception as cleanup_err:
                    logger.warning(f"Could not delete local directory {local_dir}: {cleanup_err}")
                    
            except Exception as e:
                logger.exception(f"Failed to generate/upload master playlist for {video_hash}: {e}")
        else:
            logger.info(f"Profile {profile} completed for {video_hash}, waiting for other profiles...")

    return {"status": "completed", "video_hash": video_hash, "profile": profile, "all_done": all_done}


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
    logger.info(f"Skipping DASH transcoding (disabled in config) for video {video_hash}")
    
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
    dash_url = None
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

