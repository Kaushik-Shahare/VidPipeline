import logging
import httpx

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession
from crud.video import (get_video_by_hash, create_video, get_all_videos)
from schemas.video import VideoSchema, VideoInitSchema
from core.database import get_db
from utils.kafka import send_video_processing_message
from utils.azure_blob import (
    upload_chunk_to_azure,
    merge_chunks_to_source,
    signed_url,
    blob_path_from_location,
    generate_container_sas_token,
    get_uploaded_chunks,
    download_video_blob,
)

router = APIRouter(prefix="/videos", tags=["videos"])

logger = logging.getLogger(__name__)

logger = logging.getLogger(__name__)


# Generate a unique video hash (Consistent Hashing)
def generate_video_hash(title: str, total_chunks: int) -> str:
    import hashlib
    hash_input = f"{title}-{total_chunks}"
    return hashlib.sha256(hash_input.encode()).hexdigest()
    

@router.post("/init")
async def init_video_upload(payload: VideoInitSchema, db: AsyncSession = Depends(get_db)) : 
    """Initialize a video upload session with resumable upload support.
    
    Security checks:
    - Validates file size limits (max 5GB)
    - Validates file type by MIME type
    - Generates unique video hash
    
    Resumable uploads:
    - If video already exists and is in 'uploading' status, returns existing session
    - Includes list of already uploaded chunks so client can resume
    - Client only needs to upload missing chunks
    """
    # Validate file size (max 5GB)
    MAX_FILE_SIZE = 5 * 1024 * 1024 * 1024  # 5GB
    if payload.file_size > MAX_FILE_SIZE:
        raise HTTPException(
            status_code=400, 
            detail=f"File size {payload.file_size} exceeds maximum allowed size of {MAX_FILE_SIZE} bytes"
        )
    
    # Validate MIME type from client (will be verified again on finalize)
    ALLOWED_VIDEO_TYPES = [
        "video/mp4", "video/quicktime", "video/x-msvideo", 
        "video/x-matroska", "video/webm", "video/mpeg"
    ]
    if payload.mime_type and payload.mime_type not in ALLOWED_VIDEO_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"File type {payload.mime_type} not allowed. Accepted: {', '.join(ALLOWED_VIDEO_TYPES)}"
        )
    
    video_hash = generate_video_hash(payload.title, payload.total_chunks)
    
    # Check if video with the same hash already exists
    existing_video = await get_video_by_hash(db, video_hash)
    if existing_video:
        # If video is still uploading, support resumable upload
        if existing_video.status == "uploading":
            # Get list of chunks already uploaded to Azure
            uploaded_chunks = get_uploaded_chunks(video_hash)
            missing_chunks = [i for i in range(existing_video.total_chunks) if i not in uploaded_chunks]
            
            return {
                **VideoSchema.model_validate(existing_video).model_dump(),
                "resumable": True,
                "uploaded_chunks": uploaded_chunks,
                "missing_chunks": missing_chunks
            }
        
        # If video is already processing or completed, return it as-is
        return existing_video

    # Create a new Video entry in the database
    video_data = {
        "video_hash": video_hash,
        "title": payload.title,
        "description": payload.description,
        "total_chunks": payload.total_chunks,
        "status": "uploading"
    }
    new_video = await create_video(db, video_data)
    
    if not new_video:
        raise HTTPException(status_code=500, detail="Failed to initialize video upload")

    return {
        **VideoSchema.model_validate(new_video).model_dump(),
        "resumable": False,
        "uploaded_chunks": [],
        "missing_chunks": list(range(new_video.total_chunks))
    }


@router.post("/upload_chunk/{video_hash}/{chunk_index}")
async def upload_chunk(
    video_hash: str,
    chunk_index: int,
    chunk: UploadFile = File(...),
    db: AsyncSession = Depends(get_db)
):
    """Upload a video chunk to Azure Blob Storage.
    
    Chunks are stored under video_hash/chunk_{chunk_index} in Azure.
    After all chunks are uploaded, they will be merged into source.mp4.
    """
    # Verify video exists and is in uploading status
    video = await get_video_by_hash(db, video_hash)
    if not video:
        raise HTTPException(status_code=404, detail="Video not found")
    
    if video.status != "uploading":
        raise HTTPException(status_code=400, detail=f"Video is in {video.status} status, cannot upload chunks")
    
    # Validate chunk index
    if chunk_index < 0 or chunk_index >= video.total_chunks:
        raise HTTPException(
            status_code=400,
            detail=f"Chunk index {chunk_index} out of range [0, {video.total_chunks-1}]"
        )
    
    # Read chunk data
    chunk_bytes = await chunk.read()
    if not chunk_bytes:
        raise HTTPException(status_code=400, detail="Empty chunk data")
    
    # Upload chunk to Azure
    try:
        await upload_chunk_to_azure(video_hash, chunk_index, chunk_bytes)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload chunk to Azure: {str(e)}")
    
    # Update received_chunks count
    video.received_chunks += 1
    await db.commit()
    await db.refresh(video)
    
    return {
        "video_hash": video_hash,
        "chunk_index": chunk_index,
        "chunk_size": len(chunk_bytes),
        "received_chunks": video.received_chunks,
        "total_chunks": video.total_chunks
    }

@router.post("/finalize/{video_hash}")
async def upload_video_finalize(video_hash: str, db: AsyncSession = Depends(get_db)):
    """Finalize video upload by merging all chunks and starting preprocessing.
    
    This endpoint:
    1. Verifies all chunks have been uploaded
    2. Merges chunks into source.mp4 in Azure
    3. Extracts video metadata (dimensions, codec, etc.)
    4. Updates video status to 'preprocessing'
    5. Triggers preprocessing task (compression + virus scan)
    """
    # Check the video hash
    video = await get_video_by_hash(db, video_hash)
    if not video: 
        raise HTTPException(status_code=404, detail="Video not found")

    # Video already processing or completed
    if video.status != "uploading": 
        raise HTTPException(status_code=400, detail="Video upload already finalized or in processing")

    # Ensure all chunks have been received
    if video.received_chunks != video.total_chunks:
        raise HTTPException(
            status_code=400, 
            detail=f"Not all chunks have been uploaded. Received: {video.received_chunks}/{video.total_chunks}"
        )

    # Merge all chunks into source.mp4
    try:
        blob_url = await merge_chunks_to_source(video_hash, video.total_chunks)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to merge chunks: {str(e)}")

    # Update video status and URL
    # Metadata extraction will be done in preprocessing task (avoids double download)
    video.status = "preprocessing"
    video.url = blob_url
    
    await db.commit()
    await db.refresh(video)

    # Trigger preprocessing via Kafka (compression + virus scan)
    # Send to video_preprocessing topic which will be consumed by preprocessing workers
    from utils.kafka import send_kafka_message
    
    await send_kafka_message(
        topic='video_preprocessing',
        message={
            'video_hash': video_hash,
            'video_path': f"{video_hash}/source.mp4"
        }
    )
    
    logger.info(f"Sent video {video_hash} to video_preprocessing topic")
    
    return {
        "message": "Video upload finalized and preprocessing started", 
        "video_hash": video_hash, 
        "video_url": video.url,
        "status": "preprocessing"
    }


@router.get("", response_model=list[VideoSchema])
async def list_videos(db: AsyncSession = Depends(get_db)):
    videos = await get_all_videos(db)
    signed_videos: list[VideoSchema] = []

    for video in videos:
        video_data = VideoSchema.model_validate(video).model_dump()

        # For HLS streaming, use the proxy endpoint instead of direct Azure URLs
        # This ensures SAS tokens are properly handled for all playlist/segment requests
        if video_data.get("hls_url"):
            # Extract just the path after video_hash (e.g., "hls/master.m3u8")
            # Original: https://...blob.../videos/{video_hash}/hls/master.m3u8
            # Proxy:    /videos/stream/{video_hash}/hls/master.m3u8
            video_data["hls_url"] = f"/videos/stream/{video.video_hash}/hls/master.m3u8"
        
        # Still sign other URLs (thumbnail, source) for direct access
        sas_token: str | None = None
        for field in ("url", "dash_url", "thumbnail_url"):
            value = video_data.get(field)
            blob_path = blob_path_from_location(value)
            if not blob_path:
                continue
            try:
                if sas_token is None:
                    sas_token = generate_container_sas_token()
                    video_data["azure_sas_token"] = sas_token
                video_data[field] = signed_url(blob_path, sas_token=sas_token)
            except Exception as exc:  # Azure misconfiguration shouldn't break API
                logger.warning("Failed to generate signed URL for %s (%s): %s", field, blob_path, exc)
        if sas_token is None:
            video_data.setdefault("azure_sas_token", None)
        signed_videos.append(VideoSchema(**video_data))

    return signed_videos


@router.get("/stream/{video_hash}/{path:path}")
async def stream_hls_proxy(video_hash: str, path: str, db: AsyncSession = Depends(get_db)):
    """
    Proxy endpoint for HLS streaming that automatically appends SAS tokens to Azure Blob requests.
    This solves the issue where HLS.js can't access variant playlists and segments without authentication.
    
    Example: /videos/stream/abc123/hls/1080p/playlist.m3u8
    """
    # Verify video exists
    video = await get_video_by_hash(db, video_hash)
    if not video:
        raise HTTPException(status_code=404, detail="Video not found")
    
    # Generate SAS token for this video's container
    sas_token = generate_container_sas_token()
    
    # Build the Azure Blob URL
    blob_path = f"{video_hash}/{path}"
    azure_url = signed_url(blob_path, sas_token=sas_token)
    
    # Fetch the content from Azure
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(azure_url, timeout=30.0)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.error(f"Failed to fetch {blob_path}: {exc}")
            raise HTTPException(status_code=502, detail=f"Failed to fetch content from storage: {exc}")
    
    # Determine content type
    content_type = response.headers.get("Content-Type", "application/octet-stream")
    
    # For .m3u8 playlists, ensure correct MIME type
    if path.endswith(".m3u8"):
        content_type = "application/vnd.apple.mpegurl"
    elif path.endswith(".ts"):
        content_type = "video/mp2t"
    
    return Response(
        content=response.content,
        media_type=content_type,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "public, max-age=3600",
        }
    )
