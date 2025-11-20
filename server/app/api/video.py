import logging
import httpx

from fastapi import APIRouter, Depends, HTTPException, UploadFile
from fastapi.responses import Response
from sqlalchemy.ext.asyncio import AsyncSession
from crud.video import (get_video_by_hash, create_video, get_all_videos)
from schemas.video import VideoSchema, VideoInitSchema
from core.database import get_db
from utils.kafka import send_video_processing_message
from utils.azure_blob import (
    stage_video_chunk,
    commit_video_upload,
    signed_url,
    blob_path_from_location,
    generate_container_sas_token,
)

router = APIRouter(prefix="/videos", tags=["videos"])

logger = logging.getLogger(__name__)


# Generate a unique video hash (Consistent Hashing)
def generate_video_hash(title: str, total_chunks: int) -> str:
    import hashlib
    hash_input = f"{title}-{total_chunks}"
    return hashlib.sha256(hash_input.encode()).hexdigest()
    

@router.post("/init")
async def init_video_upload(payload: VideoInitSchema, db: AsyncSession = Depends(get_db)) : 
    video_hash = generate_video_hash(payload.title, payload.total_chunks)
    
    # Check if video with the same hash already exists
    existing_video = await get_video_by_hash(db, video_hash)
    if existing_video:
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

    return new_video


@router.post("/upload_chunk/{video_hash}/{chunk_index}")
async def upload_video_chunk(video_hash: str, chunk_index: int, chunk_data: UploadFile, db: AsyncSession = Depends(get_db)):
    # Check the video hash
    video = await get_video_by_hash(db, video_hash)
    if not video: 
        raise HTTPException(status_code=404, detail="Video not found")

    # Check if chunk index is valid and not already received
    if chunk_index < 0 or chunk_index >= video.total_chunks:
        raise HTTPException(status_code=400, detail="Invalid chunk index")

    chunk_bytes = await chunk_data.read()
    if not chunk_bytes:
        raise HTTPException(status_code=400, detail="Chunk data is empty")

    try:
        stage_video_chunk(video_hash, chunk_index, chunk_bytes)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    # Update received chunks count
    video.received_chunks += 1
    await db.commit()
    await db.refresh(video)

    # forced delay
    # import asyncio
    # await asyncio.sleep(1)

    return {"message": f"Chunk {chunk_index} uploaded successfully", "video_hash": video_hash, "received_chunks": video.received_chunks}

@router.post("/finalize/{video_hash}")
async def upload_video_finalize(video_hash: str, db: AsyncSession = Depends(get_db)):
    # Check the video hash
    video = await get_video_by_hash(db, video_hash)
    if not video: 
        raise HTTPException(status_code=404, detail="Video not found")

    # Video already processing or completed
    if video.status != "uploading": raise HTTPException(status_code=400, detail="Video upload already finalized or in processing")

    # Ensure all chunks have been received
    if video.received_chunks != video.total_chunks:
        raise HTTPException(status_code=400, detail="Not all chunks have been uploaded")

    try:
        blob_url = commit_video_upload(video_hash, video.total_chunks)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    # Update video status and URL
    video.status = "processing"
    video.url = blob_url
    await db.commit()
    await db.refresh(video)

    # Trigger async processing via Kafka (non-blocking)
    await send_video_processing_message(video_hash, f"{video_hash}/source.mp4")
    
    return {"message": "Video upload finalized and processing started", "video_hash": video_hash, "video_url": video.url}


@router.get("", response_model=list[VideoSchema])
async def list_videos(db: AsyncSession = Depends(get_db)):
    videos = await get_all_videos(db)
    signed_videos: list[VideoSchema] = []

    for video in videos:
        video_data = VideoSchema.from_orm(video).dict()

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
