from fastapi import APIRouter, Depends, HTTPException, UploadFile
import os
from sqlalchemy.ext.asyncio import AsyncSession
from crud.video import (get_video_by_hash, create_video, get_all_videos)
from schemas.video import VideoSchema, VideoVarientSchema, VideoInitSchema
from core.database import get_db
from utils.kafka import send_video_processing_message

router = APIRouter(prefix="/videos", tags=["videos"])


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

    # Save to temporary storage (Local filesystem for dev)
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    temp_dir = os.path.join(base_dir, "media", "uploads", video_hash, "temp")
    os.makedirs(temp_dir, exist_ok=True)
    chunk_path = os.path.join(temp_dir, f"chunk_{chunk_index}")
    with open(chunk_path, "wb") as buffer:
        buffer.write(await chunk_data.read())
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

    # Merge chunks and process video (Placeholder logic)
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    temp_dir = os.path.join(base_dir, "media", "uploads", video_hash, "temp")
    final_video_path = os.path.join(base_dir, "media", "uploads", video_hash, "final_video.mp4")
    
    with open(final_video_path, "wb") as final_video_file:
        for i in range(video.total_chunks):
            chunk_path = os.path.join(temp_dir, f"chunk_{i}")
            with open(chunk_path, "rb") as chunk_file:
                final_video_file.write(chunk_file.read())
    
    # Update video status and URL
    video.status = "processing"
    video.url = final_video_path
    await db.commit()
    await db.refresh(video)

    # Delete Temp Chunks with recursive deletion 
    import shutil
    shutil.rmtree(temp_dir)

    # Trigger async processing 
    await send_video_processing_message(video_hash, final_video_path)
    
    return {"message": "Video upload finalized and processing started", "video_hash": video_hash, "video_url": video.url}


@router.get("", response_model=list[VideoSchema])
async def list_videos(db: AsyncSession = Depends(get_db)):
    videos = await get_all_videos(db)
    return videos
