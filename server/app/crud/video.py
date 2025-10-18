from fastapi import Depends
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from models.video import Video
from schemas.video import VideoSchema
from core.database import get_db

async def get_all_videos(db: AsyncSession) -> list[Video]:
    result = await db.execute(select(Video))
    return result.scalars().all()

async def get_video_by_hash(db: AsyncSession, video_hash: str) -> Video | None:
    result = await db.execute(select(Video).where(Video.video_hash == video_hash))
    return result.scalars().first()

async def create_video(db: AsyncSession, video_data: dict) -> Video:
    new_video = Video(**video_data)
    db.add(new_video)
    await db.commit()
    await db.refresh(new_video)
    return new_video

async def update_video_details(video_hash: str, update_data: dict, db: AsyncSession = Depends(get_db)) -> Video | None:
    video = await get_video_by_hash(db, video_hash)
    if not video:
        return None
    for key, value in update_data.items():
        setattr(video, key, value)
    await db.commit()
    await db.refresh(video)
    return video
