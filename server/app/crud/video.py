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


async def mark_profile_complete(video_hash: str, profile: str, db: AsyncSession) -> tuple[Video | None, bool]:
    """Mark a profile as complete and check if all profiles are done.
    
    Returns:
        Tuple of (Video object, all_profiles_done boolean)
    """
    video = await get_video_by_hash(db, video_hash)
    if not video:
        return None, False
    
    # Map profile name to database column
    profile_map = {
        '144p': 'profile_144p_done',
        '360p': 'profile_360p_done',
        '480p': 'profile_480p_done',
        '720p': 'profile_720p_done',
        '1080p': 'profile_1080p_done',
        'thumbnail': 'thumbnail_done',
    }
    
    column_name = profile_map.get(profile)
    if column_name:
        setattr(video, column_name, True)
        await db.commit()
        await db.refresh(video)
    
    # Check if all profiles are complete
    all_done = (
        video.profile_144p_done and
        video.profile_360p_done and
        video.profile_480p_done and
        video.profile_720p_done and
        video.profile_1080p_done and
        video.thumbnail_done
    )
    
    return video, all_done
