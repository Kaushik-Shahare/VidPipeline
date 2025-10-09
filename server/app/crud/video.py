from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from models.video import Video, VideoVarient
from schemas.video import VideoSchema, VideoVarientSchema

async def get_video_by_hash(db: AsyncSession, video_hash: str) -> Video | None:
    result = await db.execute(select(Video).where(Video.video_hash == video_hash))
    return result.scalars().first()

async def create_video(db: AsyncSession, video_data: dict) -> Video:
    new_video = Video(**video_data)
    db.add(new_video)
    await db.commit()
    await db.refresh(new_video)
    return new_video
