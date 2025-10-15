from sqlalchemy import Column, Integer, String, ForeignKey, func, DateTime
from core.database import Base

class Video(Base):
    __tablename__ = "videos"

    id = Column(Integer, primary_key=True, index=True, nullable=False)
    video_hash = Column(String, unique=True, index=True, nullable=False)
    title = Column(String, index=True, nullable=False)
    description = Column(String, index=False, nullable=True)
    total_chunks = Column(Integer, index=False, nullable=False)
    received_chunks = Column(Integer, default=0, index=False, nullable=False)
    status = Column(String, index=True, nullable=False)  # e.g., 'uploading', 'processing', 'completed'
    url = Column(String, unique=True, index=False, nullable=True)
    hls_url = Column(String, unique=True, index=False, nullable=True)
    dash_url = Column(String, unique=True, index=False, nullable=True)
    created_at = Column(DateTime, server_default=func.now(), index=False, nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), index=False, nullable=False)

    def __repr__(self):
        return f"<Video(id={self.id}, title={self.title}, url={self.url})>"


class VideoVarient(Base):
    __tablename__ = "video_varients"

    id = Column(Integer, primary_key=True, index=True, nullable=False)
    video_id = Column(Integer, ForeignKey("videos.id"), index=True, nullable=False)
    resolution = Column(String, index=True, nullable=False) # e.g., '1080p', '720p'
    format = Column(String, index=True, nullable=False) # e.g., 'HLS', 'DASH'
    url = Column(String, unique=True, index=False, nullable=False)
    created_at = Column(DateTime, server_default=func.now(), index=False, nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), index=False, nullable=False)

    def __repr__(self):
        return f"<VideoVarient(id={self.id}, video_id={self.video_id}, resolution={self.resolution}, format={self.format})>"
