from pydantic import BaseModel, Field
from datetime import datetime

class VideoSchema(BaseModel):
    id: int
    video_hash: str
    title: str
    description: str | None
    total_chunks: int
    received_chunks: int = 0
    status: str # e.g., 'uploading', 'processing', 'completed'
    url: str | None
    hls_url: str | None
    dash_url: str | None
    thumbnail_url: str | None
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


class VideoInitSchema(BaseModel):
    title: str = Field(..., example="Sample Video")
    description: str | None = Field(None, example="This is a sample video description.")
    total_chunks: int = Field(..., example=10)
