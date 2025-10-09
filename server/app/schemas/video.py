from pydantic import BaseModel, Field

class VideoSchema(BaseModel):
    id: int
    video_hash: str
    title: str
    description: str | None = None
    total_chunks: int
    received_chunks: int = 0
    status: str # e.g., 'uploading', 'processing', 'completed'
    url: str | None = None
    created_at: str
    updated_at: str

    class Config:
        orm_mode = True


class VideoVarientSchema(BaseModel):
    id: int
    video_id: int
    resolution: str # e.g., '1080p', '720p'
    format: str # e.g., 'HLS', 'DASH'
    url: str
    created_at: str
    updated_at: str

    class Config:
        orm_mode = True

class VideoInitSchema(BaseModel):
    title: str = Field(..., example="Sample Video")
    description: str | None = Field(None, example="This is a sample video description.")
    total_chunks: int = Field(..., example=10)
