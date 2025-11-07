from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime

class VideoSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

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
    azure_sas_token: str | None = None
    created_at: datetime
    updated_at: datetime

class VideoInitSchema(BaseModel):
    title: str = Field(..., example="Sample Video")
    description: str | None = Field(None, example="This is a sample video description.")
    total_chunks: int = Field(..., example=10)
