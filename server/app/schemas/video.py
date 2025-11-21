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
    
    # Video metadata
    width: int | None = None
    height: int | None = None
    duration: int | None = None
    codec: str | None = None
    actual_mime_type: str | None = None
    
    created_at: datetime
    updated_at: datetime

class VideoInitSchema(BaseModel):
    title: str = Field(..., example="Sample Video")
    description: str | None = Field(None, example="This is a sample video description.")
    total_chunks: int = Field(..., example=10)
    file_size: int = Field(..., example=104857600, description="Total file size in bytes")
    mime_type: str | None = Field(None, example="video/mp4", description="MIME type of the file")
