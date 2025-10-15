from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from api import video
from core.database import engine, Base
from workers.video_processing import consume_video_processing_message
import asyncio
from fastapi.staticfiles import StaticFiles
import os
import mimetypes

# For creating tables if they don't exist
@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_event_loop()
    task = loop.create_task(consume_video_processing_message())

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Length", "Content-Range"],
)

app.include_router(video.router)

# Ensure correct MIME types for streaming manifests
mimetypes.add_type('application/dash+xml', '.mpd')
mimetypes.add_type('application/vnd.apple.mpegurl', '.m3u8')

# Serve media uploads statically with CORS
base_dir = os.path.abspath(os.path.dirname(__file__))
media_dir = os.path.join(base_dir, 'media')
if os.path.isdir(media_dir):
    app.mount('/media', StaticFiles(directory=media_dir), name='media')

