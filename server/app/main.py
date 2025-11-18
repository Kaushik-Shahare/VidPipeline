from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from api import video
from core.database import engine, Base
from fastapi.staticfiles import StaticFiles
import os
import mimetypes
import logging
import asyncio

# For creating tables if they don't exist
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    logger = logging.getLogger(__name__)
    logger.info("FastAPI application started")
    
    yield
    
    # Shutdown
    logger.info("FastAPI application shutting down")

# Logging configuration
LOG_FILE  = "logs/main.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode='a'),
        logging.StreamHandler()
    ]
)

uvicorn_logger = logging.getLogger("uvicorn")
uvicorn_logger.handlers = logging.getLogger().handlers
uvicorn_logger.setLevel(logging.INFO)

uvicorn_access = logging.getLogger("uvicorn.access")
uvicorn_access.handlers = logging.getLogger().handlers
uvicorn_access.setLevel(logging.INFO)


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

