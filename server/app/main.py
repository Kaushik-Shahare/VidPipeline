from fastapi import FastAPI
from contextlib import asynccontextmanager
from api import video
from core.database import engine, Base
import asyncio

# For creating tables if they don't exist
@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield


app = FastAPI(lifespan=lifespan)
app.include_router(video.router)

