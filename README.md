# VidPipeline

Raw, professional, and precise. A local video streaming playground for learning HLS and DASH pipelines.

## Overview

VidPipeline is a hands-on, experimental video streaming server to learn and demo:

- Chunked uploads for large files
- Asynchronous transcoding to HLS (.m3u8) and DASH (.mpd)
- Side-by-side playback in the browser

It’s ideal for understanding adaptive streaming, portfolio showcases, or interview demos.

## Tech Stack

- Backend: Python 3.12, FastAPI, SQLAlchemy (Async)
- Video Processing: FFmpeg (h264_videotoolbox on macOS, fallback libx264), AAC
- Task Queue: Celery with Redis backend
- Message Broker: Kafka (Aiokafka consumer/producer) for API-to-Worker decoupling
- Frontend: Vanilla JS (upload + playback pages)
- Database: SQLite (dev). Compatible with async PostgreSQL

**Architecture**: API → Kafka → Consumer (in lifespan) → Celery → FFmpeg Processing

See [KAFKA_ARCHITECTURE.md](KAFKA_ARCHITECTURE.md) for detailed architecture documentation.

## Project Layout

```
VidPipeline/
├─ client/
│  ├─ index.html          # Chunked upload UI (example)
│  └─ streaming.html      # HLS/DASH side-by-side player
├─ server/
│  └─ app/
│     ├─ main.py                  # FastAPI app entry (Kafka consumer in lifespan)
│     ├─ celery_app.py            # Celery configuration
│     ├─ api/video.py             # Upload + finalize endpoints
│     ├─ utils/ffmpeg_util.py     # HLS/DASH transcoding helpers
│     ├─ utils/kafka.py           # Kafka producer & consumer
│     ├─ utils/celery_tasks.py    # Celery task utilities (deprecated)
│     ├─ tasks/video_tasks.py     # Celery video processing tasks
│     ├─ crud/                    # DB ops
│     ├─ models/                  # SQLAlchemy models
│     ├─ schemas/                 # Pydantic schemas
│     ├─ core/database.py         # Async DB engine + session
│     └─ media/                   # Served at /media (uploads + outputs)
├─ kafka/
│  └─ docker-compose.yaml         # Local Kafka
├─ KAFKA_ARCHITECTURE.md          # Detailed architecture documentation
└─ README.md
```

## Prerequisites

- Python 3.12+
- FFmpeg installed locally (macOS: brew install ffmpeg)
- Redis (for Celery backend)
- Docker + Docker Compose (for Kafka)

## Docker Setup

1. Clone

```bash
git clone https://github.com/Kaushik-Shahare/VidPipeline.git
cd VidPipeline
```

2. Build and start all services using Docker Compose

```bash
docker compose up -d
```

## Manual Setup

1. Clone

````bash
git clone https://github.com/Kaushik-Shahare/VidPipeline.git
cd VidPipeline
``` 2. Start Kafka (for async processing)

```bash
cd kafka
docker compose up -d
cd ..
````

3. Backend environment

```bash
cd server/app
python -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Create .env file from example
cp .env.example .env
# Edit .env with your configuration
```

4. Start Redis (for Celery)

```bash
# macOS
brew install redis
brew services start redis

# Or using Docker
docker run -d -p 6379:6379 redis:alpine
```

5. Start Celery Worker

```bash
cd server/app
celery -A celery_app worker --loglevel=info -Q video_processing
```

Keep this terminal running.

6. Run FastAPI

In a new terminal:

```bash
cd server/app
source venv/bin/activate
uvicorn main:app --reload --port 8000
```

Notes:

- The app statically serves media at `/media` with proper MIME types for `.mpd` and `.m3u8`.
- The Kafka consumer is started inside the FastAPI lifespan event.
- Messages flow: API → Kafka → Consumer (in lifespan) → Celery → Processing

5. Frontend

Run a simple HTTP server to avoid file:// CORS:

```bash
cd ../../client
python3 -m http.server 8080
# Open http://localhost:8080/streaming.html
```

## Usage

1. Upload via the client UI (index.html) or API:

- Initialize: POST /videos/init
- Upload chunks: POST /videos/upload_chunk/{video_hash}/{chunk_index}
- Finalize: POST /videos/finalize/{video_hash}

2. Processing

- Finalize sends a message to Kafka. The Kafka consumer (running in FastAPI lifespan) picks it up and sends it to Celery.
- Celery worker runs FFmpeg to produce:
  - HLS: `/media/uploads/<hash>/hls/playlist.m3u8`
  - DASH: `/media/uploads/<hash>/dash/manifest.mpd`
  - Thumbnail: `/media/uploads/<hash>/thumbnail.jpg`
- The DB is updated with `status=completed`, `url` (HLS), `hls_url`, `dash_url`, `thumbnail_url`.

3. Playback

- Open `client/streaming.html` served over HTTP. It fetches `/videos`, then plays both HLS and DASH using URLs served by the FastAPI app.

## Troubleshooting

- FFmpeg input not found:
  - Ensure `finalize` created the file at an absolute path.
  - Check Kafka consumer logs in FastAPI output.
  - Verify Celery worker is running and consuming tasks.
  - Check logs: `server/app/logs/main.log`.
- CORS on manifests/segments:
  - Access the client via HTTP (not file://).
  - Ensure backend is running and `/media` is mounted (see `main.py`).
- Kafka not reachable:
  - Confirm docker compose is up and broker is on `localhost:9092`.
  - Check `KAFKA_BOOTSTRAP_SERVERS` in `.env` file.
- Celery tasks not running:
  - Ensure Redis is running (`redis-cli ping` should return PONG).
  - Verify Celery worker is started with correct queue: `-Q video_processing`.
  - Check `REDIS_URL` in `.env` file.
- Messages stuck in Kafka:
  - Check consumer group status: `kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group video-processing-group`
  - Look for consumer lag in the output.

## Why VidPipeline

- Learn chunked uploads and async processing
- Understand adaptive streaming (HLS vs DASH)
- Experience Kafka-based message-driven architecture
- Learn Celery task queue integration
- Great for demos and portfolio work
- Lightweight, local, production-like pipeline with fault tolerance

## License

MIT

## Future Improvements

- Live streaming (WebSockets)
- Enhanced monitoring and metrics (Prometheus/Grafana)
- Auth and multi-user support
- Dockerize full stack
- Multi-region Kafka deployment
- Dead letter queue for failed messages
- Video quality selection and multiple bitrates
