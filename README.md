# VidPipeline

Raw, practical, and intentionally engineered — a local video streaming playground built for understanding HLS/DASH streaming pipelines, async video processing, and distributed system patterns.

## Overview

VidPipeline lets you test, demo, or learn the full video processing pipeline:

- Chunked uploads for large video files
- Asynchronous transcoding to HLS (`.m3u8`) and DASH (`.mpd`)
- Side‑by‑side playback for comparison
- Kafka + Celery powered background processing

It’s ideal for:

- Learning adaptive bitrate streaming
- Building portfolio‑ready backend projects
- Interview discussions around distributed systems
- Experimenting with real transcoding workflows

## Architecture

```
Client → FastAPI → Kafka → Kafka Consumer → Celery → FFmpeg → Media Output
```

**Message Flow**

1. Upload chunks → finalize upload
2. FastAPI pushes message to Kafka
3. Kafka consumer receives message
4. Celery schedules transcoding
5. FFmpeg generates HLS + DASH outputs

All processing is async and decoupled.

## Tech Stack

- **Backend:** Python 3.12, FastAPI, SQLAlchemy Async
- **Video Processing:** FFmpeg (hardware acceleration where possible)
- **Async Tasks:** Celery (Redis backend)
- **Messaging Queue:** Kafka (Aiokafka)
- **Frontend:** Vanilla JS demo pages
- **Database:** SQLite (dev), supports PostgreSQL

## Project Structure

```
VidPipeline/
├─ client/
│  ├─ index.html
│  └─ streaming.html
├─ server/app/
│  ├─ main.py
│  ├─ celery_app.py
│  ├─ api/video.py
│  ├─ utils/
│  │  ├─ azure_blob.py
│  │  ├─ celery_task.py
│  │  ├─ ffmpeg_util.py
│  │  └─ kafka.html
│  ├─ tasks/video_tasks.py
│  ├─ models/
│  ├─ schemas/
│  ├─ crud/
│  ├─ core/database.py
│  └─ media/
└─ README.md
```

---

# Setup Using Docker (Full Stack)

This is the easiest, most production‑like setup.

## 1. Clone the Repository

```bash
git clone https://github.com/Kaushik-Shahare/VidPipeline.git
cd VidPipeline
```

## 2. Create Environment Files

You need **two** `.env` files:

### **A) Root `.env` file (VidPipeline/.env)**

Used by docker‑compose for global services.

```
DATABASE_URL=sqlite+aiosqlite:///./server.db
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_VIDEO_TOPIC=video_processing
REDIS_URL=redis://redis:6379/0

AZURE_STORAGE_CONNECTION_STRING=
AZURE_STORAGE_CONTAINER=videos
AZURE_STORAGE_URL=
```

### **B) Backend `.env` file (server/app/.env)**

```
DATABASE_URL=sqlite+aiosqlite:///./server.db
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_VIDEO_TOPIC=video_processing
REDIS_URL=redis://redis:6379/0

AZURE_STORAGE_CONNECTION_STRING=
AZURE_STORAGE_CONTAINER=videos
AZURE_STORAGE_URL=
```

## 3. Start Services

From project root:

```bash
docker compose up -d
```

This starts:

- Kafka broker
- Zookeeper
- Redis
- FastAPI backend
- Celery workers
- Kafka consumers
- Kafka UI ([http://localhost:8081](http://localhost:8081))
- Frontend (optional depending on compose config)

## 4. Verify Kafka Topic Exists

```bash
docker exec -it kafka \
  kafka-topics --bootstrap-server kafka:29092 --describe --topic video_processing
```

You should see **PartitionCount > 1** if using a multi‑partition setup.

## 5. Access the App

- Backend API: [http://localhost:8000/docs](http://localhost:8000/docs)
- Kafka UI: [http://localhost:8081](http://localhost:8081)
- Frontend demo: [http://localhost:3000](http://localhost:3000) (if enabled)

---

# Manual Setup (Without Docker)

Useful if you want to run Kafka via Docker but everything else locally.

## 1. Clone

```bash
git clone https://github.com/Kaushik-Shahare/VidPipeline.git
cd VidPipeline
```

## 2. Start Kafka

```bash
cd kafka
docker compose up -d
cd ..
```

## 3. Backend Setup

```bash
cd server/app
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Edit `.env` values.

## 4. Start Redis

```bash
docker run -d -p 6379:6379 redis:alpine
```

## 5. Start Celery

```bash
cd server/app
celery -A celery_app worker --loglevel=info -Q video_processing
```

## 6. Start FastAPI

```bash
uvicorn main:app --reload --port 8000
```

## 7. Frontend Static Server

```bash
cd client
python3 -m http.server 8080
```

---

# Usage

1. Upload video using `client/index.html`
2. Call `POST /videos/finalize/{hash}`
3. FastAPI pushes a message → Kafka
4. Consumer sends message → Celery
5. FFmpeg generates:

   - HLS playlist + segments
   - DASH manifest + segments
   - Thumbnail

6. View results in `client/streaming.html`

Media served at `/media` via FastAPI.

---

# Troubleshooting

### FFmpeg errors

- File paths incorrect → check upload directory
- Permissions issue inside Docker
- macOS hardware acceleration not supported → fallback to libx264

### Kafka not receiving messages

- Confirm topic exists
- Check consumer errors in backend logs
- Verify `KAFKA_BOOTSTRAP_SERVERS=kafka:29092`

### Celery tasks not running

- Check Redis running
- Ensure worker queue matches: `-Q video_processing`

### HLS/DASH not playing

- Must be served over HTTP, not `file://`
- Check MIME types in FastAPI static configuration

---

# Future Improvements

Planned extensions for full observability and production‑grade monitoring:

- **Grafana** (dashboards)
- **Prometheus** (metrics scraping)
- **Node Exporter** (host-level metrics)
- **Grafana Loki** (centralized logs)
- **Alertmanager** (optional)

These will provide:

- System load visualization
- Worker/task execution metrics
- Kafka consumer lag monitoring
- FFmpeg processing time breakdown

---

# License

MIT

---

VidPipeline is built to understand real distributed video pipelines — adaptive streaming, background workloads, and message-driven design — without the overhead of enterprise infrastructure.
