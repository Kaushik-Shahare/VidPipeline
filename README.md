VidPipeline
===========

Raw, professional, and precise. A local video streaming playground for learning HLS and DASH pipelines.

Overview
--------

VidPipeline is a hands-on, experimental video streaming server to learn and demo:

- Chunked uploads for large files
- Asynchronous transcoding to HLS (.m3u8) and DASH (.mpd)
- Side-by-side playback in the browser

It’s ideal for understanding adaptive streaming, portfolio showcases, or interview demos.

Tech Stack
---------

- Backend: Python 3.12, FastAPI, SQLAlchemy (Async)
- Video Processing: FFmpeg (h264_videotoolbox on macOS, fallback libx264), AAC
- Messaging: Kafka (Aiokafka consumer/producer)
- Frontend: Vanilla JS (upload + playback pages)
- Database: SQLite (dev). Compatible with async PostgreSQL

Project Layout
--------------

```
VidPipeline/
├─ client/
│  ├─ index.html          # Chunked upload UI (example)
│  └─ streaming.html      # HLS/DASH side-by-side player
├─ server/
│  └─ app/
│     ├─ main.py                  # FastAPI app entry
│     ├─ api/video.py             # Upload + finalize endpoints
│     ├─ utils/ffmpeg_util.py     # HLS/DASH transcoding helpers
│     ├─ utils/kafka.py           # Producer (enqueue processing)
│     ├─ workers/video_processing.py # Kafka consumer (transcoding + DB update)
│     ├─ crud/                    # DB ops
│     ├─ models/                  # SQLAlchemy models
│     ├─ schemas/                 # Pydantic schemas
│     ├─ core/database.py         # Async DB engine + session
│     └─ media/                   # Served at /media (uploads + outputs)
├─ kafka/
│  └─ docker-compose.yaml         # Local Kafka
└─ README.md
```

Prerequisites
-------------

- Python 3.12+
- FFmpeg installed locally (macOS: brew install ffmpeg)
- Docker + Docker Compose (for Kafka)

Setup
-----

1) Clone

```bash
git clone https://github.com/<your-username>/VidPipeline.git
cd VidPipeline
```

2) Start Kafka (for async processing)

```bash
cd kafka
docker compose up -d
cd ..
```

3) Backend environment

```bash
cd server/app
python -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

4) Run FastAPI

```bash
uvicorn main:app --reload --port 8000
```

Notes:
- The app statically serves media at `/media` with proper MIME types for `.mpd` and `.m3u8`.
- The Kafka consumer is started inside the FastAPI lifespan.

5) Frontend

Run a simple HTTP server to avoid file:// CORS:

```bash
cd ../../client
python3 -m http.server 8080
# Open http://localhost:8080/streaming.html
```

Usage
-----

1) Upload via the client UI (index.html) or API:
- Initialize: POST /videos/init
- Upload chunks: POST /videos/upload_chunk/{video_hash}/{chunk_index}
- Finalize: POST /videos/finalize/{video_hash}

2) Processing
- Finalize enqueues a Kafka message. The worker consumes it, runs FFmpeg to produce:
  - HLS: `/media/uploads/<hash>/hls/playlist.m3u8`
  - DASH: `/media/uploads/<hash>/dash/manifest.mpd`
- The DB is updated with `status=completed`, `url` (HLS), `hls_url`, `dash_url`.

3) Playback
- Open `client/streaming.html` served over HTTP. It fetches `/videos`, then plays both HLS and DASH using URLs served by the FastAPI app.

Troubleshooting
---------------

- FFmpeg input not found:
  - Ensure `finalize` created the file at an absolute path and that Kafka processed it.
  - Check logs: `server/app/ffmpeg.log`, `server/app/worker.log`.
- CORS on manifests/segments:
  - Access the client via HTTP (not file://).
  - Ensure backend is running and `/media` is mounted (see `main.py`).
- Kafka not reachable:
  - Confirm docker compose is up and broker is on `localhost:9092`.

Why VidPipeline
---------------

- Learn chunked uploads and async processing
- Understand adaptive streaming (HLS vs DASH)
- Great for demos and portfolio work
- Lightweight, local, production-like pipeline

License
-------

MIT

Future Improvements
-------------------

- Live streaming (WebSockets)
- Automatic thumbnail generation
- Auth and multi-user support
- Dockerize full stack
