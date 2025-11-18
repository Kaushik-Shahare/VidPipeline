FROM python:3.12.3-slim

WORKDIR /app

# System dependencies
RUN apt-get update && \
    apt-get install -y build-essential && \
    apt-get install -y ffmpeg libsm6 libxext6 &&\
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create logs directory
RUN mkdir -p /app/logs

EXPOSE 8000

# Default command (can be overridden in k8s)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
