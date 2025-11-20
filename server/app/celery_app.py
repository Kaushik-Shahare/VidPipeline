from celery import Celery
from dotenv import load_dotenv
import os
load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Create Celery app
celery_app = Celery(
    "video_tasks", 
    broker=REDIS_URL,
    backend=REDIS_URL
)

celery_app.conf.update(
    task_routes={
        'tasks.process_video': {'queue': 'video_processing'},
    },
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    imports=('tasks.video_tasks',),
    
    # Worker heartbeat and timeout settings to prevent worker death during long FFmpeg tasks
    broker_heartbeat=0,  # Disable broker heartbeat (use worker heartbeat instead)
    worker_prefetch_multiplier=1,  # Only fetch one task at a time to avoid blocking
    worker_max_tasks_per_child=10,  # Restart worker after 10 tasks to prevent memory leaks
    
    # Task execution limits
    task_soft_time_limit=1800,  # 30 minutes soft limit (allows cleanup)
    task_time_limit=2100,  # 35 minutes hard limit
    task_acks_late=True,  # Only acknowledge task after completion
    task_reject_on_worker_lost=True,  # Requeue task if worker dies
    
    # Result backend settings
    result_expires=3600,  # Results expire after 1 hour
    result_backend_transport_options={'master_name': 'mymaster'},
)

# Import and register the task
from tasks.video_tasks import create_process_video_task

# Create and register the task
process_video = create_process_video_task(celery_app)

# Explicitly register the task with the app
celery_app.register_task(process_video)

# Register profile-specific task factory
from tasks.video_tasks import create_process_profile_task
process_profile = create_process_profile_task(celery_app)
celery_app.register_task(process_profile)

# Register thumbnail task
from tasks.video_tasks import create_process_thumbnail_task
process_thumbnail = create_process_thumbnail_task(celery_app)
celery_app.register_task(process_thumbnail)

celery = celery_app
