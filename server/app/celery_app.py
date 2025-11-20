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
    imports=('tasks.video_tasks',),  # Auto-discover tasks
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
