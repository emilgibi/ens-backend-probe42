# app/core/utils/celery_worker.py

import asyncio
from celery.schedules import crontab
from celery.utils.log import get_task_logger
from celery.app.control import Inspect
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.core.scheduling.periodic_scheduling import run_periodic_schedule
from app.core.security.jwt import create_jwt_token
from app.core.utils.celery_app import celery_app
from app.core.config import get_settings
# from app.core.analysis.analysis import run_analysis
# from app.core.analysis.fallback import trigger_from_db_if_needed
from app.core.utils.db_utils import *
from app.core.utils.redis_client import rdb, SESSION_SET_KEY, VALIDATION_SESSION_SET_KEY
from app.models import STATUS

logger = get_task_logger(__name__)

# Set up Celery Beat schedule
celery_app.conf.beat_schedule = {
    "tasks.periodic_group_monitoring_runner": {
        "task": "tasks.periodic_group_monitoring_runner",
        "schedule": crontab(hour=12, minute=30),  # 12:30 PM every day
    },
    # Runs every hour
    "periodic_group_monitoring_runner_hourly": {
        "task": "tasks.periodic_group_monitoring_runner",
        "schedule": crontab(minute=0, hour="*/1"),
    },
    # Runs every 30 minutes
    # "periodic_group_monitoring_runner_every_30_min": {
    #     "task": "tasks.periodic_group_monitoring_runner",
    #     "schedule": crontab(minute=0, hour="*/1"),
    # },
    "tasks.fallback_for_periodic_group_monitoring_runner": {
        "task": "tasks.fallback_for_periodic_group_monitoring_runner",
        "schedule": crontab(minute=0, hour="*/6"),
    },
    "tasks.fallback_for_validation_monitoring_runner": {
        "task": "tasks.fallback_for_validation_monitoring_runner",
        "schedule": crontab(minute=0, hour="*/7"),
    },
    # "tasks.periodic_group_monitoring_runner": {
    #     "task": "tasks.periodic_group_monitoring_runner",
    #     "schedule": crontab(minute="*/6"),
    # },
    # "tasks.fallback_for_periodic_group_monitoring_runner": {
    #     "task": "tasks.fallback_for_periodic_group_monitoring_runner",
    #     "schedule": crontab(minute="*/3"),
    # },
    # "tasks.fallback_for_validation_monitoring_runner": {
    #     "task": "tasks.fallback_for_validation_monitoring_runner",
    #     "schedule": crontab(minute="*/2"),
    # }
}


def safe_async_run(coro):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop = asyncio.get_event_loop()
    return loop.run_until_complete(coro)


# Task 1: Trigger monitoring.schedule_and_trigger_sessions
from celery import shared_task
@shared_task(name="tasks.periodic_group_monitoring_runner")
async def periodic_group_monitoring_trigger_sessions():
    logger.info(" Celery Task STARTED: periodic_group_monitoring_trigger_sessions")
    logger.info(" Running Celery Beat : periodic_group_monitoring_trigger_sessions")
    logger.info(" Read from schedule_monitoring table (get active groups)")
    settings = get_settings()
    engine = create_async_engine(settings.sqlalchemy_database_uri, echo=True)
    async_session = async_sessionmaker(engine, expire_on_commit=False)
    async def run_trigger():
        async with async_session() as session:
            try: 
                await run_periodic_schedule(session)
            except Exception as e:
                logger.error(f" tasks.periodic_group_monitoring_runner error: {e}")

    try:
        await run_trigger()
        # safe_async_run(run_trigger())
        logger.info(" tasks.periodic_group_monitoring_runner trigger completed.")
    except Exception as e:
        logger.error(f" tasks.periodic_group_monitoring_runner error: {e}")

# # Task 2: Trigger fallback for periodic group monitoring runner
@shared_task(name="tasks.fallback_for_periodic_group_monitoring_runner")
def fallback_periodic_group_monitoring_runner():
    logger.info("[Screening] Checking if queue is empty...")

    inspector: Inspect = celery_app.control.inspect()
    queue_name = "analysis_session_queue"

    active = inspector.active() or {}
    reserved = inspector.reserved() or {}

    active_count = sum(
        sum(1 for task in tasks if task.get("delivery_info", {}).get("routing_key") == queue_name)
        for tasks in active.values()
    )
    reserved_count = sum(
        sum(1 for task in tasks if task.get("delivery_info", {}).get("routing_key") == queue_name)
        for tasks in reserved.values()
    )
    total_tasks = active_count + reserved_count

    redis_set_size = rdb.scard(SESSION_SET_KEY)
    logger.info(f"Redis SET Size: {redis_set_size}")
    logger.info(f"Celery total_tasks: {total_tasks}")

    if total_tasks == 0:
        logger.info("[Screening] Queue & Redis are both empty. Running fallback...")
        settings = get_settings()
        engine = create_async_engine(settings.sqlalchemy_database_uri, echo=True)
        async_session = async_sessionmaker(engine, expire_on_commit=False)

        async def run_trigger():
            async with async_session() as session:
                requeued_screening_queue = await fallback_analysis_trigger_from_db(session)
                logger.error(f"Requeued_screening_queue: {requeued_screening_queue}")
        try:
            safe_async_run(run_trigger())
            # await run_trigger()
        except Exception as e:
            logger.error(f"Fallback trigger error: {e}")
    else:
        logger.info("[Screening] Queue not empty. Skipping fallback.")

# # Task 3: Trigger fallback for validation group monitoring runner
@shared_task(name="tasks.fallback_for_validation_monitoring_runner")
def fallback_validation_monitoring_runner():
    logger.info("[Screening] Checking if queue is empty...")

    inspector: Inspect = celery_app.control.inspect()
    queue_name = "validation_session_queue"

    active = inspector.active() or {}
    reserved = inspector.reserved() or {}

    active_count = sum(
        sum(1 for task in tasks if task.get("delivery_info", {}).get("routing_key") == queue_name)
        for tasks in active.values()
    )
    reserved_count = sum(
        sum(1 for task in tasks if task.get("delivery_info", {}).get("routing_key") == queue_name)
        for tasks in reserved.values()
    )
    total_tasks = active_count + reserved_count

    redis_set_size = rdb.scard(VALIDATION_SESSION_SET_KEY)
    logger.info(f"Redis SET Size: {redis_set_size}")
    logger.info(f"Celery total_tasks: {total_tasks}")

    if total_tasks == 0:
        logger.info("[Screening] Queue & Redis are both empty. Running fallback...")
        settings = get_settings()
        engine = create_async_engine(settings.sqlalchemy_database_uri, echo=True)
        async_session = async_sessionmaker(engine, expire_on_commit=False)

        async def run_trigger():
            async with async_session() as session:
                requeued_screening_queue = await fallback_validation_trigger_from_db(session)
                logger.error(f"Requeued_screening_queue: {requeued_screening_queue}")
        try:
            safe_async_run(run_trigger())
            # await run_trigger()
        except Exception as e:
            logger.error(f"Fallback trigger error: {e}")
    else:
        logger.info("[Screening] Queue not empty. Skipping fallback.")

# # === Periodic Queue Task ===
# @celery_app.task(
#     bind=True,
#     name="process_periodic_session_queue",
#     queue="analysis_session_queue",
#     max_retries=3
# )
# def process_periodic_session_queue(self, session_id: str):
#     print(f"[Celery] STARTED processing session ID: {session_id}")
#     logger.info(f"[Celery] STARTED processing session ID: {session_id}")
#     try:
#         # === Your actual processing logic here ===
#         # Example placeholder
#         print(f" Performing processing logic for {session_id}...")
#         # Take session ID
#         logger.info(f"Starting run_full_pipeline_background for {session_id}")
        
#         try:
#             # Generate JWT token
#             jwt_token = create_jwt_token("application_backend", "development")
#         except Exception as e:
#             logger.error(f"Error generating JWT token: {str(e)}")
#             raise

#         try:
#             # Step 1: Make HTTP request to trigger supplier name validation
#             trigger_run_analysis_response = trigger_run_analysis(session_id, jwt_token.access_token)
#             logger.info(f"c {trigger_run_analysis_response}")
#         except Exception as e:
#             logger.error(f"Error triggering supplier validation:{str(e)}")
#             raise
        
#         # Simulated processing time (e.g., call external API or DB logic)
#         # asyncio.sleep(180)

#         # Remove session from Redis ONLY if processing was successful
#         rdb.srem(SESSION_SET_KEY, session_id)
#         logger.info(f"[Celery] Finished processing session: {session_id}")
#         return f"[Celery] Finished processing session: {session_id}"

#     except Exception as e:
#         logger.error(f"[Celery] Error processing session {session_id}: {e}")
#         raise self.retry(exc=e, countdown=60)  # retry after 60 seconds
