import os
import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from neo4j import AsyncGraphDatabase

# APScheduler imports
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from starlette.responses import JSONResponse

from app.api.api_router import api_router, auth_router
from app.core.config import get_settings
from app.core.utils.celery_worker import periodic_group_monitoring_trigger_sessions
from app.schemas.logger import logger

app = FastAPI(
    title="minimal fastapi postgres template",
    version="6.1.0",
    description="https://github.com/20230028426_EYGS/coe-ens-application-backend.git",
    openapi_url="/openapi.json",
    docs_url="/",
)

app.include_router(auth_router)
app.include_router(api_router)

# Sets all CORS enabled origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        str(origin).rstrip("/")
        for origin in get_settings().security.backend_cors_origins
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Guards against HTTP Host Header attacks
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=get_settings().security.allowed_hosts,
)

# ----------------------------
# Startup: DB check + Scheduler
# ----------------------------
@app.on_event("startup")
async def startup_event():
    # Neo4j health check
    try:
        driver = AsyncGraphDatabase.driver(
            os.environ.get("GRAPHDB__URI"),
            auth=(os.environ.get("GRAPHDB__USER"), os.environ.get("GRAPHDB__PASSWORD")),
        )
        async with driver.session() as session:
            await session.run("RETURN 1")
        logger.info("Neo4j connection established.")
    except Exception as e:
        logger.warning(f"Failed to connect to Neo4j: {str(e)}")

    async def _pgm_wrapper(task_name: str):
        try:
            now = datetime.datetime.now().isoformat(timespec="seconds")
            logger.info(f"[{now}] Running job: {task_name}")
            await periodic_group_monitoring_trigger_sessions()
        except Exception as e:
            logger.error(f"_pgm_wrapper error: {e}")
    # Start APScheduler
    scheduler = AsyncIOScheduler()
    if get_settings().allow.periodicity:
        # Every 1 hour
        scheduler.add_job(
            _pgm_wrapper,
            IntervalTrigger(hours=1),
            kwargs={"task_name": "tasks.periodic_group_monitoring_runner"},
            id="job_every_hour",
            replace_existing=True,
            max_instances=3,
            coalesce=True,
            misfire_grace_time=60
        )
        

        # Every day at 12:30 AM
        scheduler.add_job(
            _pgm_wrapper,
            CronTrigger(hour=0, minute=30),
            kwargs={"task_name": "tasks.periodic_group_monitoring_runner"},
            id="job_daily_1230am",
            replace_existing=True,
            max_instances=3,
            coalesce=True,
            misfire_grace_time=60
        )

        scheduler.start()
        app.state.scheduler = scheduler
        logger.info("APScheduler started with 2 jobs.")

@app.on_event("shutdown")
async def shutdown_event():
    scheduler = getattr(app.state, "scheduler", None)
    if scheduler:
        logger.info("Shutting down APScheduler...")
        scheduler.shutdown(wait=False)  # don't wait for running jobs
        logger.info("APScheduler shutdown complete.")