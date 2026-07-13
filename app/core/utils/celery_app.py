from celery import Celery
import os
from dotenv import load_dotenv

load_dotenv()

BROKER_URL = os.getenv("CELERY_BROKER_URL")

celery_app = Celery("ens_tasks", broker=BROKER_URL, backend="rpc://")

celery_app.conf.update(
    broker_transport_options={
        'confirm_publish': True,
    },
    broker_connection_timeout=30,        # ← correct key for pyamqp
    broker_connection_retry=True,
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=3,
    broker_pool_limit=None,              # ← disables connection pooling, avoids stale connections
)

import app.core.utils.celery_worker