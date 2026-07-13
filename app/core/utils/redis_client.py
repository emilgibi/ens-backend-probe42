import redis
import os
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL")
SESSION_SET_KEY = "analysis_session_queue_session_ids"
VALIDATION_SESSION_SET_KEY = "validation_session_queue_session_ids"
rdb = redis.Redis.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_keepalive=True,
    health_check_interval=30,  # checks connection every 30s
    retry_on_timeout=True,
)