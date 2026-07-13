# app/api/endpoints/streaming.py

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from typing import Optional
import asyncpg
import asyncio
import json
from urllib.parse import quote_plus

from app.core.config import get_settings
from app.schemas.logger import logger

router = APIRouter()

encoded_password = quote_plus(get_settings().database.password.get_secret_value())
DATABASE_URL = (
    f"postgresql://{get_settings().database.username}:{encoded_password}@"
    f"{get_settings().database.hostname}:{get_settings().database.port}/{get_settings().database.db}"
)


@router.websocket("/ws/session-status")
async def websocket_session_status(
    websocket: WebSocket,
    session_id: Optional[str] = Query(None, description="Session ID"),
):
    await websocket.accept()
    conn: Optional[asyncpg.Connection] = None

    channel_name = "session_id_status_channel"
    session_notification_queue: asyncio.Queue = asyncio.Queue()

    async def handle_session_notification(connection, pid, channel, payload):
        try:
            data = json.loads(payload)
        except Exception:
            data = {"raw": payload}
        await session_notification_queue.put(data)

    try:
        conn = await asyncpg.connect(DATABASE_URL)
        logger.info("CONNECTION ESTABLISHED")
        logger.info(f"SESSION ID ---> {session_id}")

        await conn.add_listener(channel_name, handle_session_notification)

        while True:
            try:
                # Don’t block forever: allows shutdown/reload cancellation to be processed quickly
                payload = await asyncio.wait_for(session_notification_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                logger.info("session-status websocket cancelled (shutdown/reload)")
                raise

            if session_id:
                if payload.get("session_id") == session_id:
                    await websocket.send_text(json.dumps(payload))
            else:
                await websocket.send_text(json.dumps(payload))

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected")
    except asyncio.CancelledError:
        logger.info("session-status websocket cancelled (outer)")
        raise
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        try:
            await websocket.send_text(f"Error: {str(e)}")
        except Exception:
            pass
    finally:
        if conn:
            logger.info("CONNECTION CLOSING")
            # Ensure cleanup cannot hang forever
            try:
                await asyncio.wait_for(
                    conn.remove_listener(channel_name, handle_session_notification),
                    timeout=2.0,
                )
            except Exception as e:
                logger.warning(f"remove_listener failed: {e}")

            try:
                await asyncio.wait_for(conn.close(), timeout=2.0)
            except Exception as e:
                logger.warning(f"conn.close failed: {e}")


@router.websocket("/ws/ensid-status")
async def websocket_ensid_status(
    websocket: WebSocket,
    session_id: str = Query(..., description="Session ID"),
):
    await websocket.accept()
    conn: Optional[asyncpg.Connection] = None

    channel_name = "ens_id_status_channel"
    ensid_notification_queue: asyncio.Queue = asyncio.Queue()

    async def handle_ensid_notification(connection, pid, channel, payload):
        try:
            data = json.loads(payload)
        except Exception:
            data = {"raw": payload}
        await ensid_notification_queue.put(data)

    try:
        conn = await asyncpg.connect(DATABASE_URL)
        logger.info("CONNECTION ESTABLISHED")

        await conn.add_listener(channel_name, handle_ensid_notification)

        while True:
            try:
                payload = await asyncio.wait_for(ensid_notification_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                logger.info("ensid-status websocket cancelled (shutdown/reload)")
                raise

            if payload.get("session_id") == session_id:
                await websocket.send_text(json.dumps(payload))

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected")
    except asyncio.CancelledError:
        logger.info("ensid-status websocket cancelled (outer)")
        raise
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        try:
            await websocket.send_text(f"Error: {str(e)}")
        except Exception:
            pass
    finally:
        if conn:
            logger.info("CONNECTION CLOSING")
            try:
                await asyncio.wait_for(
                    conn.remove_listener(channel_name, handle_ensid_notification),
                    timeout=2.0,
                )
            except Exception as e:
                logger.warning(f"remove_listener failed: {e}")

            try:
                await asyncio.wait_for(conn.close(), timeout=2.0)
            except Exception as e:
                logger.warning(f"conn.close failed: {e}")