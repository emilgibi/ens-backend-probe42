from typing import Annotated, List, Union
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession
from app.schemas.requests import BulkPayload, SinglePayloadItem
from app.schemas.responses import *
from app.core.supplier.report import *
from app.api import deps
import pandas as pd
import io
from app.core.config import get_settings

router = APIRouter()
from fastapi import APIRouter, Query
from fastapi.responses import Response
from typing import Optional
from app.models import NotificationType, NotificationType, User
NotificationTypeOrEmpty = Union[NotificationType, Literal[""]]
router = APIRouter()

@router.get("/download-report/")
async def download_report(
    session_id: str = Query(..., description="Session ID"),
    ens_id: str = Query(..., description="ENS ID"),
    type_of_file: str = Query(..., description="Type of file (e.g., docx, pdf, csv)"),
    current_user: User = Depends(deps.get_current_user)
):
    try:
        print("xyz")
        file_data, result = await report_download(session_id, ens_id, ens_id, type_of_file)
        print("abc")

        if file_data is None:
            print("error")
            return {"error": result}

        # Determine media type based on file type
        media_types = {
            "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "pdf": "application/pdf",
            "csv": "text/csv"
        }
        media_type = media_types.get(type_of_file.lower(), "application/octet-stream")

        return Response(
            content=file_data,
            media_type=media_type,
            headers={"Content-Disposition": f"attachment; filename={result}"}
        )

    except Exception as e:
        return {"error": str(e)}


@router.get("/bulk-download-report/")
async def bulk_download_report(session_id: str = Query(..., description="Session ID"),
    current_user: User = Depends(deps.get_current_user)):
    try:
        file_data, result = await report_bulk_download(session_id)

        if file_data is None:
            return {"error": result}

        return Response(
            content=file_data,
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename={result}"}
        )

    except Exception as e:
        return {"error": str(e)}

@router.get("/download-notification-csv")
async def download_notification_csv(
    startdate: str = Query(..., alias="start date", description="YYYY-MM-DD (inclusive)"),
    enddate: str = Query(..., alias="end date", description="YYYY-MM-DD (inclusive)"),
    notificationtypes: Optional[list[NotificationTypeOrEmpty]] = Query( default=None, description="Repeat param; each value may be a NotificationType or ''"),
    session: AsyncSession = Depends(deps.get_session),
    current_user: User = Depends(deps.get_current_user)):
    try:
        return await download_notification_csv_(
            session=session,
            start_date=startdate,
            end_date=enddate,
            notificationtypes = notificationtypes
        )
    except HTTPException:
        raise
    except Exception as e:
        return {"error": str(e)}


@router.get("/group-bulk-download-report")
async def screener_bulk_download_report(
    group_id: str,
    session: AsyncSession = Depends(deps.get_session),
    current_user: User = Depends(deps.get_current_user),
):
    try:
        zip_data, filename = await r2_screener_report_bulk_download_by_source(session, group_id)

        if not zip_data:
            return {"error": "No files found for the given session IDs."}

        return Response(
            content=zip_data,
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except Exception as e:
        return {"error": str(e)}
    
@router.get("/get-images/")
async def get_images(
    google_image_name: str = Query(..., description="Image name prefix (e.g. 'ens_id' or 'ens_id/logo')"),
    current_user: User = Depends(deps.get_current_user)
):
    try:
        print("google....")
        print(google_image_name)
        images = await get_google_images_from_blob(google_image_name)
        return {"images": images}
    except HTTPException:
        raise
    except Exception as e:
        return {"error": str(e)}