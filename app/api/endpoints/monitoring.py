from typing import List, Union
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession
from app.schemas.requests import BulkPayload, SinglePayloadItem,continuous_monitoring_bulk
from app.schemas.responses import *
from app.core.supplier.report import *
from app.api import deps
import pandas as pd
import io
from app.core.config import get_settings
from app.core.Monitoring.monitoring import run_continuous_monitoring
from fastapi import APIRouter, Query
from fastapi.responses import Response
from typing import Optional
from app.models import User
import asyncio
from fastapi.responses import JSONResponse

router = APIRouter()

@router.get("/continuous/")
async def run_cm(
    ens_id: str = Query(..., description="ens id"),
    status: bool = Query(..., description="cm status"),
    current_user: User = Depends(deps.get_current_user),
    session: AsyncSession = Depends(deps.get_session)
                          ):
    print("entered")
    try:
        response= await run_continuous_monitoring(ens_id,status,session)
        return response
    except Exception as e:
        return {"status_code": 500, "success": False, "message": "Entered exception block", "error": str(e)}


@router.post("/continuousbulk/")
async def run_cm_bulk(
    payload: continuous_monitoring_bulk,
    current_user: User = Depends(deps.get_current_user),
    session: AsyncSession = Depends(deps.get_session)
):

    sem = asyncio.Semaphore(5)

    async def run_cm(ens_id: str, status: bool) -> tuple[str, Union[dict, str]]:
        async with sem:
            try:
                result = await run_continuous_monitoring(ens_id, status, session)
                return ens_id, result
            except Exception as e:
                return ens_id, {
                    "status_code": 500,
                    "success": False,
                    "message": "Entered exception block",
                    "error": str(e)
                }

    tasks = [
        run_cm(item.ens_id, item.status)
        for item in payload.data
    ]

    results = await asyncio.gather(*tasks)

    # Process results
    results_dict = {ens_id: result for ens_id, result in results}
    status_codes = [result["status_code"] for result in results_dict.values()]

    if all(code == 500 for code in status_codes):
        # All failed — raise 500 with results only
        raise HTTPException(
            status_code=500,
            detail={
                "results": results_dict
            }
        )

    elif any(code == 500 for code in status_codes):
        # Some failed — return 207 with results
        return JSONResponse(
            status_code=207,
            content={
                "results": results_dict
            }
        )

    else:
        # All succeeded — return 200 with results
        return {
            "results": results_dict
        }
