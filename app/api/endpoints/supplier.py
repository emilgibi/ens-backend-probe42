from typing import List, Literal, Optional
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, Field

from app.core.scheduling.periodic_scheduling import dev_trigger_analysis_
from app.core.utils.celery_worker import periodic_group_monitoring_trigger_sessions

from app.api.deps import get_current_user, get_session
from app.core.Monitoring.monitoring import process_webhook_logic
from app.schemas.requests import BulkPayload, ClientConfigurationRequest, SinglePayloadItem, SessionCreationRequest
from app.schemas.responses import *
from app.core.supplier.supplier import *
from app.api import deps
import pandas as pd
import io
from app.schemas.logger import logger
from app.schemas.requests import *
router = APIRouter()

from datetime import date


# @router.post("/upload-supplier-list", response_model=UserResponse, description="Get current user")
# async def read_current_user(
#     current_user: User = Depends(deps.get_current_user),
# ) -> User:
#     return current_user

@router.post("/upload-excel", response_model=ResponseMessage, status_code=status.HTTP_201_CREATED)
async def upload_excel(
    client_id: Optional[str] = Query(None),
    file: UploadFile = File(...),
    session: AsyncSession = Depends(deps.get_session),
    current_user_id: User = Depends(deps.get_current_user),
):
    try:
        print("entered")
        # Check if a file was uploaded
        if not file:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No file uploaded"
            )

        user_email_data = await get_universe_ens_data(
            table_name="users_table",
            required_columns=["user_id", "email"],
            ens_ids=None,
            session=session
        )

        user_record = next(
            (row for row in user_email_data if row["user_id"] == current_user_id["user_id"]),
            None
        )

        if not user_record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User not found for user_id {current_user_id['user_id']}"
            )

        user_id = user_record["user_id"]
        user_email = user_record["email"]

        client_id = '73c75148-78c6-41a7-94ad-cb8a1ffe5575'
        sheet_data = await process_excel_file(file, client_id, user_id, user_email, session)
        response = ResponseMessage(
            status="success",
            data=sheet_data,  
            message="Excel file processed successfully"
        )
        return response

    except HTTPException as http_err:
        # Return structured error responses for HTTP exceptions
        raise http_err

    except Exception as error:
        # Handle unexpected errors
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process the Excel file: {str(error)}"
        )


@router.get("/get-supplier-data", response_model=ResponseMessage, status_code=status.HTTP_200_OK)
async def get_supplier_data(
    session_id: str, 
    page_no: int = Query(1, ge=1),       
    rows_per_page: int = Query(10, le=1000), 
    final_validation_status: Literal["", "review", "auto_reject", "auto_accept"] = "",
    session: AsyncSession = Depends(deps.get_session),
    current_user: User = Depends(deps.get_current_user)
):
    try:
        # Validate session_id
        if not session_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No session_id provided"
            )

        # Fetch data from DB
        sheet_data = await get_session_supplier(session_id, page_no, rows_per_page, final_validation_status, session)

        return ResponseMessage(
            status="success",
            data=sheet_data,
            message="Successfully retrieved data"
        )

    except HTTPException as http_err:
        raise http_err  # Pass FastAPI exceptions as they are

    except Exception as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve data: {str(error)}"
        )
   
   
@router.put("/update-suggestions-bulk", response_model=ResponseMessage, status_code=status.HTTP_200_OK)
async def accept_suggestions_bulk(payload: BulkPayload, session: AsyncSession = Depends(deps.get_session),
    current_user: User = Depends(deps.get_current_user)):
    try:
        # Validate status field
        if payload.status.lower().strip() not in ["accept", "reject"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid status. Use 'accept' or 'reject'."
            )

        # Call the function to update suggestions in bulk
        update_res = await update_suggestions_bulk(payload, session)

        # If update failed or no rows were updated, handle accordingly
        if not update_res or update_res.get("status") != "success":
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to process suggestions update."
            )

        # Return success response
        return ResponseMessage(
            status="success",
            data={"data": update_res},
            message=f"Suggestions for session {payload.session_id} have been {payload.status}."
        )

    except HTTPException as http_err:
        raise http_err  # Re-raise FastAPI exceptions with proper status codes

    except Exception as error:
        # Handle unexpected errors
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error occurred: {str(error)}"
        )

@router.put("/update-suggestions-single", response_model=ResponseMessage)
async def accept_suggestions_single(
    session_id: str,
    payload: List[SinglePayloadItem], 
    session: AsyncSession = Depends(deps.get_session),
    current_user: User = Depends(deps.get_current_user)
):
    if not payload:
        raise HTTPException(
            status_code=400, 
            detail="Payload is empty. Please provide valid data."
        )

    # Validate each item's status
    for item in payload:
        if item.status.strip().lower() not in ["accept", "reject"]:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid status '{item.status}' for ens_id {item.ens_id}. Use 'accept' or 'reject'."
            )

    try:
        update_res = await update_suggestions_single(payload, session_id, session)

        if update_res.get("status") == "error":
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to update suggestions: {update_res.get('message')}"
            )

        return ResponseMessage(
            status="success",
            data=update_res,  # Include response data
            message=f"Suggestions have been updated successfully for {len(payload)} items."
        )

    except HTTPException as http_err:
        raise http_err  # Re-raise FastAPI-specific HTTP exceptions

    except Exception as error:
        logger.error(f"Unexpected error: {error}")
        raise HTTPException(
            status_code=500, 
            detail=f"An unexpected error occurred: {str(error)}"
        )
   
@router.get("/get-main-supplier-data", response_model=ResponseMessage)
async def get_main_supplier_data(
    session_id: str, 
    page_no: int = Query(1, ge=1),       
    rows_per_page: int = Query(10, le=1000), 
    session: AsyncSession = Depends(deps.get_session),
    current_user: User = Depends(deps.get_current_user)
):
    try:
        # Validate session_id
        if not session_id:
            raise HTTPException(status_code=400, detail="No session_id provided.")

        # Fetch supplier data
        sheet_data = await get_main_session_supplier(session_id, page_no, rows_per_page, session)

        # If no data found, raise a 404 error
        if not sheet_data.get("data"):
            raise HTTPException(status_code=404, detail=f"No supplier data found for session_id: {session_id}")

        return ResponseMessage(
            status="success",
            data=sheet_data,  # Include data as a dictionary
            message="Successfully retrieved data."
        )

    except HTTPException as http_err:
        raise http_err  # Re-raise HTTP exceptions to maintain proper status codes

    except Exception as error:
        logger.error(f"Unexpected error: {error}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve data: {str(error)}"
        ) 

 
@router.get("/get-main-supplier-data-compiled", response_model=ResponseMessage)
async def get_main_supplier_data_compiled(
    session_id: str, 
    page_no: int = Query(1, ge=1),       
    rows_per_page: int = Query(10, le=1000), 
    session: AsyncSession = Depends(deps.get_session),
    current_user: User = Depends(deps.get_current_user)
):
    try:
        # Validate session_id
        if not session_id:
            raise HTTPException(status_code=400, detail="No session_id provided.")

        # Fetch supplier data
        sheet_data = await get_main_session_supplier_compiled(session_id, page_no, rows_per_page, session)

        # If no data found, raise a 404 error
        if not sheet_data.get("data"):
            raise HTTPException(status_code=404, detail=f"No supplier data found for session_id: {session_id}")

        return ResponseMessage(
            status="success",
            data=sheet_data,  # Include data as a dictionary
            message="Successfully retrieved data."
        )

    except HTTPException as http_err:
        raise http_err  # Re-raise HTTP exceptions to maintain proper status codes

    except Exception as error:
        logger.error(f"Unexpected error: {error}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve data: {str(error)}"
        ) 

@router.get("/get-session-screening-status", response_model=ResponseMessage)
async def get_session_screening_status_data(
    page_no: int = Query(1, ge=1),       
    rows_per_page: int = Query(10, le=1000), 
    screening_analysis_status: Optional[Literal["", "active", "not_started"]] = "",
    session: AsyncSession = Depends(deps.get_session),
    current_user: User = Depends(deps.get_current_user)
):
    try:
        # Fetch screening status data
        sheet_data = await get_session_screening_status(page_no, rows_per_page, screening_analysis_status, session)

        # Ensure the data exists
        if not sheet_data["data"]:
            raise HTTPException(
                status_code=404, 
                detail="No screening status data found."
            )

        return ResponseMessage(
            status="success",
            data=sheet_data,  
            message="Successfully Retrieved Data"
        )

    except HTTPException as http_err:
        # Raise FastAPI HTTPExceptions for correct status codes
        raise http_err

    except Exception as error:
        logger.error(f"Unexpected error: {error}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve data: {str(error)}"
        )


@router.get("/get-nomatch-count", response_model=ResponseMessage, status_code=status.HTTP_200_OK)
async def get_nomatch(
    session_id: str,
    session: AsyncSession = Depends(deps.get_session),
    current_user: User = Depends(deps.get_current_user)
):
    try:
        # Validate session_id
        if not session_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No session_id provided"
            )

        # Fetch data from DB
        nomatch_data = await get_nomatch_count(session_id, session)

        return ResponseMessage(
            status="success",
            data=nomatch_data,
            message="Successfully retrieved data"
        )

    except HTTPException as http_err:
        raise http_err  # Pass FastAPI exceptions as they are

    except Exception as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve data: {str(error)}"
        )
 
 
@router.post(
    "/client-configuration",
    description="Client Configuration",
    status_code=status.HTTP_201_CREATED,
)
async def client_configuration(
    client_configuration: ClientConfigurationRequest,
    current_user_id: User = Depends(deps.get_current_user),
    session: AsyncSession = Depends(deps.get_session)
):
    try:
        client_config_response = await client_config(client_configuration, session)
        response = ResponseMessage(
            status="success",
            data=client_config_response,  
            message="Client config processed successfully"
        )
        return response

    except HTTPException as http_err:
        # Return structured error responses for HTTP exceptions
        raise http_err

    except Exception as error:
        # Handle unexpected errors
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to processing client config: {str(error)}"
        )


@router.post(
    "/process-webhook-response",
    response_model=WebhookProcessingResponse,
    description="Process webhook response using response_id and trigger analysis"
)
async def process_webhook_response(
    request: WebhookResponseRequest,
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user)
):
    try:
        result = await process_webhook_logic(
            request=request,
            session=session
        )

        # Trigger analysis
        await dev_trigger_analysis_(result["session_id"], session)

        return WebhookProcessingResponse(
            status=True,
            message=f"Webhook response processed successfully. Analysis triggered for session: {result['session_id']}",
            session_id=result["session_id"],
            tracking_id=result["tracking_id"],
            ens_ids_processed=len(result["ens_ids"])
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process webhook response: {str(e)}"
        )

@router.post("/create-session", response_model=SessionCreationResponse, status_code=status.HTTP_201_CREATED)
async def create_session_api(
    request: SessionCreationRequest,
    current_user=Depends(deps.get_current_user),
    session: AsyncSession = Depends(deps.get_session)
):

    return await create_session_from_ens_ids_with_session(
        ens_ids=request.ens_ids,
        session_id=request.session_id,
        source=request.source,
        source_id=request.source_id,
        session=session
    )


@router.post("/generate-ondemand-screening", response_model=ENSProcessingResponse,
             responses={
                 429: {
                     "model": ErrorResponseTooManyRequests,
                     "description": "Too Many Requests - Daily report limit reached."
                 },
                 423: {
                     "model": ErrorResponseAlreadyQueued,
                     "description": "This entity is already queued for screening. Please try again later."
                 }
             }
             )
async def process_ens_id(
        request: ENSProcessingRequest,
        background_tasks: BackgroundTasks,
        current_user: dict = Depends(deps.get_current_user),
        session: AsyncSession = Depends(deps.get_session)
):
    """
    API endpoint to process ENS IDs and create a session.
    """
    try:
        session_id = str(uuid.uuid4())
        logger.info(f"Generated session_id: {session_id} for user: {current_user['user_id']}")

        if not request.ens_ids:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="ENS IDs list cannot be empty"
            )

        for ens_id in request.ens_ids:
            runs, count = await get_dynamic_ens_data("ensid_screening_status", ["create_time", "update_time", "id", "overall_status"], ens_id=ens_id, session_id=None, session=session)

            today = date.today()

            count_in_queue = sum(
                1 for row in runs
                if row['create_time'].date() == today and (row['overall_status'] == "QUEUED")  # TODO CHANGE LOGIC WHEN QUEUE READY
            )

            if count_in_queue >= 1:
                raise HTTPException(
                    status_code=status.HTTP_423_LOCKED,
                    detail="This entity is already queued for screening. Please try again later."
                )

            count_completed = sum(
                1 for row in runs
                if row['create_time'].date() == today and row['overall_status'] == "COMPLETED"
            )

            per_day_quota = 3
            if count_completed >= per_day_quota:
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=f"You have reached your quota of {per_day_quota} reports for this entity for today. Please try again later or contact an administrator for support."
                )

        # Fetch user email based on user_id
        user_email_data = await get_universe_ens_data(
            table_name="users_table",
            required_columns=["user_id", "email"],
            ens_ids=None,
            session=session
        )

        user_email = next(
            (row["email"] for row in user_email_data if row["user_id"] == current_user["user_id"]),
            None
        )

        if not user_email:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Email not found for user_id {current_user['user_id']}"
            )
        result = await create_session_from_ens_ids_with_session(
            ens_ids=request.ens_ids,
            session_id=session_id,
            source="OD",
            source_id=user_email,
            session=session
        )
        await dev_trigger_analysis_(session_id, session)

        response = ENSProcessingResponse(
            session_id=result["session_id"],
            rows_inserted=result["rows_inserted"],
            session_screening_status=result["session_screening_status"],
            ens_ids_processed=result["ens_ids_processed"]
        )
        logger.info(f"Successfully processed {len(request.ens_ids)} ENS IDs for session {session_id}")
        return response

    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(ve)
        )
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Error processing ENS IDs: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process ENS IDs: {str(e)}"
        )