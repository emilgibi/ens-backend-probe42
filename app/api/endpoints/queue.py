from typing import Literal
from fastapi import APIRouter, Depends, File, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.scheduling.periodic_scheduling import dev_trigger_analysis_, dev_trigger_entity_validation_, get_all_session_ids_in_queue, queue_trigger_analysis_, queue_trigger_entity_validation_
from app.core.utils.celery_worker import fallback_periodic_group_monitoring_runner, periodic_group_monitoring_trigger_sessions
from app.models import User
from app.schemas.responses import *
from app.api import deps


router = APIRouter()

# @router.post("/celery-beat-periodic-scheduling/")
# async def queue_trigger_analysis(session: AsyncSession = Depends(deps.get_session), current_user_id: User = Depends(deps.get_current_user)):
#     # 1. Save to DB
#     try:
#         session_config_response = await periodic_group_monitoring_trigger_sessions()
#         response = ResponseMessage(
#             status="success",
#             data=session_config_response,  
#             message="Periodic Session config processed successfully"
#         )
#         return response

#     except HTTPException as http_err:
#         # Return structured error responses for HTTP exceptions
#         raise http_err

#     except Exception as error:
#         # Handle unexpected errors
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Failed to processing client config: {str(error)}"
#         ) 


@router.get("/queue-sessions")
async def get_queue_session_ids(queue_name: Literal["", "analysis_session_queue", "validation_session_queue"], session: AsyncSession = Depends(deps.get_session), current_user_id: User = Depends(deps.get_current_user)):
    queue = await get_all_session_ids_in_queue(queue_name)
    return {"queue_name": queue_name, "queue": queue}



# @router.post("/queue-trigger-analysis/")
# async def queue_trigger_analysis(session_id: str, session: AsyncSession = Depends(deps.get_session), current_user_id: User = Depends(deps.get_current_user)):
#     # 1. Save to DB
#     try:
#         client_config_response = await queue_trigger_analysis_(session_id, session)
#         response = ResponseMessage(
#             status="success",
#             data=client_config_response,  
#             message="Client config processed successfully"
#         )
#         return response

#     except HTTPException as http_err:
#         # Return structured error responses for HTTP exceptions
#         raise http_err

#     except Exception as error:
#         # Handle unexpected errors
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Failed to processing client config: {str(error)}"
#         ) 


@router.post("/queue-trigger-entity-validation/")
async def queue_trigger_entity_validation(session_id: str, session: AsyncSession = Depends(deps.get_session), current_user_id: User = Depends(deps.get_current_user)):
    # 1. Save to DB
    try:
        client_config_response = await queue_trigger_entity_validation_(session_id, session)
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


@router.post("/queue-trigger-analysis/")
async def queue_trigger_analysis(session_id: str, session: AsyncSession = Depends(deps.get_session), current_user_id: User = Depends(deps.get_current_user)):
    # 1. Save to DB
    try:
        client_config_response = await queue_trigger_analysis_(session_id, session)
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

@router.post("/develop-trigger-entity-validation")
async def dev_trigger_entity_validation(session_id: str, session: AsyncSession = Depends(deps.get_session), current_user_id: User = Depends(deps.get_current_user)):
    # 1. Save to DB
    try:
        client_config_response = await dev_trigger_entity_validation_(session_id, session)
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


@router.post("/develop-trigger-analysis")
async def dev_trigger_analysis(session_id: str, session: AsyncSession = Depends(deps.get_session), current_user_id: User = Depends(deps.get_current_user)):
    # 1. Save to DB
    try:
        client_config_response = await dev_trigger_analysis_(session_id, session)
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

