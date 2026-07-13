from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from app.schemas.requests import *
from app.schemas.responses import *
from app.core.supplier.universe import *
from app.api import deps
from app.models import User
from app.schemas.requests import *
import traceback
router = APIRouter()

@router.post("/get-submodal-profile")
async def get_profile(request: UniverseSubModalItem,
                      session: AsyncSession = Depends(deps.get_session),
                      current_user: User = Depends(deps.get_current_user)
                      ):
    try:
        request = request.dict()
        ens_id = request.get("ens_id","")
        session_id = request.get("session_id", None)
        if session_id == "string":  # Remove swagger placeholder if any
            session_id = None

        transformed_data = await compile_company_profile(ens_id, session_id, session)

        return transformed_data

    except HTTPException as http_err:
        # Return structured error responses for HTTP exceptions
        raise http_err

    except Exception as error:
        # Handle unexpected errors
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve profile for ens_id: {str(error)}"
        )

@router.post("/get-submodal-findings")
async def get_findings(request: SubModalItem,
                       session: AsyncSession = Depends(deps.get_session),
                       current_user: User = Depends(deps.get_current_user)):

    try:
        request = request.dict()
        ens_id = request.get("ens_id","")
        session_id = request.get("session_id", None)
        if session_id == "string":  # Remove swagger placeholder if any
            session_id = None

        transformed_data = await compile_company_findings(ens_id, session_id, session)
        print("--------> data\n", transformed_data)
        return transformed_data

    except HTTPException as http_err:
        # Return structured error responses for HTTP exceptions
        raise http_err

    except Exception as error:
        # Handle unexpected errors
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve findings for ens_id: {str(error)}"
        )
