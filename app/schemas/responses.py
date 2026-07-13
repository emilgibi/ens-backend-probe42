from datetime import datetime
from typing import Dict, Optional
from pydantic import BaseModel, ConfigDict, EmailStr, Field


class BaseResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)


class AccessTokenResponse(BaseResponse):
    token_type: str = "Bearer"
    access_token: str
    expires_at: int
    refresh_token: str
    refresh_token_expires_at: int


class UserResponse(BaseResponse):
    user_group: str
    user_id: str

class ResponseMessage(BaseModel):
    status: str
    data: Dict # data is now a dictionary
    message: str

class APIKeyResponse(BaseModel):
    api_key: str
    expires_at: Optional[datetime]
    is_active: bool = True

class ENSProcessingResponse(BaseModel):
    session_id: str
    rows_inserted: int
    session_screening_status: str
    ens_ids_processed: int

class SessionCreationResponse(BaseModel):
    session_id: str
    rows_inserted: int
    session_screening_status: str
    ens_ids_processed: int

class WebhookProcessingResponse(BaseModel):
    status: bool
    message: str
    session_id: Optional[str] = None
    tracking_id: Optional[str] = None
    ens_ids_processed: Optional[int] = None

class ErrorResponseTooManyRequests(BaseModel):
    detail: str = Field(..., example="You have reached your limit for reports generated for this entity in one day. Please try again later or contact an administrator for support.")

class ErrorResponseAlreadyQueued(BaseModel):
        detail: str = Field(..., example="This entity is already queued for screening. Please try again later.")