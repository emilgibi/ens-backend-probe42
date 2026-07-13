from fastapi import  HTTPException, requests, status
from app.core.config import get_settings
from app.core.tprp.tprp import trigger_analysis, trigger_supplier_validation
from app.core.utils.db_utils import *
from app.core.utils.redis_client import rdb, SESSION_SET_KEY
from typing import List, TypedDict
from app.models import *
from app.schemas.logger import logger


class GroupDecision(TypedDict):
    group_id: str
    mapping_type: str            # "NEW_SESSION" | "SKIPPED" | "RETRY"
    ens_ids_to_run: Optional[List[str]]
    retry_count: int
async def decide_group_run(
    session: AsyncSession,
    group_id: str,
    stale_minutes: int = 30,
) -> GroupDecision:
    """
    ORM/Core version of the scheduling decision:
      - Looks at the latest run for the group
      - If all sessions are terminal (COMPLETED/FAILED) and at least one is COMPLETED
        => NEW_SESSION with full ENS set
      - If any session IN_PROGRESS and there was fresh ENS activity => SKIPPED
      - Else => RETRY with FAILED/NOT_STARTED/NULL/STALE(IN_PROGRESS|STARTED) ENS only
    """

    md  = Base.metadata
    sgm = md.tables["session_group_mapping"]
    sss = md.tables["session_screening_status"]
    eg  = md.tables["ens_schedule_group_mapping"]
    ess = md.tables["ensid_screening_status"]

    # 1) Latest group_run (by create_time, id)
    last_run = (
        select(
            sgm.c.group_id,
            sgm.c.source_id.label("group_run_id"),
        )
        .where(sgm.c.group_id == group_id)
        .order_by(sgm.c.create_time.desc(), sgm.c.id.desc())
        .limit(1)
        .cte("last_run")
    )

    # 2) All sessions in that latest run
    run_sessions = (
        select(sgm.c.session_id)
        .join(
            last_run,
            and_(
                last_run.c.group_id == sgm.c.group_id,
                last_run.c.group_run_id == sgm.c.source_id,
            ),
        )
        .cte("run_sessions")
    )

    # 3) Latest status per session (run-level)  DISTINCT ON (session_id)
    latest_per_session = (
        select(
            sss.c.session_id,
            sss.c.overall_status,
            sss.c.update_time,
        )
        .where(sss.c.session_id.in_(select(run_sessions.c.session_id)))
        .order_by(sss.c.session_id, sss.c.update_time.desc(), sss.c.id.desc())
        .distinct(sss.c.session_id)  # PostgreSQL DISTINCT ON
        .cte("latest_per_session")
    )
    lps = latest_per_session

    # 4) Session aggregates (updated names & logic)
    rs_lps = run_sessions.outerjoin(lps, lps.c.session_id == run_sessions.c.session_id)
    agg = (
        select(
            func.count().label("total_sessions"),
            # terminal = COMPLETED or FAILED
            func.count().filter(
                lps.c.overall_status.in_([STATUS.COMPLETED.value, STATUS.FAILED.value])
            ).label("terminal_sessions"),
            # only COMPLETED
            func.count().filter(
                lps.c.overall_status == STATUS.COMPLETED.value
            ).label("completed_only_sessions"),
            # only IN_PROGRESS
            func.count().filter(
                lps.c.overall_status == STATUS.IN_PROGRESS.value
            ).label("inprog_sessions"),
        )
        .select_from(rs_lps)
        .cte("agg")
    )
    a = agg

    # 5) This group's ENS universe
    group_ens = (
        select(eg.c.ens_id)
        .where(eg.c.group_id == group_id)
        .cte("group_ens")
    )
    ge = group_ens

    # 6) Any ENS updated within the last N minutes inside the latest run?
    stale_iv = func.make_interval(0, 0, 0, 0, 0, stale_minutes, 0)  # minutes slot
    ess_join = ge.join(
        ess,
        and_(
            ess.c.ens_id == ge.c.ens_id,
            ess.c.session_id.in_(select(run_sessions.c.session_id)),
        ),
    )
    recent_touch_subq = (
        select(1)
        .select_from(ess_join)
        .where(ess.c.update_time >= func.now() - stale_iv)
        .limit(1)
    )
    ens_recent_touch = select(
        func.exists(recent_touch_subq).label("has_recent_touch")
    ).cte("ens_recent_touch")
    ert = ens_recent_touch

    # 7) Latest per-ENS status within the latest run (NULL if never touched in this run)
    ge_ess_lj = ge.outerjoin(
        ess,
        and_(
            ess.c.ens_id == ge.c.ens_id,
            ess.c.session_id.in_(select(run_sessions.c.session_id)),
        ),
    )
    ens_latest = (
        select(
            ge.c.ens_id,
            ess.c.overall_status,
            ess.c.update_time,
        )
        .select_from(ge_ess_lj)
        .order_by(ge.c.ens_id, ess.c.update_time.desc(), ess.c.id.desc())
        .distinct(ge.c.ens_id)  # DISTINCT ON (ens_id)
        .cte("ens_latest")
    )
    el = ens_latest

    # 8) Case 3 retry subset
    retry_ens = (
        select(el.c.ens_id)
        .where(
            or_(
                el.c.overall_status == STATUS.FAILED.value,
                el.c.overall_status == STATUS.NOT_STARTED.value,
                el.c.overall_status.is_(None),
                and_(
                    el.c.overall_status.in_([STATUS.IN_PROGRESS.value, STATUS.STARTED.value]),
                    or_(
                        el.c.update_time.is_(None),
                        el.c.update_time < func.now() - stale_iv,
                    ),
                ),
            )
        )
        .cte("retry_ens")
    )
    re_ = retry_ens

    # 9) Aggregate retry subset
    agg_retry = (
        select(
            func.array_agg(aggregate_order_by(re_.c.ens_id, re_.c.ens_id)).label("retry_ens_ids"),
            func.count().label("retry_count"),
        )
        .select_from(re_)
        .cte("agg_retry")
    )
    ar = agg_retry

    # Scalar subqueries for CASE branches
    full_ens_array = (
        select(func.array_agg(aggregate_order_by(ge.c.ens_id, ge.c.ens_id)))
        .select_from(ge)
        .scalar_subquery()
    )
    retry_ids_array = select(ar.c.retry_ens_ids).scalar_subquery()
    retry_count_sq = select(ar.c.retry_count).scalar_subquery()

    # Case-1 uses terminal_sessions + completed_only_sessions
    all_completed = and_(
        a.c.total_sessions > 0,
        a.c.terminal_sessions == a.c.total_sessions,
        a.c.completed_only_sessions > 0,
    )
    inprog_fresh = and_(a.c.inprog_sessions > 0, ert.c.has_recent_touch)

    mapping_type = case(
        (all_completed, "NEW_SESSION"),
        (inprog_fresh, "SKIPPED"),
        else_="RETRY",
    ).label("mapping_type")

    ens_ids_to_run = case(
        (all_completed, full_ens_array),
        (inprog_fresh, literal(None)),
        else_=retry_ids_array,
    ).label("ens_ids_to_run")

    retry_count_expr = case(
        (all_completed, 0),
        (inprog_fresh, 0),
        else_=retry_count_sq,
    ).label("retry_count")

    final_q = select(
        literal(group_id).label("group_id"),
        mapping_type,
        ens_ids_to_run,
        retry_count_expr,
    )

    res = await session.execute(final_q)
    row = res.first()
    if not row:
        return GroupDecision(group_id=group_id, mapping_type="RETRY", ens_ids_to_run=[], retry_count=0)

    ens_ids: Optional[List[str]]
    if row.ens_ids_to_run is None:
        ens_ids = None  # SKIPPED → None
    else:
        ens_ids = list(row.ens_ids_to_run) if row.ens_ids_to_run else []

    return GroupDecision(
        group_id=row.group_id,
        mapping_type=row.mapping_type,
        ens_ids_to_run=ens_ids,
        retry_count=int(row.retry_count or 0),
    )

async def run_periodic_schedule(session):
    try:
        logger.info(f" Initiating get active group info ")
        get_active_group_info_res = await get_active_group_info(session)
        logger.info(f" Completed get_active_group_info : {get_active_group_info_res}")
        filtered = []
        for g in get_active_group_info_res:
            decision = await decide_group_run(session, g["group_id"])
            # logger.info(f"decision['mapping_type']: {decision["mapping_type"]}")
            if decision["mapping_type"] == "SKIPPED": continue
            
            ens_ids = decision.get("ens_ids_to_run") or []
            g["mapping_type"] = decision["mapping_type"]
            g["ens_ids"] = ens_ids
            g["ens_count"] = len(ens_ids)
            filtered.append(g)

        # keep same list object but drop SKIPPED entries
        get_active_group_info_res[:] = filtered

        logger.info(f" Completed get_active_group_info->decide_group_run : {get_active_group_info_res}")
            
        process_group_sessionids_ = []
        if get_active_group_info_res and len(get_active_group_info_res):
            logger.info(f" Initiating process groups ")
            process_group_response = await process_groups(session, get_active_group_info_res)
            logger.info(f" Completed process groups : {process_group_response}")
            process_group_sessionids_ = process_group_response['data'] if 'data' in process_group_response else []
            logger.info(f"process group sessionids_: {process_group_sessionids_}")

            return { "data": process_group_sessionids_, "message": "Successful", "status": 200}

        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No session id fount to Schedule"
            )

    except ValueError as ve:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid: {str(ve)}"
        )

    except HTTPException as http_err:
        raise http_err  # Re-raise FastAPI HTTP exceptions

    except Exception as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error running periodic schedule: {str(error)}"
        )
    

async def get_all_session_ids_in_queue(queue_name: str) -> list[str]:
    # Mapping queue names to Redis set keys
    queue_map = {
        "analysis_session_queue": SESSION_SET_KEY,
        "validation_session_queue": VALIDATION_SESSION_SET_KEY
    }

    # Validate the queue name
    if queue_name not in queue_map:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid queue name: {queue_name}"
        )

    redis_key = queue_map[queue_name]
    session_ids = rdb.smembers(redis_key)

    # Convert byte strings to normal strings if Redis returns bytes
    session_ids = [sid.decode("utf-8") if isinstance(sid, bytes) else sid for sid in session_ids]

    return session_ids

async def queue_trigger_entity_validation_(session_id, session) -> Dict:
    try:
        session_supplier_data = await get_dynamic_ens_data(
            table_name="upload_supplier_master_data", 
            required_columns=['session_id'], 
            ens_id="", 
            session_id=session_id, 
            session=session
        )

        if not session_supplier_data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No records found for session_id: {session_id}")
        try: 
            # Submit to name_validation_queue
            submit_result = await submit_session_validation(session_id, session)

            return {
                "message": "Upsert completed successfully",
                "celery_task_id": submit_result["task_id"]
            }
        except Exception as error:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Unhandled error: {str(error)}"
            )


    except ValueError as ve:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid input: {str(ve)}"
        )

    except SQLAlchemyError as sa_err:
        logger.error(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}

    except HTTPException:
        raise

    except Exception as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unhandled error: {str(error)}"
        )


async def queue_trigger_analysis_(session_id, session) -> Dict:
    try:
        session_supplier_data = await get_dynamic_ens_data(
            table_name="supplier_master_data", 
            required_columns=['session_id'], 
            ens_id="", 
            session_id=session_id, 
            session=session
        )

        if not session_supplier_data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Validation Review not completed for the session id: {session_id}")
        
        try: 
            # Submit to name_validation_queue
            submit_result = await submit_session_analysis(session_id, session)

            return {
                "message": "Upsert completed successfully",
                "celery_task_id": submit_result["task_id"]
            }
        except Exception as error:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Unhandled error: {str(error)}"
            )


    except ValueError as ve:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid input: {str(ve)}"
        )

    except SQLAlchemyError as sa_err:
        logger.error(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}

    except HTTPException:
        raise

    except Exception as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unhandled error: {str(error)}"
        )


# trigger validation & analysis
 
async def dev_trigger_entity_validation_(session_id, session) -> Dict:
    try:
        session_supplier_data = await get_dynamic_ens_data(
            table_name="upload_supplier_data",
            required_columns=['session_id'], 
            ens_id="", 
            session_id=session_id, 
            session=session
        )

        if not session_supplier_data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No records found for session_id: {session_id}")
        # Take session ID
        logger.info(f"Starting dev_trigger_entity_validation_ for {session_id}")
        
        try:
            # Generate JWT token
            jwt_token = create_jwt_token("application_backend", "development")
        except Exception as e:
            logger.error(f"Error generating JWT token: {str(e)}")
            raise HTTPException(status_code=500, detail="Error generating JWT token:")

        try:
            # Step 1: Make HTTP request to trigger supplier name validation
            response = trigger_supplier_validation(session_id, jwt_token.access_token)
            logger.info(f"Trigger Name Validation Response: {response}")
            
            # Return the exact response received from the service
            return response

        except Exception as e:
            logger.error(f"Error triggering supplier validation: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to trigger supplier name validation.")


    except ValueError as ve:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid input: {str(ve)}"
        )

    except SQLAlchemyError as sa_err:
        logger.error(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}

    except HTTPException:
        raise

    except Exception as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unhandled error: {str(error)}"
        )


async def dev_trigger_analysis_(session_id, session) -> Dict:
    try:
        session_supplier_data = await get_dynamic_ens_data(
            table_name="supplier_master_data", 
            required_columns=['session_id'], 
            ens_id="", 
            session_id=session_id, 
            session=session
        )

        if not session_supplier_data:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Validation Review not completed for the session id: {session_id}")
        
        try:
            # Generate JWT token
            jwt_token = create_jwt_token("application_backend", "development")
        except Exception as e:
            logger.error(f"Error generating JWT token: {str(e)}")
            raise HTTPException(status_code=500, detail="Error generating JWT token:")

        try:
            # Step 1: Make HTTP request to trigger supplier name validation
            response = trigger_analysis(session_id, jwt_token.access_token)
            logger.info(f"Trigger Analysis Response: {response}")
            
            # Return the exact response received from the service
            return response

        except Exception as e:
            logger.error(f"Error triggering supplier validation: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to Trigger Analysis.")



    except ValueError as ve:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Invalid input: {str(ve)}"
        )

    except SQLAlchemyError as sa_err:
        logger.error(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}

    except HTTPException:
        raise

    except Exception as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unhandled error: {str(error)}"
        )


# async def trigger_run_analysis(session_id: str, auth_token: str):
#     """
#     Sends a POST request to trigger supplier validation.

#     :param session_id: The session ID to be sent in the request body.
#     :param auth_token: The Bearer token for authorization.
#     :return: Response JSON or error message.
#     """
#     url = get_settings().urls.analysis_orchestration +"/analysis/trigger-analysis"

#     # Request payload
#     payload = {
#         "session_id": session_id
#     }

#     # Request headers
#     headers = {
#         "accept": "application/json",
#         "Authorization": f"Bearer {auth_token}",
#         "Content-Type": "application/json"
#     }

#     try:
#         # Making the POST request
#         response = requests.post(url, json=payload, headers=headers)
#         logger.info("Run Analysis response", response)
#         # Check response status
#         response.raise_for_status()  # Raise error for bad status codes

#         # Return JSON response
#         return response.json()
#     except requests.exceptions.RequestException as e:
#         return {"error": str(e)}
    