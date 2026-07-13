from math import ceil
import os
import requests
import uuid
from typing import Dict
from fastapi import Depends, logger, HTTPException, status
from neo4j import AsyncGraphDatabase
from sqlalchemy import Integer, and_, bindparam, cast, func, literal, or_, text, tuple_,  update, not_, case
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.future import select
from app.core.utils.redis_client import VALIDATION_SESSION_SET_KEY, rdb, SESSION_SET_KEY

from app.core.config import get_settings
from app.core.security.jwt import create_jwt_token
from app.models import STATUS, Base, FinalStatus, FinalValidatedStatus, OribisMatchStatus, GroupIntervals, PeriodicSessionType
from app.api import deps
from sqlalchemy.dialects.postgresql import insert, aggregate_order_by
from sqlalchemy.orm import aliased
from datetime import timedelta

from app.schemas.logger import logger
from app.core.utils.redis_client import rdb, SESSION_SET_KEY
from typing import List
from celery import current_app
import redis
from fastapi import Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, select, update
from typing import   Any
import json
from app.schemas.responses import *
from app.schemas.requests import *
from app.core.database_session import _ASYNC_ENGINE, SessionFactory

async def get_dynamic_ens_data(
        table_name: str,
        required_columns: list,
        ens_id: str = "",
        session_id: str = "",
        session=None,
        **kwargs
):
    try:
        extra_filters = kwargs.get('extra_filters', {})

        # Validate if table exists
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Table '{table_name}' does not exist in the database schema."
            )

        # Prepare columns to select
        columns_to_select = [getattr(table_class.c, column) for column in required_columns]
        query = select(*columns_to_select)

        # Apply filters
        if ens_id:
            query = query.where(table_class.c.ens_id == str(ens_id)).distinct()
        if session_id:
            query = query.where(table_class.c.session_id == str(session_id))
        query = query.order_by(table_class.c.update_time.desc(), table_class.c.id.desc())
        # Execute query to check if session_id or ens_id exists
        exists_query = select(func.count()).select_from(table_class)

        if ens_id:
            exists_query = exists_query.where(table_class.c.ens_id == str(ens_id))
        if session_id:
            exists_query = exists_query.where(table_class.c.session_id == str(session_id))

        exists_result = await session.execute(exists_query)
        record_count = exists_result.scalar()

        if record_count == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No data found for the given session_id or ens_id."
            )

        # Apply validation status filter
        if extra_filters:
            final_validation_status = extra_filters.get("final_validation_status", "").strip().lower()
            if final_validation_status:
                if final_validation_status == 'review':
                    query = query.where(table_class.c.final_validation_status == FinalValidatedStatus.REVIEW)
                elif final_validation_status == 'auto_reject':
                    query = query.where(table_class.c.final_validation_status == FinalValidatedStatus.AUTO_REJECT)
                elif final_validation_status == 'auto_accept':
                    query = query.where(table_class.c.final_validation_status == FinalValidatedStatus.AUTO_ACCEPT)
                                
            # add additional filter[optional] where screening_ana_status != 'NOT_STARTED'
            screening_analysis_status = extra_filters.get("screening_analysis_status", "").strip().lower()
            if screening_analysis_status:
                if screening_analysis_status == 'active':
                    query = query.where(table_class.c.screening_analysis_status != STATUS.NOT_STARTED)
                elif screening_analysis_status == 'not_started':
                    query = query.where(table_class.c.screening_analysis_status == STATUS.NOT_STARTED)

            # Validate pagination inputs
            offset = extra_filters.get("offset", 0)
            limit = extra_filters.get("limit", 10000)

            if not isinstance(offset, int) or offset < 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="'offset' must be a non-negative integer."
                )
            if not isinstance(limit, int) or limit <= 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="'limit' must be a positive integer."
                )

            # Count total rows before pagination
            total_count_query = select(func.count()).select_from(table_class)
            if query._where_criteria:
                total_count_query = total_count_query.filter(*query._where_criteria)

            total_count_result = await session.execute(total_count_query)
            total_count = total_count_result.scalar()

            # Apply offset and limit
            query = query.offset(offset).limit(limit)

            # print("_______query____", query, "\n offset", offset, "\n limit", limit)
        # Execute query
        result = await session.execute(query)
        columns = result.keys()
        rows = result.all()

        formatted_res = [dict(zip(columns, row)) for row in rows]
        try:
            total_count
        except:
            total_count = len(formatted_res)

        logger.debug(f"formatted_res______ {formatted_res}")
        return formatted_res, total_count

    except HTTPException as http_err:
        raise http_err  # Pass FastAPI exceptions as they are

    except SQLAlchemyError as sa_err:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(sa_err)}"
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {str(e)}"
        )

async def update_dynamic_ens_data(
    table_name: str,
    kpi_data: dict,
    ens_id: str,
    session: AsyncSession = Depends(deps.get_session)
):
    """
    Update the specified table dynamically with the provided kpi_data based on ens_id.

    :param session: AsyncSession = Depends(deps.get_session) - The database session.
    :param table_name: str - The name of the table to update.
    :param kpi_data: dict - The dictionary of KPI data to update.
    :param ens_id: str - The ID to filter the record that needs to be updated.
    :return: dict - The result of the update operation.
    """
    try:
        # Get the table class dynamically
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise ValueError(f"Table '{table_name}' does not exist in the database schema.")
        
        # Prepare the update values
        update_values = {key: value for key, value in kpi_data.items() if value is not None}
        
        # Build the update query
        query = update(table_class).where(table_class.c.ens_id == str(ens_id)).values(update_values)
        
        # Execute the query
        result = await session.execute(query)
        
        # Commit the transaction
        await session.commit()
        
        # Return success response
        return {"status": "success", "message": "Data updated successfully."}

    except ValueError as ve:
        # Handle the case where the table does not exist
        logger.error(f"Error: {ve}")
        return {"error": str(ve), "status": "failure"}
    
    except SQLAlchemyError as sa_err:
        # Handle SQLAlchemy-specific errors
        logger.error(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}
    
    except Exception as e:
        # Catch any other exceptions
        logger.error(f"An unexpected error occurred: {e}")
        return {"error": "An unexpected error occurred", "status": "failure"}

async def insert_dynamic_ens_data(
    table_name: str,
    kpi_data: list,
    ens_id: str,
    session_id: str,
    session: AsyncSession = Depends(deps.get_session)
):
    try:
        # Get the table class dynamically
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise ValueError(f"Table '{table_name}' does not exist in the database schema.")
        
        # Add `ens_id` and `session_id` to each row in `kpi_data`
        rows_to_insert = [
            {**row, "ens_id": ens_id, "session_id": session_id}
            for row in kpi_data
        ]
        
        # Build the insert query
        query = insert(table_class).values(rows_to_insert)
        
        # Execute the query
        await session.execute(query)
        
        # Commit the transaction
        await session.commit()
        
        # Return success response
        return {"status": "success", "message": f"Inserted {len(rows_to_insert)} rows successfully."}

    except ValueError as ve:
        # Handle the case where the table does not exist
        logger.error(f"Error: {ve}")
        return {"error": str(ve), "status": "failure"}
    
    except SQLAlchemyError as sa_err:
        # Handle SQLAlchemy-specific errors
        logger.error(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}
    
    except Exception as e:
        # Catch any other exceptions
        logger.error(f"An unexpected error occurred: {e}")
        return {"error": "An unexpected error occurred", "status": "failure"}
    
async def insert_dynamic_data(
    table_name: str,
    data: list,
    session: AsyncSession = Depends(deps.get_session)
):
    """
    Insert data dynamically into the specified table without additional constraints.
    
    Args:
        table_name (str): Name of the table where data will be inserted.
        kpi_data (list): List of dictionaries containing the data to insert.
        session (AsyncSession): Async database session.
    
    Returns:
        dict: A dictionary with the status and message of the operation.
    """
    try:
        # Get the table class dynamically from metadata
        session = SessionFactory()
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise ValueError(f"Table '{table_name}' does not exist in the database schema.")
        # Get valid column names from the table schema
        valid_columns = set(table_class.columns.keys())

        # Filter data: Keep only valid columns (ignore any extra columns)
        cleaned_data = [
            {key: value for key, value in row.items() if key in valid_columns}
            for row in data
        ]

        # If no valid data remains after filtering, return an error
        if not cleaned_data:
            return {"status": "failure", "message": "No valid data left after filtering extra columns."}

        # Insert filtered data into the table
        query = insert(table_class).values(cleaned_data)
        logger.debug(f"query:  {query}")
        # Execute the insert query
        result = await session.execute(query)  # `result` stores the execution details
        logger.debug(f"rowcount:  {result.rowcount}")
        # Commit the transaction
        await session.commit()

        # Get the number of rows inserted
        rows_inserted = result.rowcount
        logger.info(f"{rows_inserted} row(s) were inserted into the {table_name} table.")
        
        # Return success response
        return {"status": "success", "message": f"Inserted {rows_inserted} rows successfully.", "rows_inserted": rows_inserted}

    except ValueError as ve:
        # Handle cases where the table does not exist
        logger.error(f"Error: {ve}")
        return {"error": str(ve), "status": "failure"}
    
    except SQLAlchemyError as sa_err:
        # Handle SQLAlchemy-specific errors
        logger.error(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}
    
    except Exception as e:
        # Catch any unexpected errors
        logger.error(f"An unexpected error occurred: {e}")
        return {"error": "An unexpected error occurred", "status": "failure"}
    
async def upsert_session_screening_status(
    columns_data: list,
    session_id: str,
    session: AsyncSession = Depends(deps.get_session)
):
    try:
        # Get the table class dynamically
        table_class = Base.metadata.tables.get("session_screening_status")
        if table_class is None:
            raise ValueError(f"Table 'session_screening_status' does not exist in the database schema.")

        # Deduplicate the rows based on session_id
        unique_records = {}
        for record in columns_data:
            record["session_id"] = session_id
            # Use session_id as the key to deduplicate rows
            unique_records[record["session_id"]] = record

        # Convert the dictionary back to a list
        deduplicated_columns_data = list(unique_records.values())

        # Extract column names dynamically
        columns = list(deduplicated_columns_data[0].keys())

        # Prepare bulk insert statement using PostgreSQL ON CONFLICT
        stmt = insert(table_class).values(deduplicated_columns_data)

        # Modify ON CONFLICT to use session_id and update the non-unique fields
        stmt = stmt.on_conflict_do_update(
            index_elements=["session_id"],  # Index on session_id, no unique constraint
            set_={col: stmt.excluded[col] for col in columns if col != "session_id"}
        ).returning(table_class)

        # Execute bulk upsert
        result = await session.execute(stmt)
        await session.commit()

        # Fetch the inserted/updated rows
        return {"message": "Upsert completed", "data": result.fetchall()}

    except ValueError as ve:
        # Handle the case where the table does not exist
        logger.error(f"Error: {ve}")
        return {"error": str(ve), "status": "failure"}
    
    except SQLAlchemyError as sa_err:
        # Handle SQLAlchemy-specific errors
        logger.error(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}
    
    except Exception as e:
        # Catch any other exceptions
        logger.error(f"An unexpected error occurred: {e}")
        return {"error": "An unexpected error occurred", "status": "failure"}
async def update_supplier_master_data(session, session_id) -> Dict:
    try:
        # Fetch table metadata dynamically
        upload_supplier_master_table = Base.metadata.tables.get("upload_supplier_data")
        supplier_master_table = Base.metadata.tables.get("supplier_master_data")

        if upload_supplier_master_table is None or supplier_master_table is None:
            raise HTTPException(
                status_code=404,
                detail="Table 'upload_supplier_data' or 'supplier_master_data' does not exist in the database schema."
            )

        required_columns = [
            "name", "cin_id", "bid", "ens_id", "identifier", "identifier_type", "entity_type",
            "session_id", "validation_status", "final_status", "uploaded_name", "uploaded_external_vendor_id", "uploaded_identifier",
            "uploaded_identifier_type", "uploaded_entity_type", "uploaded_address", "uploaded_client_onboarding_date", "uploaded_client_msme_status", "uploaded_client_z_altman_type"
        ]
        columns_to_select = [
            getattr(upload_supplier_master_table.c, column) for column in required_columns
        ]

        print("columns_to_select_______", columns_to_select)
        query = select(*columns_to_select).where(
            and_(
                or_(
                    upload_supplier_master_table.c.final_status == FinalStatus.ACCEPTED
                ),
                upload_supplier_master_table.c.session_id == session_id,
                upload_supplier_master_table.c.identifier.isnot(None)  # Ensure bvd_id is not NULL
            )
        )

        result = await session.execute(query)
        columns = result.keys()

        # Fetch all rows from the result
        rows = result.fetchall()

        # If no valid records are found, return success response and exit early
        if not rows:
            return {
                "status": "success",
                "message": f"No valid records found for session_id: {session_id}. No updates were performed."
            }

        # Prepare rows for insertion
        RENAME_MAP = {"uploaded_external_vendor_id": "external_vendor_id"}

        rows_to_insert = [
            {RENAME_MAP.get(k, k): v for k, v in zip(columns, row)}
            for row in rows
        ]

        print("rows_to_insert__________", rows_to_insert)
        # Insert or update the rows into the supplier_master_table
        query2 = insert(supplier_master_table).values(rows_to_insert)
        query2 = query2.on_conflict_do_update(
            index_elements=["ens_id", "session_id"],
            set_={col: query2.excluded[col] for col in rows_to_insert[0].keys() if col not in ["ens_id", "session_id"]}
        ).returning(*supplier_master_table.c)
        # Return inserted/updated rows

        result = await session.execute(query2)
        inserted_or_updated_rows = result.fetchall()

        # If no rows were inserted or updated, return a success message instead of an error
        if not inserted_or_updated_rows:
            return {
                "status": "success",
                "message": "No changes were made as no new data was available for insertion or update."
            }

        # Commit the transaction
        await session.commit()

        # Return success response
        return {
            "status": "success",
            "message": f"Inserted or updated {len(inserted_or_updated_rows)} rows successfully."
        }

    except HTTPException as http_err:
        raise http_err  # Re-raise known HTTP errors

    except SQLAlchemyError as db_err:
        # Handle database-specific errors
        raise HTTPException(
            status_code=500,
            detail=f"Database error occurred: {str(db_err)}"
        )

    except Exception as error:
        # Handle unexpected errors
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {str(error)}"
        )

async def validate_user_request(current_user, session: AsyncSession = Depends(deps.get_session)):
    # Get the tables from metadata
    supplier_screening_table = Base.metadata.tables.get("session_screening_status")
    upload_supplier_data = Base.metadata.tables.get("upload_supplier_data")
    user_table = Base.metadata.tables.get("users_table")
    if supplier_screening_table is None or upload_supplier_data is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="One or more required tables do not exist in the database schema."
        )

    # Alias for readability (optional)
    s = aliased(supplier_screening_table)
    u = aliased(upload_supplier_data)
    ut = aliased(user_table)
    # Extract user group & user ID correctly
    user_group, user_id = current_user["user_group"], current_user["user_id"]

    # Build the async query
    one_hour_ago = datetime.utcnow() - timedelta(hours=1)

    query = select(func.count(func.distinct(tuple_(s.c.session_id, s.c.overall_status, ut.c.user_group)))).select_from(
        s.join(u, s.c.session_id == u.c.session_id).join(ut, u.c.user_id == ut.c.user_id)
    ).where(
        s.c.overall_status == STATUS.IN_PROGRESS.value,
        u.c.user_id == user_id,
        ut.c.user_group == user_group, 
        s.c.create_time >= one_hour_ago
    )
        
    result = await session.execute(query)
    logger.debug(f"result.scalar() {result}")
    count = result.scalar_one_or_none()  # Returns None if no rows found
    logger.debug(f"Query Result: {count}")
    return count

async def run_neo4j_query(cypher_query: str) -> dict:
    try:

        URI = os.environ.get("GRAPHDB__URI")
        USER = os.environ.get("GRAPHDB__USER")
        PASSWORD = os.environ.get("GRAPHDB__PASSWORD")

        async with AsyncGraphDatabase.driver(URI, auth=(USER, PASSWORD)) as driver:
            async with driver.session() as session:
                result = await session.run(cypher_query)

                # Try fetching records (for read queries)
                try:
                    records = await result.data()
                    return {
                        "status": "pass",
                        "message": "Query executed successfully.",
                        "records": records
                    }
                except Exception:
                    # For write queries that don't return anything
                    return {
                        "status": "pass",
                        "message": "Query executed successfully. No return values."
                    }

    except Exception as e:
        return {
            "status": "fail",
            "message": f"Error executing query: {str(e)}"
        }

async def default_head_graph(client_id, session):
    """
    Creates a root 'Aramco' node with a unique ID in the Neo4j graph.
    """

    cypher_query = f'''
    CREATE (a:Company {{name: "Aramco", id: "{client_id}"}})
    '''

    # Run the Cypher query
    result = await run_neo4j_query(cypher_query)

    return {
        "status": "pass",
        "message": "Aramco node created successfully.",
        "client_id": client_id,
        "neo4j_result": result,
    }


async def upsert_session_config(client_id_, session_id_, session) -> Dict:
    try:
        # Get the table class dynamically
        table_class = Base.metadata.tables.get("client_configuration")
        if table_class is None:
            raise ValueError(f"Table 'client_configuration' does not exist in the database schema.")

        # Prepare columns to select
        query = select(table_class).where((table_class.c.client_id == str(client_id_)) &
            (table_class.c.module_enabled_status == True))


        # Execute
        result = await session.execute(query)
        columns = result.keys()
        rows = result.all()

        formatted_res = [dict(zip(columns, row)) for row in rows]
        upserted_records = []
        logger.debug(f"formatted_res {formatted_res}")
        if formatted_res and len(formatted_res):
            table_class = Base.metadata.tables.get("session_configuration")
            if table_class is None:
                raise ValueError(f"Table 'session_configuration' does not exist in the database schema.")

            for i in range(len(formatted_res)):
                logger.debug(f"formatted_res[i]['module_enabled_status'] {formatted_res[i]['module_enabled_status']}")
                if formatted_res[i]['module_enabled_status']:
                    new_row = {
                        'client_id': client_id_,
                        'session_id': session_id_,
                        'module' : formatted_res[i]['kpi_theme'],
                        'module_active_status' : bool(formatted_res[i]['module_enabled_status'])
                    }
                    stmt = insert(table_class).values(**new_row)

                    stmt = stmt.on_conflict_do_update(
                        index_elements=[
                            "session_id", "module"
                        ],
                        set_={
                            'module_active_status': stmt.excluded.module_active_status
                        }
                    )
                    
                    logger.debug(f"stmt {stmt}")
                    # Execute
                    await session.execute(stmt)

                    # Add to upserted record
                    upserted_records.append(new_row)

            # Commit once after all upserts
            await session.commit()

        return {"message": "Upsert completed", "data": upserted_records}
    except ValueError as ve:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid file format: {str(ve)}"
        )

    except HTTPException as http_err:
        raise http_err  # Re-raise FastAPI HTTP exceptions

    except SQLAlchemyError as sa_err:
        # Handle SQLAlchemy-specific errors
        logger.error(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}

    except Exception as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing session config: {str(error)}"
        )


async def get_latest_session_for_ens_id(
        table_name: str,
        required_columns: list,
        ens_id: str = "",
        session=None,
):
    try:
        session_id = False

        # Validate if table exists
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Table '{table_name}' does not exist in the database schema."
            )

        # Prepare columns to select
        required_columns += ["update_time", "id"]
        columns_to_select = [getattr(table_class.c, column) for column in required_columns]
        query = select(*columns_to_select)

        # Apply filters
        if ens_id:
            query = query.where(table_class.c.ens_id == str(ens_id) and table_class.c.overall_status == "COMPLETED").distinct()
        if session_id:
            query = query.where(table_class.c.session_id == str(session_id))

        query = query.order_by(table_class.c.update_time.desc(), table_class.c.id.desc())
        query = query.limit(1)


        # Execute query to check if session_id or ens_id exists
        exists_query = select(func.count()).select_from(table_class)

        if ens_id:
            exists_query = exists_query.where(table_class.c.ens_id == str(ens_id) and table_class.c.overall_status == "COMPLETED")
        if session_id:
            exists_query = exists_query.where(table_class.c.session_id == str(session_id))

        exists_result = await session.execute(exists_query)
        record_count = exists_result.scalar()

        if record_count == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No data found for the given session_id or ens_id."
            )

        # Execute query
        result = await session.execute(query)
        columns = result.keys()
        rows = result.all()

        if rows:
            formatted_res = [dict(zip(columns, rows[0]))]  # Get the first (top) row
        else:
            formatted_res = []  # Return empty if no rows are returned

        return formatted_res

    except HTTPException as http_err:
        raise http_err  # Pass FastAPI exceptions as they are

    except SQLAlchemyError as sa_err:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(sa_err)}"
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {str(e)}"
        )

async def get_dynamic_ens_data_for_session(
        table_name: str,
        required_columns: list,
        ens_id: str,
        session_id: str,
        session: AsyncSession = Depends(deps.get_session)
):
    try:
        # session = SessionFactory()
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise ValueError(
                f"Table '{table_name}' does not exist in the database schema."
            )

        if required_columns == ["all"]:
            columns_to_select = [table_class.c[column] for column in table_class.c.keys()]
        else:
            columns_to_select = [getattr(table_class.c, column) for column in required_columns]

        query = select(*columns_to_select)

        if ens_id:
            query = query.where(table_class.c.ens_id == str(ens_id)).distinct()

        if session_id:
            query = query.where(table_class.c.session_id == str(session_id))

        result = await session.execute(query)

        columns = result.keys()
        rows = result.all()

        formatted_res = [
            dict(zip(columns, row)) for row in rows
        ]

        await session.close()
        return formatted_res

    except ValueError as ve:
        print(f"Error: {ve}")
        return []

    except SQLAlchemyError as sa_err:
        print(f"Database error: {sa_err}")
        return []

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []


def extract_tracking_id(response_data: Dict[Any, Any]) -> Optional[str]:

    try:
        grid_inquiry_info = response_data.get("gridAlertInfo", {}).get("gridInquiryRec", {}).get("gridInquiryInfo", {})
        tracking_id = grid_inquiry_info.get("tracking")

        return tracking_id
    except Exception as e:
        logger.error(f"Error extracting tracking ID: {e}")
        return None


async def get_ens_ids_from_tracking(tracking_id: str, session: AsyncSession) -> Optional[List[str]]:
    """
    Map tracking ID to ENS IDs from grid_pm_tracking table
    for organization and management categorie
    """
    try:
        query = text("""
            SELECT primary_id, category 
            FROM grid_pm_tracking 
            WHERE grid_tracking_id = :tracking_id
        """)

        result = await session.execute(query, {"tracking_id": tracking_id})
        row = result.fetchone()

        if not row:
            logger.warning(f"No record found for tracking_id: {tracking_id}")
            return None

        primary_id = row.primary_id
        category = row.category.lower() if row.category else ""

        logger.info(f"Found primary_id: {primary_id}, category: {category} for tracking_id: {tracking_id}")

        if "organization" in category:
            return [primary_id]

        elif "management" in category:
            management_query = text("""
                SELECT associated_ens_id 
                FROM management_association 
                WHERE contact_id = :contact_id
                AND associated_ens_id IS NOT NULL
            """)

            management_result = await session.execute(management_query, {"contact_id": primary_id})
            management_rows = management_result.fetchall()

            if management_rows:
                ens_ids = []
                for row in management_rows:
                    if row.associated_ens_id:
                        associated_ens_id = row.associated_ens_id
                        logger.debug(
                            f"Processing associated_ens_id: {associated_ens_id}, type: {type(associated_ens_id)}")

                        if isinstance(associated_ens_id, str):
                            try:
                                parsed_id = json.loads(associated_ens_id)
                                if isinstance(parsed_id, list):
                                    ens_ids.extend(parsed_id)
                                else:
                                    ens_ids.append(str(parsed_id))
                            except (json.JSONDecodeError, TypeError):
                                # If it's not JSON, treat as regular string
                                ens_ids.append(associated_ens_id)
                        elif isinstance(associated_ens_id, list):
                            # If it's already a list, extend instead of append
                            ens_ids.extend([str(item) for item in associated_ens_id])
                        else:
                            # Convert to string if it's some other type
                            ens_ids.append(str(associated_ens_id))

                logger.info(f"Found {len(ens_ids)} associated ENS IDs for contact_id: {primary_id}: {ens_ids}")
                return ens_ids
            else:
                logger.warning(f"No associated ENS IDs found for contact_id: {primary_id}")
                return None

        else:
            logger.warning(f"Unknown category: {category} for tracking_id: {tracking_id}")
            return None

    except Exception as e:
        logger.error(f"Error getting ENS IDs for tracking_id {tracking_id}: {e}")
        return None


async def get_webhook_response_data(response_id: str, session: AsyncSession) -> Optional[Dict[Any, Any]]:
    """
    Fetch webhook response data from webhook_response table using response_id
    """
    try:
        query = text("""
            SELECT response 
            FROM webhook_response 
            WHERE response_id = :response_id
        """)

        result = await session.execute(query, {"response_id": response_id})
        row = result.fetchone()

        if row and row.response:
            # Parse JSON response
            return json.loads(row.response) if isinstance(row.response, str) else row.response
        return None

    except Exception as e:
        logger.error(f"Error fetching webhook response for response_id {response_id}: {e}")
        return None


async def update_webhook_response(response_id: str, ens_ids: List[str], session_id: str,session: AsyncSession) -> None:
    """
    Update webhook_response table with ENS IDs and session ID
    """
    try:
        # Convert list of ENS IDs to JSON string for storage
        ens_ids_json = json.dumps(ens_ids)

        update_query = text("""
            UPDATE webhook_response 
            SET ens_id = :ens_ids_json, 
                session_id = :session_id, 
                update_time = :update_time
            WHERE response_id = :response_id
        """)

        current_time = datetime.utcnow()

        result = await session.execute(update_query, {
            "ens_ids_json": ens_ids_json,
            "session_id": session_id,
            "update_time": current_time,
            "response_id": response_id
        })

        if result.rowcount == 0:
            raise ValueError(f"No webhook response found with response_id: {response_id}")

        await session.commit()

        logger.info(
            f"Webhook response updated with response_id: {response_id}, ens_ids: {ens_ids}, session_id: {session_id}")

    except Exception as e:
        logger.error(f"Error updating webhook response: {e}")
        await session.rollback()
        raise

async def get_universe_ens_data(
    table_name: str,
    required_columns: list,
    ens_ids: list = None,
    session: AsyncSession = Depends(deps.get_session)
):
    try:
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise ValueError(
                f"Table '{table_name}' does not exist in the database schema."
            )

        if required_columns == ["all"]:
            columns_to_select = [table_class]
        else:
            columns_to_select = [getattr(table_class.c, column) for column in required_columns]

        query = select(*columns_to_select)

        if ens_ids:
            query = query.where(table_class.c.ens_id.in_(ens_ids)).distinct()

        result = await session.execute(query)

        columns = result.keys()
        rows = result.all()

        formatted_res = [dict(zip(columns, row)) for row in rows]

        return formatted_res

    except ValueError as ve:
        logger.error(f"Error: {ve}")
        return []
    except SQLAlchemyError as sa_err:
        logger.error(f"Database error: {sa_err}")
        return []
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return []


async def create_session_from_ens_ids_with_session(
        ens_ids: list,
        session_id: str,
        source: str,
        source_id: str,
        session: AsyncSession
):
    """
    Create a session using provided ENS IDs list along with session_id, source, and source_id.
    """
    try:
        data = await get_universe_ens_data(
            table_name="entity_universe",
            required_columns=[
                "ens_id",
                "id",
                "bvd_id",
                "name",
                "name_international",
                "address",
                "postcode",
                "city",
                "country",
                "phone_or_fax",
                "email_or_website",
                "national_id",
                "state",
                "unmodified_name",
                "external_vendor_id"
            ],
            ens_ids=ens_ids,
            session=session
        )
        logger.debug(f"Universe data fetched: {data}")

        if not data:
            raise ValueError("No data found for the given ENS IDs")

        # Group data by ENS ID and add session_id
        data_by_ens = {}
        for row in data:
            filtered_row = {
                "ens_id": row.get("ens_id"),
                "session_id": session_id,
                "bvd_id": row.get("bvd_id"),
                "name": row.get("name"),
                "name_international": row.get("name_international"),
                "address": row.get("address"),
                "postcode": row.get("postcode"),
                "city": row.get("city"),
                "country": row.get("country"),
                "phone_or_fax": row.get("phone_or_fax"),
                "email_or_website": row.get("email_or_website"),
                "national_id": row.get("national_id"),
                "state": row.get("state"),
                "validation_status": FinalValidatedStatus.VALIDATED.value,
                "report_generation_status": STATUS.NOT_STARTED.value,
                "final_status": FinalStatus.ACCEPTED.value,
                "uploaded_name": row.get("unmodified_name"),
                "external_vendor_id": row.get("external_vendor_id"),
            }

            eid = row.get("ens_id")
            if eid:
                data_by_ens.setdefault(eid, []).append(filtered_row)

        total_inserted = 0
        for ens_id, rows in data_by_ens.items():
            result = await insert_dynamic_ens_data(
                table_name="supplier_master_data",
                kpi_data=rows,
                ens_id=ens_id,
                session_id=session_id,
                session=session
            )
            inserted = result.get("rows_inserted", 0)
            total_inserted += inserted

        screening_status_data = [{
            "overall_status": STATUS.NOT_STARTED,
            "list_upload_status": STATUS.SKIPPED,
            "supplier_name_validation_status": STATUS.SKIPPED,
            "screening_analysis_status": STATUS.NOT_STARTED,
            "source": source,
            "source_id": source_id
        }]
        response = await upsert_session_screening_status(
            screening_status_data,
            session_id,
            session
        )

        if response.get("message") != "Upsert completed":
            raise Exception("Session screening status insertion failed")

        return {
            "session_id": session_id,
            "rows_inserted": total_inserted,
            "session_screening_status": "Updated",
            "ens_ids_processed": len(ens_ids)
        }

    except Exception as e:
        logger.error(f"Error in creating session from ENS IDs with session_id {session_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Session creation failed: {str(e)}"
        )


async def get_ens_data(
        table_name: str,
        required_columns: list,
        ens_id: str = "",
        session=None,
        **kwargs
):
    try:
        extra_filters = kwargs.get('extra_filters', {})

        # Validate if table exists
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Table '{table_name}' does not exist in the database schema."
            )

        required_columns = required_columns + ["update_time", "id"]
        # Prepare columns to select
        columns_to_select = [getattr(table_class.c, column) for column in required_columns]
        query = select(*columns_to_select)

        # Apply filters
        if ens_id:
            query = query.where(table_class.c.ens_id == str(ens_id)).distinct()

        query = query.order_by(table_class.c.update_time.desc(), table_class.c.id.desc())
        # Execute query to check if session_id or ens_id exists
        exists_query = select(func.count()).select_from(table_class)

        if ens_id:
            exists_query = exists_query.where(table_class.c.ens_id == str(ens_id))

        exists_result = await session.execute(exists_query)
        record_count = exists_result.scalar()

        if record_count == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No data found for the given ens_id."
            )

        # Execute query
        result = await session.execute(query)
        columns = result.keys()
        rows = result.all()

        formatted_res = [dict(zip(columns, row)) for row in rows]

        if len(formatted_res) > 1:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Duplicate entries found for this ens_id"
            )

        logger.debug(f"formatted_res______ {formatted_res}")
        return formatted_res

    except HTTPException as http_err:
        raise http_err  # Pass FastAPI exceptions as they are

    except SQLAlchemyError as sa_err:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(sa_err)}"
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {str(e)}"
        )

# async def get_active_group_info(
#         session: AsyncSession = Depends(deps.get_session)
# ):
#     try:
#         # session = SessionFactory()
#         schedule_monitoring = Base.metadata.tables.get("schedule_monitoring")
#         ens_schedule_group_mapping = Base.metadata.tables.get("ens_schedule_group_mapping")
#         if (schedule_monitoring, ens_schedule_group_mapping) is None:
#             raise ValueError(
#                 f"Table 'schedule_monitoring' or 'ens_schedule_group_mapping' does not exist in the database schema."
#             )

        
#         query = text("""
            
        
            # WITH group_with_next_run AS (
            # SELECT
            #     sm.group_id,
            #     sm.group_name,
            #     sm.interval,
            #     sm.frequency,
            #     sm.start_date,
            #     sm.last_scheduled_date,
            #     sm.status,
            #     sm.next_run_date AS stored_next_run_date,
            #     CASE sm.interval
            #         WHEN 'HOUR'    THEN make_interval(hours  => sm.frequency)
            #         WHEN 'DAY'     THEN make_interval(days   => sm.frequency)
            #         WHEN 'WEEK'    THEN make_interval(days   => 7 * sm.frequency)
            #         WHEN 'MONTH'   THEN make_interval(months => sm.frequency)
            #         WHEN 'QUARTER' THEN make_interval(months => 3 * sm.frequency)
            #         WHEN 'YEAR'    THEN make_interval(years  => sm.frequency)
            #     END AS interval_value
            # FROM public.schedule_monitoring sm
            # WHERE sm.status = 'ACTIVE'
            #     AND sm.frequency IS NOT NULL
            #     AND sm.frequency > 0
            # ),

            # calc_next AS (
            # SELECT
            #     gw.*,
            #     CASE
            #     WHEN gw.interval = 'HOUR' THEN
            #         CASE
            #         WHEN gw.last_scheduled_date IS NOT NULL AND gw.last_scheduled_date >= gw.start_date
            #             THEN gw.last_scheduled_date + gw.interval_value
            #         WHEN now() < gw.start_date
            #             THEN gw.start_date
            #         ELSE gw.start_date + make_interval(
            #                 hours => (floor(extract(epoch FROM (now() - gw.start_date))
            #                                 / (3600 * gw.frequency))::int * gw.frequency)
            #             )
            #         END
            #     WHEN gw.interval = 'DAY' THEN
            #         CASE
            #         WHEN gw.last_scheduled_date IS NOT NULL AND gw.last_scheduled_date >= gw.start_date
            #             THEN gw.last_scheduled_date + gw.interval_value
            #         WHEN now() < gw.start_date
            #             THEN gw.start_date
            #         ELSE gw.start_date + make_interval(
            #                 days => (floor(extract(epoch FROM (now() - gw.start_date))
            #                                 / (86400 * gw.frequency))::int * gw.frequency)
            #             )
            #         END
            #     ELSE
            #         CASE
            #         WHEN gw.last_scheduled_date IS NOT NULL AND gw.last_scheduled_date >= gw.start_date
            #             THEN gw.last_scheduled_date + gw.interval_value
            #         ELSE gw.start_date
            #         END
            #     END AS calculated_next_run_date
            # FROM group_with_next_run gw
            # WHERE gw.interval_value IS NOT NULL
            # ),

            # effective_next AS (
            # SELECT
            #     c.*,
            #     CASE
            #     WHEN c.stored_next_run_date IS NOT NULL
            #         AND c.stored_next_run_date >= c.start_date
            #         THEN c.stored_next_run_date
            #     ELSE GREATEST(c.calculated_next_run_date, c.start_date)
            #     END AS eff_next_run_date
            # FROM calc_next c
            # ),

            # ens_grouped AS (
            # SELECT
            #     e.group_id,
            #     e.group_name,
            #     e.interval,
            #     e.frequency,
            #     e.start_date,
            #     e.last_scheduled_date,
            #     e.interval_value,
            #     e.eff_next_run_date AS next_run_date,
            #     array_agg(eg.ens_id ORDER BY eg.ens_id) AS ens_ids,
            #     COUNT(eg.ens_id) AS ens_count
            # FROM effective_next e
            # JOIN public.ens_schedule_group_mapping eg
            #     ON e.group_id = eg.group_id
            # WHERE
            #     (
            #     -- overdue
            #     e.eff_next_run_date <= now()
            #     OR
            #     -- within the current window, using effective next_run_date
            #     (
            #         (e.interval = 'HOUR'
            #         AND e.eff_next_run_date >= date_trunc('hour', now())
            #         AND e.eff_next_run_date <  date_trunc('hour', now()) + (interval '1 hour' * e.frequency))
            #     OR
            #         (e.interval = 'DAY'
            #         AND e.eff_next_run_date >= date_trunc('day', now())
            #         AND e.eff_next_run_date <  date_trunc('day', now()) + (interval '1 day' * e.frequency))
            #     OR
            #         (e.interval NOT IN ('HOUR','DAY')
            #         AND e.eff_next_run_date >= date_trunc('day', now())
            #         AND e.eff_next_run_date <  date_trunc('day', now()) + interval '1 day')
            #     )
            #     )
            # GROUP BY
            #     e.group_id, e.group_name, e.interval, e.frequency, e.start_date,
            #     e.last_scheduled_date, e.interval_value, e.eff_next_run_date
            # )

            # SELECT *
            # FROM ens_grouped
            # ORDER BY group_id;

#             """)

#         result = await session.execute(query)
#         columns = result.keys()
#         rows = result.all()

#         formatted_res = [
#             dict(zip(columns, row)) for row in rows
#         ]

#         await session.close()
#         return formatted_res


#     except ValueError as ve:
#         print(f"Error: {ve}")
#         return []

#     except SQLAlchemyError as sa_err:
#         print(f"Database error: {sa_err}")
#         return []

#     except Exception as e:
#         print(f"An unexpected error occurred: {e}")
#         return []

async def get_active_group_info(
    session: AsyncSession = Depends(deps.get_session),
):
    try:
        sm = Base.metadata.tables["schedule_monitoring"]
        eg = Base.metadata.tables["ens_schedule_group_mapping"]

        interval_value = case(
            (sm.c.interval == GroupIntervals.HOUR,    func.make_interval(0, 0, 0, 0, sm.c.frequency, 0, 0)),
            (sm.c.interval == GroupIntervals.DAY,     func.make_interval(0, 0, 0, sm.c.frequency, 0, 0, 0)),
            (sm.c.interval == GroupIntervals.WEEK,    func.make_interval(0, 0, sm.c.frequency, 0, 0, 0, 0)),
            (sm.c.interval == GroupIntervals.MONTH,   func.make_interval(0, sm.c.frequency, 0, 0, 0, 0, 0)),
            (sm.c.interval == GroupIntervals.QUARTER, func.make_interval(0, sm.c.frequency * 3, 0, 0, 0, 0, 0)),
            (sm.c.interval == GroupIntervals.YEAR,    func.make_interval(sm.c.frequency, 0, 0, 0, 0, 0, 0)),
            else_=None,
        ).label("interval_value")

        # Active groups with stored next_run_date
        group_with_next_run = (
            select(
                sm.c.group_id,
                sm.c.group_name,
                sm.c.interval,
                sm.c.frequency,
                sm.c.start_date,
                sm.c.last_scheduled_date,
                sm.c.status,
                sm.c.next_run_date.label("stored_next_run_date"),
                interval_value,
            )
            .where(
                sm.c.status == STATUS.ACTIVE,
                sm.c.frequency.isnot(None),
                sm.c.frequency > 0,
            )
            .cte("group_with_next_run")
        )
        gw = group_with_next_run
        now_ = func.now()

        # Align-from-start helpers (only used when no valid last_scheduled_date)
        elapsed_sec_hour = func.extract("epoch", now_ - gw.c.start_date)
        periods_hour = cast(func.floor(elapsed_sec_hour / (3600 * gw.c.frequency)), Integer)
        aligned_hours = periods_hour * gw.c.frequency
        aligned_hour_ts = gw.c.start_date + func.make_interval(0, 0, 0, 0, aligned_hours, 0, 0)

        elapsed_sec_day = func.extract("epoch", now_ - gw.c.start_date)
        periods_day = cast(func.floor(elapsed_sec_day / (86400 * gw.c.frequency)), Integer)
        aligned_days = periods_day * gw.c.frequency
        aligned_day_ts = gw.c.start_date + func.make_interval(0, 0, 0, aligned_days, 0, 0, 0)

        # ===== calculated_next_run_date WITH GUARD =====
        calculated_next_run_date = case(
            (gw.c.interval == GroupIntervals.HOUR,
                case(
                    (and_(gw.c.last_scheduled_date.isnot(None), gw.c.last_scheduled_date >= gw.c.start_date),
                        gw.c.last_scheduled_date + gw.c.interval_value),
                    (now_ < gw.c.start_date, gw.c.start_date),
                    else_=aligned_hour_ts,
                )
            ),
            (gw.c.interval == GroupIntervals.DAY,
                case(
                    (and_(gw.c.last_scheduled_date.isnot(None), gw.c.last_scheduled_date >= gw.c.start_date),
                        gw.c.last_scheduled_date + gw.c.interval_value),
                    (now_ < gw.c.start_date, gw.c.start_date),
                    else_=aligned_day_ts,
                )
            ),
            else_=case(
                (and_(gw.c.last_scheduled_date.isnot(None), gw.c.last_scheduled_date >= gw.c.start_date),
                    gw.c.last_scheduled_date + gw.c.interval_value),
                else_=gw.c.start_date,
            ),
        ).label("calculated_next_run_date")

        # ===== effective_next: prefer stored_next_run_date if present & >= start_date =====
        effective_next = (
            select(
                gw.c.group_id,
                gw.c.group_name,
                gw.c.interval,
                gw.c.frequency,
                gw.c.start_date,
                gw.c.last_scheduled_date,
                gw.c.status,
                gw.c.interval_value,
                gw.c.stored_next_run_date,
                calculated_next_run_date,
                case(
                    (
                        and_(
                            gw.c.stored_next_run_date.isnot(None),
                            gw.c.stored_next_run_date >= gw.c.start_date,
                        ),
                        gw.c.stored_next_run_date,
                    ),
                    else_=func.greatest(calculated_next_run_date, gw.c.start_date),
                ).label("eff_next_run_date"),
            )
        ).cte("effective_next")
        e = effective_next

        # Windows (note: hour window is 'frequency' hours)
        hour_start = func.date_trunc("hour", now_)
        hour_end   = hour_start + func.make_interval(0, 0, 0, 0, 1, 0, 0)

        day_start  = func.date_trunc("day", now_)
        day_end    = day_start + func.make_interval(0, 0, 0, e.c.frequency, 0, 0, 0)
        today_end  = day_start + func.make_interval(0, 0, 0, 1, 0, 0, 0)

        ens_ids_agg = func.array_agg(aggregate_order_by(eg.c.ens_id, eg.c.ens_id)).label("ens_ids")

        ens_grouped = (
            select(
                e.c.group_id,
                e.c.group_name,
                e.c.interval,
                e.c.frequency,
                e.c.start_date,
                e.c.last_scheduled_date,
                e.c.interval_value,
                e.c.eff_next_run_date.label("next_run_date"),
                ens_ids_agg,
                func.count(eg.c.ens_id).label("ens_count"),
            )
            .join(eg, e.c.group_id == eg.c.group_id)
            .where(
                or_(
                    e.c.eff_next_run_date <= now_,  # overdue
                    and_(  # HOUR: [this hour, this hour + frequency hours)
                        e.c.interval == GroupIntervals.HOUR,
                        e.c.eff_next_run_date >= hour_start,
                        e.c.eff_next_run_date <  hour_end,
                    ),
                    and_(  # DAY: [today, today + frequency days)
                        e.c.interval == GroupIntervals.DAY,
                        e.c.eff_next_run_date >= day_start,
                        e.c.eff_next_run_date <  day_end,
                    ),
                    and_(  # Others: just today
                        e.c.interval.notin_([GroupIntervals.HOUR, GroupIntervals.DAY]),
                        e.c.eff_next_run_date >= day_start,
                        e.c.eff_next_run_date <  today_end,
                    ),
                )
            )
            .group_by(
                e.c.group_id,
                e.c.group_name,
                e.c.interval,
                e.c.frequency,
                e.c.start_date,
                e.c.last_scheduled_date,
                e.c.interval_value,
                e.c.eff_next_run_date,
            )
            .cte("ens_grouped")
        )

        final_query = select(ens_grouped).order_by(ens_grouped.c.group_id)
        result = await session.execute(final_query)
        return [dict(row) for row in result.mappings().all()]

    except (ValueError, SQLAlchemyError) as e:
        print(f"get_active_group_info error: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []

# async def postprocess_active_groups_option_b(
#         session: AsyncSession = Depends(deps.get_session)
# ):
#     try:
#         # session = SessionFactory()
#         schedule_monitoring = Base.metadata.tables.get("schedule_monitoring")
#         ens_schedule_group_mapping = Base.metadata.tables.get("ens_schedule_group_mapping")
#         if (schedule_monitoring, ens_schedule_group_mapping) is None:
#             raise ValueError(
#                 f"Table 'schedule_monitoring' or 'ens_schedule_group_mapping' does not exist in the database schema."
#             )

        
#         query = text("""
       
                
            # WITH
            # -- 0) Input group
            # input_group AS (
            # SELECT 'f7ef1552-c9bc-4b50-99e4-a146f0e2a4e2'::text AS group_id
            # ),

            # -- 1) Latest group_run (by create_time, id)
            # last_run AS (
            # SELECT sgm.group_id, sgm.source_id AS group_run_id
            # FROM public.session_group_mapping sgm
            # JOIN input_group ig ON ig.group_id = sgm.group_id
            # ORDER BY sgm.create_time DESC, sgm.id DESC
            # LIMIT 1
            # ),

            # -- 2) All sessions in that latest run
            # run_sessions AS (
            # SELECT sgm.session_id
            # FROM public.session_group_mapping sgm
            # JOIN last_run lr
            #     ON lr.group_id     = sgm.group_id
            # AND lr.group_run_id = sgm.source_id
            # ),

            # -- 3) Latest status per session (run-level)
            # latest_per_session AS (
            # SELECT DISTINCT ON (sss.session_id)
            #         sss.session_id,
            #         sss.overall_status,
            #         sss.update_time
            # FROM public.session_screening_status sss
            # WHERE sss.session_id IN (SELECT session_id FROM run_sessions)
            # ORDER BY sss.session_id, sss.update_time DESC, sss.id DESC
            # ),

            # -- 4) Session aggregates
            # agg AS (
            # SELECT
            #     COUNT(*) AS total_sessions,
            #     COUNT(*) FILTER (WHERE lps.overall_status IN ('COMPLETED','FAILED')) AS terminal_sessions,
            #     COUNT(*) FILTER (WHERE lps.overall_status = 'COMPLETED')              AS completed_only_sessions,
            #     COUNT(*) FILTER (WHERE lps.overall_status = 'IN_PROGRESS')            AS inprog_sessions
            # FROM run_sessions rs
            # LEFT JOIN latest_per_session lps ON lps.session_id = rs.session_id
            # ),

            # -- 5) This group's ENS universe
            # group_ens AS (
            # SELECT eg.ens_id
            # FROM public.ens_schedule_group_mapping eg
            # JOIN input_group ig ON ig.group_id = eg.group_id
            # ),

            # -- 6) Any ENS updated within the last 30 minutes inside the latest run?
            # -- (If you want ONLY non-terminal touches, add:
            # --  AND COALESCE(ess.overall_status,'IN_PROGRESS') NOT IN ('COMPLETED','FAILED','SKIPPED'))
            # ens_recent_touch AS (
            # SELECT EXISTS (
            #     SELECT 1
            #     FROM public.ensid_screening_status ess
            #     JOIN group_ens ge ON ge.ens_id = ess.ens_id
            #     WHERE ess.session_id IN (SELECT session_id FROM run_sessions)
            #     AND ess.update_time >= now() - INTERVAL '30 minutes'
            # ) AS has_recent_touch
            # ),

            # -- 7) Latest per-ENS status within the latest run (NULL if never touched in this run)
            # ens_latest AS (
            # SELECT DISTINCT ON (ge.ens_id)
            #         ge.ens_id,
            #         ess.overall_status,
            #         ess.update_time
            # FROM group_ens ge
            # LEFT JOIN public.ensid_screening_status ess
            #     ON ess.ens_id = ge.ens_id
            # AND ess.session_id IN (SELECT session_id FROM run_sessions)
            # ORDER BY ge.ens_id, ess.update_time DESC, ess.id DESC
            # ),

            # -- 8) Case 3 retry subset (FAILED / NOT_STARTED / NULL / stale IN_PROGRESS|STARTED)
            # retry_ens AS (
            # SELECT el.ens_id
            # FROM ens_latest el
            # WHERE el.overall_status = 'FAILED'
            #     OR el.overall_status = 'NOT_STARTED'
            #     OR el.overall_status IS NULL
            #     OR (
            #         el.overall_status IN ('IN_PROGRESS','STARTED')
            #     AND (el.update_time IS NULL OR el.update_time < now() - INTERVAL '30 minutes')
            #     )
            # ),

            # -- 9) Aggregate retry subset
            # agg_retry AS (
            # SELECT
            #     COALESCE(ARRAY_AGG(ens_id ORDER BY ens_id), ARRAY[]::text[]) AS retry_ens_ids,
            #     COUNT(*) AS retry_count
            # FROM retry_ens
            # )

            # -- 10) Final decision (always returns one row)
            # SELECT
            # ig.group_id,
            # CASE
            #     -- Case 1: all sessions terminal AND at least one COMPLETED
            #     WHEN a.total_sessions > 0
            #     AND a.terminal_sessions = a.total_sessions
            #     AND a.completed_only_sessions > 0
            #     THEN 'NEW_SESSION'

            #     -- Case 2: some IN_PROGRESS and there was fresh ENS activity
            #     WHEN a.inprog_sessions > 0 AND ert.has_recent_touch
            #     THEN 'SKIPPED'

            #     -- Case 3: fallback → RETRY with affected ENS
            #     ELSE 'RETRY'
            # END AS mapping_type,

            # CASE
            #     WHEN a.total_sessions > 0
            #     AND a.terminal_sessions = a.total_sessions
            #     AND a.completed_only_sessions > 0
            #     THEN ARRAY(SELECT ens_id FROM group_ens ORDER BY ens_id)      -- NEW_SESSION → full set
            #     WHEN a.inprog_sessions > 0 AND ert.has_recent_touch
            #     THEN NULL                                                      -- SKIPPED → no scheduling
            #     ELSE (SELECT retry_ens_ids FROM agg_retry)                       -- RETRY → affected ENS
            # END AS ens_ids_to_run,

            # CASE
            #     WHEN a.total_sessions > 0
            #     AND a.terminal_sessions = a.total_sessions
            #     AND a.completed_only_sessions > 0
            #     THEN 0
            #     WHEN a.inprog_sessions > 0 AND ert.has_recent_touch
            #     THEN 0
            #     ELSE (SELECT retry_count FROM agg_retry)
            # END AS retry_count

            # FROM input_group ig
            # CROSS JOIN agg a
            # CROSS JOIN ens_recent_touch ert
            # LIMIT 1;



#             """)

#         result = await session.execute(query)
#         columns = result.keys()
#         rows = result.all()

#         formatted_res = [
#             dict(zip(columns, row)) for row in rows
#         ]

#         await session.close()
#         return formatted_res


#     except ValueError as ve:
#         print(f"Error: {ve}")
#         return []

#     except SQLAlchemyError as sa_err:
#         print(f"Database error: {sa_err}")
#         return []

#     except Exception as e:
#         print(f"An unexpected error occurred: {e}")
#         return []

async def process_groups(session, group_info_list):
    table_name = "session_group_mapping"
    BATCH_SIZE = get_settings().allow.periodicbatch
    session_id_list = []
    try:
        from app.core.scheduling.periodic_scheduling import dev_trigger_analysis_
        # Get the table class dynamically
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise ValueError(f"Table '{table_name}' does not exist in the database schema.")

        rows_to_insert = []

        for group in group_info_list:
            # logger.info(f"group['mapping_type']: {group["mapping_type"]}, {type(group["mapping_type"])}")
            group_id = group["group_id"]
            ens_ids = group["ens_ids"]
            group_run_id = str(uuid.uuid4())  # Step 8
            mapping_type = (
                PeriodicSessionType.NEW_SESSION.value
                if group["mapping_type"] == "NEW_SESSION"
                else PeriodicSessionType.RETRY_SESSION.value
                if group["mapping_type"] == "RETRY"
                else PeriodicSessionType.SKIPPED.value
            )
            # logger.info(f"mapping_type: {mapping_type}, {type(mapping_type)}")

            total_batches = ceil(len(ens_ids) / BATCH_SIZE)
            for i in range(total_batches):
                batch_ens_ids = ens_ids[i * BATCH_SIZE : (i + 1) * BATCH_SIZE]
                print(f"batch_ens_ids : {batch_ens_ids}")
                session_id = str(uuid.uuid4())  # Step 9

                # Step 10: Add entry for each session
                row = {
                    "group_id": group_id,
                    "source_id": group_run_id,
                    "session_id": session_id,
                    "mapping_type": mapping_type
                }
                rows_to_insert.append(row)
                session_id_list.append(session_id)

                await create_session_from_ens_ids_with_session(
                    ens_ids=batch_ens_ids,
                    session_id=session_id,
                    source="PD",
                    source_id=group_run_id,
                    session=session
                )

                logger.info(f" Initiating Dev trigger analysis : {session_id}")
                # Step 1: Submit to Celery only if not already queued
                # await submit_session_analysis(session_id, session) 
                await dev_trigger_analysis_(session_id, session)
                logger.info(f" Completed Dev trigger analysis : {session_id}")

        # Build and execute the insert query
        query = insert(table_class).values(rows_to_insert)
        await session.execute(query)
        await session.commit()

        logger.info(f" Inserted {len(rows_to_insert)} session mappings successfully.")
        return {"status": "success", "message": f"Inserted {len(rows_to_insert)} rows successfully.", "data" : session_id_list}

    except ValueError as ve:
        logger.error(f" Error: {ve}")
        return {"error": str(ve), "status": "failure"}

    except SQLAlchemyError as sa_err:
        logger.error(f" Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}

    except Exception as e:
        logger.error(f" Unexpected error: {e}")
        return {"error": "An unexpected error occurred", "status": "failure"}

async def submit_session_analysis(session_id: str, session): 
    try:
        print("check 1")
        already_exists = False
        if rdb.sismember(SESSION_SET_KEY, session_id):
            print("check 1.1")
            already_exists = True
        print("check 2")
        rdb.sadd(SESSION_SET_KEY, session_id)
        result = current_app.send_task(
            "process_analysis_session_queue",
            args=[session_id],
            queue="analysis_session_queue",
            connect_timeout=30,
        )
        print("check 3")
        res = {}
        if result.id:
            print("check 3.1")
        # Step 2: Prepare status update data
            data = [{
                "overall_status": STATUS.QUEUED,
                "screening_analysis_status": STATUS.QUEUED
            }]


            # Step 4: Upsert status into DB
            response = await upsert_session_screening_status(data, session_id, session)
            print("response_________", response)
            if response.get("message") == "Upsert completed":
                res["session_screening_status"] = "Updated"
        return {
            "already_exists": already_exists,
            "task_id": result.id
        }

    except redis.exceptions.ConnectionError as ce:
        logger.error(f"Redis connection failed while submitting session {session_id}: {ce}")
        raise HTTPException(status_code=503, detail="Unable to connect to Redis. Please try again later.")
    
    except Exception as e:
        logger.exception(f"Unexpected error while submitting session {session_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error while submitting session.")


async def submit_session_validation(session_id: str, session): 
    try:
        already_exists = False
        if rdb.sismember(VALIDATION_SESSION_SET_KEY, session_id):
            already_exists = True

        rdb.sadd(VALIDATION_SESSION_SET_KEY, session_id)
        result = current_app.send_task(
            "process_validation_session_queue",
            args=[session_id],
            queue="validation_session_queue"
        )
        res = {}
        if result.id:
        # Step 2: Prepare status update data
            data = [{
                "supplier_name_validation_status": STATUS.QUEUED
            }]


            # Step 4: Upsert status into DB
            response = await upsert_session_screening_status(data, session_id, session)
            print("response_________", response)
            if response.get("message") == "Upsert completed":
                res["session_screening_status"] = "Updated"
        return {
            "already_exists": already_exists,
            "task_id": result.id
        }

    except redis.exceptions.ConnectionError as ce:
        logger.error(f"Redis connection failed while submitting session {session_id}: {ce}")
        raise HTTPException(status_code=503, detail="Unable to connect to Redis. Please try again later.")
    
    except Exception as e:
        logger.exception(f"Unexpected error while submitting session {session_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error while submitting session.")
      
# === SCREENING fallback ===
async def fallback_analysis_trigger_from_db(session):
    try:
        table_class = Base.metadata.tables.get("session_screening_status")
        if table_class is None:
            raise ValueError("Table 'session_screening_status' does not exist.")

        query = select(table_class.c.session_id).where(
            and_(
                table_class.c.screening_analysis_status.in_([STATUS.QUEUED.value, STATUS.IN_PROGRESS.value, STATUS.NOT_STARTED.value]),
                table_class.c.overall_status.in_([STATUS.NOT_STARTED.value, STATUS.IN_PROGRESS.value]),
                table_class.c.screening_analysis_status != STATUS.COMPLETED.value,
                table_class.c.source == 'PD'
            )
        )
        result = await session.execute(query)
        session_ids = [row[0] for row in result.fetchall()]
        logger.info(f"Fallback eligible sessions: {session_ids}")

        requeued = []
        
        for session_id in session_ids:
            submit_result = await submit_session_analysis(session_id, session)
            logger.info("submit_result_", submit_result)
            logger.info(f" Requeued to screening_queue: {session_id}")
            requeued.append(session_id)
            
        return requeued

    except (ValueError, SQLAlchemyError, Exception) as e:
        logger.error(f"Screening fallback error: {e}")
        return []

async def fallback_validation_trigger_from_db(session):
    try:
        table_class = Base.metadata.tables.get("session_screening_status")
        if table_class is None:
            raise ValueError("Table 'session_screening_status' does not exist.")

        query = select(table_class.c.session_id).where(
            and_(
                table_class.c.supplier_name_validation_status.in_([STATUS.QUEUED.value, STATUS.IN_PROGRESS.value, STATUS.NOT_STARTED.value]),
                table_class.c.overall_status.in_([STATUS.IN_PROGRESS.value]),
                table_class.c.screening_analysis_status != STATUS.COMPLETED.value
            )
        )
        result = await session.execute(query)
        session_ids = [row[0] for row in result.fetchall()]
        logger.info(f"Fallback eligible sessions: {session_ids}")

        requeued = []
        
        for session_id in session_ids:
            submit_result = await submit_session_validation(session_id, session)
            logger.info("submit_result_", submit_result)
            logger.info(f" Requeued to validation_queue: {session_id}")
            requeued.append(session_id)
            
        return requeued

    except (ValueError, SQLAlchemyError, Exception) as e:
        logger.error(f"Validation fallback error: {e}")
        return []


async def add_ens_id_to_contact(
    table_name: str,
    contact_id: str,
    ens_id: str,
    session: AsyncSession = Depends(deps.get_session)
):
    try:
        session = SessionFactory()
        table = Base.metadata.tables.get(table_name)
        if table is None:
            raise ValueError(f"Table '{table_name}' does not exist in the database schema.")

        if 'contact_id' not in table.columns or 'associated_ens_id' not in table.columns:
            raise ValueError(f"Required columns not found in '{table_name}'.")

        where_clause = table.c.contact_id == contact_id

        exists_query = select(table.c.contact_id).where(where_clause)
        result = await session.execute(exists_query)
        row_exists = result.scalar() is not None

        if not row_exists:
            return {
                "status": "failure",
                "message": f"No row found with contact_id '{contact_id}'."
            }

        update_stmt = (
            update(table)
            .where(where_clause)
            .values({
                'associated_ens_id': case(
                    (
                        not_(func.array_position(table.c.associated_ens_id, ens_id).isnot(None)),
                        func.array_append(table.c.associated_ens_id, ens_id)
                    ),
                    else_=table.c.associated_ens_id
                )
            })
        )

        await session.execute(update_stmt)
        await session.commit()

        return {
            "status": "success",
            "message": f"ENS ID '{ens_id}' added to contact_id '{contact_id}' if not already present."
        }

    except ValueError as ve:
        logger.error(f"Error: {ve}")
        return {"status": "failure", "error": str(ve)}

    except SQLAlchemyError as sa_err:
        await session.rollback()
        logger.error(f"Database error: {sa_err}")
        return {"status": "failure", "error": f"Database error: {sa_err}"}

    except Exception as e:
        await session.rollback()
        logger.error(f"Unexpected error: {e}")
        return {"status": "failure", "error": f"Unexpected error: {e}"}



async def upsert_dynamic_ens_data(
    table_name: str,
    columns_data: list,
    ens_id: str,
    group_id: str,
    session: AsyncSession = Depends(deps.get_session)
):
    try:
        session = SessionFactory()
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise ValueError(f"Table '{table_name}' does not exist in the database schema.")

        rows_to_insert = [
            {**row, "ens_id": ens_id, "group_id": group_id}
            for row in columns_data
        ]

        query = insert(table_class).values(rows_to_insert).on_conflict_do_update(
            index_elements=["ens_id", "group_id"],
            set_={col: getattr(insert(table_class).excluded, col) for col in rows_to_insert[0].keys() if col not in ["ens_id", "group_id"]}
        )

        await session.execute(query)
        await session.commit()

        return {"status": "success", "message": f"Upserted {len(rows_to_insert)} rows successfully."}

    except ValueError as ve:
        logger.error(f"Error: {ve}")
        return {"error": str(ve), "status": "failure"}

    except SQLAlchemyError as sa_err:
        await session.rollback()
        logger.error(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}

    except Exception as e:
        await session.rollback()
        logger.error(f"An unexpected error occurred: {e}")
        return {"error": f"An unexpected error occurred: {e}", "status": "failure"}


async def add_ens_id_and_fetch_contact(
    table_name: str,
    contact_id: str,
    ens_id: str,
    required_columns: list = ["all"],
):
    session = SessionFactory()
    try:
        table = Base.metadata.tables.get(table_name)
        if table is None:
            raise ValueError(f"Table '{table_name}' does not exist in the database schema.")

        if 'contact_id' not in table.columns or 'associated_ens_id' not in table.columns:
            raise ValueError(f"Required columns not found in '{table_name}'.")

        where_clause = table.c.contact_id == contact_id

        exists_query = select(table.c.contact_id).where(where_clause)
        result = await session.execute(exists_query)
        row_exists = result.scalar() is not None

        if not row_exists:
            insert_stmt = insert(table).values(
                contact_id=contact_id,
                associated_ens_id=[ens_id]
            )
            await session.execute(insert_stmt)
            await session.commit()
        else:
            update_stmt = (
                update(table)
                .where(where_clause)
                .values({
                    'associated_ens_id': case(
                        (
                            not_(func.array_position(table.c.associated_ens_id, ens_id).isnot(None)),
                            func.array_append(table.c.associated_ens_id, ens_id)
                        ),
                        else_=table.c.associated_ens_id
                    )
                })
            )
            await session.execute(update_stmt)
            await session.commit()

        if required_columns == ["all"]:
            columns_to_select = [table.c[column] for column in table.columns.keys()]
        else:
            columns_to_select = []
            for col in required_columns:
                if col in table.columns:
                    columns_to_select.append(table.c[col])
                else:
                    raise ValueError(f"Column '{col}' does not exist in '{table_name}'.")

        select_query = select(*columns_to_select).where(where_clause)
        select_result = await session.execute(select_query)
        rows = select_result.all()
        columns = select_result.keys()
        formatted_result = [dict(zip(columns, row)) for row in rows]

        return {
            "status": "success",
            "message": f"ENS ID '{ens_id}' upserted for contact_id '{contact_id}'.",
            "data": formatted_result
        }

    except ValueError as ve:
        print(f"Error: {ve}")
        return {"status": "failure", "error": str(ve), "data": []}

    except SQLAlchemyError as sa_err:
        await session.rollback()
        print(f"Database error: {sa_err}")
        return {"status": "failure", "error": f"Database error: {sa_err}", "data": []}

    except Exception as e:
        await session.rollback()
        print(f"An unexpected error occurred: {e}")
        return {"status": "failure", "error": f"Unexpected error: {e}", "data": []}

    finally:
        await session.close()


async def get_dynamic_ens_data_cm(
        table_name: str,
        required_columns: list,
        ens_id: str = "",
        session_id: str = "",
        session=None,
        **kwargs
):
    try:
        extra_filters = kwargs.get('extra_filters', {})

        # Validate if table exists
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Table '{table_name}' does not exist in the database schema."
            )

        # Prepare columns to select
        columns_to_select = [getattr(table_class.c, column) for column in required_columns]
        if 'update_time' not in required_columns:
            columns_to_select.append(table_class.c.update_time)
        if 'id' not in required_columns:
            columns_to_select.append(table_class.c.id)
        query = select(*columns_to_select)

        # Apply filters
        if ens_id:
            query = query.where(table_class.c.ens_id == str(ens_id)).distinct()
        if session_id:
            query = query.where(table_class.c.session_id == str(session_id))
        query = query.order_by(table_class.c.update_time.desc(), table_class.c.id.desc())
        # Execute query to check if session_id or ens_id exists
        exists_query = select(func.count()).select_from(table_class)

        if ens_id:
            exists_query = exists_query.where(table_class.c.ens_id == str(ens_id))
        if session_id:
            exists_query = exists_query.where(table_class.c.session_id == str(session_id))

        exists_result = await session.execute(exists_query)
        record_count = exists_result.scalar()

        if record_count == 0:
            return [], 0

        # Apply validation status filter
        if extra_filters:
            final_validation_status = extra_filters.get("final_validation_status", "").strip().lower()
            if final_validation_status:
                if final_validation_status == 'review':
                    query = query.where(table_class.c.final_validation_status == FinalValidatedStatus.REVIEW)
                elif final_validation_status == 'auto_reject':
                    query = query.where(table_class.c.final_validation_status == FinalValidatedStatus.AUTO_REJECT)
                elif final_validation_status == 'auto_accept':
                    query = query.where(table_class.c.final_validation_status == FinalValidatedStatus.AUTO_ACCEPT)

            # add additional filter[optional] where screening_ana_status != 'NOT_STARTED'
            screening_analysis_status = extra_filters.get("screening_analysis_status", "").strip().lower()
            if screening_analysis_status:
                if screening_analysis_status == 'active':
                    query = query.where(table_class.c.screening_analysis_status != STATUS.NOT_STARTED)
                elif screening_analysis_status == 'not_started':
                    query = query.where(table_class.c.screening_analysis_status == STATUS.NOT_STARTED)

            # Validate pagination inputs
            offset = extra_filters.get("offset", 0)
            limit = extra_filters.get("limit", 10000)

            if not isinstance(offset, int) or offset < 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="'offset' must be a non-negative integer."
                )
            if not isinstance(limit, int) or limit <= 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="'limit' must be a positive integer."
                )

            # Count total rows before pagination
            total_count_query = select(func.count()).select_from(table_class)
            if query._where_criteria:
                total_count_query = total_count_query.filter(*query._where_criteria)

            total_count_result = await session.execute(total_count_query)
            total_count = total_count_result.scalar()

            # Apply offset and limit
            query = query.offset(offset).limit(limit)

            # print("_______query____", query, "\n offset", offset, "\n limit", limit)
        # Execute query
        result = await session.execute(query)
        columns = result.keys()
        rows = result.all()

        formatted_res = [dict(zip(columns, row)) for row in rows]
        try:
            total_count
        except:
            total_count = len(formatted_res)

        logger.debug(f"formatted_res______ {formatted_res}")
        return formatted_res, total_count

    except HTTPException as http_err:
        raise http_err  # Pass FastAPI exceptions as they are

    except SQLAlchemyError as sa_err:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(sa_err)}"
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {str(e)}"
        )