from app.core.utils.db_utils import *
import requests
import httpx
from app.core.config import get_settings
from app.core.security.jwt import create_jwt_token
import asyncio
from app.core.utils.db_utils import *
from urllib.parse import urlencode

from app.models import User

done_name = []
async def run_continuous_monitoring(ens_id, status, session):

    try:
        org_response = 0
        #check if its false
        if status==False:
            logger.debug("status is false")
            column = [{'group_id': 'CM001', 'status': 'PAUSED'}]
            result = await upsert_dynamic_ens_data("ens_continuous_group_mapping", column, ens_id, "CM001",
                                                   session=session)
            logger.debug(result)
            return {"status_code": 200, "success": True, "message": "CM existed, status is paused"}
        column = ['ens_id']
        data = await get_dynamic_ens_data_cm("ens_continuous_group_mapping", required_columns=column, ens_id=None, session_id=None,
                                          session=session)
        logger.debug(data)
        data = data[0]
        if data:
            data= [n['ens_id'] for n in data]
            if ens_id in data:
                column = [{'group_id': 'CM001', 'status': 'ACTIVE'}]
                result = await upsert_dynamic_ens_data("ens_continuous_group_mapping", column, ens_id, "CM001",
                                                       session=session)
                logger.debug(result)
                return {"status_code": 200, "success": True, "message": "CM existed, status is active"}

        #fetch data to set portfolio
        column = ['name', 'city', 'country', 'management']
        retrieved_data = await get_dynamic_ens_data_cm("entity_universe",required_columns=column, ens_id=ens_id,session_id=None, session=session)
        retrieved_data = retrieved_data[0][0]
        name = retrieved_data.get('name','')
        city = retrieved_data.get('city','')
        country = retrieved_data.get('country','')
        management = retrieved_data.get('management',[])
        logger.debug(f"details: {name},{city},{country}")
        try:
            # Generate JWT token
            jwt_token = create_jwt_token("application_backend", "development")
        except Exception as e:
            logger.error(f"Error generating JWT token: {e}")
            raise
        orbis_url = get_settings().urls.orbis_engine

        headers = {
            "Authorization": f"Bearer {jwt_token.access_token}",
            "Content-Type": "application/json"
        }
        logger.debug("check 1")
        async with httpx.AsyncClient() as client:
            params = {
                "primaryId": ens_id,
                "name": name,
                "city": city,
                "country": country
            }
            query_string = urlencode(params)
            url = f"{orbis_url}/api/v1/orbis/grid/portfolio/companies?{query_string}"
            max_retries = 3
            attempt = 0
            response = None
            while attempt <= max_retries:
                response = await client.get(url,timeout=20.0)
                logger.info(f"Org Attempt {attempt + 1} — Response status: {response.status_code}")
                if response.status_code == 200 or response.status_code == 201:
                    # if response.status_code == 200:
                    #     column = [{'group_id': 'CM001', 'status': 'ACTIVE'}]
                    #     result = await upsert_dynamic_ens_data("ens_continuous_group_mapping", column, ens_id, "CM001",
                    #                                            session=session)
                    #     logger.debug(result)
                    break
                attempt += 1
                await asyncio.sleep(1)

            if response is None or (response.status_code != 200 and response.status_code !=201):
                logger.error("Org - Failed to get a successful response after retries.")
                return {"status_code": 500, "success": False, "message": "Failed while setting CM for Organization"}
            org_response = response.status_code
        if isinstance(management, list):
            if len(management) > 0:
                column = ['primary_id', 'category']
                #get all the portfolio set contact id
                data=await get_dynamic_ens_data_cm("grid_pm_tracking",required_columns=column,ens_id=None,session_id=None,session=session)
                if not data:
                    all_primary_id = []
                else:
                    data = data[0] if isinstance(data, list) and len(data) > 0 else []
                    all_primary_id = [item["primary_id"] for item in data if item.get("category") == "management"]
                async with httpx.AsyncClient() as client:
                    tasks = []
                    for contact in management:
                        tasks.append(process(contact, client, orbis_url, headers,city,country,all_primary_id,ens_id,session))
                    results = await asyncio.gather(*tasks)
                    logger.debug(results)
                    if 500 in results:
                        return {"status_code": 500, "success": False, "message": "All management not set for CM"}
                    logger.info("---CM: Mng portfolio set---")
            else:
                logger.info("No management found")
        else:
            logger.info("No mangement")

        if org_response == 200:
            column = [{'group_id': 'CM001', 'status': 'ACTIVE'}]
            result = await upsert_dynamic_ens_data("ens_continuous_group_mapping", column, ens_id, "CM001",
                                                   session=session)
            logger.debug(result)
        logger.info("---CM: Org portfolio set---")
        logger.info("---CM: Org and Mng portfolio set---")
        return {"status_code": 200, "success": True, "message": "CM set for Organization and Management"}



    except Exception as e:
        logger.error(str(e))
        return {"status_code": 500, "success": False, "message": "Entered exception block", "error": str(e)}


async def process(contact, client, orbis_url, headers, city, country,all_primary_id,ens_id,session):
    semaphore = asyncio.Semaphore(2)
    async with semaphore:
        contact_id = contact.get("id")
        personnel_name = contact.get("name")
        indicators = [contact.get("pep_indicator",""), contact.get("media_indicator",""), contact.get("sanctions_indicator",""), contact.get("watchlist_indicator","")]

        if contact_id in all_primary_id:
            logger.info("tracking ID already exist")
            result=await add_ens_id_to_contact("management_association",contact_id,ens_id,session)
            logger.debug(result)
            return 200
        else:
            if ("Yes" in indicators) and (personnel_name not in done_name):
                try:
                    logger.debug("ran")
                    done_name.append(personnel_name)
                    url = f"{orbis_url}/api/v1/orbis/grid/portfolio/personnels?primaryId={contact_id}&name={personnel_name}&city={city}&country={country}"
                    max_retries = 3
                    attempt = 0
                    response = None
                    while attempt <= max_retries:
                        response = await client.get(url, timeout=50.0)
                        logger.info(f"Mng Org Attempt {attempt + 1} — Response status: {response.status_code}")
                        if response.status_code == 200 or response.status_code==201:
                            if response.status_code == 200:
                                result = await add_ens_id_and_fetch_contact("management_association", contact_id, ens_id)
                                logger.debug(result)
                            break
                        attempt += 1
                        await asyncio.sleep(1)
                    if response is None or (response.status_code != 200 and response.status_code != 201):
                        logger.error("Mng - Failed to get a successful response after retries.")
                        return 500

                    return response.status_code
                except Exception as e:
                    import traceback
                    logger.error(traceback.format_exc())
                    logger.error(f"Error - Request failed for GRID search by Name - {personnel_name}: {str(e)}")
                    return 500
            else:
                logger.debug("No indicator")
                return 200

async def process_webhook_logic(request, session: AsyncSession) -> dict:

    response_data = await get_webhook_response_data(request.response_id, session)
    if not response_data:
        raise HTTPException(
            status_code=204,
            detail=f"No webhook response found for response_id: {request.response_id}"
        )

    # Extract tracking ID
    tracking_id = extract_tracking_id(response_data)
    if not tracking_id:
        raise HTTPException(
            status_code=204,
            detail="Tracking ID not found in webhook response"
        )

    logger.info(f"Processing webhook response_id: {request.response_id}, tracking_id: {tracking_id}")

    # Map to ENS IDs
    ens_ids = await get_ens_ids_from_tracking(tracking_id, session)
    if not ens_ids:
        raise HTTPException(
            status_code=204,
            detail=f"ENS IDs not found for tracking_id: {tracking_id}"
        )

    active_ens_ids = []
    for ens_id in ens_ids:
        ens_data, _ = await get_dynamic_ens_data(
            table_name="ens_continuous_group_mapping",
            required_columns=["update_time", "id", "ens_id", "status"],
            ens_id=ens_id,
            session=session
        )
        if not ens_data:
            continue

        status = ens_data[0].get("status")
        if status != "PAUSED":
            active_ens_ids.append(ens_id)

    if not active_ens_ids:
        raise HTTPException(
            status_code=204, detail=f"All ENS IDs are in PAUSED state for tracking_id: {tracking_id}"
        )

    # Generate session ID
    session_id = str(uuid.uuid4())

    # Update DB
    await update_webhook_response(
        response_id=request.response_id,
        ens_ids=ens_ids,
        session_id=session_id,
        session=session
    )

    # Create session
    await create_session_from_ens_ids_with_session(
        ens_ids=ens_ids,
        session_id=session_id,
        source=request.source,
        source_id=ens_ids[0],
        session=session
    )

    return {
        "session_id": session_id,
        "tracking_id": tracking_id,
        "ens_ids": ens_ids,
    }