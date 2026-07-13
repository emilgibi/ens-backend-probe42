from neo4j import AsyncGraphDatabase, exceptions as neo4j_exceptions
import pycountry
from app.core.utils.db_utils import *
from collections import defaultdict
from app.core.config import get_settings

URI = get_settings().graphdb.uri
USER = get_settings().graphdb.user
PASSWORD = get_settings().graphdb.password


def get_country_name(code: str) -> str:
    country = pycountry.countries.get(alpha_2=code.upper())
    return country.name if country else code


async def compile_company_profile(ens_id: str, session_id, session):

    logger.debug("IN FUNC")
    logger.debug(session_id)

    if not session_id:
        logger.debug("aaaa")
        latest_session_id, last_screened_date = await pull_latest_session_id(ens_id, session)
    else:
        latest_session_id = session_id
        last_screened_date = None

    logger.debug(latest_session_id)
    logger.debug("HERE PULL PROFILE")
    profile = await pull_profile(ens_id, latest_session_id, session)
    logger.debug("HERE PULL RATINGS")
    ratings = await pull_ratings(ens_id, latest_session_id, session)

    compiled_findings = {
        "profile": profile,
        "ratings": ratings,
        "metadata": {
            "ens_id": ens_id,
            "last_session_id": latest_session_id,
            "last_screened_date": last_screened_date
        }
    }

    return compiled_findings


async def compile_company_findings(ens_id: str, session_id, session):

    if not session_id:
        logger.debug("aaaa")
        latest_session_id, last_screened_date = await pull_latest_session_id(ens_id, session)
    else:
        latest_session_id = session_id
        last_screened_date = None

    ratings = await pull_ratings(ens_id, latest_session_id, session)
    findings = await pull_kpis(ens_id, latest_session_id, session)
    google_image_name = await pull_google_image_name(ens_id, latest_session_id, session)  # <-- new

    compiled_findings = {
        "ratings": ratings,
        "findings": findings,
        "google_image_name": google_image_name,  # <-- new
        "metadata": {
            "ens_id": ens_id,
            "last_session_id": latest_session_id,
            "last_screened_date": last_screened_date
        }
    }

    return compiled_findings


async def pull_latest_session_id(ens_id: str, session):
    # GET THE LATEST COMPLETED SESSION
    session_id_row = await get_ens_data("entity_universe",
                                                         required_columns=["last_session_id", "last_screened_date"],
                                                         ens_id=ens_id, session=session)
    session_id = session_id_row[0].get("last_session_id")
    last_screened_date = session_id_row[0].get("last_screened_date")

    return session_id, last_screened_date


async def pull_profile(ens_id: str, latest_session_id: str, session):

    universe_required_cols = ["overall_supplier_rating", "unmodified_name" ,"create_time", "external_vendor_id","update_time"] #external_vendor_id

    entity_latest_data = await get_ens_data("entity_universe", universe_required_cols, ens_id, session)
    entity_latest_data = entity_latest_data[0]
    logger.info("ENTITY LATEST DATa")
    logger.info(entity_latest_data)

    copr_required_cols = ["name", "location", "address", 'website', 'e_filing_status', 'category',
                           'pan_id', 'alias', 'incorporation_date', 'revenue',
                           'shareholders', 'key_executives', "employee",]
    copr = await get_dynamic_ens_data_for_session('company_profile', copr_required_cols, ens_id, latest_session_id,
                                                  session)
    copr = copr[0]

    return copr | entity_latest_data


async def pull_kpis(ens_id: str, session_id: str, session):
    theme_mappings = {
        "legal": ["LEG"],
        "financials": ["FSTB"],
        "adverse_media": ["NWS"],
        "entity_existence": ["B2B", "DOM"],
        "cyber_esg": ["CYB", "ESG"]
    }  # change this to from DB

    reverse_area_mapping = {code: theme for theme, codes in theme_mappings.items() for code in codes}

    required_columns = ["kpi_area", "kpi_code", "kpi_definition", "kpi_rating", "kpi_flag", "kpi_details"]
    kpi_table_name = [ 'finance', 'legal', 'entity_existance', 'adverse_media','cyber_esg']

    gather_all_kpis = []
    for table_name in kpi_table_name:
        res_kpis = await get_dynamic_ens_data_for_session(table_name, required_columns, ens_id, session_id, session)
        gather_all_kpis.extend(res_kpis)

    grouped_data = defaultdict(list)
    for theme in theme_mappings:
        grouped_data[theme] = []

    for item in gather_all_kpis:
        if item['kpi_flag']:
            kpi_theme = reverse_area_mapping.get(item['kpi_area'], False)  # get theme if in current mapping
            if kpi_theme:
                grouped_data[kpi_theme].append(item)

    screening_kpis_dict = dict(grouped_data)

    return screening_kpis_dict



async def pull_ratings(ens_id: str, latest_session_id: str, session):

    required_columns = ["kpi_area", "kpi_code", "kpi_definition", "kpi_rating", "update_time"]
    res_ratings = await get_dynamic_ens_data_for_session("ovar", required_columns, ens_id, latest_session_id, session)
    theme_ratings = {}
    for rating_row in res_ratings:
        if rating_row.get("kpi_rating", "").lower() != "deactivated":
            theme_ratings.update({
                rating_row.get("kpi_code", "").replace(" ", "_"): rating_row.get("kpi_rating", "")
            })

    return theme_ratings


def simple_dedup(data, key1, key2):
    seen = set()
    result = []

    for item in data:
        key = (item[key1], item[key2])
        if key not in seen:
            seen.add(key)
            result.append(item)

    return result

async def pull_google_image_name(ens_id: str, session_id: str, session):
    result = await get_dynamic_ens_data_for_session(
        "external_supplier_data",
        ["google_image_name"],
        ens_id,
        session_id,
        session
    )
    if result:
        return result[0].get("google_image_name", None)
    return None
