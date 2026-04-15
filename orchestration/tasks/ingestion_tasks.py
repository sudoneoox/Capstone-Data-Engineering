"""
Prefect @task wrappers around src/ingestion/ clients
Each task is independently retryable and logs into Prefect's UI

Never run directly
"""

from __future__ import annotations
import logging
import json
from pathlib import Path
from tracemalloc import start
from datetime import datetime

from src.utils.config import ROOT, get_config
from prefect import task

logger = logging.getLogger(__name__)

# Where raw API responses land before being pushed to a cloud provider
API_DATA_DIR = ROOT / "data" / "api_raw"


def _write_seed_api(source: str, filename: str, payload: object) -> Path:
    """Write raw JSON payload to the API data landing zone."""
    dest_dir = API_DATA_DIR / source
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / filename
    dest.write_text(json.dumps(payload, indent=2, default=str))
    logger.info("Wrote json file: %s", dest)
    return dest


# --------------------------------------------
# INFO: Kaggle (one-time backfill)
# --------------------------------------------
@task(retries=1, tags=["ingestion", "kaggle"])
def ensure_kaggle_seeds(force: bool = False) -> dict[str, bool]:
    """Download all Kaggle datasets into dbt/seeds/kaggle/ if not present"""
    from src.ingestion.kaggle_downloader import download_all_kaggle

    return download_all_kaggle(force=force)


# --------------------------------------------
# INFO: O*NET (bulk download)
# --------------------------------------------
@task(retries=2, retry_delay_seconds=30, tags=["ingestion", "onet"])
def ensure_onet_seeds(force: bool = False) -> bool:
    """Download O*NET bulk data into dbt/seeds/onet/ if not present"""
    from src.ingestion.onet_downloader import download_onet

    return download_onet(force=force)


# --------------------------------------------
# INFO: BLS
# --------------------------------------------


@task(retries=3, retry_delay_seconds=60, tags=["ingestion", "bls"])
def fetch_bls_series(
    series_ids: list[str],
    start_year: int,
    end_year: int,
) -> Path:
    """Fetch BLS time-series and land raw JSON in API_DATA_DIR"""

    from src.ingestion.bls_client import BLSClient

    client = BLSClient()
    data = client.fetch_series(series_ids, start_year, end_year)

    return _write_seed_api("bls", f"bls_{start_year}_{end_year}.json", data)


# --------------------------------------------
# INFO: Adzuna
# --------------------------------------------
@task(retries=2, retry_delay_seconds=20, tags=["ingestion", "adzuna"])
def fetch_adzuna_jobs(
    what: str,
    where: str = "",
    max_pages: int = 25,
) -> Path:
    """Paginate Adzuna search and load results in API_DATA_DIR"""
    from src.ingestion.adzuna_client import AdzunaClient

    client = AdzunaClient()
    jobs = client.search_jobs_all_pages(what, where, max_pages=max_pages)

    for job in jobs:
        job["_search_query"] = what
        job["_search_location"] = where or "all"
        job["_fetched_at"] = datetime.now().isoformat()

    safe_name = what.replace(" ", "_").lower()
    safe_loc = where.replace(" ", "_").replace(",", "").lower() or "all"
    ts = datetime.now().strftime("%Y%m%d")
    filename = f"adzuna_{safe_name}_{safe_loc}_{ts}.json"

    return _write_seed_api("adzuna", filename, jobs)


# --------------------------------------------
# INFO: FRED
# --------------------------------------------
@task(retries=2, retry_delay_seconds=20, tags=["ingestion", "fred"])
def fetch_fred_series(
    series_ids: list[str],
    start: str | None = None,
    end: str | None = None,
) -> Path:
    """Fetch FRED series and land as JSON in API_DATA_DIR"""
    from src.ingestion.fred_client import FREDClient

    client = FREDClient()
    df = client.get_multiple_series(series_ids, start=start, end=end)

    # Convert DataFrame -> Parquet
    payload = {
        "series_ids": series_ids,
        "data": json.loads(df.to_json(orient="index", date_format="iso")),
    }

    return _write_seed_api("fred", "fred_series.json", payload)


# --------------------------------------------
# INFO: ACS (Census Bureau - annual refresh)
# --------------------------------------------
@task(retries=2, retry_delay_seconds=30, tags=["ingestion", "acs"])
def fetch_acs_metro_profiles() -> Path:
    """Fetch ACS demographic/economic profiles for target metros"""
    from src.ingestion.acs_client import ACSClient

    cfg = get_config()
    ingestion = cfg.metadata.get("ingestion", {}).get("acs", {})
    variables = ingestion.get("variables", [])
    geos = ingestion.get("geographies", [])
    fips_codes = [g["fips"] for g in geos]

    client = ACSClient()
    data = client.fetch_metro_profiles(variables, fips_codes)
    return _write_seed_api("acs", "acs_metro_profiles.json", data)


# --------------------------------------------
# INFO: Databricks
# --------------------------------------------


@task(retries=1, tags=["ingestion", "databricks"])
def upload_api_sources_to_databricks():
    """Upload API Files to Databricks using sdk"""
    from src.ingestion.databricks_uploader import upload_to_databricks

    upload_to_databricks()
