"""
Prefect @task wrappers around src/ingestion/ clients
Each task is independently retryable and logs into Prefect's UI

Never run directly
"""

# TODO: Add tasks for:
# - fetch_bls_series
# - fetch_adzuna_jobs
# - fetch_fred_series

from __future__ import annotations
import logging
import json
from pathlib import Path
from tracemalloc import start


from src.utils.config import ROOT
from prefect import task

logger = logging.getLogger(__name__)

# Where raw API responses land before dbt touches them
SEED_API_DIR = ROOT / "dbt" / "seeds" / "apis"


def _write_seed_api(source: str, filename: str, payload: object) -> Path:
    """Write raw JSON payload to the seed/api landing zone."""
    dest_dir = SEED_API_DIR / source
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / filename
    dest.write_text(json.dumps(payload, indent=2, default=str))
    logger.info("Wrote bronze file: %s", dest)
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
    """Fetch BLS time-series and land raw JSON in dbt/seeds/bls/"""

    from src.ingestion.bls_client import BLSClient

    client = BLSClient()
    data = client.fetch_series(series_ids, start_year, end_year)

    return _write_seed_api("bls", f"bls_{start_year}_{end_year}.json", data)
