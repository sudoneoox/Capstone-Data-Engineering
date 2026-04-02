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


from src.utils.config import ROOT
from prefect import task

logger = logging.getLogger(__name__)


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
