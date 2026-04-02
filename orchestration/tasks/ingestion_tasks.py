"""
Prefect @task wrappers around src/ingestion/ clients
Each task is independently retryable and logs into Prefect's UI

Never run directly
"""

# TODO: Add tasks for:
# - fetch_bls_series
# - fetch_adzuna_jobs
# - fetch_fred_series
# - ensure_onet_seeds

from __future__ import annotations
import logging
import json
from pathlib import Path


from src.utils.config import ROOT
from prefect import task

logger = logging.getLogger(__name__)

# --------------------------------------------
# Kaggle (one-time backfill)
# --------------------------------------------


@task(retries=1, tags=["ingestion", "kaggle"])
def ensure_kaggle_seeds(force: bool = False) -> dict[str, bool]:
    from src.ingestion.kaggle_downloader import download_all_kaggle

    return download_all_kaggle(force=force)
