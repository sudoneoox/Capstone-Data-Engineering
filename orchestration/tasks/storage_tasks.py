"""
Prefect tasks for Parquet conversion and GCS upload.
These run AFTER ingestion tasks, BEFORE dbt.
"""

from __future__ import annotations

import logging

from prefect import task

logger = logging.getLogger(__name__)


# ------------------------------------------------
# INFO: Parquet conversion
# ------------------------------------------------
@task(retries=1, tags=["storage", "parquet"])
def convert_all_to_parquet() -> dict[str, int]:
    """Convert all API JSON + seed CSVs to Parquet."""
    from src.ingestion.parquet_converter import convert_all

    results = convert_all()
    return {source: len(files) for source, files in results.items()}


@task(retries=1, tags=["storage", "parquet"])
def convert_api_to_parquet() -> dict[str, int]:
    """Convert only API JSON responses to Parquet."""
    from src.ingestion.parquet_converter import convert_api_sources

    results = convert_api_sources()
    return {source: len(files) for source, files in results.items()}


@task(retries=1, tags=["storage", "parquet"])
def convert_seeds_to_parquet() -> dict[str, int]:
    """Convert only seed CSVs to Parquet."""
    from src.ingestion.parquet_converter import convert_seed_sources

    results = convert_seed_sources()
    return {source: len(files) for source, files in results.items()}


# ------------------------------------------------
# INFO: GCS upload
# ------------------------------------------------
@task(retries=2, retry_delay_seconds=30, tags=["storage", "gcs"])
def upload_parquet_to_gcs() -> dict[str, int]:
    """Upload all Parquet files from data/parquet/ to GCS raw/."""
    from src.ingestion.gcs_uploader import upload_to_gcs

    return upload_to_gcs()
