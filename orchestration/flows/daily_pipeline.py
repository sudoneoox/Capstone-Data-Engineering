"""
Master Prefect flow that orchestrates the full ingestion -> transform pipeline

Run with Prefect CLI:
    prefect flow serve orchestration/flows/daily_pipeline.py:daily_pipeline

Or via CLI entry point:
    scripts/with-secrets.sh python -m orchestration.flows.daily_pipeline --mode full
"""

from __future__ import annotations

import argparse
import logging
import logging.config
import sys
from datetime import datetime
import yaml
from prefect import flow
from src.utils.config import ROOT, get_config

logger = logging.getLogger(__name__)

# ------------------------------------------------------------
# INFO: Logging bootstrap (runs once before any flows)
# ------------------------------------------------------------


def _setup_logging() -> None:
    log_conf = ROOT / "conf" / "logging.yml"
    if log_conf.exists():
        with log_conf.open() as f:
            cfg = yaml.safe_load(f)
        (ROOT / "logs").mkdir(exist_ok=True)
        logging.config.dictConfig(cfg)
    else:
        logging.basicConfig(level=logging.INFO)


# ------------------------------------------------------------
# INFO: Sub-flows (logical pipeline stage)
# ------------------------------------------------------------


@flow(name="ingest-seeds", log_prints=True)
def ingest_seeds(force: bool = False) -> None:
    """
    One-time / on-demand: download static files (O*NET, Kaggle)
    These only re-download if the marker file is missing or force=True
    """

    from orchestration.tasks.ingestion_tasks import (
        ensure_kaggle_seeds,
        ensure_onet_seeds,
    )

    ensure_kaggle_seeds(force=force)
    ensure_onet_seeds(force=force)


# TODO:
@flow(name="ingest-apis", log_prints=True)
def ingest_apis() -> None:
    """
    Recurring: pull fresh data from live APIs
    Runs on every pipeline execution
    """
    from orchestration.tasks.ingestion_tasks import (
        fetch_fred_series,
        fetch_bls_series,
        fetch_adzuna_jobs,
    )

    current_year = datetime.now().year

    # INFO: ---- BLS ----
    # OES wages, JOLTS openings, CES employment, LAUS unemployment
    bls_series = [
        "LNS14000000",  # unemployment rate
        "CES0000000001",  # total nonfarm employment
        "JTS000000000000000JOL",  # JOLTS job openings
    ]
    fetch_bls_series(bls_series, start_year=current_year - 3, end_year=current_year)

    # INFO: ---- Adzuna ----
    search_queries = [
        ("data engineer", ""),
        ("data analyst", ""),
        ("software engineer", ""),
        ("data scientist", ""),
    ]

    for what, where in search_queries:
        fetch_adzuna_jobs(what=what, where=where, max_pages=5)

    # INFO: ---- FRED ----
    fred_series = [
        "UNRATE",  # unemployment rate (cross-check with BLS)
        "JTSJOL",  # JOLTS openings (cross-check)
        "CPIAUCSL",  # CPI for salary normalization
        "FEDFUNDS",  # fed funds rate (economic context)
    ]
    fetch_fred_series(fred_series, start=f"{current_year - 3}-01-01")


# TODO:
@flow(name="transform", log_prints=True)
def transform() -> None:
    """
    Run dbt: seeds -> bronze -> silver -> gold -> docs (testing in between)
    Prefect manages stage ordering: dbt manages model dependencies
    """
    # TODO: from orchestration.tasks.dbt_tasks import (
    #     dbt_build_gold,
    #     dbt_build_silver,
    #     dbt_build_bronze,
    #     dbt_docs_generate,
    #     dbt_seed
    # )
    # dbt_seed()
    # dbt_build_bronze()
    # dbt_build_silver()
    # dbt_build_gold()
    # dbt_docs_generate()


# ------------------------------------------------------------
# INFO: Master flow
# ------------------------------------------------------------
@flow(name="daily-pipeline", log_prints=True)
def daily_pipeline(mode: str = "full", force_seeds: bool = False) -> None:
    """
    Master orchestration flow.

    Modes
    -----
    full        : seeds + APIs + transform  (scheduled daily)
    ingest      : seeds + APIs only         (debug / backfill)
    seeds       : static downloads only     (first run / force refresh)
    apis        : live API fetch only       (incremental refresh)
    transform   : dbt only                  (re-transform existing data)
    """

    config = get_config()
    config.validate()

    logger.info("===== daily_pipeline START (mode=%s) ====", mode)

    if mode in ("full", "seeds"):
        ingest_seeds(force=force_seeds)
    if mode in ("full", "ingest", "apis"):
        ingest_apis()
    if mode in ("full", "ingest", "transform"):
        transform()

    logger.info("====== daily_pipeline END =====")


# ------------------------------------------------------------
# INFO: CLI entry point
# ------------------------------------------------------------
def main() -> None:
    _setup_logging()

    parser = argparse.ArgumentParser(description="Job Market Intelligence Pipeline")
    parser.add_argument(
        "--mode",
        choices=["full", "ingest", "seeds", "apis", "transform"],
        default="full",
    )
    parser.add_argument(
        "--force-seeds",
        action="store_true",
        help="Re-download seeds even if marker files exist",
    )

    args = parser.parse_args()
    daily_pipeline(mode=args.mode, force_seeds=args.force_seeds)


if __name__ == "__main__":
    main()
