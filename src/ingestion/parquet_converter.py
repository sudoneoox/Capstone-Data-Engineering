"""
Convert raw JSON (API responses) and CSV (seeds) to Parquet format.

Reads from:
    - data/api_raw/{source}/*.json (API responses)
    - dbt/seeds/{source}/*.csv     (Kaggle, O*NET seeds)

Writes to:
    - data/parquet/source/*.parquet

This runs AFTER ingestion, BEFORE GCS upload
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.config import ROOT

logger = logging.getLogger(__name__)

API_RAW_DIR = ROOT / "data" / "api_raw"
SEED_DIR = ROOT / "data" / "seeds"
PARQUET_DIR = ROOT / "data" / "parquet"
SKIP_FILES: set[str] = {"linkedin_1_3m_job_summary.csv"}


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def convert_json_to_parquet(json_path: Path, dest_dir: Path) -> Path | None:
    """
    Convert a single JSON file to Parquet.

    Handles three shapes:
    1. List of dicts -> Dataframe directly
    2. BLS-style nested {"Results": {"series": [...]}}
    3. FRED-style {"series_ids": [...], "data": {...}}
    """

    dest_dir.mkdir(parents=True, exist_ok=True)
    parquet_name = json_path.stem + ".parquet"
    dest = dest_dir / parquet_name

    try:
        raw = json.loads(json_path.read_text(encoding="utf-8"))
    except (json.json.JSONDecodeError, UnicodeDecodeError):
        logger.error("Failed to parse JSON: %s", json_path)
        return None

    df = None

    if isinstance(raw, list):
        df = pd.json_normalize(raw)

    elif isinstance(raw, dict):
        if "Results" in raw and "series" in raw["Results"]:
            rows = []
            for series in raw["Results"]["series"]:
                sid = series.get("seriesID", "unknown")
                for obs in series.get("data", []):
                    obs["seriesID"] = sid
                    rows.append(obs)
            if rows:
                df = pd.json_normalize(rows)

        elif "data" in raw and "series_ids" in raw:
            df = pd.DataFrame.from_dict(raw["data"], orient="index")
            df.index.name = "date"
            df = df.reset_index()

        else:
            df = pd.json_normalize(raw)

    if df is None or df.empty:
        logger.warning("No data extracted from %s -- skipping", json_path)
        return None

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, dest, compression="snappy")
    logger.info("Converted %s -> %s (%d rows)", json_path.name, dest, len(df))
    return None


def convert_csv_to_parquet(csv_path: Path, dest_dir: Path) -> Path | None:
    """Convert a single CSV to Parquet with snappy compression"""

    if csv_path.name in SKIP_FILES:
        logger.info("Skipping CSV file: %s", csv_path)
        return None

    dest_dir.mkdir(parents=True, exist_ok=True)
    parquet_name = csv_path.stem + ".parquet"
    dest = dest_dir / parquet_name

    try:
        read_kwargs: dict = {"low_memory": False}
        df = pd.read_csv(csv_path, **read_kwargs)
    except Exception:
        logger.exception("Failed to read CSV: %s", csv_path)
        return None

    if df.empty:
        logger.warning("Empty CSV: %s -- skipping", csv_path)
        return None

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, dest, compression="snappy")
    logger.info("Converted %s -> %s (%d rows)", csv_path.name, dest, len(df))
    return dest


def convert_api_sources() -> dict[str, list[Path]]:
    """Convert all JSON files in data/api_raw/ to Parquet."""
    results: dict[str, list[Path]] = {}

    if not API_RAW_DIR.exists():
        logger.warning("No api_raw directory found at %s", API_RAW_DIR)
        return results

    for source_dir in sorted(API_RAW_DIR.iterdir()):
        if not source_dir.is_dir():
            continue
        source = source_dir.name
        dest = _ensure_dir(PARQUET_DIR / source)
        converted = []

        for json_file in sorted(source_dir.glob("*.json")):
            result = convert_json_to_parquet(json_file, dest)
            if result:
                converted.append(result)
        results[source] = converted
        logger.info("API Source '%s': %d files converted", source, len(converted))

    return results


def convert_seed_sources() -> dict[str, list[Path]]:
    """Convert all CSV files in dbt/seeds/ to Parquet."""
    results: dict[str, list[Path]] = {}

    if not SEED_DIR.exists():
        logger.warning("No seeds directory found at %s", SEED_DIR)
        return results

    for source_dir in sorted(SEED_DIR.iterdir()):
        if not source_dir.is_dir():
            continue
        source = source_dir.name
        dest = _ensure_dir(PARQUET_DIR / source)
        converted = []

        for csv_file in sorted(source_dir.rglob("*.csv")):
            relative = csv_file.relative_to(source_dir)
            file_dest = _ensure_dir(dest / relative.parent)
            result = convert_csv_to_parquet(csv_file, file_dest)
            if result:
                converted.append(result)

        results[source] = converted
        logger.info("Seed source '%s': %d files converted", source, len(converted))
    return results


def convert_all() -> dict[str, list[Path]]:
    """Convert everything to Parquet - APIs + seeds"""
    results: dict[str, list[Path]] = {}
    results.update(convert_api_sources())
    results.update(convert_seed_sources())

    total = sum(len(v) for v in results.values())
    logger.info(
        "Total Parquet conversion: %d files across %d sources", total, len(results)
    )
    return results
