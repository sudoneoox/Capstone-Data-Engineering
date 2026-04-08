"""
Upload Parquet files from data/parquet to GCS.

Uploads to:
    gs://{bucket}/raw/{source}/{file}.parquet

Authentication:
    uses GOOGLE_APPLICATION_CREDENTIALS env var (path to SA key JSON).
    This is the standard Google auth pattern the SDK reads it automatically

Requires:
    pip install google-cloud-storage
"""

from __future__ import annotations

import logging
from pathlib import Path

from google.cloud import storage

from src.utils.config import ROOT, get_config

logger = logging.getLogger(__name__)

PARQUET_DIR = ROOT / "data" / "parquet"


def upload_to_gcs(
    *,
    bucket_name: str | None = None,
    prefix: str = "raw",
    parquet_dir: Path = PARQUET_DIR,
) -> dict[str, int]:
    """
    Upload all Parquet files to GCS under {prefix}/{source}/.

    Returns dict mapping source name -> number of files uploaded.
    """

    cfg = get_config()

    if bucket_name is None:
        bucket_name = cfg.secret_by_env_name("GCS_BUCKET_NAME")
    if not bucket_name:
        raise EnvironmentError(
            "GCS_BUCKET_NAME must be set in environment or passed explicitly"
        )

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    results: dict[str, int] = {}

    if not parquet_dir.exists():
        logger.warning("No parquet directory at %s -- nothing to upload", parquet_dir)
        return results

    for source_dir in sorted(parquet_dir.iterdir()):
        if not source_dir.is_dir():
            continue

        source = source_dir.name
        count = 0

        for parquet_file in sorted(source_dir.rglob("*.parquet")):
            relative = parquet_file.relative_to(parquet_dir)
            blob_path = f"{prefix}/{relative}"

            blob = bucket.blob(blob_path)
            blob.upload_from_filename(str(parquet_file))
            count += 1

            logger.debug(
                "Uploaded %s → gs://%s/%s", parquet_file.name, bucket_name, blob_path
            )

        results[source] = count
        logger.info(
            "GCS upload '%s': %d files → gs://%s/%s/%s/",
            source,
            count,
            bucket_name,
            prefix,
            source,
        )

    total = sum(results.values())
    logger.info(
        "GCS upload complete: %d files across %d sources → gs://%s/%s/",
        total,
        len(results),
        bucket_name,
        prefix,
    )
    return results
