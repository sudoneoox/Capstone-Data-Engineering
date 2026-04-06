"""
One-time backfill -- downloads static datasets into dbt/seeds/kaggle

Requires:
    - 'kaggle' CLI installed (pip install kaggle)
    - ~/.kaggle/kaggle.json with your API key
        OR KAGGLE_USERNAME + KAGGLE_KEY in the env.secrets
"""

from __future__ import annotations
import subprocess
import logging
from pathlib import Path
from src.utils.config import ROOT


logger = logging.getLogger(__name__)

KAGGLE_SOURCES: list[str] = [
    "asaniczka/1-3m-linkedin-jobs-and-skills-2024",
    "asaniczka/data-science-job-postings-and-skills",
    "arshkon/linkedin-job-postings",
]

DEFAULT_SEED_DIR = ROOT / "dbt" / "seeds" / "kaggle"


def download_kaggle_dataset(
    slug: str, dest_dir: Path, *, force: bool = False, unzip: bool = True
) -> bool:
    """
    Downloads a single Kaggle dataset.

    Returns True if downloaded, False if skipped
    """

    safe_name = slug.replace("/", "_")
    marker = dest_dir / f".{safe_name}_complete"

    if marker.exists() and not force:
        logger.info("Kaggle dataset %s already present -- skipping", slug)
        return False

    dest_dir.mkdir(parents=True, exist_ok=True)
    print("HEREEEEEEEEEEEEEEEEEEEEE", dest_dir)

    cmd = ["kaggle", "datasets", "download", "-d", slug, "-p", str(dest_dir)]
    if unzip:
        cmd.append("--unzip")

    logger.info("Downloading kaggle dataset: %s -> %s", slug, dest_dir)
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        logger.error("Kaggle download failed for %s: %s", slug, result.stderr.strip())
        raise RuntimeError(f"Kaggle CLI failed for {slug}: {result.stderr.strip()}")

    marker.touch()
    logger.info("Kaggle dataset %s downloaded successfully", slug)
    return True


def download_all_kaggle(
    *, force: bool = False, seed_dir: Path = DEFAULT_SEED_DIR
) -> dict[str, bool]:
    """
    Download all configured Kaggle datasets.

    Returns dict mapping slug -> whether it was freshly downloaded.
    """

    results: dict[str, bool] = {}

    for slug in KAGGLE_SOURCES:
        dataset_name = slug.split("/")[1]
        dest = seed_dir / dataset_name
        try:
            results[slug] = download_kaggle_dataset(slug, dest, force=force)
        except RuntimeError:
            logger.exception("Failed to download %s -- continuing with others", slug)
            results[slug] = False

    downloaded = sum(1 for v in results.values() if v)
    logger.info(
        "Kaggle batch complete: %d/%d freshly downloaded",
        downloaded,
        len(KAGGLE_SOURCES),
    )

    return results
