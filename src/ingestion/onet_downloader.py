"""
O*NET bulk-download client.

Downloads the tab-delimeted ZIP from onetcenter.org, extracts only
the files we need, and drops them into dbt/seeds/onet/

Uses a marker-file pattern for idempotency: if the marker for the current ONET_VERSION already exists, the download is skipped unless force=True

No API key required
"""

from __future__ import annotations

import io
import logging
import zipfile
from pathlib import Path

import requests

from src.utils.config import AppConfig, get_config, ROOT

logger = logging.getLogger(__name__)

# NOTE: Bump this when O*NET publishes a new quarterly release
ONET_VERSION = "30_2"


# Only extract the files we actually use as seeds.
# Full dataset has 40 files; no need to seed all of them.
TARGET_FILES: dict[str, str] = {
    "Occupation Data.txt": "occupation_data.txt",
    "Skills.txt": "skills.txt",
    "Knowledge.txt": "knowledge.txt",
    "Abilities.txt": "abilities.txt",
    "Technology Skills.txt": "technology_skills.txt",
    "Alternate Titles.txt": "alternate_titles.txt",
    "Job Zones.txt": "job_zones.txt",
    "Task Statements.txt": "task_statements.txt",
    "Emerging Tasks.txt": "emerging_tasks.txt",
    "Work Activities.txt": "work_activities.txt",
    "Education, Training, and Experience.txt": "education_training_experience.txt",
}

DEFAULT_SEED_DIR = ROOT / "dbt" / "seeds" / "onet"


def download_onet(
    seed_dir: Path = DEFAULT_SEED_DIR,
    *,
    force: bool = False,
    config: AppConfig | None = None,
) -> bool:
    """
    Download and extract O*NET bulk data into dbt seeds

    Returns Treu if new data was downloaded, False if skipped.
    """
    cfg = config or get_config()
    meta = cfg.metadata.get("download_urls", {}).get("onet", {})
    base_url = meta.get("base_url", "https://www.onetcenter.org/dl_files/database")

    zip_url = f"{base_url}/db_{ONET_VERSION}_text.zip"
    marker_file = seed_dir / f".onet_{ONET_VERSION}_complete"

    # ---- idempotency check ------- #
    if marker_file.exists() and not force:
        logger.info(
            "O*NET %s already present at %s -- skipping", ONET_VERSION, seed_dir
        )
        return False

    # ---- download ----- #
    logger.info("Downloading O*NET %s from %s", ONET_VERSION, zip_url)
    resp = requests.get(zip_url, timeout=120)
    resp.raise_for_status()
    logger.info("Download complete (%.1f MB)", len(resp.content) / 1_048_576)

    # ---- extract ----- #
    seed_dir.mkdir(parents=True, exist_ok=True)
    extracted = 0

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        for archived_name in zf.namelist():
            basename = Path(archived_name).name
            if basename in TARGET_FILES:
                dest = seed_dir / TARGET_FILES[basename]
                dest.write_bytes(zf.read(archived_name))
                logger.debug("   extracted %s -> %s", basename, dest)
                extracted += 1

    if extracted == 0:
        logger.error(
            "No target files found in ZIP -- archive structure may have changed"
        )
        raise RuntimeError(
            "O*NET ZIP contained none of the expected files (manual intervention needed)"
        )

    # ---- marker files ----- #
    marker_file.touch()
    logger.info(
        "O*NET %s: extracted: %d target files into %s",
        ONET_VERSION,
        extracted,
        len(TARGET_FILES),
        seed_dir,
    )

    return True
