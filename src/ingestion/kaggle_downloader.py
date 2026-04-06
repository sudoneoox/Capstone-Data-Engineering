"""
One-time backfill -- downloads static datasets into dbt/seeds/kaggle

Requires:
    - 'kaggle' CLI installed (pip install kaggle)
    - ~/.kaggle/kaggle.json with your API key
        OR KAGGLE_USERNAME + KAGGLE_KEY in the env.secrets
"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path

from src.utils.config import ROOT

logger = logging.getLogger(__name__)

DEFAULT_SEED_DIR = ROOT / "dbt" / "seeds" / "kaggle"

# slug -> local folder name
KAGGLE_SOURCES: dict[str, str] = {
    "asaniczka/1-3m-linkedin-jobs-and-skills-2024": "linkedin_jobs_2024_large",
    "asaniczka/data-science-job-postings-and-skills": "data_science_jobs",
    "arshkon/linkedin-job-postings": "linkedin_jobs",
}

# local folder name -> {relative old path: relative new path}
KAGGLE_FILE_RENAMES: dict[str, dict[str, str]] = {
    "linkedin_jobs_2024_large": {
        "job_skills.csv": "linkedin_1_3m_job_skills.csv",
        "job_summary.csv": "linkedin_1_3m_job_summary.csv",
        "linkedin_job_postings.csv": "linkedin_1_3m_job_postings.csv",
    },
    "data_science_jobs": {
        "job_postings.csv": "ds_job_postings.csv",
        "job_skills.csv": "ds_job_skills.csv",
        "job_summary.csv": "ds_job_summary.csv",
    },
    "linkedin_jobs": {
        "postings.csv": "linkedin_postings.csv",
        "companies/companies.csv": "companies/linkedin_companies.csv",
        "companies/company_industries.csv": "companies/linkedin_company_industries.csv",
        "companies/company_specialities.csv": "companies/linkedin_company_specialities.csv",
        "companies/employee_counts.csv": "companies/linkedin_employee_counts.csv",
        "jobs/benefits.csv": "jobs/linkedin_benefits.csv",
        "jobs/job_industries.csv": "jobs/linkedin_job_industries.csv",
        "jobs/job_skills.csv": "jobs/linkedin_job_skills.csv",
        "jobs/salaries.csv": "jobs/linkedin_salaries.csv",
        "mappings/industries.csv": "mappings/linkedin_mapping_industries.csv",
        "mappings/skills.csv": "mappings/linkedin_mapping_skills.csv",
    },
}


def rename_dataset_files(dataset_dir: Path, rename_map: dict[str, str]) -> None:
    """
    Rename downloaded files within a dataset directory so dbt seed names are unique.
    Paths in rename_map are relative to dataset_dir.
    """
    for old_rel, new_rel in rename_map.items():
        old_path = dataset_dir / old_rel
        new_path = dataset_dir / new_rel

        if not old_path.exists():
            logger.warning("Expected file to rename not found: %s", old_path)
            continue

        new_path.parent.mkdir(parents=True, exist_ok=True)

        if new_path.exists():
            logger.info("Target renamed file already exists, skipping: %s", new_path)
            continue

        old_path.rename(new_path)
        logger.info("Renamed %s -> %s", old_path, new_path)


def download_kaggle_dataset(
    slug: str,
    dest_dir: Path,
    *,
    force: bool = False,
    unzip: bool = True,
    rename_map: dict[str, str] | None = None,
) -> bool:
    """
    Download a single Kaggle dataset.

    Returns True if downloaded, False if skipped.
    """

    safe_name = slug.replace("/", "_")
    marker = dest_dir / f".{safe_name}_complete"

    if marker.exists() and not force:
        logger.info("Kaggle dataset %s already present -- skipping", slug)
        return False

    if force and dest_dir.exists():
        logger.info(
            "Force enabled; existing dataset directory will be reused: %s", dest_dir
        )

    dest_dir.mkdir(parents=True, exist_ok=True)

    cmd = ["kaggle", "datasets", "download", "-d", slug, "-p", str(dest_dir)]
    if unzip:
        cmd.append("--unzip")

    logger.info("Downloading kaggle dataset: %s -> %s", slug, dest_dir)
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        logger.error("Kaggle download failed for %s: %s", slug, result.stderr.strip())
        raise RuntimeError(f"Kaggle CLI failed for {slug}: {result.stderr.strip()}")

    if rename_map:
        rename_dataset_files(dest_dir, rename_map)

    marker.touch()
    logger.info("Kaggle dataset %s downloaded successfully", slug)
    return True


def download_all_kaggle(
    *,
    force: bool = False,
    seed_dir: Path = DEFAULT_SEED_DIR,
) -> dict[str, bool]:
    """
    Download all configured Kaggle datasets.

    Returns dict mapping slug -> whether it was freshly downloaded.
    """

    results: dict[str, bool] = {}

    for slug, folder_name in KAGGLE_SOURCES.items():
        dest = seed_dir / folder_name
        rename_map = KAGGLE_FILE_RENAMES.get(folder_name, {})

        try:
            results[slug] = download_kaggle_dataset(
                slug,
                dest,
                force=force,
                rename_map=rename_map,
            )
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
