"""
Prefect @task wrappers around dbt CLI commands.
Follows the design principle: Prefect runs dbt by *layer/selector*,
dbt manages individual model dependencies internally
"""

from __future__ import annotations

import logging
import subprocess

from prefect import task
from src.utils.config import ROOT

logger = logging.getLogger(__name__)
DBT_PROJECT_DIR = ROOT / "dbt"


def _run_dbt(
    args: list[str], *, project_dir=DBT_PROJECT_DIR
) -> subprocess.CompletedProcess:
    """Run a dbt CLI command and stream output."""
    cmd = ["dbt"] + args + ["--project-dir", str(project_dir)]

    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(project_dir))

    if result.stdout:
        for line in result.stdout.strip().splitlines():
            logger.info("[dbt] %s", line)

    if result.returncode != 0:
        logger.error("[dbt stderr] %s", result.stderr.strip())
        raise RuntimeError(f"dbt command failed: {' '.join(cmd)}")

    return result


@task(retries=1, tags=["dbt"])
def dbt_seed() -> None:
    """Run dbt seed to load CSV/txt seeds into warehouse."""
    _run_dbt(["seed"])


@task(retries=1, tags=["dbt"])
def dbt_build_bronze() -> None:
    """Build silver layer models + run their tests."""
    _run_dbt(["build", "--select", "bronze"])


@task(retries=1, tags=["dbt"])
def dbt_build_silver() -> None:
    """Build silver layer models + run their tests."""
    _run_dbt(["build", "--select", "silver"])


@task(retries=1, tags=["dbt"])
def dbt_build_gold() -> None:
    """Build gold layer models + run their tests."""
    _run_dbt(["build", "--select", "gold"])


@task(retries=1, tags=["dbt"])
def dbt_test() -> None:
    """Run all dbt tests."""
    _run_dbt(["test"])


@task(tags=["dbt"])
def dbt_docs_generate() -> None:
    """Generate dbt docs (non-critical, no retries)."""
    _run_dbt(["docs", "generate"])
