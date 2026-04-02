"""
Prefect tasks for pipeline status notifications
Extend with email, etc. later
"""

# TODO: Extend with email later

from __future__ import annotations

import logging
from prefect import task

logger = logging.getLogger(__name__)


@task(tags=["notifications"])
def log_pipeline_success(flow_name: str) -> None:
    """Log a succesful message. Replace with email later."""
    logger.info("Pipeline '%s' completed succesfully", flow_name)


@task(tags=["notifications"])
def log_pipeline_failure(flow_name: str, error: str) -> None:
    """Log a failure message. Raplace with email later."""
    logger.error("Pipeline '%s' FAILED: %s", flow_name, error)
