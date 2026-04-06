from __future__ import annotations
from databricks.sdk import WorkspaceClient
from pathlib import Path

from databricks.sdk.service import catalog
from src.utils.config import ROOT, get_config
import os
import logging

DATAB_CATALOG = "analytics"
LANDING_SCHEMA = "landing"
VOLUME_NAME = "api_ingestion"
logger = logging.getLogger(__name__)


def upload_to_databricks():
    cfg = get_config()
    DATABRICKS_TOKEN = cfg.secret_by_env_name("DATABRICKS_TOKEN")
    DATABRICKS_HOST = cfg.secret_by_env_name("DATABRICKS_HOST")
    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        raise EnvironmentError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")

    w = WorkspaceClient(token=DATABRICKS_TOKEN, host=DATABRICKS_HOST)

    # Create schema if it does not already exist
    try:
        w.schemas.create(name=LANDING_SCHEMA, catalog_name=DATAB_CATALOG)
    except Exception:
        logger.info(
            "Databricks schema %s already exists in %s -- skipping",
            LANDING_SCHEMA,
            DATAB_CATALOG,
        )
        pass

    # Create volume if it does not already exist
    try:
        w.volumes.create(
            catalog_name=DATAB_CATALOG,
            schema_name=LANDING_SCHEMA,
            name=VOLUME_NAME,
            volume_type=catalog.VolumeType.MANAGED,
        )
    except Exception:
        logger.info(
            "Databricks volume %s already exists in %s/%s -- skipping",
            VOLUME_NAME,
            DATAB_CATALOG,
            LANDING_SCHEMA,
        )
        pass

    LOCAL_ROOT: Path = ROOT / "data" / "api_raw"
    VOLUME_PATH = f"/Volumes/{DATAB_CATALOG}/{LANDING_SCHEMA}/{VOLUME_NAME}"

    for local_file in LOCAL_ROOT.rglob("*"):
        if local_file.is_file():
            relative_path = local_file.relative_to(LOCAL_ROOT)
            remote_path = os.path.join(VOLUME_PATH, relative_path).replace("\\", "/")

            logger.info("Uploading %s to %s", local_file, remote_path)

            with local_file.open("rb") as f:
                w.files.upload(remote_path, f, overwrite=True)

    logger.info("Ingestion to Landing Zone Complete")
