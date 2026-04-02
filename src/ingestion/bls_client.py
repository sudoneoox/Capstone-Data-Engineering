"""
Bureau of Labor Statistics API v2 client.

This is a raw requests based client (no third-party BLS wrapper).
It reads credentials and base_url from AppConfig so everything is driven by conf/apis.yml + environment variables

Docs: https://www.bls.gov/developers/api_signature_v2.htm
"""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

from src.utils.config import AppConfig, get_config

logger = logging.getLogger(__name__)


class BLSClient:
    """Thin wrapper around BLS Public Data API v2"""

    def __init__(self, config: AppConfig | None = None) -> None:
        self.cfg = config or get_config()
        meta = self.cfg.api_meta("bls")

        self.base_url: str = meta["base_url"]
        self.api_key: str | None = self.cfg.secret_from_meta("bls")

        if not self.api_key:
            logger.warning(
                "BLS_API_KEY not set - falling back to v1 limits"
                "(25 queries/day, 10 yr max, 25 series max)"
            )

    # -----------------------------------------------------------
    # INFO: Core fetch
    # -----------------------------------------------------------

    def fetch_series(
        self,
        series_ids: list[str],
        start_year: int,
        end_year: int,
        *,
        calculations: bool = True,
        annual_average: bool = True,
        catalog: bool = False,
    ) -> dict[str, Any]:
        """
        Fetch one or more BLS time-series

        Parameters
        ----------
        series_ids: up to 50 BLS series ID strings
        start_year / end_year: 4-digit years (max 20-year span on v2)
        calculations: include net/percent change columns
        annual_average: include the M13 annual-average row
        catalog: include series description metadata (limited coverage)

        Returns
        -------
        Raw JSON dict from the API (status, Results, message, etc.)
        """

        if len(series_ids) > 50:
            raise ValueError(
                f"BLS v2 allows max 50 queries per request, got {len(series_ids)}"
            )

        if end_year - start_year > 19:
            raise ValueError(
                f"BLS v2 allows max 20-year span, got {end_year - start_year + 1}"
            )

        payload: dict[str, Any] = {
            "seriesid": series_ids,
            "startyear": str(start_year),
            "endyear": str(end_year),
            "calculations": calculations,
            "annualaverage": annual_average,
            "catalog": catalog,
        }

        if self.api_key:
            payload["registrationkey"] = self.api_key

        logger.info(
            "BLS request: %d series, %s-%s", len(series_ids), start_year, end_year
        )

        resp = requests.post(self.base_url, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        status = data.get("status", "UNKNOWN")
        if status != "REQUEST_SUCCEEDED":
            logger.error(
                "BLS API returned status=%s message=%s", status, data.get("message")
            )
            raise RuntimeError(f"BLS request failed: {status} -- {data.get('message')}")

        logger.info(
            "BLS request succeeded (%d series returned)", len(data["Results"]["series"])
        )
        return data

    def fetch_series_batched(
        self,
        series_ids: list[str],
        start_year: int,
        end_year: int,
        *,
        batch_size: int = 50,
        delay_seconds: float = 1.0,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """
        Splits a large list of series IDs into batches of <= 50 and
        fetches each with a polite delay between calls.

        Returns a flat list of series dicts from Results.series across
        all batches
        """

        all_series: list[dict[str, Any]] = []

        for i in range(0, len(series_ids), batch_size):
            batch = series_ids[i : i + batch_size]
            data = self.fetch_series(batch, start_year, end_year, **kwargs)
            all_series.extend(data["Results"]["series"])

            if i + batch_size < len(series_ids):
                logger.debug("Sleeping %.1fs betwen BLS batches", delay_seconds)
                time.sleep(delay_seconds)

            logger.info("BLS batched fetch complete: %d total series", len(all_series))

        return all_series
