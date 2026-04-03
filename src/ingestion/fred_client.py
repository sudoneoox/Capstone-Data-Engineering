"""
FRED API client.

Uses the 'fredapi' package rather than raw requests,
since fredapi already handles pagination, vintage dates, and returns pandas
objects directly.

Docs: https://fred.stlouisfed.org/docs/api/fred/
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from fredapi import Fred

from src.utils.config import AppConfig, get_config

logger = logging.getLogger(__name__)


class FREDClient:
    """Wrapper around fredapi.Fred that reads credentials from AppConfig."""

    def __init__(self, config: AppConfig | None = None) -> None:
        self.cfg = config or get_config()
        api_key = self.cfg.secret_from_meta("fred")

        if not api_key:
            raise EnvironmentError("FRED_API_KEY must be set")

        self.fred = Fred(api_key=api_key)

    def get_series(
        self,
        series_id: str,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.Series:
        """
        Fetch a single FRED series as a pandas Series with a DatetimeIndex.

        Parameters
        ----------
        series_id : e.g. "UNRATE", "JTSJOL", "CPIAUCSL"
        start / end : "YYYY-MM-DD" strings (optional)
        """

        logger.info(
            "FRED fetch: %s (%s -> %s)", series_id, start or "earliest", end or "latest"
        )
        result = self.fred.get_series(
            series_id, observation_start=start, observation_end=end
        )
        logger.info("FRED %s: %d observations returned", series_id, len(result))
        return result

    def get_multiple_series(
        self,
        series_ids: list[str],
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """
        Fetch multiple FRED series and merge into a DataFrame.
        Columns are named by series_id
        """

        frames: dict[str, pd.Series] = {}
        for sid in series_ids:
            try:
                frames[sid] = self.get_series(sid, start=start, end=end)
            except Exception:
                logger.exception("Failed to fetch FRED series %s -- skipping", sid)

        df = pd.DataFrame(frames)
        logger.info(
            "FRED multi-fetch: %d/%d series, %d rows",
            len(frames),
            len(series_ids),
            len(df),
        )

        return df

    def search(self, text: str, limit: int = 20) -> pd.DataFrame:
        """Full-text search for FRED series. Useful for discovery"""
        logger.debug("FRED search: %r (limit=%d)", text, limit)
        return self.fred.search(text).head(limit)
