"""
Adzuna job search API client

Raw 'requests'-based - no third-party wrapper
Reads app_id / app_key from AppConfig -> conf/apis.yml env references

Docs: https://developer.adzuna.com/overview
"""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

from src.utils.config import AppConfig, get_config

logger = logging.getLogger(__name__)


class AdzunaClient:
    """Thin wrapper around Adzuna job search API v1."""

    def __init__(self, config: AppConfig | None = None, country: str = "us") -> None:
        self.cfg = config or get_config()
        meta = self.cfg.api_meta("adzuna")

        # base_url in apis.yml is the stem withot /{page}
        self.base_url: str = meta["base_url"].rstrip("/")
        self.country = country

        self.app_id: str | None = self.cfg.secret_from_meta(
            "adzuna", field="app_id_env"
        )
        self.app_key: str | None = self.cfg.secret_from_meta(
            "adzuna", field="api_key_env"
        )

        if not self.app_id or not self.app_key:
            raise EnvironmentError("ADZUNA_APP_ID and ADZUNA_API_KEY must both be set")

    def search_jobs(
        self,
        what: str,
        where: str = "",
        page: int = 1,
        results_per_page: int = 50,
        *,
        sort_by: str = "date",
        max_days_old: int | None = 30,
    ) -> dict[str, Any]:
        """
        Search Adzuna job listings - single page.

        Parameters
        ----------
        what: keyword query, e.g. "data engineer"
        where: location, e.g. "new york" or a zip code
        page: 1-indexed page number
        results_per_page: max 50
        sort_by: "date" | "relevance" | "salary"
        max_days_old: only return postings newer than N days

        Returns
        -------
        Raw JSON dict with 'results', 'count', 'mean', etc.
        """

        url = f"{self.base_url}/{page}"

        params: dict[str, Any] = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "what": what,
            "results_per_page": results_per_page,
            "sort_by": sort_by,
            "content-type": "application/json",
        }

        if where:
            params["where"] = where
        if max_days_old is not None:
            params["max_days_old"] = max_days_old

        logger.debug("Adzuna request: page=%d what=%r where=%r", page, what, where)

        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        return resp.json()

    def search_jobs_all_pages(
        self,
        what: str,
        where: str = "",
        *,
        max_pages: int = 10,
        results_per_page: int = 50,
        delay_seconds: float = 0.5,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """
        Paginate through Adzuna search results.

        Returns a flat list of job dicts accross all fetched pages.
        Stops early if a page returns fewer results than requested
        (indicating end of results).
        """
        all_jobs: list[dict[str, Any]] = []

        for page in range(1, max_pages + 1):
            data = self.search_jobs(
                what, where, page=page, results_per_page=results_per_page, **kwargs
            )
            results = data.get("results", [])
            all_jobs.extend(results)

            logger.info(
                "Adzuna page %d: %d results (total so far: %d)",
                page,
                len(results),
                len(all_jobs),
            )

            # Fewer than a full page -> we've exhausted results
            if len(results) < results_per_page:
                break

            if page < max_pages:
                time.sleep(delay_seconds)

        logger.info("Adzuna fetch complete: %d total jobs for %r", len(all_jobs), what)

        return all_jobs
