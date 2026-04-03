"""
Census Bureau American Community Survey (ACS) client.

Docs: https://www.census.gov/data/developers/data-sets/acs-5year.html
"""

from __future__ import annotations

import logging
import requests
from typing import Any

from src.utils.config import AppConfig, get_config

logger = logging.getLogger(__name__)

ACS_BASE_URL = "https://api.census.gov/data"


class ACSClient:
    """Fetch ACS 5-Year Data Profile variables by metro area (CBSA)"""

    def __init__(self, config: AppConfig | None = None) -> None:
        self.cfg = config or get_config()
        self.api_key = self.cfg.secret_from_meta("acs")

        if not self.api_key:
            raise EnvironmentError("ACS_API_KEY must be set")
        ingestion = self.cfg.metadata.get("ingestion", {}).get("acs", {})
        self.vintage: str = ingestion.get("vintage", 2023)
        self.dataset: str = ingestion.get("dataset", "acs/acs5/profile")

    def fetch_metro_profiles(
        self,
        variables: list[str],
        cbsa_fips_codes: list[str],
    ) -> list[dict[str, Any]]:
        """
        Fetch ACS variables for a list of metro areas (CBSAs)

        Parameters
        ----------
        variables: ACS variable codes, e.g. ["DP03_0062E", "DP02_0068PE"]
        cbsa_fips_codes: metro FIPS codes, e.g. ["41860", "12420"]

        Returns
        -------
        List of dicts, one per metro, with variabel values + NAME
        """
        # ACS API wants comma-seperated variable list
        var_str = ",".join(["NAME"] + variables)

        # fetch all CBSAs in one call, then filter
        url = (
            f"{ACS_BASE_URL}/{self.vintage}/{self.dataset}"
            f"?get={var_str}"
            f"&for=metropolitan%20statistical%20area/micropolitan%20statistical%20area:*"
            f"&key={self.api_key}"
        )

        logger.info(
            "ACS request: %d variables, vintage=%d, all metros",
            len(variables),
            self.vintage,
        )

        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        raw = resp.json()

        # ACS returns a list-of-lists: first row is headers, rest is data
        headers = raw[0]
        rows = raw[1:]

        # convert to list of dicts
        all_metros = [dict(zip(headers, row)) for row in rows]

        # filter to requested CBSAs
        # the FIPS column name varies; it's the last column
        fips_col = headers[-1]
        filtered = [
            row for row in all_metros if row.get(fips_col) in set(cbsa_fips_codes)
        ]

        logger.info(
            "ACS: %d/%d requested metros found (out of %d total returned)",
            len(filtered),
            len(cbsa_fips_codes),
            len(all_metros),
        )

        return filtered
