"""
Sample API client param builder to see how to utilize secret loading
"""

from src.utils.config import get_config


def build_fred_params(series_id: str) -> dict[str, str]:
    cfg = get_config()
    base_url = cfg.api_meta("fred")["base_url"]
    api_key = cfg.secret_from_meta("fred")

    return {
        "base_url": base_url,
        "series_id": series_id,
        "api_key": api_key or "",
        "file_type": "json",
    }
