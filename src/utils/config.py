"""
Config File that loads settings from environment variables and dotenv/secrets sources
"""

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


ROOT = Path(__file__).resolve().parents[2]
CONF_DIR = ROOT / "conf"


class Settings(BaseSettings):
    """
    Secrets + runtime environment settings.

    Values come from the actual process environment.
    In local/dev usage, that environment should be created by:
      - op run --env-file=.env.secrets -- <command>
    or
      - op inject -i .env.secrets -o .env.runtime, then load/export it before running Python.
    """

    model_config = SettingsConfigDict(
        env_file=None,
        extra="ignore",
        case_sensitive=False,
    )

    app_env: str = Field(default="dev", alias="APP_ENV")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    bls_api_key: SecretStr | None = Field(default=None, alias="BLS_API_KEY")
    adzuna_app_id: SecretStr | None = Field(default=None, alias="ADZUNA_APP_ID")
    adzuna_api_key: SecretStr | None = Field(default=None, alias="ADZUNA_API_KEY")
    fred_api_key: SecretStr | None = Field(default=None, alias="FRED_API_KEY")
    acs_api_key: SecretStr | None = Field(default=None, alias="ACS_API_KEY")

    databricks_host: str | None = Field(default=None, alias="DATABRICKS_HOST")
    databricks_token: SecretStr | None = Field(default=None, alias="DATABRICKS_TOKEN")


@dataclass(frozen=True)
class AppConfig:
    settings: Settings
    metadata: dict[str, Any]

    def api_meta(self, name: str) -> dict[str, Any]:
        try:
            return self.metadata["apis"][name]
        except KeyError as exc:
            raise KeyError(f"Unknown API metadata entry: {name}") from exc

    def secret_by_env_name(self, env_var_name: str) -> str | None:
        value = os.getenv(env_var_name)
        return value

    def secret_from_meta(self, api_name: str, field: str = "api_key_env") -> str | None:
        meta = self.api_meta(api_name)
        env_var_name = meta.get(field)
        if not env_var_name:
            return None
        return os.getenv(env_var_name)


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


@lru_cache(maxsize=1)
def get_metadata() -> dict[str, Any]:
    return _load_yaml(CONF_DIR / "apis.yml")


@lru_cache(maxsize=1)
def get_config() -> AppConfig:
    return AppConfig(
        settings=get_settings(),
        metadata=get_metadata(),
    )
