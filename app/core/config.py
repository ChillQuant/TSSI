"""Typed application settings loaded from environment / .env file."""

from __future__ import annotations

from datetime import date
from functools import lru_cache
from typing import Optional

from pydantic import Field, SecretStr, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Global settings singleton. See .env.example for the full surface."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ---- Application ------------------------------------------------------
    app_env: str = Field(default="development")
    app_host: str = Field(default="0.0.0.0")
    app_port: int = Field(default=8000)
    log_level: str = Field(default="info")
    api_key: SecretStr = Field(default=SecretStr("change-me-in-prod"))

    # ---- Database ---------------------------------------------------------
    postgres_host: str = Field(default="timescaledb")
    postgres_port: int = Field(default=5432)
    postgres_db: str = Field(default="tssi")
    postgres_user: str = Field(default="tssi")
    postgres_password: SecretStr = Field(default=SecretStr("tssi_dev_password"))

    db_pool_size: int = Field(default=5)
    db_max_overflow: int = Field(default=10)
    db_echo: bool = Field(default=False)

    # ---- Scraper / Proxy --------------------------------------------------
    proxy_server: Optional[str] = Field(default=None)
    proxy_username: Optional[str] = Field(default=None)
    proxy_password: Optional[SecretStr] = Field(default=None)
    scraper_headless: bool = Field(default=True)
    scraper_timeout_ms: int = Field(default=30_000)

    # ---- Index ------------------------------------------------------------
    tssi_baseline_date: date = Field(default=date(2020, 1, 1))

    # ---- Demo mode --------------------------------------------------------
    # When true, the DB session dependency is swapped for a synthetic in-memory
    # feed and the baseline date is auto-anchored to 119 days ago. Lets the
    # public site render end-to-end without Docker / TimescaleDB for demos.
    demo_mode: bool = Field(default=False)

    # ---- Derived DSNs -----------------------------------------------------
    @computed_field  # type: ignore[misc]
    @property
    def async_database_url(self) -> str:
        """DSN consumed by SQLAlchemy's async engine + asyncpg."""
        pwd = self.postgres_password.get_secret_value()
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{pwd}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @computed_field  # type: ignore[misc]
    @property
    def sync_database_url(self) -> str:
        """DSN used by Alembic for migrations (psycopg2)."""
        pwd = self.postgres_password.get_secret_value()
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{pwd}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached settings accessor. Import this everywhere instead of instantiating."""
    return Settings()
