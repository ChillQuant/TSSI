"""Playwright-based scraping service for the TSSI basket.

Only lightweight symbols (asset registry, exceptions, DTOs) are re-exported
here. The Playwright-dependent pieces — :mod:`app.scraper.playwright_client`
and :mod:`app.scraper.pipeline` — must be imported explicitly from their
submodules, so non-scraper consumers (e.g. the calculation engine and the
API layer's read paths) don't transitively require Playwright to be
installed.
"""

from app.scraper.assets import ASSET_REGISTRY, AssetConfig, AssetId
from app.scraper.exceptions import (
    CaptchaDetectedError,
    ScrapeParseError,
    ScrapeTimeoutError,
    ScraperError,
)
from app.scraper.schemas import AssetObservation, IngestionReport, IngestionResult

__all__ = [
    "ASSET_REGISTRY",
    "AssetConfig",
    "AssetId",
    "AssetObservation",
    "CaptchaDetectedError",
    "IngestionReport",
    "IngestionResult",
    "ScrapeParseError",
    "ScrapeTimeoutError",
    "ScraperError",
]
