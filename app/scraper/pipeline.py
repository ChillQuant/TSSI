"""End-to-end scraping pipeline.

:func:`run_ingestion` iterates over :data:`ASSET_REGISTRY`, fetches each
target through Playwright (with bounded retry on transient timeouts),
parses the raw text into typed values, and persists the observations inside
a single async transaction. The returned :class:`IngestionReport` is
JSON-serialisable and ready to be surfaced via the API layer.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from app.db.session import AsyncSessionLocal
from app.scraper.assets import ASSET_REGISTRY, AssetConfig
from app.scraper.exceptions import (
    CaptchaDetectedError,
    ScrapeParseError,
    ScrapeTimeoutError,
    ScraperError,
)
from app.scraper.parser import parse_price, parse_weight
from app.scraper.playwright_client import FetchResult, fetch_asset_data
from app.scraper.repository import insert_observation
from app.scraper.schemas import AssetObservation, IngestionReport, IngestionResult

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Retry-wrapped fetcher. Only ScrapeTimeoutError is considered transient —
# CAPTCHA and parse errors are returned to the caller without retries.
# -----------------------------------------------------------------------------
@retry(
    reraise=True,
    retry=retry_if_exception_type(ScrapeTimeoutError),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(3),
)
async def _fetch_with_retry(cfg: AssetConfig) -> FetchResult:
    return await fetch_asset_data(
        url=cfg.url,
        price_selector=cfg.price_selector,
        weight_selector=cfg.weight_selector,
        extra_wait_selectors=cfg.extra_selectors_to_wait,
    )


def _build_observation(
    cfg: AssetConfig,
    fetched: FetchResult,
) -> AssetObservation:
    """Convert raw Playwright output into a validated :class:`AssetObservation`."""
    price = parse_price(fetched["price_text"])

    net_weight: Decimal
    if fetched["weight_text"]:
        net_weight, parsed_unit = parse_weight(
            fetched["weight_text"], expected_unit=cfg.unit_type
        )
        logger.debug(
            "parsed weight %s%s for %s", net_weight, parsed_unit, cfg.asset_id
        )
    elif cfg.fallback_net_weight is not None:
        logger.warning(
            "weight_selector produced no text for %s; using fallback %s%s",
            cfg.asset_id,
            cfg.fallback_net_weight,
            cfg.unit_type,
        )
        net_weight = cfg.fallback_net_weight
    else:
        raise ScrapeParseError(
            f"No weight available for {cfg.asset_id} and no fallback configured"
        )

    return AssetObservation(
        time=fetched["fetched_at"],
        asset_name=str(cfg.asset_id),
        platform_source=cfg.platform_source,
        nominal_price=price,
        net_weight=net_weight,
        unit_type=cfg.unit_type,
    )


async def _scrape_one(
    cfg: AssetConfig,
    session: AsyncSession,
) -> IngestionResult:
    """Run the scrape → parse → persist flow for a single asset."""
    started = time.perf_counter()
    try:
        fetched = await _fetch_with_retry(cfg)
        observation = _build_observation(cfg, fetched)
        await insert_observation(session, observation)
    except CaptchaDetectedError as exc:
        logger.error("CAPTCHA on %s: %s", cfg.asset_id, exc)
        return _failure_result(cfg, exc, started, "captcha")
    except ScrapeTimeoutError as exc:
        logger.error("Timeout scraping %s: %s", cfg.asset_id, exc)
        return _failure_result(cfg, exc, started, "timeout")
    except ScrapeParseError as exc:
        logger.error("Parse error for %s: %s", cfg.asset_id, exc)
        return _failure_result(cfg, exc, started, "parse_error")
    except ScraperError as exc:
        logger.error("Scraper error on %s: %s", cfg.asset_id, exc)
        return _failure_result(cfg, exc, started, "scraper_error")
    except Exception as exc:  # noqa: BLE001 – we surface unknowns as errors
        logger.exception("Unexpected error scraping %s", cfg.asset_id)
        return _failure_result(cfg, exc, started, "unexpected")

    duration_ms = int((time.perf_counter() - started) * 1000)
    return IngestionResult(
        asset_id=str(cfg.asset_id),
        platform_source=cfg.platform_source,
        success=True,
        observation=observation,
        duration_ms=duration_ms,
    )


def _failure_result(
    cfg: AssetConfig,
    exc: BaseException,
    started_at: float,
    kind: str,
) -> IngestionResult:
    return IngestionResult(
        asset_id=str(cfg.asset_id),
        platform_source=cfg.platform_source,
        success=False,
        observation=None,
        error_type=kind,
        error_message=str(exc),
        duration_ms=int((time.perf_counter() - started_at) * 1000),
    )


async def run_ingestion(
    session: Optional[AsyncSession] = None,
) -> IngestionReport:
    """Scrape the whole basket once and persist the results.

    If ``session`` is provided, the caller owns the transaction lifecycle
    (useful for tests). Otherwise a fresh session is opened, committed on
    success, and rolled back on error. Per-asset failures never abort the
    run — they're recorded in the returned report.
    """
    started_at = datetime.now(tz=timezone.utc)

    if session is not None:
        results = await _run_with_session(session)
        # Caller commits.
        finished_at = datetime.now(tz=timezone.utc)
        return IngestionReport(
            started_at=started_at, finished_at=finished_at, results=results
        )

    async with AsyncSessionLocal() as owned_session:
        try:
            results = await _run_with_session(owned_session)
            await owned_session.commit()
        except Exception:
            await owned_session.rollback()
            raise

    finished_at = datetime.now(tz=timezone.utc)
    return IngestionReport(
        started_at=started_at, finished_at=finished_at, results=results
    )


async def _run_with_session(session: AsyncSession) -> list[IngestionResult]:
    # Sequential (not gather) because Playwright's Chromium instances are
    # heavyweight and the five basket assets fit comfortably inside a single
    # serialized run; this also keeps proxy-session patterns deterministic.
    results: list[IngestionResult] = []
    for cfg in ASSET_REGISTRY.values():
        results.append(await _scrape_one(cfg, session))
    return results
