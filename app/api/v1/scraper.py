"""Protected scraper-trigger endpoint.

Runs the full Playwright ingestion pipeline for the basket on demand.
Single-flight semantics are enforced via a module-level ``asyncio.Lock``:
if another trigger is already running, new requests short-circuit to a
``409 Conflict`` instead of spawning a second browser concurrently.
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.deps import require_api_key
from app.scraper.pipeline import run_ingestion
from app.scraper.schemas import IngestionReport

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/scraper", tags=["scraper"])

# Prevents two concurrent scraper runs from colliding inside a single
# process — important because Playwright launches real Chromium subprocesses.
_ingestion_lock = asyncio.Lock()


@router.post(
    "/trigger",
    response_model=IngestionReport,
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(require_api_key)],
    summary="Run the scraping pipeline once, synchronously",
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Missing or invalid API key."},
        status.HTTP_409_CONFLICT: {"description": "Another ingestion is already running."},
    },
)
async def trigger_scrape() -> IngestionReport:
    if _ingestion_lock.locked():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="An ingestion run is already in progress.",
        )

    async with _ingestion_lock:
        logger.info("scraper trigger: starting ingestion run")
        report = await run_ingestion()
        logger.info(
            "scraper trigger: finished – %d ok, %d failed",
            report.success_count,
            report.failure_count,
        )
        return report
