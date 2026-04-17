"""FastAPI application entrypoint.

Wires together:
* The v1 JSON API (`/api/v1/...`) — tssi + scraper routers.
* The public website (`/`) — server-rendered Jinja2 page backed by the
  same calc engine as the API, with a ``DEMO_MODE`` toggle that swaps in
  synthetic data so the page can render without TimescaleDB present.
* Static assets at `/static`.
* `/health` (cheap liveness) and `/ready` (DB round-trip).
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, status
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text

from app import __version__
from app.api.v1.router import api_router
from app.core.config import get_settings
from app.db.session import AsyncSessionLocal, dispose_engine

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Demo-mode env priming
#
# When DEMO_MODE is enabled we want the public page to show a multi-year
# synthetic window ending today. The calc engine normalizes against
# ``settings.tssi_baseline_date``, so we auto-anchor the baseline to the
# first day of the synthetic window BEFORE Settings is instantiated. This
# block runs at import time so the already-cached settings singleton picks
# up the adjusted env when the app is instantiated below.
# -----------------------------------------------------------------------------
def _demo_env_enabled() -> bool:
    return os.environ.get("DEMO_MODE", "").strip().lower() in {"1", "true", "yes", "on"}


def _static_env_enabled() -> bool:
    """Is the process running against data/observations.csv (no DB)?"""
    return os.environ.get("STATIC_MODE", "").strip().lower() in {"1", "true", "yes", "on"}


# STATIC_MODE does not override ``TSSI_BASELINE_DATE``: the default in
# ``Settings`` is 2020-01-01. The calc engine bridges Jan 1–14 to the first
# CSV observation via leading-edge bfill, so the index can anchor on the
# contractual baseline even when the earliest row is later (e.g. 2020-01-15).
# Set ``TSSI_BASELINE_DATE`` explicitly if you need a different anchor.

if _demo_env_enabled():
    # Local import so the regular (non-demo) boot path never has to load the
    # Jinja2/web subtree just to read an env var.
    from app.web.demo_data import resolve_window_days as _resolve_window_days

    _today_bkk = (datetime.now(tz=timezone.utc) + timedelta(hours=7)).date()
    _demo_days = _resolve_window_days()
    _demo_baseline = _today_bkk - timedelta(days=_demo_days - 1)
    # Only override if the user hasn't explicitly set a baseline themselves.
    os.environ.setdefault("TSSI_BASELINE_DATE", _demo_baseline.isoformat())
    # A module-level import above (``app.db.session``) already triggered the
    # cached ``get_settings()`` call while reading the pre-adjusted env. Clear
    # that cache so the next read picks up the demo baseline.
    get_settings.cache_clear()


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Startup: verify we can talk to TimescaleDB. Skip when STATIC_MODE — there is
    # no real DB (CSV shim), so probing only produces noisy SSL/DNS warnings on
    # hosts like Vercel during ``build_static.py``.
    if not _static_env_enabled():
        try:
            async with AsyncSessionLocal() as session:
                await session.execute(text("SELECT 1"))
            logger.info("startup db probe: ok")
        except Exception as exc:  # noqa: BLE001
            logger.warning("startup db probe failed: %s", exc)
    try:
        yield
    finally:
        if not _static_env_enabled():
            await dispose_engine()


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="Thai Street Survival Index (TSSI) API",
        version=__version__,
        description=(
            "Proprietary real-time shrinkflation index for Thailand, built on "
            "a five-asset PPU micro-basket (Mahboonkrong Jasmine Rice, Mama "
            "Tom Yum Koong, 7-Eleven EZYGO Kaphrao, Crystal Drinking Water, "
            "M-150) mapped to the NSO Household Socio-Economic Survey (COICOP "
            "2018). Part of the PlainFin data ecosystem."
        ),
        lifespan=lifespan,
    )

    # ---- JSON API --------------------------------------------------------
    app.include_router(api_router)

    # ---- Public website --------------------------------------------------
    from app.web.router import router as web_router  # local to avoid boot-time cycles

    app.include_router(web_router)

    static_dir = Path(__file__).resolve().parent / "web" / "static"
    if static_dir.is_dir():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
    else:  # pragma: no cover - defensive
        logger.warning("static dir not found at %s", static_dir)

    # ---- Demo-mode dependency override ----------------------------------
    if settings.demo_mode:
        from app.web.demo_data import install_demo_overrides

        install_demo_overrides(app)
        logger.info(
            "DEMO_MODE active: swapping DB session for in-memory synthetic feed "
            "(baseline=%s)",
            settings.tssi_baseline_date,
        )

    # ---- Static-mode dependency override (CSV-backed) -------------------
    if _static_env_enabled():
        from app.db.csv_session import install_csv_overrides

        install_csv_overrides(app)
        logger.info(
            "STATIC_MODE active: swapping DB session for data/observations.csv "
            "(baseline=%s)",
            settings.tssi_baseline_date,
        )

    # ---- Meta ------------------------------------------------------------
    @app.get("/health", tags=["meta"])
    async def health() -> dict[str, str]:
        return {"status": "ok", "env": settings.app_env, "version": __version__}

    @app.get("/ready", tags=["meta"])
    async def ready() -> JSONResponse:
        try:
            async with AsyncSessionLocal() as session:
                await session.execute(text("SELECT 1"))
        except Exception as exc:  # noqa: BLE001
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "unavailable", "detail": str(exc)},
            )
        return JSONResponse(content={"status": "ready"})

    return app


app = create_app()
