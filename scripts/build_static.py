#!/usr/bin/env python3
"""Render TSSI as a static site from ``data/observations.csv`` into ``dist/``.

Pipeline
--------
1. Set ``STATIC_MODE=true`` + ``DEMO_MODE=false`` and
   ``TSSI_BASELINE_DATE`` (default **2020-01-01**, override with ``--baseline``).
2. Import the FastAPI app *after* the env is primed, clear the cached
   ``get_settings`` singleton, and install the CSV session override so
   every read goes through ``app.db.csv_session.CSVSession``.
3. Spin up a ``TestClient`` (no uvicorn, no port binding) and render:
     * ``/``                        -> ``dist/index.html``
     * ``/api/v1/tssi/current``     -> ``dist/api/v1/tssi/current.json``
     * ``/api/v1/tssi/historical``  -> ``dist/api/v1/tssi/historical.json``
     * ``/api/v1/tssi/assets/ppu``  -> ``dist/api/v1/tssi/assets/ppu.json``
4. Copy ``app/web/static`` to ``dist/static`` so the rendered HTML's
   ``<link rel="stylesheet" href="/static/...">`` references resolve on
   Cloudflare Pages.
5. Write ``dist/_redirects`` so the root ``index.html`` is the default
   landing page and deep-linked JSON endpoints keep working.

Usage::

    python scripts/build_static.py                     # full build
    python scripts/build_static.py --out /tmp/tssi     # custom output
    python scripts/build_static.py --baseline 2022-01-01  # override baseline

The output ``dist/`` directory is self-contained: every asset path is
relative to the deployment root, so ``cd dist && python -m http.server``
serves a pixel-perfect preview of the deployed site.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logger = logging.getLogger("tssi.build_static")

DEFAULT_DIST = ROOT / "dist"
DEFAULT_OBS = ROOT / "data" / "observations.csv"
STATIC_SRC = ROOT / "app" / "web" / "static"


def _today_bangkok() -> date:
    return (datetime.now(tz=timezone.utc) + timedelta(hours=7)).date()


def _prime_environment(baseline: date) -> None:
    """Set env vars the FastAPI app reads on import. MUST run before
    any ``from app.main import ...`` etc."""
    os.environ["DEMO_MODE"] = "false"
    os.environ["STATIC_MODE"] = "true"
    os.environ.setdefault("API_KEY", "static-build")
    os.environ["TSSI_BASELINE_DATE"] = baseline.isoformat()


def _write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


_APPENDIX_B_ASSETS = (
    "mahboonkrong_rice_5kg",
    "mama_tomyum_pack",
    "ezygo_kaphrao_box",
    "crystal_water_600ml",
    "m150_bottle",
)

_APPENDIX_B_ASSET_FIELDS = (
    "nominal_price_thb",
    "ppu_thb_g",
    "ppu_thb_ml",
    "net_weight_g",
    "net_weight_ml",
    "imputed_flag",
    "structural_missing",
)


def _verify_current_shape(path: Path) -> None:
    """Assert that the rendered ``/current`` JSON matches Appendix B.

    Raised errors are fatal to the static build; a malformed contract
    is never acceptable on a live deploy.
    """
    if not path.is_file():
        raise RuntimeError(f"expected current.json at {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    for top in (
        "timestamp",
        "baseline_date",
        "composite_index",
        "daily_change_pct",
        "assets",
        "metadata",
    ):
        if top not in payload:
            raise RuntimeError(f"current.json missing required field: {top}")
    assets = payload["assets"]
    if set(assets.keys()) != set(_APPENDIX_B_ASSETS):
        raise RuntimeError(
            "current.json assets mismatch. "
            f"expected={sorted(_APPENDIX_B_ASSETS)} got={sorted(assets.keys())}"
        )
    for asset_key, entry in assets.items():
        missing = [f for f in _APPENDIX_B_ASSET_FIELDS if f not in entry]
        if missing:
            raise RuntimeError(
                f"current.json[{asset_key}] missing Appendix B fields: {missing}"
            )
    metadata = payload["metadata"]
    for m in ("smoothing_applied", "status", "structural_missing"):
        if m not in metadata:
            raise RuntimeError(f"current.json.metadata missing: {m}")
    # Plan §G: explicit smoothing label check + composite-index sanity band.
    if metadata["smoothing_applied"] != "7-day rolling median":
        raise RuntimeError(
            "current.json.metadata.smoothing_applied should be "
            f"'7-day rolling median', got {metadata['smoothing_applied']!r}"
        )
    composite = payload["composite_index"]
    if composite is None or not (80.0 <= float(composite) <= 200.0):
        raise RuntimeError(
            "current.json.composite_index outside sanity band [80, 200]: "
            f"{composite}"
        )


def _verify_historical_shape(path: Path) -> None:
    """Smoke-check the historical series payload."""
    if not path.is_file():
        raise RuntimeError(f"expected historical.json at {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    for top in ("baseline_date", "baseline_composite_ppu", "points", "weights", "metadata"):
        if top not in payload:
            raise RuntimeError(f"historical.json missing required field: {top}")
    if not isinstance(payload["points"], list) or not payload["points"]:
        raise RuntimeError("historical.json.points is empty -- pipeline produced no series")
    weights = payload["weights"]
    if set(weights.keys()) != set(_APPENDIX_B_ASSETS):
        raise RuntimeError(
            "historical.json weights mismatch. "
            f"expected={sorted(_APPENDIX_B_ASSETS)} got={sorted(weights.keys())}"
        )
    weight_sum = sum(float(v) for v in weights.values())
    if abs(weight_sum - 1.0) > 1e-6:
        raise RuntimeError(
            f"historical.json weights do not sum to 1.0 (got {weight_sum:.6f})"
        )


def _copy_static(dist: Path) -> None:
    dest = dist / "static"
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(STATIC_SRC, dest)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="tssi-build-static",
        description="Pre-render the TSSI public site into dist/ for Cloudflare Pages.",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_DIST,
        help=f"Destination directory (default: {DEFAULT_DIST}).",
    )
    p.add_argument(
        "--observations",
        type=Path,
        default=DEFAULT_OBS,
        help=f"Path to observations CSV (default: {DEFAULT_OBS}).",
    )
    p.add_argument(
        "--baseline",
        default=None,
        help=(
            "TSSI baseline date (YYYY-MM-DD). Default: 2020-01-01 "
            "(index = 100 on this day; leading days backfill from first obs)."
        ),
    )
    p.add_argument(
        "--clean",
        action="store_true",
        help="Remove the output directory before building.",
    )
    p.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase logging verbosity (-v INFO, -vv DEBUG).",
    )
    return p.parse_args(argv)


def _resolve_baseline(args_baseline: Optional[str], _obs_path: Path) -> date:
    if args_baseline:
        return datetime.strptime(args_baseline, "%Y-%m-%d").date()
    return date(2020, 1, 1)


def build(
    *,
    dist: Path,
    obs_path: Path,
    baseline: date,
    clean: bool,
) -> None:
    """Render the site into ``dist``. Idempotent on re-runs."""
    if clean and dist.exists():
        shutil.rmtree(dist)
    dist.mkdir(parents=True, exist_ok=True)

    _prime_environment(baseline)

    # ---- Import the app AFTER env is primed ---------------------------------
    from app.core.config import get_settings
    get_settings.cache_clear()

    from app.main import app  # noqa: WPS433 - deliberate late import
    from app.db.csv_session import install_csv_overrides
    from fastapi.testclient import TestClient

    install_csv_overrides(app, path=obs_path)

    today = _today_bangkok()
    baseline_iso = baseline.isoformat()
    today_iso = today.isoformat()

    with TestClient(app) as client:
        # 1) Landing page
        logger.info("rendering landing page")
        r = client.get("/")
        r.raise_for_status()
        _write_bytes(dist / "index.html", r.content)

        # 1b) Methodology research-note page (full brief + live stats).
        # Written to ``methodology/index.html`` so ``/methodology`` is a
        # clean URL under Cloudflare Pages without client-side rewrites.
        logger.info("rendering methodology page")
        r = client.get("/methodology")
        r.raise_for_status()
        _write_bytes(dist / "methodology" / "index.html", r.content)

        # 2) JSON API mirrors — same shape that app/api/v1/tssi.py returns.
        endpoints = {
            "api/v1/tssi/current.json": ("/api/v1/tssi/current", None),
            "api/v1/tssi/historical.json": (
                "/api/v1/tssi/historical",
                {"start_date": baseline_iso, "end_date": today_iso},
            ),
            "api/v1/tssi/historical_verified.json": (
                "/api/v1/tssi/historical",
                {
                    "start_date": baseline_iso,
                    "end_date": today_iso,
                    "verified_only": "true",
                },
            ),
            "api/v1/tssi/assets/ppu.json": (
                "/api/v1/tssi/assets/ppu",
                {"start_date": baseline_iso, "end_date": today_iso},
            ),
        }
        for rel_path, (url, params) in endpoints.items():
            logger.info("rendering %s", url)
            resp = client.get(url, params=params)
            resp.raise_for_status()
            _write_text(dist / rel_path, resp.text)

        # 3) Meta endpoint (useful for uptime checks even on a static host)
        logger.info("rendering /health.json")
        resp = client.get("/health")
        resp.raise_for_status()
        _write_text(dist / "health.json", resp.text)

    # ---- Shape verification on the JSON mirrors --------------------------
    # Fail the build early if the Appendix B contract is broken, rather
    # than publishing a quietly malformed payload to Cloudflare.
    _verify_current_shape(dist / "api/v1/tssi/current.json")
    _verify_historical_shape(dist / "api/v1/tssi/historical.json")
    logger.info("shape verification passed")

    # ---- Static assets ---------------------------------------------------
    logger.info("copying static assets")
    _copy_static(dist)

    # ---- Cloudflare Pages routing helpers --------------------------------
    # ``_redirects`` keeps pre-2020 / legacy paths resolving to the root.
    # Cloudflare honors the Netlify-compatible format.
    _write_text(
        dist / "_redirects",
        "# TSSI static build routing\n"
        "/api /api/v1/tssi/current.json 302\n"
        "/api/v1/tssi /api/v1/tssi/current.json 302\n",
    )
    # Tell crawlers the site is publicly indexable.
    _write_text(
        dist / "robots.txt",
        "User-agent: *\nAllow: /\n",
    )

    logger.info("build complete: %s", dist)


def main() -> None:
    args = _parse_args(sys.argv[1:])

    level = logging.WARNING
    if args.verbose == 1:
        level = logging.INFO
    elif args.verbose >= 2:
        level = logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
    )

    obs_path: Path = args.observations.resolve()
    baseline = _resolve_baseline(args.baseline, obs_path)
    dist: Path = args.out.resolve()

    logger.info(
        "build target=%s observations=%s baseline=%s", dist, obs_path, baseline
    )

    build(dist=dist, obs_path=obs_path, baseline=baseline, clean=args.clean)


if __name__ == "__main__":
    main()
