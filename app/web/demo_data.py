"""Synthetic demo data for offline / no-DB rendering of the public site.

Generates a multi-year, era-aware time-series of plausible Thai staple
price-per-unit readings so the public page can render end-to-end without
TimescaleDB. The trajectory is piecewise-linear across four regimes that
mirror real Thai retail dynamics from 2020 onward:

    * 2020-01 to 2020-06  — pre-shock steady baseline (x1.00)
    * 2020-07 to 2021-12  — mild deflation from demand slump + promo wars
                             (x0.97 trough)
    * 2022-01 to 2023-12  — inflation resurgence after the Ukraine shock
                             (x1.06)
    * 2024-01 to today    — shrinkflation era with per-asset differentiation
                             (x1.10 Mama / x1.12 M-150 / x1.14 EZYGO)

On top of the regime skeleton we add a small amount of gaussian noise and
deliberate "missed scrape" gaps (scaled with the window length) so the
forward-fill code path is still exercised visibly in the UI.

The window length is configurable via ``DEMO_WINDOW_DAYS`` (default 2200,
~6 years). A fake async ``Session`` is installed as the ``get_session``
dependency override when ``DEMO_MODE=true``.
"""

from __future__ import annotations

import logging
import os
import random
from bisect import bisect_right
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import FastAPI

from app.backfill.base import classify_platform_source
from app.db.session import get_session

logger = logging.getLogger(__name__)

# Default: ~6 years. Override with the ``DEMO_WINDOW_DAYS`` env var.
DEMO_WINDOW_DAYS_DEFAULT = 2200


def _today_bangkok() -> date:
    """Today's date in Asia/Bangkok (UTC+7, no DST)."""
    return (datetime.now(tz=timezone.utc) + timedelta(hours=7)).date()


def resolve_window_days() -> int:
    """Parse ``DEMO_WINDOW_DAYS`` from the environment with a sane fallback."""
    raw = os.environ.get("DEMO_WINDOW_DAYS", "").strip()
    if not raw:
        return DEMO_WINDOW_DAYS_DEFAULT
    try:
        parsed = int(raw)
    except ValueError:
        logger.warning(
            "invalid DEMO_WINDOW_DAYS=%r, falling back to default %d",
            raw,
            DEMO_WINDOW_DAYS_DEFAULT,
        )
        return DEMO_WINDOW_DAYS_DEFAULT
    # Clamp to a useful range: one week minimum, ~20 years maximum.
    return max(7, min(parsed, 7300))


# -----------------------------------------------------------------------------
# Era model
#
# Baseline PPUs anchored to January 1, 2020 — the earliest point at which all
# five basket SKUs were plausibly on the market at recognizable prices.
# Going further back crosses into fantasy for EZYGO Kaphrao specifically, so
# we cap the "earliest possible" at 2020-01-01 in honest demos.
# -----------------------------------------------------------------------------
_ERA_BASELINE_DATE = date(2020, 1, 1)

_BASELINE_PPU_2020: dict[str, float] = {
    "mahboonkrong_rice_5kg": 180.0 / 5000.0,   # = 0.03600 THB/g
    "mama_tomyum_pack": 6.0 / 55.0,            # ≈ 0.10909 THB/g
    "ezygo_kaphrao_box": 60.0 / 210.0,         # ≈ 0.28571 THB/g
    "crystal_water_600ml": 6.0 / 600.0,        # = 0.01000 THB/ml
    "m150_bottle": 10.0 / 150.0,               # ≈ 0.06667 THB/ml
}

# Multipliers shared across all assets for the first three regimes.
_ERA_ANCHORS_COMMON: list[tuple[date, float]] = [
    (date(2020, 1, 1),  1.00),
    (date(2020, 6, 30), 1.00),
    (date(2021, 12, 31), 0.97),
    (date(2023, 12, 31), 1.06),
]

# Per-asset "today" multiplier — the divergence that makes the UI story
# interesting. Matches the directional ordering of the brief's Appendix A.4.
_ERA_ANCHORS_END: dict[str, float] = {
    "mahboonkrong_rice_5kg": 1.155,   # +15.5% brief target
    "mama_tomyum_pack": 1.0921,       # +9.21%
    "ezygo_kaphrao_box": 1.1421,      # +14.21%
    "crystal_water_600ml": 1.098,     # +9.8%
    "m150_bottle": 1.1178,            # +11.78%
}

# Per-asset live-scrape platform attribution.
_PLATFORM: dict[str, str] = {
    "mahboonkrong_rice_5kg": "lazada",
    "mama_tomyum_pack": "lazada",
    "ezygo_kaphrao_box": "7eleven",
    "crystal_water_600ml": "shopee",
    "m150_bottle": "shopee",
}

# Cutoff between "historical backfill" and "live scrape" in demo mode.
# Days on or after this offset from today are treated as live scrapes;
# days before are treated as Wayback/seed/CPI-derived rows so the public
# site's Source Mix panel has a realistic provenance story to show.
_LIVE_WINDOW_DAYS = 180


def _provenance_for(day: date, asset: str, today: date, rng: random.Random) -> str:
    """Assign a realistic ``platform_source`` to a synthetic row.

    Era mapping:
      * last ``_LIVE_WINDOW_DAYS`` days → live platform slug (``lazada`` etc.)
      * earlier days:
          - 1st of each month → ``seed:manual`` (hand-curated anchor)
          - 15th of each month → ``derived:bot_cpi`` (CPI fill-in)
          - everything else → ``wayback:<platform>`` (archive snapshot)
    """
    platform = _PLATFORM[asset]
    if (today - day).days < _LIVE_WINDOW_DAYS:
        return platform
    if day.day == 1:
        return "seed:manual"
    if day.day == 15:
        return "derived:bot_cpi"
    # A small amount of jitter keeps the bar chart from looking mechanical.
    if rng.random() < 0.02:
        return "seed:manual"
    return f"wayback:{platform}"


def _bangkok_noon_utc(d: date) -> datetime:
    """09:00 Asia/Bangkok for day ``d``, in UTC."""
    local = datetime(d.year, d.month, d.day, 9, 0, tzinfo=timezone.utc)
    return local - timedelta(hours=7)


def _anchors_for(asset: str, today: date) -> list[tuple[date, float]]:
    """Era anchor list extended to today with this asset's end multiplier."""
    end_mult = _ERA_ANCHORS_END[asset]
    # If "today" is before the final shared anchor, just stop there.
    if today <= _ERA_ANCHORS_COMMON[-1][0]:
        return list(_ERA_ANCHORS_COMMON)
    return [*_ERA_ANCHORS_COMMON, (today, end_mult)]


def _interp_multiplier(day: date, anchors: list[tuple[date, float]]) -> float:
    """Piecewise-linear multiplier interpolation over ``anchors``.

    Days before the first anchor clamp to the first multiplier; days after
    the last anchor clamp to the last multiplier. Between anchors we
    interpolate on the calendar-day difference so era widths are honored
    honestly (a one-year regime really drifts over a year, not over one
    generator step).
    """
    if not anchors:
        return 1.0
    if day <= anchors[0][0]:
        return anchors[0][1]
    if day >= anchors[-1][0]:
        return anchors[-1][1]

    # Find the surrounding anchor pair via binary search on dates.
    dates_only = [a[0] for a in anchors]
    idx = bisect_right(dates_only, day)
    d0, m0 = anchors[idx - 1]
    d1, m1 = anchors[idx]
    span_days = (d1 - d0).days
    if span_days <= 0:
        return m1
    frac = (day - d0).days / span_days
    return m0 + (m1 - m0) * frac


def generate_rows(
    days: Optional[int] = None,
    seed: int = 1729,
) -> list[dict[str, Any]]:
    """Return a list of ``{day, asset_name, ppu}`` rows spanning ``days`` days.

    Output schema matches what :func:`app.calc.engine.fetch_daily_ppu`
    receives from the real database.
    """
    days = days if days is not None else resolve_window_days()
    days = max(7, days)
    rng = random.Random(seed)
    today = _today_bangkok()
    start = today - timedelta(days=days - 1)

    # ~1 missed scrape per asset per month, scaled to the window. Skips are
    # deterministic under the seed so the rendered page is stable.
    skip_count = max(2, days // 30)
    skip_pool = list(range(5, days - 5))
    if not skip_pool:  # pragma: no cover - tiny windows
        skip_pool = list(range(days))

    skip_days: dict[str, set[int]] = {
        asset: set(rng.sample(skip_pool, min(skip_count, len(skip_pool))))
        for asset in _BASELINE_PPU_2020
    }

    rows: list[dict[str, Any]] = []
    for i in range(days):
        day = start + timedelta(days=i)
        for asset, baseline_ppu in _BASELINE_PPU_2020.items():
            if i in skip_days[asset]:
                continue
            anchors = _anchors_for(asset, today)
            mult = _interp_multiplier(day, anchors)
            trend = baseline_ppu * mult
            # ±0.8% gaussian noise around the trend, clamped positive. Noise
            # is deliberately era-independent so the signal-vs-noise on a
            # long chart still reads as trend, not jitter.
            noisy = trend + rng.gauss(0, trend * 0.008)
            ppu = max(1e-4, noisy)
            platform_source = _provenance_for(day, asset, today, rng)
            rows.append(
                {
                    "day": day,
                    "time": _bangkok_noon_utc(day),
                    "asset_name": asset,
                    "ppu": float(ppu),
                    "platform_source": platform_source,
                }
            )
    return rows


# -----------------------------------------------------------------------------
# Fake async Session that mimics the one query the calc engine runs
# -----------------------------------------------------------------------------
class _FakeMappings:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class _FakeResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> _FakeMappings:
        return _FakeMappings(self._rows)


class _FakeSession:
    """Implements just enough of ``AsyncSession`` for the calc engine path."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._all = rows

    async def execute(
        self, stmt: Any, params: Optional[dict[str, Any]] = None
    ) -> _FakeResult:
        if params and "start_date" in params and "end_date" in params:
            s = params["start_date"]
            e = params["end_date"]
            window = [r for r in self._all if s <= r["day"] <= e]
        else:
            window = self._all

        # Branch on the SQL text to serve both the PPU feed and the
        # source-mix aggregation from the same in-memory row set. This
        # keeps the fake session compatible with every calc-engine
        # helper without leaking knowledge of specific bind names.
        sql_text = str(stmt) if stmt is not None else ""

        if "source_kind" in sql_text:
            return _FakeResult(self._source_mix(window, sql_text))

        if "NOT LIKE 'derived:%'" in sql_text:
            window = [
                r for r in window
                if not r.get("platform_source", "").startswith("derived:")
            ]

        # Default: PPU feed. The calc engine consumes
        # ``(day, asset_name, ppu)`` tuples; platform_source rides along
        # harmlessly for anyone who cares to inspect it.
        return _FakeResult(window)

    @staticmethod
    def _source_mix(
        window: list[dict[str, Any]], sql_text: str
    ) -> list[dict[str, Any]]:
        """Emulate the per-year, per-bucket COUNT aggregation."""
        counts: dict[tuple[int, str], int] = defaultdict(int)
        for row in window:
            ps = row.get("platform_source") or ""
            day: date = row["day"]
            kind = str(classify_platform_source(ps))
            counts[(day.year, kind)] += 1
        out = [
            {"year": year, "source_kind": kind, "row_count": count}
            for (year, kind), count in sorted(counts.items())
        ]
        return out

    async def commit(self) -> None:  # pragma: no cover - no-op
        pass

    async def rollback(self) -> None:  # pragma: no cover - no-op
        pass


def install_demo_overrides(app: FastAPI, *, days: Optional[int] = None) -> None:
    """Swap the DB session dependency for a synthetic in-memory feed."""
    days = days if days is not None else resolve_window_days()
    rows = generate_rows(days=days)
    logger.info(
        "demo mode: generated %d synthetic PPU observations across %d days",
        len(rows),
        days,
    )

    async def _session_dep():
        yield _FakeSession(rows)

    app.dependency_overrides[get_session] = _session_dep


__all__ = [
    "DEMO_WINDOW_DAYS_DEFAULT",
    "generate_rows",
    "install_demo_overrides",
    "resolve_window_days",
]
