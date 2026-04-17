"""Public landing page for the Thai Street Survival Index.

Server-renders a single page that looks like a purpose-built country index
dashboard: hero with the current composite value, five basket cards, a
time-series chart, a recent daily-values table, and the methodology. All
numbers are computed from the same calc engine that powers the REST API,
so the HTML view and the JSON view always agree.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from app.calc.engine import (
    BASKET_WEIGHTS,
    compute_index_series,
    fetch_asset_ppu_series,
    fetch_source_mix,
)
from app.calc.schemas import AssetDailyPPU, IndexResponse
from app.core.config import Settings, get_settings
from app.db.session import get_session
from app.scraper.assets import ASSET_REGISTRY, AssetId

router = APIRouter()

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Presentation metadata for each basket member. Kept separate from the
# AssetConfig so we can tweak visuals without touching the scrape config.
_ASSET_META: dict[AssetId, dict[str, str]] = {
    AssetId.MAHBOONKRONG_RICE_5KG: {
        "display_name": "Mahboonkrong Jasmine Rice",
        "tagline": "5 kg raw staple",
        "platform": "Lazada",
        "emoji": "🌾",
        "color": "#9A7B34",  # wheat
        "unit": "g",
    },
    AssetId.MAMA_TOMYUM_PACK: {
        "display_name": "Mama Tom Yum Koong",
        "tagline": "Instant noodles",
        "platform": "Lazada",
        "emoji": "🍜",
        "color": "#F59E0B",  # amber
        "unit": "g",
    },
    AssetId.EZYGO_KAPHRAO_BOX: {
        "display_name": "7-Eleven EZYGO Kaphrao",
        "tagline": "Chilled rice box",
        "platform": "7-Eleven",
        "emoji": "🌶️",
        "color": "#10B981",  # emerald
        "unit": "g",
    },
    AssetId.CRYSTAL_WATER_600ML: {
        "display_name": "Crystal Drinking Water",
        "tagline": "600 ml PET bottle",
        "platform": "Shopee",
        "emoji": "💧",
        "color": "#2E8BCC",  # sky
        "unit": "ml",
    },
    AssetId.M150_BOTTLE: {
        "display_name": "M-150",
        "tagline": "Energy drink 150 ml",
        "platform": "Shopee",
        "emoji": "⚡",
        "color": "#EF4444",  # red
        "unit": "ml",
    },
}


def _today_bangkok() -> date:
    return (datetime.now(tz=timezone.utc) + timedelta(hours=7)).date()


def _fmt(n: float | None, places: int = 4) -> str:
    return f"{n:.{places}f}" if isinstance(n, (int, float)) else "—"


def _delta_class(value: float | None) -> str:
    if value is None:
        return "neutral"
    if value > 0:
        return "up"
    if value < 0:
        return "down"
    return "neutral"


def _arrow(value: float | None) -> str:
    if value is None or value == 0:
        return "→"
    return "▲" if value > 0 else "▼"


# Upper bound on how many points we ship to Chart.js. Beyond this we thin
# by striding so long windows (years) render crisply without exploding the
# HTML payload or client-side render cost. The table still shows raw daily
# values for the last 60 days; only the chart gets thinned.
_CHART_MAX_POINTS = 500

# Stable order + display metadata for the source-mix panel.
_SOURCE_KIND_ORDER: tuple[str, ...] = ("scrape", "archive", "seed", "derived")
_SOURCE_KIND_META: dict[str, dict[str, str]] = {
    "scrape":  {"label": "Live scrape",    "color": "#B00020"},
    "archive": {"label": "Wayback archive", "color": "#262A33"},
    "seed":    {"label": "Seed (manual)",   "color": "#8C96A0"},
    "derived": {"label": "CPI-derived",     "color": "#D9D0C4"},
}


def _shape_source_mix(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Pivot per-year per-bucket counts into a shape the template can render.

    Returns ``{"years": [{"year": 2020, "total": 1040, "segments": [
    {"kind": "scrape", "label": ..., "color": ..., "count": 200,
     "pct": 19.2}, ...]}, ...], "totals": {"scrape": ...}}``.
    Segments always include every known source kind, even with zero
    count, so the rendered bars line up visually across years.
    """
    if not rows:
        return {"years": [], "totals": {}, "has_data": False}

    per_year: dict[int, dict[str, int]] = {}
    totals: dict[str, int] = {k: 0 for k in _SOURCE_KIND_ORDER}
    for r in rows:
        year = int(r["year"])
        kind = str(r["source_kind"])
        count = int(r["row_count"])
        per_year.setdefault(year, {k: 0 for k in _SOURCE_KIND_ORDER})
        # Unknown buckets collapse into 'scrape' so the display doesn't lie.
        key = kind if kind in _SOURCE_KIND_ORDER else "scrape"
        per_year[year][key] = per_year[year].get(key, 0) + count
        totals[key] = totals.get(key, 0) + count

    years_out: list[dict[str, Any]] = []
    for year in sorted(per_year):
        buckets = per_year[year]
        total = sum(buckets.values())
        if total <= 0:
            continue
        segments: list[dict[str, Any]] = []
        for kind in _SOURCE_KIND_ORDER:
            count = buckets.get(kind, 0)
            segments.append(
                {
                    "kind": kind,
                    "label": _SOURCE_KIND_META[kind]["label"],
                    "color": _SOURCE_KIND_META[kind]["color"],
                    "count": count,
                    "pct": round(count / total * 100.0, 1) if total else 0.0,
                }
            )
        years_out.append({"year": year, "total": total, "segments": segments})

    return {
        "years": years_out,
        "totals": totals,
        "has_data": any(v > 0 for v in totals.values()),
    }


def _thin_parallel(arrays: list[list[Any]], target: int = _CHART_MAX_POINTS) -> list[list[Any]]:
    """Down-sample parallel arrays to at most ``target`` points.

    All input arrays must be equal length; each output array preserves the
    same striding so positions still line up across series. The final index
    is always kept so the latest-known value is never dropped off the right
    edge of the chart.
    """
    if not arrays:
        return arrays
    n = len(arrays[0])
    if n <= target:
        return arrays
    step = max(1, (n + target - 1) // target)
    idxs = list(range(0, n, step))
    if idxs[-1] != n - 1:
        idxs.append(n - 1)
    return [[a[i] for i in idxs] for a in arrays]


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def landing(
    request: Request,
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> HTMLResponse:
    baseline = settings.tssi_baseline_date
    today = _today_bangkok()

    # If today is before baseline for some exotic configuration, collapse to
    # a single-day window so the template can still render politely.
    if today < baseline:
        today = baseline

    series: IndexResponse = await compute_index_series(
        session,
        start_date=baseline,
        end_date=today,
        baseline_date=baseline,
    )
    ppu_rows: list[AssetDailyPPU] = await fetch_asset_ppu_series(
        session, start_date=baseline, end_date=today
    )
    source_mix_rows = await fetch_source_mix(
        session, start_date=baseline, end_date=today
    )
    source_mix = _shape_source_mix(source_mix_rows)

    # Index per-asset PPU rows by (day, asset_name) for O(1) table lookup.
    ppu_by_day: dict[date, dict[str, AssetDailyPPU]] = {}
    for row in ppu_rows:
        ppu_by_day.setdefault(row.day, {})[row.asset_name] = row

    # --- Asset cards -------------------------------------------------------
    asset_cards: list[dict[str, Any]] = []
    days_sorted = sorted(ppu_by_day.keys())

    # Locate the latest observed (non-imputed) price / weight pair per asset
    # so each card can surface a real retail price, not just an imputed one.
    latest_real: dict[str, AssetDailyPPU] = {}
    for row in ppu_rows:
        if row.imputed:
            continue
        prev = latest_real.get(row.asset_name)
        if prev is None or row.day > prev.day:
            latest_real[row.asset_name] = row

    for cfg in ASSET_REGISTRY.values():
        meta = _ASSET_META[cfg.asset_id]
        asset_key = str(cfg.asset_id)
        baseline_ppu = None
        current_ppu = None
        if days_sorted:
            first = ppu_by_day[days_sorted[0]].get(asset_key)
            last = ppu_by_day[days_sorted[-1]].get(asset_key)
            if first and first.ppu > 0:
                baseline_ppu = first.ppu
            if last and last.ppu > 0:
                current_ppu = last.ppu

        delta_pct: float | None = None
        if baseline_ppu and current_ppu:
            delta_pct = (current_ppu - baseline_ppu) / baseline_ppu * 100.0

        # Real nominal price + net weight (from the most recent non-imputed
        # row for this asset). Falls back to registry defaults if no real
        # observation has landed in the window.
        observed = latest_real.get(asset_key)
        nominal_price = None
        net_weight_display = None
        if observed is not None and observed.nominal_price is not None:
            nominal_price = observed.nominal_price
            net_weight_display = observed.net_weight
        elif cfg.fallback_net_weight is not None:
            net_weight_display = float(cfg.fallback_net_weight)

        asset_cards.append(
            {
                "asset_key": asset_key,
                "display_name": meta["display_name"],
                "tagline": meta["tagline"],
                "platform": meta["platform"],
                "emoji": meta["emoji"],
                "color": meta["color"],
                "unit": meta["unit"],
                "weight_pct": f"{float(cfg.weight) * 100:.0f}",
                "coicop_category": cfg.coicop_category,
                "rationale": cfg.rationale,
                "nominal_price": (
                    f"฿ {nominal_price:,.2f}" if nominal_price is not None else "—"
                ),
                "net_weight": (
                    f"{net_weight_display:,.0f} {cfg.unit_type}"
                    if net_weight_display is not None
                    else "—"
                ),
                "baseline_ppu": _fmt(baseline_ppu),
                "current_ppu": _fmt(current_ppu),
                "delta_pct": f"{delta_pct:+.2f}%" if delta_pct is not None else "—",
                "delta_pct_raw": delta_pct,
                "delta_class": _delta_class(delta_pct),
                "arrow": _arrow(delta_pct),
            }
        )

    # --- Chart payload -----------------------------------------------------
    # TSSI composite line + 5 per-asset lines each normalized to 100 at
    # their first observation in the window. None gaps let Chart.js draw
    # honest breaks on days before an asset's first scrape.
    chart_labels: list[str] = [pt.day.isoformat() for pt in series.points]
    chart_tssi: list[float] = [round(pt.index_value, 4) for pt in series.points]

    first_ppu: dict[str, float] = {}
    series_by_asset: dict[str, list[float | None]] = {a: [] for a in BASKET_WEIGHTS}
    for day in [pt.day for pt in series.points]:
        for asset_name in BASKET_WEIGHTS:
            row = ppu_by_day.get(day, {}).get(asset_name)
            if row and row.ppu > 0 and asset_name not in first_ppu:
                first_ppu[asset_name] = row.ppu
            if row and row.ppu > 0 and first_ppu.get(asset_name):
                series_by_asset[asset_name].append(
                    round(row.ppu / first_ppu[asset_name] * 100.0, 4)
                )
            else:
                series_by_asset[asset_name].append(None)

    # Thin the chart arrays when the window is long (years of daily data).
    # We stride through all series in lockstep so positions still align.
    full_length = len(chart_labels)
    (
        chart_labels_t,
        chart_tssi_t,
        rice_t,
        mama_t,
        ezygo_t,
        water_t,
        m150_t,
    ) = _thin_parallel(
        [
            chart_labels,
            chart_tssi,
            series_by_asset["mahboonkrong_rice_5kg"],
            series_by_asset["mama_tomyum_pack"],
            series_by_asset["ezygo_kaphrao_box"],
            series_by_asset["crystal_water_600ml"],
            series_by_asset["m150_bottle"],
        ]
    )

    chart_payload = {
        "labels": chart_labels_t,
        "tssi": chart_tssi_t,
        "rice": rice_t,
        "mama": mama_t,
        "ezygo": ezygo_t,
        "water": water_t,
        "m150": m150_t,
        # Hints for the client-side renderer:
        # * ``long_range`` collapses x-axis tick labels to YYYY-MM.
        # * ``raw_days`` lets the chart card surface "thinned from N days".
        "long_range": full_length > 365,
        "raw_days": full_length,
        "plotted_points": len(chart_labels_t),
    }

    # --- Table: most recent 60 days, reversed (latest first) --------------
    table_rows: list[dict[str, Any]] = []
    for point in list(series.points)[-60:][::-1]:
        day = point.day
        row_by_asset = ppu_by_day.get(day, {})
        rice = row_by_asset.get("mahboonkrong_rice_5kg")
        mama = row_by_asset.get("mama_tomyum_pack")
        ezygo = row_by_asset.get("ezygo_kaphrao_box")
        water = row_by_asset.get("crystal_water_600ml")
        m150 = row_by_asset.get("m150_bottle")
        sym_pct_pct = point.sym_pct_change * 100.0
        table_rows.append(
            {
                "day": day.isoformat(),
                "composite_ppu": _fmt(point.composite_ppu),
                "index_value": f"{point.index_value:.2f}",
                "sym_pct": f"{sym_pct_pct:+.2f}%",
                "sym_class": _delta_class(sym_pct_pct),
                "rice": _fmt(rice.ppu) if rice else "—",
                "rice_imputed": bool(rice and rice.imputed),
                "mama": _fmt(mama.ppu) if mama else "—",
                "mama_imputed": bool(mama and mama.imputed),
                "ezygo": _fmt(ezygo.ppu) if ezygo else "—",
                "ezygo_imputed": bool(ezygo and ezygo.imputed),
                "water": _fmt(water.ppu) if water else "—",
                "water_imputed": bool(water and water.imputed),
                "m150": _fmt(m150.ppu) if m150 else "—",
                "m150_imputed": bool(m150 and m150.imputed),
            }
        )

    # --- Hero numbers ------------------------------------------------------
    latest = series.latest
    hero: dict[str, Any]
    if latest is not None:
        sym_pct_pct = latest.sym_pct_change * 100.0
        hero = {
            "index_value": f"{latest.index_value:.2f}",
            "composite_ppu": _fmt(latest.composite_ppu),
            "sym_pct": f"{sym_pct_pct:+.2f}%",
            "sym_class": _delta_class(sym_pct_pct),
            "arrow": _arrow(sym_pct_pct),
            "delta_vs_100": f"{latest.index_value - 100.0:+.2f}",
            "day": latest.day.isoformat(),
        }
    else:
        hero = {
            "index_value": "—",
            "composite_ppu": "—",
            "sym_pct": "—",
            "sym_class": "neutral",
            "arrow": "→",
            "delta_vs_100": "—",
            "day": today.isoformat(),
        }

    # --- Alternative proxy rubric (brief §5) -------------------------------
    # Static editorial content, but kept server-side so the page body
    # stays pure HTML (no client-side JSON injection noise).
    alt_proxies = [
        {
            "name": "Thai Street Survival Index",
            "rigidity": 9.0,
            "cultural": 9.0,
            "shrink": 8.5,
            "verdict": "Proposed micro-basket, PPU-adjusted.",
            "highlight": True,
        },
        {
            "name": "Cafe Amazon Index",
            "rigidity": 8.0,
            "cultural": 5.0,
            "shrink": 4.0,
            "verdict": "Vulnerable to skimpflation (ice-to-liquid ratio).",
            "highlight": False,
        },
        {
            "name": "BTS Skytrain Commute Index",
            "rigidity": 9.0,
            "cultural": 4.0,
            "shrink": 8.0,
            "verdict": "Bangkok-only; politically subsidized fares.",
            "highlight": False,
        },
        {
            "name": "Gold / Baht Correlation",
            "rigidity": 10.0,
            "cultural": None,
            "shrink": 10.0,
            "verdict": "Measures capital flow, not retail consumption.",
            "highlight": False,
        },
    ]

    # --- Summary stats for the exec-summary callout ------------------------
    composite_now = hero.get("index_value", "—")
    cumulative_growth = (
        f"{series.points[-1].index_value - 100.0:+.2f}"
        if series.points
        else "—"
    )

    context: dict[str, Any] = {
        "request": request,
        "demo_mode": settings.demo_mode,
        "baseline_date": baseline.isoformat(),
        "today": today.isoformat(),
        "observation_count": len(ppu_rows),
        "series_length": len(series.points),
        "weights": BASKET_WEIGHTS,
        "asset_cards": asset_cards,
        "table_rows": table_rows,
        "hero": hero,
        "chart_data_json": json.dumps(chart_payload),
        "source_mix": source_mix,
        "metadata": {
            "smoothing_applied": series.metadata.smoothing_applied,
            "status": series.metadata.status,
            "structural_missing": list(series.metadata.structural_missing),
        },
        "alt_proxies": alt_proxies,
        "composite_now": composite_now,
        "cumulative_growth": cumulative_growth,
    }
    # Starlette >= 0.29 requires the request-first signature; the legacy
    # form (passing a context dict with "request" inside) raises a cryptic
    # "unhashable type: 'dict'" error in recent releases.
    return templates.TemplateResponse(request, "index.html", context)


# ==========================================================================
#                             METHODOLOGY PAGE
# ==========================================================================


def _compute_appendix_a3(
    series: IndexResponse, trailing_days: int = 90
) -> dict[str, Any]:
    """Trailing-window descriptive statistics for Appendix A.3.

    Returns mean / median / stddev / min / max / CV over the last
    ``trailing_days`` composite index values. All arithmetic is done
    against the post-smoothing composite that powers the headline chart.
    """
    import statistics

    if not series.points:
        return {
            "window_start": None,
            "window_end": None,
            "mean": None,
            "median": None,
            "stddev": None,
            "min": None,
            "max": None,
            "cv_pct": None,
            "sample_size": 0,
        }
    tail = series.points[-trailing_days:]
    values = [p.index_value for p in tail]
    mean = statistics.fmean(values)
    median = statistics.median(values)
    stddev = statistics.pstdev(values) if len(values) > 1 else 0.0
    return {
        "window_start": tail[0].day.isoformat(),
        "window_end": tail[-1].day.isoformat(),
        "mean": round(mean, 2),
        "median": round(median, 2),
        "stddev": round(stddev, 2),
        "min": round(min(values), 2),
        "max": round(max(values), 2),
        "cv_pct": round((stddev / mean) * 100.0, 2) if mean else None,
        "sample_size": len(tail),
    }


def _compute_appendix_a2(
    source_mix: dict[str, Any], total_days: int, total_obs: int
) -> dict[str, Any]:
    """Dataset-wide provenance shares for Appendix A.2."""
    totals = source_mix.get("totals") or {}
    total = sum(totals.values()) or total_obs or 1
    pct = {k: round(v * 100.0 / total, 1) for k, v in totals.items()}
    return {
        "total_days": total_days,
        "total_observations": total,
        "pct_scrape": pct.get("scrape", 0.0),
        "pct_archive": pct.get("archive", 0.0),
        "pct_seed": pct.get("seed", 0.0),
        "pct_derived": pct.get("derived", 0.0),
        "pct_non_official": round(
            pct.get("scrape", 0.0) + pct.get("archive", 0.0) + pct.get("seed", 0.0),
            1,
        ),
    }


@router.get("/methodology", response_class=HTMLResponse, include_in_schema=False)
async def methodology(
    request: Request,
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> HTMLResponse:
    """Long-form conceptual framework page.

    Renders the full TSSI brief as editorial prose with Appendix A
    statistics computed live from the calc engine so the published
    numbers always match the pipeline output.
    """
    today = _today_bangkok()
    baseline = settings.tssi_baseline_date

    series = await compute_index_series(
        session,
        start_date=baseline,
        end_date=today,
        baseline_date=baseline,
    )

    ppu_rows: list[AssetDailyPPU] = await fetch_asset_ppu_series(
        session,
        start_date=baseline,
        end_date=today,
    )

    source_rows = await fetch_source_mix(
        session,
        start_date=baseline,
        end_date=today,
    )
    source_mix = _shape_source_mix(source_rows)

    # Rebuild per-asset decomposition using first-vs-last raw PPU. Same
    # shape as on the landing page so the two views stay consistent.
    ppu_by_day: dict[date, dict[str, AssetDailyPPU]] = {}
    for row in ppu_rows:
        ppu_by_day.setdefault(row.day, {})[row.asset_name] = row
    days_sorted = sorted(ppu_by_day.keys())

    decomposition: list[dict[str, Any]] = []
    for cfg in ASSET_REGISTRY.values():
        meta = _ASSET_META[cfg.asset_id]
        asset_key = str(cfg.asset_id)
        baseline_ppu = None
        current_ppu = None
        if days_sorted:
            first = ppu_by_day[days_sorted[0]].get(asset_key)
            last = ppu_by_day[days_sorted[-1]].get(asset_key)
            if first and first.ppu > 0:
                baseline_ppu = first.ppu
            if last and last.ppu > 0:
                current_ppu = last.ppu
        delta_pct = None
        if baseline_ppu and current_ppu:
            delta_pct = (current_ppu - baseline_ppu) / baseline_ppu * 100.0
        decomposition.append(
            {
                "display_name": meta["display_name"],
                "coicop_category": cfg.coicop_category,
                "weight_pct": f"{float(cfg.weight) * 100:.0f}",
                "baseline_ppu": _fmt(baseline_ppu),
                "current_ppu": _fmt(current_ppu),
                "unit": cfg.unit_type,
                "delta_pct": f"{delta_pct:+.2f}%" if delta_pct is not None else "—",
                "delta_pct_raw": delta_pct,
                "delta_class": _delta_class(delta_pct),
                "rationale": cfg.rationale,
            }
        )
    # Sort by contribution, largest first.
    decomposition.sort(
        key=lambda d: d["delta_pct_raw"] if d["delta_pct_raw"] is not None else -999,
        reverse=True,
    )

    # NSO weight mapping table (static from the brief, but reflecting
    # the live weights so the two never drift out of sync).
    nso_table = []
    for cfg in ASSET_REGISTRY.values():
        nso_table.append(
            {
                "coicop": cfg.coicop_category,
                "asset": _ASSET_META[cfg.asset_id]["display_name"],
                "weight_pct": f"{float(cfg.weight) * 100:.0f}%",
            }
        )

    total_days = (
        (series.points[-1].day - series.points[0].day).days + 1
        if series.points
        else 0
    )
    total_obs = len(ppu_rows)

    appendix_a2 = _compute_appendix_a2(source_mix, total_days, total_obs)
    appendix_a3 = _compute_appendix_a3(series, trailing_days=90)

    latest_index = (
        round(series.points[-1].index_value, 2) if series.points else None
    )
    cumulative_growth = (
        round(series.points[-1].index_value - 100.0, 2)
        if series.points
        else None
    )

    context: dict[str, Any] = {
        "request": request,
        "demo_mode": settings.demo_mode,
        "baseline_date": baseline.isoformat(),
        "today": today.isoformat(),
        "latest_index": latest_index,
        "cumulative_growth": cumulative_growth,
        "nso_table": nso_table,
        "decomposition": decomposition,
        "appendix_a2": appendix_a2,
        "appendix_a3": appendix_a3,
        "metadata": {
            "smoothing_applied": series.metadata.smoothing_applied,
            "status": series.metadata.status,
            "structural_missing": list(series.metadata.structural_missing),
        },
    }
    return templates.TemplateResponse(request, "methodology.html", context)
