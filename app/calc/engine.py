"""TSSI composite index calculation.

The public surface is a small set of functions that the API and the
server-rendered landing page call directly:

* :func:`fetch_daily_ppu`          -- raw, per-asset, per-day price /
                                      weight / PPU feed.
* :func:`compute_index_series`     -- imputed, smoothed, weight-renormed,
                                      baseline-normalized composite series.
* :func:`compute_current_index`    -- convenience wrapper around the above
                                      returning just the latest point.
* :func:`fetch_asset_ppu_series`   -- per-asset daily feed with imputed
                                      and structural-missing flags.
* :func:`fetch_latest_asset_detail`-- latest nominal-price, net-weight,
                                      PPU, imputation metadata per asset.

All timestamps are interpreted in **Asia/Bangkok** so that a "day" means a
Thai calendar day (local midnight -> local midnight), which is what a
consumer of a Thai CPI-style index intuitively expects.

Mathematical contract (matches the Thai Street Survival Index brief):

    1. Per asset i, let P_i(t) be the PPU on day t.
       Forward-fill gaps up to 3 consecutive days (brief §3.2); after that
       the asset is flagged ``structural_missing`` and dropped from the
       composite for the affected day.

    2. Smooth per-asset PPU with a 7-day right-aligned rolling median
       (brief §3.3) before aggregation.

    3. Composite PPU:   C(t) = Σ_i w_i' · P_i(t),
       where w_i' is the basket weight renormalized over the assets that
       are present for day t (i.e. not structurally missing):

            w = {
              mahboonkrong_rice_5kg: 0.25,  mama_tomyum_pack:    0.15,
              ezygo_kaphrao_box:     0.35,  crystal_water_600ml: 0.15,
              m150_bottle:           0.10,
            }

    4. Baseline date B (default 2020-01-01) gives C(B), normalized to 100.
       Index:          I(t) = 100 · C(t) / C(B).

    5. Symmetric percent change vs. baseline:
            S(t) = 2 · (C(t) - C(B)) / (C(t) + C(B))
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.calc.schemas import AssetDailyPPU, IndexMetadata, IndexPoint, IndexResponse
from app.scraper.assets import ASSET_REGISTRY

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Basket weights: derived from the single source of truth (ASSET_REGISTRY),
# keyed by the string stored in ``tssi_raw_data.asset_name``.
# -----------------------------------------------------------------------------
BASKET_WEIGHTS: dict[str, float] = {
    str(cfg.asset_id): float(cfg.weight) for cfg in ASSET_REGISTRY.values()
}

_EXPECTED_ASSETS: tuple[str, ...] = tuple(BASKET_WEIGHTS.keys())

# Unit types per asset, derived from the registry. Used by the API layer
# to pick between ``_g`` and ``_ml`` field names on the /current response.
ASSET_UNITS: dict[str, str] = {
    str(cfg.asset_id): str(cfg.unit_type) for cfg in ASSET_REGISTRY.values()
}

# Cap for forward-fill imputation. Per the brief §3.2, an asset that
# misses more than this many consecutive days is flagged as structurally
# missing rather than silently imputed.
MAX_FFILL_DAYS: int = 3

# Window for the per-asset PPU rolling median smoother (brief §3.3).
ROLLING_MEDIAN_DAYS: int = 7


# -----------------------------------------------------------------------------
# SQL: per-asset, per-day mean of PPU / nominal_price / net_weight in
# Bangkok local time. We compute the averages on the read side rather
# than relying on a continuous aggregate so that data ingested within
# the current refresh window is immediately visible in the index.
#
# Two variants -- one permissive (includes CPI-derived and other
# ``derived:*`` rows), one strict (scraped + archived + seed only).
# -----------------------------------------------------------------------------
_DAILY_PPU_SQL = text(
    """
    SELECT
        (time AT TIME ZONE 'Asia/Bangkok')::date AS day,
        asset_name,
        AVG(ppu)::float8 AS ppu,
        AVG(nominal_price)::float8 AS nominal_price,
        AVG(net_weight)::float8 AS net_weight
    FROM tssi_raw_data
    WHERE (time AT TIME ZONE 'Asia/Bangkok')::date >= :start_date
      AND (time AT TIME ZONE 'Asia/Bangkok')::date <= :end_date
    GROUP BY day, asset_name
    ORDER BY day, asset_name;
    """
)

_DAILY_PPU_SQL_NO_DERIVED = text(
    """
    SELECT
        (time AT TIME ZONE 'Asia/Bangkok')::date AS day,
        asset_name,
        AVG(ppu)::float8 AS ppu,
        AVG(nominal_price)::float8 AS nominal_price,
        AVG(net_weight)::float8 AS net_weight
    FROM tssi_raw_data
    WHERE (time AT TIME ZONE 'Asia/Bangkok')::date >= :start_date
      AND (time AT TIME ZONE 'Asia/Bangkok')::date <= :end_date
      AND platform_source NOT LIKE 'derived:%'
    GROUP BY day, asset_name
    ORDER BY day, asset_name;
    """
)

# Per-calendar-year row counts, bucketed by provenance. Powers the
# "Source mix" panel on the public site so visitors can see where each
# region of the series came from -- live scrape vs Wayback archive vs
# hand-curated seed anchor vs CPI-derived monthly fill.
_SOURCE_MIX_SQL = text(
    """
    SELECT
        EXTRACT(YEAR FROM (time AT TIME ZONE 'Asia/Bangkok'))::int AS year,
        CASE
            WHEN platform_source LIKE 'wayback:%' THEN 'archive'
            WHEN platform_source LIKE 'seed:%'    THEN 'seed'
            WHEN platform_source LIKE 'derived:%' THEN 'derived'
            ELSE 'scrape'
        END AS source_kind,
        COUNT(*)::bigint AS row_count
    FROM tssi_raw_data
    WHERE (time AT TIME ZONE 'Asia/Bangkok')::date >= :start_date
      AND (time AT TIME ZONE 'Asia/Bangkok')::date <= :end_date
    GROUP BY 1, 2
    ORDER BY 1, 2;
    """
)


# =============================================================================
# Data access
# =============================================================================
async def fetch_daily_ppu(
    session: AsyncSession,
    start_date: date,
    end_date: date,
    *,
    include_derived: bool = True,
) -> pd.DataFrame:
    """Return a tidy ``(day, asset_name, ppu, nominal_price, net_weight)`` frame.

    ``include_derived`` (default True) keeps CPI-derived and any other
    ``platform_source`` prefixed ``derived:`` rows in the aggregation.
    Pass ``False`` to restrict the feed to truly-observed data (live
    scrapes, Wayback archives, hand-curated seeds).

    ``nominal_price`` and ``net_weight`` may be absent when the backing
    session is a pre-Phase-B shim that still returns only ``ppu``; the
    columns are tolerated as NaN in that case so the engine still
    computes a composite, but the /current endpoint's per-asset detail
    will gracefully degrade to ``None``.
    """
    if end_date < start_date:
        raise ValueError(
            f"end_date {end_date} is before start_date {start_date}"
        )

    stmt = _DAILY_PPU_SQL if include_derived else _DAILY_PPU_SQL_NO_DERIVED
    result = await session.execute(
        stmt, {"start_date": start_date, "end_date": end_date}
    )
    rows = result.mappings().all()

    cols = ["day", "asset_name", "ppu", "nominal_price", "net_weight"]
    if not rows:
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame([dict(r) for r in rows])
    df["day"] = pd.to_datetime(df["day"])
    df["ppu"] = df["ppu"].astype(float)
    # Tolerate sessions that don't project nominal_price / net_weight yet.
    for extra in ("nominal_price", "net_weight"):
        if extra not in df.columns:
            df[extra] = float("nan")
        else:
            df[extra] = df[extra].astype(float)
    return df[cols]


# =============================================================================
# Core calculation
# =============================================================================
def _pivot_wide(
    long_df: pd.DataFrame,
    value_col: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Pivot a long (day, asset_name, value) frame to wide over the full range."""
    full_index = pd.date_range(start=start_date, end=end_date, freq="D", name="day")
    if long_df.empty:
        return pd.DataFrame(
            index=full_index, columns=list(_EXPECTED_ASSETS), dtype=float
        )
    wide = long_df.pivot_table(
        index="day",
        columns="asset_name",
        values=value_col,
        aggfunc="mean",
    )
    missing_assets = [a for a in _EXPECTED_ASSETS if a not in wide.columns]
    for asset in missing_assets:
        logger.warning("No %s observations for asset %s in requested window", value_col, asset)
        wide[asset] = float("nan")
    wide = wide.reindex(full_index)
    wide = wide.reindex(columns=list(_EXPECTED_ASSETS))
    return wide.astype(float)


def _ffill_with_cap(
    wide: pd.DataFrame,
    *,
    max_consecutive: int = MAX_FFILL_DAYS,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Forward-fill with a per-asset consecutive-gap cap.

    Returns ``(filled, imputed_mask, structural_miss_mask)`` where:

    * ``filled`` carries the most recent observation forward up to
      ``max_consecutive`` days, then emits NaN.
    * ``imputed_mask[d, a]`` is True where ``filled[d, a]`` came from a
      forward-fill (real observation => False).
    * ``structural_miss_mask[d, a]`` is True where the gap exceeded the
      cap -- the asset is considered structurally missing for that day
      and the composite should renormalize its weights to exclude it.

    A defensive ``bfill`` runs only for the leading edge (days before
    the very first observation for that asset) so the baseline day
    isn't dropped just because one asset was scraped late. Leading-edge
    backfilled cells are flagged as imputed but NOT structurally missing.
    """
    imputed = pd.DataFrame(False, index=wide.index, columns=wide.columns)
    structural = pd.DataFrame(False, index=wide.index, columns=wide.columns)
    filled = wide.copy()

    for asset in wide.columns:
        col = wide[asset]
        observed = col.notna()
        if not observed.any():
            # No data at all for this asset in the window -- mark every
            # day as structurally missing so the composite drops it.
            structural[asset] = True
            imputed[asset] = True
            continue

        # Forward-fill, tracking the streak of consecutive imputed days.
        last_value: Optional[float] = None
        streak = 0
        out_values: list[Any] = []
        for val in col.tolist():
            if pd.notna(val):
                last_value = float(val)
                streak = 0
                out_values.append(last_value)
                continue
            # NaN cell -- either pre-first-obs (handled below via bfill)
            # or a true scrape-miss gap.
            if last_value is None:
                out_values.append(float("nan"))
                continue
            streak += 1
            if streak <= max_consecutive:
                out_values.append(last_value)
            else:
                out_values.append(float("nan"))

        filled[asset] = out_values
        imputed[asset] = ~observed
        # The structural_miss mask covers days where the ffill chain has
        # broken AND the asset wasn't observed. Leading-edge NaN also
        # survives here until bfill kicks in below.
        structural[asset] = filled[asset].isna() & ~observed

    # Leading-edge backfill so the baseline day always has a value.
    # These cells remain flagged as imputed, but they're no longer
    # structurally missing because we're bridging pre-history, not a
    # scrape failure.
    leading_bfill = filled.bfill()
    bfilled_cells = filled.isna() & leading_bfill.notna()
    filled = leading_bfill
    structural = structural & ~bfilled_cells

    return filled, imputed, structural


def _apply_rolling_median(
    filled: pd.DataFrame,
    *,
    window: int = ROLLING_MEDIAN_DAYS,
) -> pd.DataFrame:
    """Right-aligned rolling median on per-asset PPU.

    Uses ``min_periods=1`` so the first ``window-1`` days still emit a
    value (a degenerate median over the partial window). This follows
    the brief's §3.3 methodology: smoothing tames dynamic-pricing /
    promotional noise without cutting the head of the series.
    """
    if filled.empty:
        return filled
    return filled.rolling(window=window, min_periods=1).median()


def _composite(
    filled: pd.DataFrame,
    structural_miss_mask: pd.DataFrame,
) -> tuple[pd.Series, list[str]]:
    """Weight-renormalized composite PPU per day.

    When one or more assets are structurally missing for a day, their
    weights are dropped from the numerator and the remaining weights
    are renormalized to sum to 1.0. Emits NaN for days where *every*
    asset is structurally missing (defensive; shouldn't happen for a
    live basket).

    Returns ``(composite_series, ever_structural_missing)`` where
    ``ever_structural_missing`` is the list of asset keys that were
    structurally missing on at least one day in the window (used by
    the metadata block).
    """
    weights = pd.Series(BASKET_WEIGHTS)
    aligned = filled[weights.index]
    miss_aligned = structural_miss_mask[weights.index]

    # Broadcast weights across days, zero them out where the asset is
    # structurally missing, then renormalize row-wise.
    broadcast = pd.DataFrame(
        [weights.values] * len(aligned),
        index=aligned.index,
        columns=weights.index,
    )
    effective = broadcast.where(~miss_aligned, 0.0)
    row_sum = effective.sum(axis=1)
    # Avoid division-by-zero: set rows with zero total weight to NaN.
    safe_sum = row_sum.where(row_sum > 0.0, other=float("nan"))
    normalized = effective.div(safe_sum, axis=0)

    composite = aligned.mul(normalized, axis=1).sum(axis=1, skipna=True)
    # When every weight was zeroed, the sum collapses to 0; detect and
    # promote to NaN so downstream code sees it as missing.
    composite = composite.where(row_sum > 0.0, other=float("nan"))
    composite.name = "composite_ppu"

    ever_missing = sorted(
        asset for asset in weights.index if bool(miss_aligned[asset].any())
    )
    return composite, ever_missing


async def compute_index_series(
    session: AsyncSession,
    *,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    baseline_date: Optional[date] = None,
    include_derived: bool = True,
    smoothed: bool = True,
) -> IndexResponse:
    """Return the full index series from ``start_date`` to ``end_date``.

    If ``start_date`` is omitted, it defaults to ``baseline_date`` so the
    returned series always starts at the normalization anchor.

    ``include_derived`` (default True) keeps ``derived:*`` rows (e.g.
    BoT CPI-anchored synthetic fills) in the composite. Set to False
    when the caller wants a strict scraped-only index -- the result
    still honors ffill + the 3-day cap, but only real observations
    get propagated.

    ``smoothed`` (default True) applies the 7-day rolling median filter
    per brief §3.3. Setting it to False returns an un-smoothed series
    for transparency / debugging.
    """
    settings = get_settings()
    baseline_date = baseline_date or settings.tssi_baseline_date
    end_date = end_date or _today_bangkok()
    start_date = start_date or baseline_date

    fetch_from = min(start_date, baseline_date)

    if end_date < fetch_from:
        raise ValueError(
            f"Requested end_date {end_date} is before fetch window start {fetch_from}"
        )
    if end_date < baseline_date:
        raise ValueError(
            f"end_date {end_date} is before baseline_date {baseline_date}; "
            "cannot normalize against a baseline that hasn't happened yet"
        )

    long_df = await fetch_daily_ppu(
        session, fetch_from, end_date, include_derived=include_derived
    )

    ppu_wide = _pivot_wide(long_df, "ppu", fetch_from, end_date)
    filled, _imputed, structural = _ffill_with_cap(ppu_wide)

    smoothing_label = "7-day rolling median" if smoothed else "none"
    if smoothed:
        filled = _apply_rolling_median(filled)

    composite, ever_missing = _composite(filled, structural)

    if composite.isna().all():
        logger.warning(
            "Insufficient data to compute TSSI between %s and %s",
            fetch_from,
            end_date,
        )
        return IndexResponse(
            baseline_date=baseline_date,
            baseline_composite_ppu=0.0,
            points=[],
            weights=dict(BASKET_WEIGHTS),
            metadata=IndexMetadata(
                smoothing_applied=smoothing_label,
                status="insufficient",
                structural_missing=list(_EXPECTED_ASSETS),
            ),
        )

    baseline_ts = pd.Timestamp(baseline_date)
    if baseline_ts not in composite.index:
        raise ValueError(
            f"Baseline date {baseline_date} is not covered by the calculation window"
        )

    baseline_value = float(composite.loc[baseline_ts])
    if pd.isna(baseline_value) or baseline_value <= 0:
        raise ValueError(
            f"Non-positive / missing composite PPU at baseline {baseline_date}: {baseline_value}"
        )

    index_value = composite * (100.0 / baseline_value)
    sym_pct_change = 2.0 * (composite - baseline_value) / (composite + baseline_value)

    start_ts = pd.Timestamp(start_date)
    points: list[IndexPoint] = []
    latest_missing_today: list[str] = []
    end_ts = pd.Timestamp(end_date)
    for ts in composite.index:
        if ts < start_ts:
            continue
        c_val = composite.loc[ts]
        if pd.isna(c_val):
            continue
        points.append(
            IndexPoint(
                day=ts.date(),
                composite_ppu=float(c_val),
                index_value=float(index_value.loc[ts]),
                sym_pct_change=float(sym_pct_change.loc[ts]),
            )
        )
        if ts == end_ts:
            latest_missing_today = [
                a for a in _EXPECTED_ASSETS if bool(structural.loc[ts, a])
            ]

    # Metadata reflects the state at the newest rendered point.
    if not points:
        status = "insufficient"
    elif latest_missing_today:
        status = "partial"
    else:
        status = "operational"

    return IndexResponse(
        baseline_date=baseline_date,
        baseline_composite_ppu=baseline_value,
        points=points,
        weights=dict(BASKET_WEIGHTS),
        metadata=IndexMetadata(
            smoothing_applied=smoothing_label,
            status=status,
            structural_missing=latest_missing_today or ever_missing,
        ),
    )


async def compute_current_index(
    session: AsyncSession,
    *,
    include_derived: bool = True,
    smoothed: bool = True,
) -> IndexResponse:
    """Latest single point, baseline-anchored."""
    today = _today_bangkok()
    full = await compute_index_series(
        session,
        start_date=today,
        end_date=today,
        include_derived=include_derived,
        smoothed=smoothed,
    )
    return full


# =============================================================================
# Per-asset PPU feed (used by GET /tssi/assets/ppu)
# =============================================================================
async def fetch_source_mix(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
) -> list[dict]:
    """Return per-year, per-provenance row counts."""
    if end_date < start_date:
        raise ValueError(
            f"end_date {end_date} is before start_date {start_date}"
        )
    result = await session.execute(
        _SOURCE_MIX_SQL, {"start_date": start_date, "end_date": end_date}
    )
    rows = result.mappings().all()
    return [
        {
            "year": int(r["year"]),
            "source_kind": str(r["source_kind"]),
            "row_count": int(r["row_count"]),
        }
        for r in rows
    ]


async def fetch_asset_ppu_series(
    session: AsyncSession,
    *,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    include_derived: bool = True,
    smoothed: bool = True,
) -> list[AssetDailyPPU]:
    """Return per-asset daily PPU (ffilled + capped + optionally smoothed)."""
    settings = get_settings()
    end_date = end_date or _today_bangkok()
    start_date = start_date or settings.tssi_baseline_date

    long_df = await fetch_daily_ppu(
        session, start_date, end_date, include_derived=include_derived
    )
    ppu_wide = _pivot_wide(long_df, "ppu", start_date, end_date)
    filled, imputed, structural = _ffill_with_cap(ppu_wide)
    price_wide = _pivot_wide(long_df, "nominal_price", start_date, end_date)
    weight_wide = _pivot_wide(long_df, "net_weight", start_date, end_date)
    # Carry the most recent nominal-price / net-weight observation forward
    # so the /current endpoint and per-asset detail can always report a
    # pair even on days that only received an imputed PPU.
    price_wide = price_wide.ffill().bfill()
    weight_wide = weight_wide.ffill().bfill()

    if smoothed:
        filled = _apply_rolling_median(filled)

    records: list[AssetDailyPPU] = []
    for ts, row in filled.iterrows():
        for asset_name in _EXPECTED_ASSETS:
            value = row[asset_name]
            if pd.isna(value):
                # Structurally missing days emit a record with ppu=0 and
                # the flags set, so consumers that loop over days still
                # see a row per asset. We guard against emitting a noisy
                # zero by skipping leading-edge pre-first-obs cells.
                if bool(structural.loc[ts, asset_name]):
                    records.append(
                        AssetDailyPPU(
                            day=ts.date(),
                            asset_name=asset_name,
                            ppu=0.0,
                            imputed=True,
                            structural_missing=True,
                            nominal_price=None,
                            net_weight=None,
                        )
                    )
                continue

            raw_price = price_wide.loc[ts, asset_name] if asset_name in price_wide.columns else float("nan")
            raw_weight = weight_wide.loc[ts, asset_name] if asset_name in weight_wide.columns else float("nan")
            records.append(
                AssetDailyPPU(
                    day=ts.date(),
                    asset_name=asset_name,
                    ppu=float(value),
                    imputed=bool(imputed.loc[ts, asset_name]),
                    structural_missing=False,
                    nominal_price=(None if pd.isna(raw_price) else float(raw_price)),
                    net_weight=(None if pd.isna(raw_weight) else float(raw_weight)),
                )
            )
    return records


async def fetch_latest_asset_detail(
    session: AsyncSession,
    *,
    end_date: Optional[date] = None,
    baseline_date: Optional[date] = None,
    include_derived: bool = True,
    smoothed: bool = True,
) -> tuple[date, dict[str, dict[str, Any]], pd.DataFrame]:
    """Latest-day per-asset nominal-price / net-weight / PPU snapshot.

    Returns a tuple ``(day, detail, structural_mask_row)`` where
    ``detail`` is ``{asset_name: {"ppu": float, "nominal_price":
    Optional[float], "net_weight": Optional[float], "imputed": bool,
    "structural_missing": bool}}`` and ``structural_mask_row`` is the
    row of the structural-miss mask aligned to that day (for the
    renormalized composite weight calculation in the /current
    endpoint, if callers need it).
    """
    settings = get_settings()
    end_date = end_date or _today_bangkok()
    baseline_date = baseline_date or settings.tssi_baseline_date
    fetch_from = min(baseline_date, end_date)

    long_df = await fetch_daily_ppu(
        session, fetch_from, end_date, include_derived=include_derived
    )
    ppu_wide = _pivot_wide(long_df, "ppu", fetch_from, end_date)
    filled, imputed, structural = _ffill_with_cap(ppu_wide)

    # Pull nominal price / net weight separately from strictly-real rows
    # (scrape / wayback / seed) so the Appendix B display doesn't inherit
    # the derived-rows convention of encoding PPU as (price, weight=1).
    real_long_df = await fetch_daily_ppu(
        session, fetch_from, end_date, include_derived=False
    )
    if real_long_df.empty:
        # Nothing strictly-real in the window -- fall back to whatever
        # the full feed provides so at least the unit math still renders.
        real_long_df = long_df
    price_wide = (
        _pivot_wide(real_long_df, "nominal_price", fetch_from, end_date).ffill().bfill()
    )
    weight_wide = (
        _pivot_wide(real_long_df, "net_weight", fetch_from, end_date).ffill().bfill()
    )

    if smoothed:
        filled = _apply_rolling_median(filled)

    ts = pd.Timestamp(end_date)
    if ts not in filled.index:
        # Fall back to the most recent index we have.
        ts = filled.index.max()

    day = ts.date()
    detail: dict[str, dict[str, Any]] = {}
    for asset in _EXPECTED_ASSETS:
        val = filled.loc[ts, asset] if ts in filled.index else float("nan")
        raw_price = price_wide.loc[ts, asset] if ts in price_wide.index else float("nan")
        raw_weight = weight_wide.loc[ts, asset] if ts in weight_wide.index else float("nan")
        struct = bool(structural.loc[ts, asset]) if ts in structural.index else True
        imp = bool(imputed.loc[ts, asset]) if ts in imputed.index else True
        detail[asset] = {
            "ppu": (None if pd.isna(val) else float(val)),
            "nominal_price": (None if pd.isna(raw_price) else float(raw_price)),
            "net_weight": (None if pd.isna(raw_weight) else float(raw_weight)),
            "imputed": imp,
            "structural_missing": struct,
        }
    struct_row = structural.loc[ts] if ts in structural.index else structural.iloc[-1]
    return day, detail, struct_row


# =============================================================================
# Helpers
# =============================================================================
def _today_bangkok() -> date:
    """Today's date in Bangkok local time, regardless of container TZ."""
    now_utc = datetime.now(tz=timezone.utc)
    bangkok_now = now_utc + timedelta(hours=7)
    return bangkok_now.date()


__all__ = [
    "ASSET_UNITS",
    "BASKET_WEIGHTS",
    "MAX_FFILL_DAYS",
    "ROLLING_MEDIAN_DAYS",
    "compute_current_index",
    "compute_index_series",
    "fetch_asset_ppu_series",
    "fetch_daily_ppu",
    "fetch_latest_asset_detail",
    "fetch_source_mix",
]
