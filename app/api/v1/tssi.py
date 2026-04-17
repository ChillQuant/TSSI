"""Public TSSI read endpoints.

* ``GET /tssi/current``       – Appendix B-shaped latest composite with
                                per-asset nominal price, net weight, and
                                PPU plus the imputed flag and a metadata
                                block (smoothing, status, structural
                                missing).
* ``GET /tssi/historical``    – full time-series between two dates.
* ``GET /tssi/assets/ppu``    – per-asset PPU spread (ffilled, with an
                                ``imputed`` flag so consumers can render the
                                nominal-vs-real divergence honestly).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.calc.engine import (
    ASSET_UNITS,
    BASKET_WEIGHTS,
    compute_index_series,
    fetch_asset_ppu_series,
    fetch_latest_asset_detail,
)
from app.calc.schemas import AssetDailyPPU, IndexResponse
from app.core.config import Settings, get_settings
from app.db.session import get_session

router = APIRouter(prefix="/tssi", tags=["tssi"])


# -----------------------------------------------------------------------------
# Response envelopes (Appendix B shape for /tssi/current)
# -----------------------------------------------------------------------------
class CurrentAssetEntry(BaseModel):
    """Per-asset detail block on the /current response.

    Exactly one of ``net_weight_g`` / ``net_weight_ml`` and one of
    ``ppu_thb_g`` / ``ppu_thb_ml`` will be populated, matching the
    asset's canonical unit from the registry.
    """

    model_config = ConfigDict(frozen=True)

    nominal_price_thb: Optional[float] = Field(
        default=None,
        description="Average retail THB on the latest day, after ffill.",
    )
    net_weight_g: Optional[float] = Field(default=None)
    net_weight_ml: Optional[float] = Field(default=None)
    ppu_thb_g: Optional[float] = Field(default=None)
    ppu_thb_ml: Optional[float] = Field(default=None)
    imputed_flag: bool = Field(
        description=(
            "True when the latest day's value was forward-filled from a "
            "prior observation rather than observed directly."
        )
    )
    structural_missing: bool = Field(
        default=False,
        description=(
            "True when the asset has been missing for more than 3 "
            "consecutive days (brief §3.2). Excluded from the composite."
        ),
    )


class CurrentIndexMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)

    smoothing_applied: str = Field(
        description="Smoothing methodology applied to per-asset PPU.",
    )
    status: Literal["operational", "partial", "insufficient"] = Field(
        description=(
            "'operational' = every asset observed; 'partial' = one or more "
            "assets structurally missing; 'insufficient' = composite "
            "cannot be computed."
        ),
    )
    structural_missing: list[str] = Field(
        default_factory=list,
        description="Asset keys currently structurally missing.",
    )


class CurrentIndexResponse(BaseModel):
    """Envelope returned by ``GET /tssi/current``.

    Matches the brief's Appendix B contract. Consumers should treat the
    numeric fields as nullable when the pipeline is warming up.
    """

    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    baseline_date: date
    composite_index: Optional[float]
    daily_change_pct: Optional[float]
    assets: dict[str, CurrentAssetEntry]
    metadata: CurrentIndexMetadata


class AssetPPUResponse(BaseModel):
    """Wrapper returned by ``GET /tssi/assets/ppu``."""

    model_config = ConfigDict(frozen=True)

    start_date: date
    end_date: date
    series: list[AssetDailyPPU]


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _today_bangkok() -> date:
    """Today in Asia/Bangkok (UTC+7, no DST)."""
    return (datetime.now(tz=timezone.utc) + timedelta(hours=7)).date()


def _validate_range(start: date, end: date) -> None:
    if end < start:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"end_date ({end}) must be on or after start_date ({start}).",
        )


def _asset_entry_from_detail(
    asset_name: str, detail: dict[str, object]
) -> CurrentAssetEntry:
    unit = ASSET_UNITS.get(asset_name, "g")
    ppu_val = detail.get("ppu")
    price_val = detail.get("nominal_price")
    weight_val = detail.get("net_weight")

    payload: dict[str, object] = {
        "nominal_price_thb": price_val,
        "imputed_flag": bool(detail.get("imputed", False)),
        "structural_missing": bool(detail.get("structural_missing", False)),
    }
    if unit == "g":
        payload["net_weight_g"] = weight_val
        payload["ppu_thb_g"] = ppu_val
    else:
        payload["net_weight_ml"] = weight_val
        payload["ppu_thb_ml"] = ppu_val
    return CurrentAssetEntry(**payload)


# -----------------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------------
@router.get(
    "/current",
    response_model=CurrentIndexResponse,
    summary="Latest composite TSSI with per-asset detail (Appendix B shape)",
)
async def get_current(
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> CurrentIndexResponse:
    """Return the most recent TSSI composite point, baseline-anchored,
    in the shape defined by the TSSI brief Appendix B.
    """
    today = _today_bangkok()
    baseline = settings.tssi_baseline_date

    # We need the composite AND the prior-day composite to compute the
    # daily change percent, so pull the series from baseline through
    # today instead of just the single latest day.
    series: IndexResponse = await compute_index_series(
        session,
        start_date=baseline,
        end_date=today,
        baseline_date=baseline,
    )
    day, detail, _struct = await fetch_latest_asset_detail(
        session,
        end_date=today,
        baseline_date=baseline,
    )

    latest = series.latest
    composite_index: Optional[float] = (
        float(latest.index_value) if latest is not None else None
    )
    daily_change_pct: Optional[float] = None
    if latest is not None:
        # Daily change % over the composite PPU (not the index value, which
        # is itself a 100 · C(t)/C(B) scaling -- a daily % on the PPU and
        # on the index are mathematically identical since they share a
        # constant baseline divisor).
        if len(series.points) >= 2:
            prev = series.points[-2]
            if prev.composite_ppu > 0:
                daily_change_pct = (
                    (latest.composite_ppu / prev.composite_ppu) - 1.0
                ) * 100.0
        if daily_change_pct is None:
            daily_change_pct = 0.0

    assets_payload: dict[str, CurrentAssetEntry] = {
        asset_name: _asset_entry_from_detail(asset_name, detail.get(asset_name, {}))
        for asset_name in BASKET_WEIGHTS.keys()
    }

    if day is not None:
        # Expose the latest day as a Bangkok-midnight UTC timestamp so
        # downstream consumers can key off a deterministic wall-clock.
        ts_day = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    else:
        ts_day = datetime.now(tz=timezone.utc)

    return CurrentIndexResponse(
        timestamp=ts_day,
        baseline_date=baseline,
        composite_index=composite_index,
        daily_change_pct=daily_change_pct,
        assets=assets_payload,
        metadata=CurrentIndexMetadata(
            smoothing_applied=series.metadata.smoothing_applied,
            status=series.metadata.status,
            structural_missing=list(series.metadata.structural_missing),
        ),
    )


@router.get(
    "/historical",
    response_model=IndexResponse,
    summary="TSSI time-series for charting",
)
async def get_historical(
    start_date: date = Query(..., description="Inclusive window start (Bangkok local date)."),
    end_date: date = Query(..., description="Inclusive window end (Bangkok local date)."),
    baseline_date: Optional[date] = Query(
        None,
        description="Override the normalization anchor (defaults to TSSI_BASELINE_DATE).",
    ),
    verified_only: bool = Query(
        False,
        description=(
            "If true, exclude ``derived:*`` observations (e.g. BoT CPI-anchored "
            "synthetic fills) from the composite. The result will only reflect "
            "scraped, archived, and hand-curated (seed) anchors."
        ),
    ),
    smoothed: bool = Query(
        True,
        description=(
            "Apply the 7-day rolling median per-asset PPU smoother described "
            "in the brief §3.3 before aggregation. Pass ``smoothed=false`` "
            "to see the raw, un-smoothed series."
        ),
    ),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> IndexResponse:
    _validate_range(start_date, end_date)

    effective_baseline = baseline_date or settings.tssi_baseline_date
    if end_date < effective_baseline:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"end_date {end_date} is before baseline_date {effective_baseline}; "
                "cannot normalize against a baseline that hasn't happened yet."
            ),
        )

    today = _today_bangkok()
    if end_date > today:
        # Keep the endpoint honest: we never project into the future.
        end_date = today

    return await compute_index_series(
        session,
        start_date=start_date,
        end_date=end_date,
        baseline_date=effective_baseline,
        include_derived=not verified_only,
        smoothed=smoothed,
    )


@router.get(
    "/assets/ppu",
    response_model=AssetPPUResponse,
    summary="Per-asset PPU spread (ffilled)",
)
async def get_asset_ppu(
    start_date: Optional[date] = Query(
        None,
        description="Inclusive start date. Defaults to the configured baseline.",
    ),
    end_date: Optional[date] = Query(
        None,
        description="Inclusive end date. Defaults to today (Asia/Bangkok).",
    ),
    verified_only: bool = Query(
        False,
        description="If true, exclude ``derived:*`` rows (CPI-anchored fills).",
    ),
    smoothed: bool = Query(
        True,
        description=(
            "Apply the 7-day rolling median per-asset smoother. Pass "
            "``smoothed=false`` to see the raw per-asset PPU."
        ),
    ),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> AssetPPUResponse:
    effective_start = start_date or settings.tssi_baseline_date
    effective_end = end_date or _today_bangkok()
    _validate_range(effective_start, effective_end)

    series = await fetch_asset_ppu_series(
        session,
        start_date=effective_start,
        end_date=effective_end,
        include_derived=not verified_only,
        smoothed=smoothed,
    )
    return AssetPPUResponse(
        start_date=effective_start,
        end_date=effective_end,
        series=series,
    )
