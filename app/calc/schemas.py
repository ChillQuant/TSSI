"""DTOs returned by the calculation engine."""

from __future__ import annotations

from datetime import date
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class AssetDailyPPU(BaseModel):
    """Per-asset, per-day PPU after forward-filling gaps."""

    model_config = ConfigDict(frozen=True)

    day: date
    asset_name: str
    ppu: float = Field(ge=0)
    imputed: bool = Field(
        default=False,
        description="True if this row was forward-filled rather than observed.",
    )
    structural_missing: bool = Field(
        default=False,
        description=(
            "True when the asset has been missing for more than 3 consecutive "
            "days and the ffill chain has been broken (brief §3.2)."
        ),
    )
    nominal_price: Optional[float] = Field(
        default=None,
        description="Average nominal THB price for the day, when observed.",
    )
    net_weight: Optional[float] = Field(
        default=None,
        description=(
            "Net weight / volume in canonical units (g for solids, ml for liquids)."
        ),
    )


class IndexPoint(BaseModel):
    """A single composite-index sample."""

    model_config = ConfigDict(frozen=True)

    day: date
    composite_ppu: float
    index_value: float = Field(description="Baseline-normalized, 100 at baseline date.")
    sym_pct_change: float = Field(
        description=(
            "Symmetric percent change vs. baseline, in the range (-2, +2) "
            "where 0.0 == baseline."
        ),
    )


class IndexMetadata(BaseModel):
    """Observability metadata for a single computed series."""

    model_config = ConfigDict(frozen=True)

    smoothing_applied: str = Field(
        default="7-day rolling median",
        description=(
            "Smoothing / noise-reduction step applied to per-asset PPU before "
            "the weighted composite. Matches the brief §3.3 methodology."
        ),
    )
    status: Literal["operational", "partial", "insufficient"] = Field(
        default="operational",
        description=(
            "'operational' = every basket asset observed through the output "
            "day; 'partial' = at least one asset is structurally missing "
            "(>3 consecutive days) and the composite renormalizes weights "
            "over the remaining assets; 'insufficient' = the composite "
            "cannot be computed at all for the window."
        ),
    )
    structural_missing: list[str] = Field(
        default_factory=list,
        description="Asset keys currently flagged as structurally missing.",
    )


class IndexResponse(BaseModel):
    """Payload for the /tssi/current and /tssi/historical endpoints."""

    baseline_date: date
    baseline_composite_ppu: float
    points: list[IndexPoint]
    weights: dict[str, float]
    metadata: IndexMetadata = Field(default_factory=IndexMetadata)

    @property
    def latest(self) -> Optional[IndexPoint]:
        return self.points[-1] if self.points else None
