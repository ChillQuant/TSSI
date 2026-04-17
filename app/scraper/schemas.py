"""Pydantic DTOs shared between the scraper and the API layer."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class AssetObservation(BaseModel):
    """A fully-parsed scrape result, ready for persistence."""

    model_config = ConfigDict(frozen=True)

    time: datetime
    asset_name: str
    platform_source: str
    nominal_price: Decimal = Field(ge=0)
    net_weight: Decimal = Field(gt=0)
    unit_type: str

    @property
    def ppu(self) -> Decimal:
        """Client-side mirror of the DB-side generated column."""
        return self.nominal_price / self.net_weight


class IngestionResult(BaseModel):
    """Per-asset outcome of a single scrape attempt."""

    asset_id: str
    platform_source: str
    success: bool
    observation: Optional[AssetObservation] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    duration_ms: int


class IngestionReport(BaseModel):
    """Aggregate outcome of a pipeline run across the whole basket."""

    started_at: datetime
    finished_at: datetime
    results: list[IngestionResult]

    @property
    def success_count(self) -> int:
        return sum(1 for r in self.results if r.success)

    @property
    def failure_count(self) -> int:
        return sum(1 for r in self.results if not r.success)
