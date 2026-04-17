"""SQLAlchemy ORM models for TSSI.

The schema mirrors `db/init/01_init_hypertable.sql` exactly so that either
path (init-script on first boot *or* Alembic migration) produces an identical
database shape.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    CheckConstraint,
    Computed,
    Index,
    Numeric,
    PrimaryKeyConstraint,
    String,
)
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class TSSIRawData(Base):
    """Raw scraped observation for a single (asset, platform, timestamp) tuple.

    This table is converted into a TimescaleDB hypertable on `time`. The PPU
    column is a STORED generated column, computed by Postgres itself from
    ``nominal_price / net_weight``; application code must never insert into it.
    """

    __tablename__ = "tssi_raw_data"

    # ---- Time / identity --------------------------------------------------
    time: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
    )
    asset_name: Mapped[str] = mapped_column(String(64), nullable=False)
    platform_source: Mapped[str] = mapped_column(String(64), nullable=False)

    # ---- Measurements -----------------------------------------------------
    nominal_price: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    net_weight: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    unit_type: Mapped[str] = mapped_column(String(4), nullable=False)

    # ---- Derived ----------------------------------------------------------
    ppu: Mapped[Decimal] = mapped_column(
        Numeric(18, 6),
        Computed("nominal_price / NULLIF(net_weight, 0)", persisted=True),
        nullable=True,
    )

    __table_args__ = (
        PrimaryKeyConstraint("time", "asset_name", "platform_source", name="pk_tssi_raw_data"),
        CheckConstraint("nominal_price >= 0", name="ck_tssi_nominal_price_nonneg"),
        CheckConstraint("net_weight > 0", name="ck_tssi_net_weight_positive"),
        CheckConstraint("unit_type IN ('g', 'ml')", name="ck_tssi_unit_type"),
        Index("idx_tssi_asset_time", "asset_name", "time"),
        Index("idx_tssi_platform_time", "platform_source", "time"),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return (
            f"<TSSIRawData {self.asset_name}@{self.platform_source} "
            f"t={self.time.isoformat() if self.time else None} "
            f"price={self.nominal_price} weight={self.net_weight}{self.unit_type} "
            f"ppu={self.ppu}>"
        )
