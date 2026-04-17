"""Hand-curated CSV importer.

The seed file is the **source of truth for known retail anchors** —
receipts, news reports, archived product pages that aren't in the
Wayback Machine, or conversations with the 7-Eleven store across the
street. It's also the input to the BoT-CPI derive adapter, which
multiplies these known points against a monthly CPI series to fill in
the months between anchors.

File format (``data/seed/tssi_seed.csv``):

    day,asset_name,platform_source,nominal_price,net_weight,unit_type,note
    2020-01-15,mama_tomyum_pack,seed:manual,6.00,55,g,receipt from Tops Rama 4
    2020-01-15,m150_bottle,seed:manual,10.00,150,ml,retail list price
    2020-01-15,mahboonkrong_rice_5kg,seed:estimate,180.00,5000,g,2020 Q1 retail
    ...

Rules:

* ``day`` is a Bangkok-local date (YYYY-MM-DD). We materialize each row
  at 09:00 Bangkok time (02:00 UTC) to keep them comfortably inside the
  target day regardless of DST quirks in downstream consumers.
* ``platform_source`` may be any string; if omitted we fall back to
  ``seed:manual``. Rows flagged ``derived:*`` belong in the BoT-CPI
  adapter and will be ignored here to keep provenance buckets clean.
* Empty / malformed rows are skipped with a WARNING, not a hard failure,
  because a single garbled line shouldn't halt the backfill.
"""

from __future__ import annotations

import csv
import logging
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import AsyncIterator, Optional

from app.scraper.schemas import AssetObservation

logger = logging.getLogger(__name__)

# A fixed time-of-day for every seed observation, chosen so that the UTC
# conversion lands on the same Bangkok calendar day regardless of DST
# (Thailand has no DST, but downstream consumers might).
_BANGKOK_NOON = time(9, 0)
_BANGKOK_OFFSET = timedelta(hours=7)

_DEFAULT_SEED_PATH = (
    Path(__file__).resolve().parents[2] / "data" / "seed" / "tssi_seed.csv"
)

_EXPECTED_HEADERS = {
    "day",
    "asset_name",
    "nominal_price",
    "net_weight",
    "unit_type",
}

_ALLOWED_UNITS = {"g", "ml"}


def _bangkok_day_to_utc(d: date) -> datetime:
    """09:00 Asia/Bangkok for the given day, expressed in UTC."""
    local = datetime.combine(d, _BANGKOK_NOON)
    return (local - _BANGKOK_OFFSET).replace(tzinfo=timezone.utc)


def _parse_date(token: str) -> date:
    return datetime.strptime(token.strip(), "%Y-%m-%d").date()


def _parse_decimal(token: str, *, field: str, raw_row: int) -> Decimal:
    try:
        return Decimal(token.strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(
            f"seed row {raw_row}: bad {field} {token!r}: {exc}"
        ) from exc


class SeedCSVSource:
    """``HistoricalSource`` backed by a local CSV of known anchor points."""

    name = "seed"

    def __init__(
        self,
        path: Optional[Path] = None,
        *,
        default_platform_source: str = "seed:manual",
    ) -> None:
        self.path = Path(path) if path is not None else _DEFAULT_SEED_PATH
        self.default_platform_source = default_platform_source

    async def iter_observations(
        self,
        *,
        since: Optional[date] = None,
        until: Optional[date] = None,
    ) -> AsyncIterator[AssetObservation]:
        if not self.path.is_file():
            logger.warning("seed CSV not found at %s — nothing to backfill", self.path)
            return

        with self.path.open("r", newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames is None or not _EXPECTED_HEADERS.issubset(
                set(reader.fieldnames)
            ):
                raise ValueError(
                    f"seed CSV {self.path} missing headers; "
                    f"need superset of {sorted(_EXPECTED_HEADERS)}"
                )

            for line_no, row in enumerate(reader, start=2):  # start=2 for header
                # Lightweight comment support: rows whose ``day`` cell
                # starts with '#' are ignored silently so the CSV can
                # carry inline provenance notes.
                day_cell = (row.get("day") or "").lstrip()
                if not day_cell or day_cell.startswith("#"):
                    continue
                try:
                    obs = self._row_to_observation(row, line_no)
                except ValueError as exc:
                    logger.warning("skipping %s", exc)
                    continue
                if since is not None and obs.time.date() < since:
                    continue
                if until is not None and obs.time.date() > until:
                    continue
                yield obs

    def _row_to_observation(
        self, row: dict[str, str], line_no: int
    ) -> AssetObservation:
        asset_name = (row.get("asset_name") or "").strip()
        if not asset_name:
            raise ValueError(f"seed row {line_no}: empty asset_name")

        platform_source = (
            (row.get("platform_source") or "").strip() or self.default_platform_source
        )
        # We intentionally ignore derived:* rows here; those belong to the
        # BoT CPI adapter so the two provenance buckets don't mix.
        if platform_source.lower().startswith("derived:"):
            raise ValueError(
                f"seed row {line_no}: derived:* platform_source belongs in "
                f"bot_cpi adapter, not the seed CSV"
            )

        day = _parse_date(row["day"])
        price = _parse_decimal(row["nominal_price"], field="nominal_price", raw_row=line_no)
        weight = _parse_decimal(row["net_weight"], field="net_weight", raw_row=line_no)
        unit_type = (row.get("unit_type") or "").strip().lower()

        if unit_type not in _ALLOWED_UNITS:
            raise ValueError(
                f"seed row {line_no}: unit_type must be one of "
                f"{sorted(_ALLOWED_UNITS)}, got {unit_type!r}"
            )
        if price < 0:
            raise ValueError(f"seed row {line_no}: negative nominal_price {price}")
        if weight <= 0:
            raise ValueError(f"seed row {line_no}: non-positive net_weight {weight}")

        return AssetObservation(
            time=_bangkok_day_to_utc(day),
            asset_name=asset_name,
            platform_source=platform_source,
            nominal_price=price,
            net_weight=weight,
            unit_type=unit_type,
        )


__all__ = ["SeedCSVSource"]
