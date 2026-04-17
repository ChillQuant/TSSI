"""CPI-anchored synthetic fill between hand-curated seed anchors.

Background
----------
The Bank of Thailand publishes a monthly Consumer Price Index with a
fine-grained sub-series for *processed food, beverages, tobacco* (group
01.1.5 in the current classification). For the months between two
hand-curated price anchors we use that index to *derive* a plausible
monthly PPU, tagged ``platform_source = "derived:bot_cpi"``.

This is obviously not a scraped observation — the ``derived:`` prefix
makes that provenance explicit to the calc engine (see
:func:`app.backfill.base.classify_platform_source`) and to the public
site's "source mix" panel. The calc engine can be asked to exclude
derived rows via an ``include_derived=False`` flag when verified-only
numbers are required.

Input files (both CSV, shipped empty skeletons the deployer fills in):

``data/seed/tssi_seed.csv``
    Hand-curated anchor points — the *numerators* of the ratio.
``data/cpi/thailand_food_bev_cpi.csv``
    Monthly CPI values with columns ``year_month,cpi_value``.

Strategy
--------
1. For each asset, pick every anchor from the seed CSV and build a set
   of ``{month_key -> (anchor_date, anchor_ppu, anchor_cpi)}``.
2. For every month between the earliest and latest anchor (bounded by
   the caller's ``since`` / ``until``), if there is no real anchor in
   that month, derive ``ppu_m = anchor_ppu * (cpi_m / anchor_cpi)``
   where ``(anchor_ppu, anchor_cpi)`` is the nearest preceding anchor
   (ffill semantics).
3. Emit one observation per derived month at the 15th, 09:00 Bangkok,
   with ``platform_source = "derived:bot_cpi"``.

The adapter is deterministic and re-entrant; combined with the existing
``ON CONFLICT DO NOTHING`` insert, re-running it is always a no-op for
months that already have a real observation.
"""

from __future__ import annotations

import csv
import logging
from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import AsyncIterator, Optional

from app.backfill.seed import SeedCSVSource
from app.scraper.assets import ASSET_REGISTRY, AssetId
from app.scraper.schemas import AssetObservation

logger = logging.getLogger(__name__)

_BANGKOK_NOON = time(9, 0)
_BANGKOK_OFFSET = timedelta(hours=7)

_DEFAULT_CPI_PATH = (
    Path(__file__).resolve().parents[2] / "data" / "cpi" / "thailand_food_bev_cpi.csv"
)


@dataclass(frozen=True, slots=True)
class _Anchor:
    """A seed anchor point flattened for month-wise lookup."""

    day: date
    asset_name: str
    ppu: Decimal


def _parse_year_month(token: str) -> date:
    """Accept ``YYYY-MM`` or ``YYYY-MM-DD``; normalize to the 1st of the month."""
    token = token.strip()
    for fmt in ("%Y-%m", "%Y-%m-%d"):
        try:
            return datetime.strptime(token, fmt).date().replace(day=1)
        except ValueError:
            continue
    raise ValueError(f"bad year_month token {token!r}")


def _load_cpi_series(path: Path) -> dict[date, Decimal]:
    """Parse the CPI CSV into ``{first-of-month -> index_value}``."""
    if not path.is_file():
        logger.warning("CPI CSV not found at %s — CPI derive yields nothing", path)
        return {}
    out: dict[date, Decimal] = {}
    with path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None or "year_month" not in reader.fieldnames or (
            "cpi_value" not in reader.fieldnames
        ):
            raise ValueError(
                f"CPI CSV {path} missing headers; need 'year_month' and 'cpi_value'"
            )
        for line_no, row in enumerate(reader, start=2):
            # Skip comment rows whose ``year_month`` starts with ``#``.
            ym_cell = (row.get("year_month") or "").lstrip()
            if not ym_cell or ym_cell.startswith("#"):
                continue
            try:
                month_key = _parse_year_month(row["year_month"])
                value = Decimal(row["cpi_value"].strip())
            except (ValueError, InvalidOperation, AttributeError) as exc:
                logger.warning("CPI row %d skipped: %s", line_no, exc)
                continue
            if value <= 0:
                logger.warning("CPI row %d skipped: non-positive value", line_no)
                continue
            out[month_key] = value
    return out


def _month_iter(start: date, end: date):
    """Yield the 1st-of-month for every month in ``[start, end]`` inclusive."""
    current = start.replace(day=1)
    end_key = end.replace(day=1)
    while current <= end_key:
        yield current
        # Advance by one month.
        year = current.year + (current.month // 12)
        month = (current.month % 12) + 1
        current = date(year, month, 1)


async def _collect_seed_anchors(
    seed: SeedCSVSource, since: Optional[date], until: Optional[date]
) -> dict[str, list[_Anchor]]:
    """Group real anchors by asset_name, sorted by day ascending."""
    grouped: dict[str, list[_Anchor]] = defaultdict(list)
    async for obs in seed.iter_observations(since=since, until=until):
        # We only want real-world anchors — derived:* rows from other
        # tooling shouldn't feed back into a re-derivation.
        if obs.platform_source.lower().startswith("derived:"):
            continue
        grouped[obs.asset_name].append(
            _Anchor(day=obs.time.date(), asset_name=obs.asset_name, ppu=obs.ppu)
        )
    for anchors in grouped.values():
        anchors.sort(key=lambda a: a.day)
    return grouped


def _nearest_preceding(
    anchors: list[_Anchor], target: date
) -> Optional[_Anchor]:
    """Return the most recent anchor on or before ``target``."""
    if not anchors:
        return None
    days = [a.day for a in anchors]
    idx = bisect_right(days, target) - 1
    if idx < 0:
        # ``target`` is before the first anchor — use the first one anyway.
        # (An anchor from later is a worse proxy than one from earlier, but
        # in practice CPI series start well before retail anchors.)
        return anchors[0]
    return anchors[idx]


def _bangkok_day_to_utc(d: date) -> datetime:
    """09:00 Asia/Bangkok for the given day, expressed in UTC."""
    local = datetime.combine(d, _BANGKOK_NOON)
    return (local - _BANGKOK_OFFSET).replace(tzinfo=timezone.utc)


class BotCpiSource:
    """``HistoricalSource`` that derives monthly PPUs from CPI + seed anchors."""

    name = "bot_cpi"

    def __init__(
        self,
        *,
        seed_source: Optional[SeedCSVSource] = None,
        cpi_path: Optional[Path] = None,
    ) -> None:
        self._seed = seed_source or SeedCSVSource()
        self._cpi_path = Path(cpi_path) if cpi_path is not None else _DEFAULT_CPI_PATH

    async def iter_observations(
        self,
        *,
        since: Optional[date] = None,
        until: Optional[date] = None,
    ) -> AsyncIterator[AssetObservation]:
        cpi = _load_cpi_series(self._cpi_path)
        if not cpi:
            return

        anchors_by_asset = await _collect_seed_anchors(self._seed, since=since, until=until)
        if not anchors_by_asset:
            logger.info("bot_cpi: no seed anchors available — nothing to derive")
            return

        # The effective window: caller bounds, intersected with CPI coverage
        # and the span of available seed anchors.
        cpi_months = sorted(cpi.keys())
        cpi_start, cpi_end = cpi_months[0], cpi_months[-1]
        window_start = max(cpi_start, since or cpi_start)
        window_end = min(cpi_end, until or cpi_end)
        if window_end < window_start:
            return

        for asset_name, anchors in anchors_by_asset.items():
            # Month keys that already contain a real anchor — we skip those
            # so the derived series interleaves cleanly with the real one.
            real_months = {a.day.replace(day=1) for a in anchors}
            for month_key in _month_iter(window_start, window_end):
                if month_key in real_months:
                    continue
                anchor = _nearest_preceding(anchors, month_key)
                if anchor is None:
                    continue
                anchor_month = anchor.day.replace(day=1)
                anchor_cpi = cpi.get(anchor_month)
                cpi_now = cpi.get(month_key)
                if anchor_cpi is None or cpi_now is None:
                    # Missing CPI months are possible at the series edges;
                    # skip rather than extrapolate.
                    continue
                ratio = cpi_now / anchor_cpi
                # NUMERIC(12, 4) in the DB — 4 fractional digits is the
                # effective precision ceiling for ``nominal_price``.
                derived_ppu = (anchor.ppu * ratio).quantize(Decimal("0.0001"))
                if derived_ppu <= 0:
                    continue
                # Re-materialize the PPU as a (price, weight=1) pair so it
                # slots into the unchanged schema. The calc engine reads
                # only the generated ``ppu`` column, so weight=1 is an
                # honest representation of a derived PPU anchor. The
                # unit_type is still looked up from the registry so the
                # DB's CHECK constraint passes.
                try:
                    cfg = ASSET_REGISTRY[AssetId(asset_name)]
                    unit_type = cfg.unit_type
                except (KeyError, ValueError):
                    logger.warning(
                        "bot_cpi: unknown asset %s in seed — skipping", asset_name
                    )
                    continue
                obs_day = date(month_key.year, month_key.month, 15)
                yield AssetObservation(
                    time=_bangkok_day_to_utc(obs_day),
                    asset_name=asset_name,
                    platform_source="derived:bot_cpi",
                    nominal_price=derived_ppu,
                    net_weight=Decimal("1"),
                    unit_type=unit_type,
                )


__all__ = ["BotCpiSource"]
