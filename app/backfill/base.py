"""Shared contract for every historical-data source.

A :class:`HistoricalSource` is an async iterable of
:class:`~app.scraper.schemas.AssetObservation`. Concrete adapters
(Wayback CDX, hand-curated CSV seed, BoT CPI-derived interpolation) all
satisfy this protocol, which lets the ``scripts/backfill.py`` CLI treat
them uniformly and lets tests mock a source with an async generator.

The ``platform_source`` field on every emitted observation is
**namespaced** so the run-vs-derived-vs-archived distinction is visible
at the SQL level without needing a schema change:

==========================  ====================================================
platform_source             meaning
==========================  ====================================================
``lazada`` / ``shopee`` …   live scrape from the retail surface (Phase 2)
``wayback:<platform>``      Internet Archive snapshot of that retail surface
``seed:manual``             hand-curated observation from receipts / news
``derived:bot_cpi``         CPI-anchored synthetic fill between known points
==========================  ====================================================
"""

from __future__ import annotations

from datetime import date
from enum import StrEnum
from typing import AsyncIterator, Optional, Protocol, runtime_checkable

from app.scraper.schemas import AssetObservation


class SourceKind(StrEnum):
    """Coarse classification of where an observation came from."""

    SCRAPE = "scrape"
    ARCHIVE = "archive"
    SEED = "seed"
    DERIVED = "derived"


# Map every known ``platform_source`` prefix to a :class:`SourceKind`.
# The matcher below uses the first key to match (prefix-before-colon),
# falling back to ``SCRAPE`` for bare platform names (``lazada`` etc.).
SOURCE_KIND_PREFIXES: dict[str, SourceKind] = {
    "wayback": SourceKind.ARCHIVE,
    "seed": SourceKind.SEED,
    "derived": SourceKind.DERIVED,
}


def classify_platform_source(platform_source: str) -> SourceKind:
    """Return the provenance bucket implied by a ``platform_source`` string.

    Strings without a ``"prefix:"`` segment are treated as live scrapes.
    Unknown prefixes are conservatively classified as ``SCRAPE`` so a
    future adapter that forgets to register here still ingests cleanly.
    """
    if ":" in platform_source:
        prefix = platform_source.split(":", 1)[0].lower()
        return SOURCE_KIND_PREFIXES.get(prefix, SourceKind.SCRAPE)
    return SourceKind.SCRAPE


@runtime_checkable
class HistoricalSource(Protocol):
    """Async-iterable supplier of historical TSSI observations.

    Implementations MUST:

    * Emit observations whose ``time`` is in UTC (the DB column is
      ``TIMESTAMPTZ``; Bangkok-local conversion happens in the calc
      engine on read).
    * Use a namespaced ``platform_source`` — see :data:`SOURCE_KIND_PREFIXES`.
    * Be safe to re-run: the repository layer upserts with
      ``ON CONFLICT DO NOTHING`` on ``(time, asset_name, platform_source)``.
    """

    name: str

    def iter_observations(
        self,
        *,
        since: Optional[date] = None,
        until: Optional[date] = None,
    ) -> AsyncIterator[AssetObservation]:
        ...


__all__ = [
    "HistoricalSource",
    "SourceKind",
    "SOURCE_KIND_PREFIXES",
    "classify_platform_source",
]
