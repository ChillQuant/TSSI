"""Historical-backfill adapters for the TSSI basket.

Each adapter emits :class:`app.scraper.schemas.AssetObservation` records
so they flow through the same idempotent ``INSERT ... ON CONFLICT DO
NOTHING`` path used by the live scraper, distinguished only by a
namespaced ``platform_source`` string (``wayback:lazada``,
``seed:manual``, ``derived:bot_cpi`` …). Keeping the schema flat means
the hypertable, the calc engine and the API layer all treat live scrapes
and backfills as first-class citizens without any migration work.

Light symbols only are re-exported here so depending modules (tests, the
CLI, the calc engine) don't drag in ``httpx`` / ``pandas`` unless they
actually need a concrete adapter.
"""

from app.backfill.base import (
    HistoricalSource,
    SourceKind,
    SOURCE_KIND_PREFIXES,
    classify_platform_source,
)

__all__ = [
    "HistoricalSource",
    "SourceKind",
    "SOURCE_KIND_PREFIXES",
    "classify_platform_source",
]
