"""Persistence helpers for scraped observations.

The ``tssi_raw_data.ppu`` column is GENERATED ALWAYS — this module deliberately
does **not** include it in INSERTs. The composite primary key
``(time, asset_name, platform_source)`` makes the upsert naturally idempotent:
re-running a scrape inside the same second for the same asset is a no-op.
"""

from __future__ import annotations

import logging

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import TSSIRawData
from app.scraper.schemas import AssetObservation

logger = logging.getLogger(__name__)


async def insert_observation(
    session: AsyncSession,
    observation: AssetObservation,
) -> bool:
    """Insert an observation, skipping silently on primary-key conflict.

    Returns ``True`` if a row was written, ``False`` if it was a duplicate.
    The caller owns transaction boundaries — this function does NOT commit.
    """
    stmt = (
        pg_insert(TSSIRawData)
        .values(
            time=observation.time,
            asset_name=observation.asset_name,
            platform_source=observation.platform_source,
            nominal_price=observation.nominal_price,
            net_weight=observation.net_weight,
            unit_type=observation.unit_type,
        )
        .on_conflict_do_nothing(
            index_elements=["time", "asset_name", "platform_source"]
        )
    )
    result = await session.execute(stmt)
    written = (result.rowcount or 0) > 0
    if not written:
        logger.debug(
            "duplicate observation skipped: %s@%s t=%s",
            observation.asset_name,
            observation.platform_source,
            observation.time.isoformat(),
        )
    return written
