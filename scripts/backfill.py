#!/usr/bin/env python3
"""Historical-backfill CLI for TSSI.

Pumps observations from one or more :mod:`app.backfill` sources into
``tssi_raw_data``. Re-running is always safe — the existing insert path
uses ``ON CONFLICT DO NOTHING`` on ``(time, asset_name, platform_source)``.

Examples::

    # Dry-run the seed CSV, show what would be written.
    python scripts/backfill.py --source seed --dry-run

    # Backfill Wayback snapshots since 2022 for all five basket assets.
    python scripts/backfill.py --source wayback --since 2022-01-01

    # Fill gaps via BoT CPI anchors for a specific window.
    python scripts/backfill.py --source cpi --since 2020-01-01 --until 2023-12-31

    # Run every source (seed first, then wayback, then cpi — CPI uses
    # seed output as its anchor set, so that ordering matters).
    python scripts/backfill.py --source all --since 2020-01-01

Exit codes:
    0  at least one row successfully inserted (or clean dry-run)
    1  no source produced any rows
    2  argument / configuration error
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import AsyncIterator, Iterable, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.backfill.base import HistoricalSource, classify_platform_source  # noqa: E402
from app.backfill.bot_cpi import BotCpiSource  # noqa: E402
from app.backfill.seed import SeedCSVSource  # noqa: E402
from app.backfill.wayback import WaybackSource  # noqa: E402
from app.db.session import AsyncSessionLocal  # noqa: E402
from app.scraper.repository import insert_observation  # noqa: E402
from app.scraper.schemas import AssetObservation  # noqa: E402

logger = logging.getLogger("tssi.backfill")


@dataclass(slots=True)
class RunStats:
    """Per-source bookkeeping for the final report."""

    source: str
    yielded: int = 0
    inserted: int = 0
    duplicates: int = 0
    failures: int = 0
    by_provenance: dict[str, int] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.by_provenance is None:
            self.by_provenance = defaultdict(int)


def _parse_date(token: str) -> date:
    try:
        return datetime.strptime(token, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"invalid date {token!r} (expected YYYY-MM-DD): {exc}"
        )


def _build_sources(selection: list[str]) -> list[HistoricalSource]:
    """Instantiate each selected adapter, in dependency order.

    ``cpi`` depends on ``seed`` (it multiplies seed anchors against CPI),
    so when ``all`` is chosen we order them so seed rows are written to
    the DB before CPI derive runs — though because BoT-CPI reads the CSV
    directly rather than the DB, the ordering is only a convention.
    """
    selection = [s.lower() for s in selection]
    if "all" in selection:
        selection = ["seed", "wayback", "cpi"]

    sources: list[HistoricalSource] = []
    for name in selection:
        if name == "seed":
            sources.append(SeedCSVSource())
        elif name == "wayback":
            sources.append(WaybackSource())
        elif name in {"cpi", "bot_cpi"}:
            sources.append(BotCpiSource())
        else:
            raise argparse.ArgumentTypeError(
                f"unknown source {name!r} — expected seed|wayback|cpi|all"
            )
    return sources


async def _iter_source(
    source: HistoricalSource,
    *,
    since: Optional[date],
    until: Optional[date],
) -> AsyncIterator[AssetObservation]:
    async for obs in source.iter_observations(since=since, until=until):
        yield obs


async def _run_source(
    source: HistoricalSource,
    *,
    since: Optional[date],
    until: Optional[date],
    dry_run: bool,
    batch_size: int,
) -> RunStats:
    stats = RunStats(source=source.name)

    async def flush(batch: list[AssetObservation]) -> None:
        if not batch:
            return
        if dry_run:
            # Count everything as "would-insert" in dry-run; the
            # deduplication math still gets exercised on the real run.
            for obs in batch:
                stats.inserted += 1
                stats.by_provenance[str(classify_platform_source(obs.platform_source))] += 1
            batch.clear()
            return

        async with AsyncSessionLocal() as session:
            try:
                for obs in batch:
                    try:
                        written = await insert_observation(session, obs)
                    except Exception:  # noqa: BLE001
                        stats.failures += 1
                        logger.exception(
                            "failed to insert obs %s %s @ %s",
                            obs.asset_name,
                            obs.platform_source,
                            obs.time.isoformat(),
                        )
                        continue
                    if written:
                        stats.inserted += 1
                        stats.by_provenance[
                            str(classify_platform_source(obs.platform_source))
                        ] += 1
                    else:
                        stats.duplicates += 1
                await session.commit()
            except Exception:  # noqa: BLE001
                await session.rollback()
                raise
        batch.clear()

    batch: list[AssetObservation] = []
    async for obs in _iter_source(source, since=since, until=until):
        stats.yielded += 1
        batch.append(obs)
        if len(batch) >= batch_size:
            await flush(batch)
    await flush(batch)

    # Adapters that hold a persistent network session should release it.
    close = getattr(source, "close", None)
    if close is not None:
        try:
            await close()
        except Exception:  # noqa: BLE001 - best-effort cleanup
            logger.debug("close() on %s failed", source.name, exc_info=True)

    return stats


def _print_report(results: Iterable[RunStats], *, dry_run: bool) -> None:
    print("=" * 64)
    print(f"TSSI backfill report ({'DRY RUN' if dry_run else 'LIVE'})")
    print("=" * 64)
    total_in = total_yield = total_dup = total_fail = 0
    provenance_total: dict[str, int] = defaultdict(int)
    for r in results:
        print(f"[{r.source}]  yielded={r.yielded}  inserted={r.inserted}  "
              f"duplicates={r.duplicates}  failures={r.failures}")
        for k, v in sorted(r.by_provenance.items()):
            print(f"    - {k}: {v}")
            provenance_total[k] += v
        total_yield += r.yielded
        total_in += r.inserted
        total_dup += r.duplicates
        total_fail += r.failures
    print("-" * 64)
    print(f"TOTAL  yielded={total_yield}  inserted={total_in}  "
          f"duplicates={total_dup}  failures={total_fail}")
    if provenance_total:
        print("  provenance mix:")
        for k, v in sorted(provenance_total.items()):
            print(f"    - {k}: {v}")
    print("=" * 64)


def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="tssi-backfill",
        description="Historical backfill runner for the Thai Street Survival Index.",
    )
    p.add_argument(
        "--source",
        action="append",
        required=True,
        choices=["seed", "wayback", "cpi", "bot_cpi", "all"],
        help="Source adapter(s) to run. Repeat to run several in order, or pass 'all'.",
    )
    p.add_argument(
        "--since",
        type=_parse_date,
        default=None,
        help="Earliest observation date to emit (YYYY-MM-DD). Default: no lower bound.",
    )
    p.add_argument(
        "--until",
        type=_parse_date,
        default=None,
        help="Latest observation date to emit (YYYY-MM-DD). Default: no upper bound.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Iterate sources and print the report without touching the database.",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Rows per DB commit. Larger = faster but more rollback blast radius.",
    )
    p.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase logging verbosity (-v INFO, -vv DEBUG).",
    )
    return p


async def _amain(argv: list[str]) -> int:
    parser = _build_argparser()
    args = parser.parse_args(argv)

    level = logging.WARNING
    if args.verbose == 1:
        level = logging.INFO
    elif args.verbose >= 2:
        level = logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
    )

    try:
        sources = _build_sources(args.source)
    except argparse.ArgumentTypeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.since and args.until and args.until < args.since:
        print("error: --until must be on or after --since", file=sys.stderr)
        return 2

    reports: list[RunStats] = []
    for source in sources:
        logger.info("running source %s", source.name)
        try:
            stats = await _run_source(
                source,
                since=args.since,
                until=args.until,
                dry_run=args.dry_run,
                batch_size=max(args.batch_size, 1),
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("source %s aborted: %s", source.name, exc)
            stats = RunStats(source=source.name, failures=1)
        reports.append(stats)

    _print_report(reports, dry_run=args.dry_run)

    any_inserted = any(r.inserted > 0 for r in reports)
    return 0 if any_inserted else 1


def main() -> None:
    raise SystemExit(asyncio.run(_amain(sys.argv[1:])))


if __name__ == "__main__":
    main()
