#!/usr/bin/env python3
"""Historical-backfill CLI that writes to ``data/observations.csv``.

Sibling to :mod:`scripts.backfill`, which upserts into the Postgres
hypertable. This variant is for the **static-publishing pipeline**: it
reuses every ``app.backfill`` adapter (seed CSV, Wayback CDX, BoT CPI
derive) but flushes the emitted :class:`AssetObservation` stream into
``data/observations.csv`` — the single source-of-truth file the static
build reads.

Re-runs are safe: rows are deduplicated on the
``(day, asset_name, platform_source)`` triple by pre-loading whatever is
already on disk, so re-running the CPI adapter after a seed edit adds
only the genuinely-new months.

Examples::

    # Dry-run the seed CSV, show what would be written.
    python scripts/backfill_to_csv.py --source seed --dry-run

    # Backfill Wayback snapshots since 2022 for every basket asset.
    python scripts/backfill_to_csv.py --source wayback --since 2022-01-01

    # Fill gaps via BoT CPI anchors for a specific window.
    python scripts/backfill_to_csv.py --source cpi --since 2020-01-01 --until 2026-12-31

    # Run every source (seed -> wayback -> cpi; CPI uses seed as anchors).
    python scripts/backfill_to_csv.py --source all --since 2020-01-01

Exit codes:
    0  at least one row appended (or clean dry-run)
    1  no source produced any rows
    2  argument / configuration error
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import AsyncIterator, Iterable, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.backfill.base import HistoricalSource, classify_platform_source  # noqa: E402
from app.backfill.bot_cpi import BotCpiSource  # noqa: E402
from app.backfill.seed import SeedCSVSource  # noqa: E402
from app.backfill.wayback import WaybackSource  # noqa: E402
from app.scraper.schemas import AssetObservation  # noqa: E402

logger = logging.getLogger("tssi.backfill_to_csv")

# Default output file. Overridable via --out.
DEFAULT_OUT = ROOT / "data" / "observations.csv"

# Column order for observations.csv. MUST stay in sync with the header
# in data/observations.csv and with app/db/csv_session.py::_load_frame.
CSV_FIELDS = (
    "day",
    "asset_name",
    "nominal_price",
    "net_weight",
    "unit_type",
    "platform_source",
    "note",
)

_BANGKOK_OFFSET = timedelta(hours=7)


@dataclass(slots=True)
class RunStats:
    """Per-source bookkeeping for the final report."""

    source: str
    yielded: int = 0
    appended: int = 0
    duplicates: int = 0
    failures: int = 0
    by_provenance: dict[str, int] = field(default_factory=lambda: defaultdict(int))


def _parse_date(token: str) -> date:
    try:
        return datetime.strptime(token, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"invalid date {token!r} (expected YYYY-MM-DD): {exc}"
        )


def _build_sources(selection: list[str]) -> list[HistoricalSource]:
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
                f"unknown source {name!r} -- expected seed|wayback|cpi|all"
            )
    return sources


def _obs_bangkok_day(obs: AssetObservation) -> date:
    """Convert the observation's UTC timestamp back to a Bangkok date."""
    return (obs.time.astimezone(timezone.utc) + _BANGKOK_OFFSET).date()


def _obs_key(obs: AssetObservation) -> tuple[str, str, str]:
    """Dedup key matching the natural uniqueness of a daily anchor."""
    return (
        _obs_bangkok_day(obs).isoformat(),
        obs.asset_name,
        obs.platform_source,
    )


def _load_existing_keys(path: Path) -> set[tuple[str, str, str]]:
    """Scan the target CSV (skipping comments) and return existing dedup keys."""
    keys: set[tuple[str, str, str]] = set()
    if not path.is_file():
        return keys
    with path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            day_cell = (row.get("day") or "").lstrip()
            if not day_cell or day_cell.startswith("#"):
                continue
            key = (
                day_cell,
                (row.get("asset_name") or "").strip(),
                (row.get("platform_source") or "").strip(),
            )
            keys.add(key)
    return keys


def _ensure_output(path: Path) -> None:
    """Create the observations CSV with a bare header if it doesn't exist."""
    if path.is_file():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        writer.writeheader()


def _format_decimal(value) -> str:
    """Render a Decimal without trailing scientific notation; keep trimmed."""
    s = format(value, "f")
    # Trim a trailing decimal point that ``format(Decimal("5"), "f")`` leaves.
    return s.rstrip("0").rstrip(".") if "." in s else s


def _obs_to_row(obs: AssetObservation, note: str = "") -> dict[str, str]:
    return {
        "day": _obs_bangkok_day(obs).isoformat(),
        "asset_name": obs.asset_name,
        "nominal_price": _format_decimal(obs.nominal_price),
        "net_weight": _format_decimal(obs.net_weight),
        "unit_type": obs.unit_type,
        "platform_source": obs.platform_source,
        "note": note,
    }


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
    out_path: Path,
    dedup_keys: set[tuple[str, str, str]],
    dry_run: bool,
) -> RunStats:
    stats = RunStats(source=source.name)

    # Open once in append mode for the whole run; each adapter flushes as it
    # emits so a partial failure still persists the earlier rows.
    fh = None
    writer = None
    if not dry_run:
        fh = out_path.open("a", newline="", encoding="utf-8")
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)

    try:
        async for obs in _iter_source(source, since=since, until=until):
            stats.yielded += 1
            key = _obs_key(obs)
            if key in dedup_keys:
                stats.duplicates += 1
                continue
            bucket = str(classify_platform_source(obs.platform_source))
            if dry_run:
                stats.appended += 1
                stats.by_provenance[bucket] += 1
                dedup_keys.add(key)
                continue
            try:
                assert writer is not None
                writer.writerow(_obs_to_row(obs))
                assert fh is not None
                fh.flush()
            except Exception:  # noqa: BLE001
                stats.failures += 1
                logger.exception(
                    "failed to append obs %s %s @ %s",
                    obs.asset_name,
                    obs.platform_source,
                    key[0],
                )
                continue
            stats.appended += 1
            stats.by_provenance[bucket] += 1
            dedup_keys.add(key)
    finally:
        if fh is not None:
            fh.close()
        # Release any persistent network client the adapter holds.
        close = getattr(source, "close", None)
        if close is not None:
            try:
                await close()
            except Exception:  # noqa: BLE001
                logger.debug("close() on %s failed", source.name, exc_info=True)

    return stats


def _print_report(results: Iterable[RunStats], *, dry_run: bool, out_path: Path) -> None:
    print("=" * 64)
    print(f"TSSI backfill-to-csv report ({'DRY RUN' if dry_run else 'LIVE'})")
    print(f"target file: {out_path}")
    print("=" * 64)
    total_yield = total_app = total_dup = total_fail = 0
    provenance_total: dict[str, int] = defaultdict(int)
    for r in results:
        print(
            f"[{r.source}]  yielded={r.yielded}  appended={r.appended}  "
            f"duplicates={r.duplicates}  failures={r.failures}"
        )
        for k, v in sorted(r.by_provenance.items()):
            print(f"    - {k}: {v}")
            provenance_total[k] += v
        total_yield += r.yielded
        total_app += r.appended
        total_dup += r.duplicates
        total_fail += r.failures
    print("-" * 64)
    print(
        f"TOTAL  yielded={total_yield}  appended={total_app}  "
        f"duplicates={total_dup}  failures={total_fail}"
    )
    if provenance_total:
        print("  provenance mix:")
        for k, v in sorted(provenance_total.items()):
            print(f"    - {k}: {v}")
    print("=" * 64)


def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="tssi-backfill-to-csv",
        description=(
            "Historical backfill runner that writes to data/observations.csv "
            "(the source-of-truth for the static publishing pipeline)."
        ),
    )
    p.add_argument(
        "--source",
        action="append",
        required=True,
        choices=["seed", "wayback", "cpi", "bot_cpi", "all"],
        help="Source adapter(s) to run. Repeat to run several, or pass 'all'.",
    )
    p.add_argument(
        "--since",
        type=_parse_date,
        default=None,
        help="Earliest observation date to emit (YYYY-MM-DD).",
    )
    p.add_argument(
        "--until",
        type=_parse_date,
        default=None,
        help="Latest observation date to emit (YYYY-MM-DD).",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help=f"Destination CSV (default: {DEFAULT_OUT}).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Iterate sources and print the report without touching the CSV.",
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

    out_path: Path = args.out.resolve()
    if not args.dry_run:
        _ensure_output(out_path)
    dedup_keys = _load_existing_keys(out_path)
    logger.info("loaded %d existing dedup keys from %s", len(dedup_keys), out_path)

    reports: list[RunStats] = []
    for source in sources:
        logger.info("running source %s", source.name)
        try:
            stats = await _run_source(
                source,
                since=args.since,
                until=args.until,
                out_path=out_path,
                dedup_keys=dedup_keys,
                dry_run=args.dry_run,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("source %s aborted: %s", source.name, exc)
            stats = RunStats(source=source.name, failures=1)
        reports.append(stats)

    _print_report(reports, dry_run=args.dry_run, out_path=out_path)

    any_appended = any(r.appended > 0 for r in reports)
    return 0 if any_appended else 1


def main() -> None:
    raise SystemExit(asyncio.run(_amain(sys.argv[1:])))


if __name__ == "__main__":
    main()
