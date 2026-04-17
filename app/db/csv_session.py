"""In-memory ``AsyncSession`` shim backed by ``data/observations.csv``.

Purpose
-------
The static-publishing pipeline (see ``scripts/build_static.py``) needs
to render the site and serialize the JSON API endpoints **without a
running Postgres**. Rather than rewriting the calc engine, we swap the
FastAPI ``get_session`` dependency for a tiny session that answers the
three SQL statements the engine issues:

* :data:`app.calc.engine._DAILY_PPU_SQL`
* :data:`app.calc.engine._DAILY_PPU_SQL_NO_DERIVED`
* :data:`app.calc.engine._SOURCE_MIX_SQL`

Detection is done by string match on the SQL body — cheap, explicit,
and impossible to silently mis-route because any unhandled statement
raises ``NotImplementedError`` loudly.

Data model
----------
``data/observations.csv`` carries one row per (day, asset, source) with
columns ``day, asset_name, nominal_price, net_weight, unit_type,
platform_source, note``. PPU is computed client-side as
``nominal_price / net_weight`` exactly as the DB's generated column does
on the real write path.

Comment rows (``day`` cell starting with ``#``) and blank lines are
skipped during load.
"""

from __future__ import annotations

import csv
import logging
from collections.abc import AsyncIterator
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from sqlalchemy.sql.elements import TextClause

from app.backfill.base import classify_platform_source

logger = logging.getLogger(__name__)

DEFAULT_OBSERVATIONS_PATH = (
    Path(__file__).resolve().parents[2] / "data" / "observations.csv"
)


# -----------------------------------------------------------------------------
# SQL-body fingerprints used to route ``execute()`` calls. These strings
# come from ``app/calc/engine.py`` and the test suite; we match on an
# invariant substring so whitespace tweaks upstream don't break routing.
# -----------------------------------------------------------------------------
_ROUTE_DAILY_PPU = "FROM tssi_raw_data"
_ROUTE_NO_DERIVED = "NOT LIKE 'derived:%'"
_ROUTE_SOURCE_MIX = "EXTRACT(YEAR FROM"


class _FakeMappings:
    """``.mappings()`` return value: a thin wrapper exposing ``.all()``."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class _FakeResult:
    """Duck-types just enough of ``sqlalchemy.engine.Result`` for the
    three call sites in ``app.calc.engine``."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> _FakeMappings:
        return _FakeMappings(self._rows)


class CSVSession:
    """Async-session stand-in. Implements only ``execute()``.

    The frame is loaded lazily on first query so scripts that import the
    module but never issue SQL pay no I/O cost.
    """

    def __init__(self, path: Optional[Path] = None) -> None:
        self._path = Path(path) if path is not None else DEFAULT_OBSERVATIONS_PATH
        self._frame: Optional[pd.DataFrame] = None

    # --- public duck-typed surface ----------------------------------------
    async def execute(
        self,
        stmt: TextClause | str,
        params: Optional[dict[str, Any]] = None,
    ) -> _FakeResult:
        sql = self._stmt_text(stmt)
        params = params or {}
        frame = self._get_frame()

        if _ROUTE_SOURCE_MIX in sql:
            rows = self._source_mix(frame, params)
        elif _ROUTE_NO_DERIVED in sql:
            rows = self._daily_ppu(frame, params, include_derived=False)
        elif _ROUTE_DAILY_PPU in sql:
            rows = self._daily_ppu(frame, params, include_derived=True)
        else:
            raise NotImplementedError(
                "CSVSession does not handle this SQL statement:\n" + sql
            )
        return _FakeResult(rows)

    # The calc engine never calls commit/rollback on its read path, but
    # if a future endpoint happens to, we absorb the call silently.
    async def commit(self) -> None:  # pragma: no cover - defensive no-op
        return None

    async def rollback(self) -> None:  # pragma: no cover - defensive no-op
        return None

    async def close(self) -> None:  # pragma: no cover - defensive no-op
        return None

    async def __aenter__(self) -> "CSVSession":  # pragma: no cover - defensive
        return self

    async def __aexit__(self, *_: object) -> None:  # pragma: no cover - defensive
        return None

    # --- internals --------------------------------------------------------
    @staticmethod
    def _stmt_text(stmt: TextClause | str) -> str:
        if isinstance(stmt, str):
            return stmt
        # ``TextClause`` from ``sqlalchemy.text(...)`` carries the raw SQL
        # in its ``.text`` attribute.
        return getattr(stmt, "text", str(stmt))

    def _get_frame(self) -> pd.DataFrame:
        if self._frame is None:
            self._frame = _load_frame(self._path)
        return self._frame

    @staticmethod
    def _normalize_date(value: Any) -> Optional[date]:
        """Coerce a query param into a plain ``datetime.date``."""
        if value is None:
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            return datetime.strptime(value, "%Y-%m-%d").date()
        raise TypeError(f"unsupported date param type: {type(value)!r}")

    def _daily_ppu(
        self,
        frame: pd.DataFrame,
        params: dict[str, Any],
        *,
        include_derived: bool,
    ) -> list[dict[str, Any]]:
        start = self._normalize_date(params.get("start_date"))
        end = self._normalize_date(params.get("end_date"))
        df = frame
        if start is not None:
            df = df[df["day"] >= pd.Timestamp(start)]
        if end is not None:
            df = df[df["day"] <= pd.Timestamp(end)]
        if not include_derived:
            df = df[~df["platform_source"].str.startswith("derived:")]

        if df.empty:
            return []

        # Per-day per-asset mean PPU / nominal_price / net_weight,
        # matching the AVG() triple in the real SQL.
        grouped = (
            df.groupby(["day", "asset_name"], as_index=False)
            .agg(
                ppu=("ppu", "mean"),
                nominal_price=("nominal_price", "mean"),
                net_weight=("net_weight", "mean"),
            )
            .sort_values(["day", "asset_name"])
        )
        return [
            {
                "day": row.day.date() if hasattr(row.day, "date") else row.day,
                "asset_name": str(row.asset_name),
                "ppu": float(row.ppu),
                "nominal_price": float(row.nominal_price),
                "net_weight": float(row.net_weight),
            }
            for row in grouped.itertuples(index=False)
        ]

    def _source_mix(
        self,
        frame: pd.DataFrame,
        params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        start = self._normalize_date(params.get("start_date"))
        end = self._normalize_date(params.get("end_date"))
        df = frame
        if start is not None:
            df = df[df["day"] >= pd.Timestamp(start)]
        if end is not None:
            df = df[df["day"] <= pd.Timestamp(end)]

        if df.empty:
            return []

        counts = (
            df.groupby(["year", "source_kind"], as_index=False)
            .size()
            .sort_values(["year", "source_kind"])
        )
        return [
            {
                "year": int(row.year),
                "source_kind": str(row.source_kind),
                "row_count": int(row.size),
            }
            for row in counts.itertuples(index=False)
        ]


# -----------------------------------------------------------------------------
# CSV loader
# -----------------------------------------------------------------------------
def _load_frame(path: Path) -> pd.DataFrame:
    """Load the observations CSV into a normalized pandas frame.

    The returned frame has columns:
        day (datetime64[ns])
        asset_name (str)
        nominal_price (float)
        net_weight (float)
        unit_type (str)
        platform_source (str)
        ppu (float)            = nominal_price / net_weight
        source_kind (str)      = classify_platform_source(...)
        year (int)             = Bangkok-local calendar year
    Comment / blank rows in the source CSV are dropped.
    """
    if not path.is_file():
        logger.warning("observations CSV not found at %s — returning empty frame", path)
        return _empty_frame()

    rows: list[dict[str, Any]] = []
    with path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            return _empty_frame()
        for line_no, row in enumerate(reader, start=2):
            day_cell = (row.get("day") or "").lstrip()
            if not day_cell or day_cell.startswith("#"):
                continue
            try:
                price = float(row["nominal_price"])
                weight = float(row["net_weight"])
                if weight <= 0:
                    logger.warning(
                        "observations row %d: non-positive net_weight, skipping",
                        line_no,
                    )
                    continue
            except (KeyError, ValueError) as exc:
                logger.warning(
                    "observations row %d: bad numeric token (%s), skipping",
                    line_no,
                    exc,
                )
                continue
            rows.append(
                {
                    "day": day_cell,
                    "asset_name": (row.get("asset_name") or "").strip(),
                    "nominal_price": price,
                    "net_weight": weight,
                    "unit_type": (row.get("unit_type") or "").strip(),
                    "platform_source": (row.get("platform_source") or "").strip(),
                    "note": row.get("note") or "",
                }
            )

    if not rows:
        return _empty_frame()

    df = pd.DataFrame(rows)
    df["day"] = pd.to_datetime(df["day"])
    df["ppu"] = df["nominal_price"] / df["net_weight"]
    df["source_kind"] = df["platform_source"].apply(
        lambda s: str(classify_platform_source(s))
    )
    df["year"] = df["day"].dt.year.astype(int)
    return df


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "day": pd.Series(dtype="datetime64[ns]"),
            "asset_name": pd.Series(dtype="str"),
            "nominal_price": pd.Series(dtype="float"),
            "net_weight": pd.Series(dtype="float"),
            "unit_type": pd.Series(dtype="str"),
            "platform_source": pd.Series(dtype="str"),
            "note": pd.Series(dtype="str"),
            "ppu": pd.Series(dtype="float"),
            "source_kind": pd.Series(dtype="str"),
            "year": pd.Series(dtype="int"),
        }
    )


# -----------------------------------------------------------------------------
# FastAPI dependency — plug into ``app.dependency_overrides[get_session]``
# -----------------------------------------------------------------------------
_SHARED_SESSION: Optional[CSVSession] = None


def _shared_session() -> CSVSession:
    global _SHARED_SESSION
    if _SHARED_SESSION is None:
        _SHARED_SESSION = CSVSession()
    return _SHARED_SESSION


async def csv_session_dep() -> AsyncIterator[CSVSession]:
    """FastAPI dep yielding a shared ``CSVSession`` so the DataFrame
    loads exactly once per build / process."""
    yield _shared_session()


def resolve_baseline_from_csv(
    path: Optional[Path] = None,
) -> Optional[date]:
    """Return the earliest non-comment ``day`` in the observations CSV.

    Optional helper: earliest non-comment ``day`` in the file (e.g. for
    audits). The app baseline is **not** auto-derived from this value;
    default remains ``2020-01-01`` via ``Settings.tssi_baseline_date``.
    Returns ``None`` if the file is missing or has no data rows.
    """
    csv_path = Path(path) if path is not None else DEFAULT_OBSERVATIONS_PATH
    if not csv_path.is_file():
        return None
    best: Optional[date] = None
    with csv_path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            cell = (row.get("day") or "").lstrip()
            if not cell or cell.startswith("#"):
                continue
            try:
                d = datetime.strptime(cell, "%Y-%m-%d").date()
            except ValueError:
                continue
            if best is None or d < best:
                best = d
    return best


def install_csv_overrides(app: Any, *, path: Optional[Path] = None) -> None:
    """Install the CSV session dep on a FastAPI app.

    Call at startup for the static build or any local ``uvicorn`` run
    configured with ``STATIC_MODE=true``.
    """
    from app.db.session import get_session  # local: avoid boot-time engine setup

    if path is not None:
        global _SHARED_SESSION
        _SHARED_SESSION = CSVSession(path=path)

    async def _dep() -> AsyncIterator[CSVSession]:
        yield _shared_session()

    app.dependency_overrides[get_session] = _dep


__all__ = [
    "CSVSession",
    "DEFAULT_OBSERVATIONS_PATH",
    "csv_session_dep",
    "install_csv_overrides",
    "resolve_baseline_from_csv",
]
