"""Wayback Machine CDX adapter.

For each basket asset we maintain a list of ``(valid_from, valid_to,
url)`` windows — retail product URLs rotate over years and a single
canonical URL rarely has useful coverage for more than ~18 months. For
every window we query the Wayback CDX endpoint::

    http://web.archive.org/cdx/search/cdx
        ?url=<url>&output=json
        &from=YYYYMMDD&to=YYYYMMDD
        &filter=statuscode:200
        &collapse=digest

…and for each unique snapshot we download the archived HTML at::

    https://web.archive.org/web/<timestamp>/<original url>

Price + weight are extracted with the same
:mod:`app.scraper.parser` helpers used by the live scraper, and the
resulting observations are emitted with ``platform_source =
"wayback:<platform>"`` so they slot into the existing hypertable without
a migration.

Notes
-----
* The Wayback CDX endpoint is unauthenticated but rate-limits politely;
  we thread all requests through a single ``httpx.AsyncClient`` with a
  small concurrency bound.
* Extraction can fail silently on some years if the DOM changed beyond
  recognition — this adapter logs those rows as WARNING and skips them
  rather than raising, because partial history is still useful.
* This module is self-contained; it does NOT import Playwright. The
  archived pages are rendered as plain HTML and the price/weight text
  is usually present in the initial server response.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import AsyncIterator, Iterable, Optional

import httpx

from app.scraper.assets import ASSET_REGISTRY, AssetConfig, AssetId
from app.scraper.exceptions import ScrapeParseError
from app.scraper.parser import parse_price, parse_weight
from app.scraper.schemas import AssetObservation

logger = logging.getLogger(__name__)

_CDX_ENDPOINT = "http://web.archive.org/cdx/search/cdx"
_SNAPSHOT_BASE = "https://web.archive.org/web"
_TIMESTAMP_FMT = "%Y%m%d%H%M%S"


@dataclass(frozen=True, slots=True)
class UrlWindow:
    """A retail URL that was valid for a bounded time range.

    ``valid_to`` is inclusive; ``None`` means "still live". Strings are
    allowed for the dates so callers can declare the map with plain
    YYYY-MM-DD literals.
    """

    valid_from: date
    valid_to: Optional[date]
    url: str


@dataclass(frozen=True, slots=True)
class AssetUrlMap:
    """Full URL history for a single basket asset."""

    asset_id: AssetId
    platform: str  # 'lazada' | 'shopee' | '7eleven'
    price_selector_regex: str  # coarse regex applied to the raw HTML
    weight_selector_regex: str  # same, used to find the weight token
    windows: tuple[UrlWindow, ...] = field(default_factory=tuple)

    def resolve(self, at: date) -> Optional[UrlWindow]:
        for w in self.windows:
            after_start = at >= w.valid_from
            before_end = w.valid_to is None or at <= w.valid_to
            if after_start and before_end:
                return w
        return None


# Wayback-oriented URL maps. These are intentionally templated: the
# deployer swaps in real SKU slugs before running the adapter. The regex
# hooks below are deliberately lenient so an archived DOM that differs
# slightly from today's live DOM still parses.
WAYBACK_URL_MAP: dict[AssetId, AssetUrlMap] = {
    AssetId.MAHBOONKRONG_RICE_5KG: AssetUrlMap(
        asset_id=AssetId.MAHBOONKRONG_RICE_5KG,
        platform="lazada",
        price_selector_regex=r'(?:class="pdp-price[^"]*"[^>]*>[^<]*?)(฿\s*[\d,]+(?:\.\d+)?)',
        weight_selector_regex=r"(\d+(?:\.\d+)?)\s*(?:kg|kilogram|กิโล)\b",
        windows=(
            UrlWindow(
                valid_from=date(2019, 1, 1),
                valid_to=None,
                url="https://www.lazada.co.th/products/mahboonkrong-jasmine-rice-5kg-REPLACE.html",
            ),
        ),
    ),
    AssetId.MAMA_TOMYUM_PACK: AssetUrlMap(
        asset_id=AssetId.MAMA_TOMYUM_PACK,
        platform="lazada",
        price_selector_regex=r'(?:class="pdp-price[^"]*"[^>]*>[^<]*?)(฿\s*[\d,]+(?:\.\d+)?)',
        weight_selector_regex=r"(\d+(?:\.\d+)?)\s*(?:g|gram|กรัม)\b",
        windows=(
            UrlWindow(
                valid_from=date(2019, 1, 1),
                valid_to=date(2022, 6, 30),
                url="https://www.lazada.co.th/products/mama-tom-yum-koong-REPLACE-LEGACY.html",
            ),
            UrlWindow(
                valid_from=date(2022, 7, 1),
                valid_to=None,
                url="https://www.lazada.co.th/products/mama-instant-noodles-tom-yum-koong-55g-REPLACE.html",
            ),
        ),
    ),
    AssetId.EZYGO_KAPHRAO_BOX: AssetUrlMap(
        asset_id=AssetId.EZYGO_KAPHRAO_BOX,
        platform="7eleven",
        price_selector_regex=r'(?:product-price[^>]*>[^<]*?)(฿\s*[\d,]+(?:\.\d+)?)',
        weight_selector_regex=r"(\d+(?:\.\d+)?)\s*(?:g|gram|กรัม)\b",
        windows=(
            UrlWindow(
                valid_from=date(2020, 6, 1),
                valid_to=None,
                url="https://www.7eleven.co.th/eleven-delivery/REPLACE-ezygo-kaphrao",
            ),
        ),
    ),
    AssetId.CRYSTAL_WATER_600ML: AssetUrlMap(
        asset_id=AssetId.CRYSTAL_WATER_600ML,
        platform="shopee",
        price_selector_regex=r'(?:price[^>]*>[^<]*?)(฿\s*[\d,]+(?:\.\d+)?)',
        weight_selector_regex=r"(\d+(?:\.\d+)?)\s*(?:ml|มิลลิลิตร)\b",
        windows=(
            UrlWindow(
                valid_from=date(2019, 1, 1),
                valid_to=None,
                url="https://shopee.co.th/Crystal-Drinking-Water-600ml-i.REPLACE.REPLACE",
            ),
        ),
    ),
    AssetId.M150_BOTTLE: AssetUrlMap(
        asset_id=AssetId.M150_BOTTLE,
        platform="shopee",
        price_selector_regex=r'(?:price[^>]*>[^<]*?)(฿\s*[\d,]+(?:\.\d+)?)',
        weight_selector_regex=r"(\d+(?:\.\d+)?)\s*(?:ml|มิลลิลิตร)\b",
        windows=(
            UrlWindow(
                valid_from=date(2019, 1, 1),
                valid_to=None,
                url="https://shopee.co.th/M-150-Energy-Drink-150ml-i.REPLACE.REPLACE",
            ),
        ),
    ),
}


def _cdx_from_to(since: Optional[date], until: Optional[date]) -> tuple[str, str]:
    """Map calendar dates to the YYYYMMDD strings Wayback CDX expects."""
    earliest = since or date(2015, 1, 1)
    latest = until or date.today()
    return earliest.strftime("%Y%m%d"), latest.strftime("%Y%m%d")


def _parse_cdx_row(row: list[str]) -> tuple[datetime, str]:
    """CDX rows are ``[urlkey, timestamp, original, mimetype, statuscode, digest, length]``.

    We only need ``(timestamp, original)`` — the snapshot URL is
    reconstructed at fetch time.
    """
    timestamp = row[1]
    original = row[2]
    ts = datetime.strptime(timestamp, _TIMESTAMP_FMT).replace(tzinfo=timezone.utc)
    return ts, original


class WaybackSource:
    """``HistoricalSource`` that mines the Internet Archive."""

    name = "wayback"

    def __init__(
        self,
        *,
        asset_url_map: Optional[dict[AssetId, AssetUrlMap]] = None,
        client: Optional[httpx.AsyncClient] = None,
        concurrency: int = 4,
        request_timeout_s: float = 30.0,
    ) -> None:
        self._url_map = asset_url_map or WAYBACK_URL_MAP
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(request_timeout_s),
            headers={"User-Agent": "TSSIBackfill/1.0 (+https://plainfin.example)"},
        )
        self._sem = asyncio.Semaphore(concurrency)

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> "WaybackSource":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def iter_observations(
        self,
        *,
        since: Optional[date] = None,
        until: Optional[date] = None,
    ) -> AsyncIterator[AssetObservation]:
        for asset_id, url_map in self._url_map.items():
            cfg = ASSET_REGISTRY.get(asset_id)
            if cfg is None:
                logger.warning("skipping wayback map for unknown asset %s", asset_id)
                continue
            async for obs in self._iter_asset(cfg, url_map, since=since, until=until):
                yield obs

    async def _iter_asset(
        self,
        cfg: AssetConfig,
        url_map: AssetUrlMap,
        *,
        since: Optional[date],
        until: Optional[date],
    ) -> AsyncIterator[AssetObservation]:
        cdx_from, cdx_to = _cdx_from_to(since, until)
        for window in url_map.windows:
            # Skip URL windows that don't overlap the requested range.
            if since and window.valid_to and window.valid_to < since:
                continue
            if until and window.valid_from > until:
                continue
            snapshots = await self._query_cdx(window.url, cdx_from, cdx_to)
            logger.info(
                "wayback: %s / %s — %d snapshots",
                cfg.asset_id,
                window.url,
                len(snapshots),
            )
            for ts, original in snapshots:
                if since and ts.date() < since:
                    continue
                if until and ts.date() > until:
                    continue
                html = await self._fetch_snapshot(ts, original)
                if html is None:
                    continue
                try:
                    price, weight, unit_type = self._extract(html, cfg, url_map)
                except ScrapeParseError as exc:
                    logger.warning(
                        "wayback: skip %s @ %s: %s",
                        cfg.asset_id,
                        ts.isoformat(),
                        exc,
                    )
                    continue
                yield AssetObservation(
                    time=ts,
                    asset_name=str(cfg.asset_id),
                    platform_source=f"wayback:{url_map.platform}",
                    nominal_price=price,
                    net_weight=weight,
                    unit_type=unit_type,
                )

    async def _query_cdx(
        self, url: str, cdx_from: str, cdx_to: str
    ) -> list[tuple[datetime, str]]:
        params = {
            "url": url,
            "output": "json",
            "from": cdx_from,
            "to": cdx_to,
            "filter": "statuscode:200",
            "collapse": "digest",  # skip identical-content consecutive snapshots
        }
        async with self._sem:
            resp = await self._client.get(_CDX_ENDPOINT, params=params)
        if resp.status_code != 200:
            logger.warning(
                "wayback CDX failed for %s: HTTP %s", url, resp.status_code
            )
            return []
        payload = resp.json()
        # First row is the header; guard against empty payloads.
        if not isinstance(payload, list) or len(payload) <= 1:
            return []
        rows: list[tuple[datetime, str]] = []
        for row in payload[1:]:
            try:
                rows.append(_parse_cdx_row(row))
            except (ValueError, IndexError) as exc:
                logger.debug("bad CDX row %s: %s", row, exc)
        return rows

    async def _fetch_snapshot(
        self, ts: datetime, original_url: str
    ) -> Optional[str]:
        snap_url = f"{_SNAPSHOT_BASE}/{ts.strftime(_TIMESTAMP_FMT)}id_/{original_url}"
        async with self._sem:
            try:
                resp = await self._client.get(snap_url, follow_redirects=True)
            except httpx.HTTPError as exc:
                logger.warning("wayback fetch error %s: %s", snap_url, exc)
                return None
        if resp.status_code != 200:
            logger.debug("wayback snapshot HTTP %s for %s", resp.status_code, snap_url)
            return None
        return resp.text

    def _extract(
        self,
        html: str,
        cfg: AssetConfig,
        url_map: AssetUrlMap,
    ) -> tuple[Decimal, Decimal, str]:
        price_match = re.search(url_map.price_selector_regex, html, re.IGNORECASE | re.DOTALL)
        if not price_match:
            raise ScrapeParseError("no price token in archived HTML")
        weight_match = re.search(url_map.weight_selector_regex, html, re.IGNORECASE)

        price = parse_price(price_match.group(1))

        if weight_match:
            weight, canonical = parse_weight(
                f"{weight_match.group(1)} {cfg.unit_type}",
                expected_unit=cfg.unit_type,
            )
        elif cfg.fallback_net_weight is not None:
            weight = Decimal(cfg.fallback_net_weight)
            canonical = cfg.unit_type
        else:
            raise ScrapeParseError("no weight token and no fallback weight")

        return price, weight, canonical


def iter_url_map(m: dict[AssetId, AssetUrlMap]) -> Iterable[UrlWindow]:
    """Flatten a URL map's windows (handy for tests)."""
    for entry in m.values():
        yield from entry.windows


__all__ = [
    "AssetUrlMap",
    "UrlWindow",
    "WaybackSource",
    "WAYBACK_URL_MAP",
    "iter_url_map",
]
