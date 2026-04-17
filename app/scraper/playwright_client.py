"""Async Playwright fetcher for a single scrape target.

The public entry point is :func:`fetch_asset_data`, which matches the
signature in the project spec::

    fetch_asset_data(url, price_selector, weight_selector)

It launches a headless Chromium, routes traffic through a proxy (if one is
configured via env vars or explicit kwargs), waits for the target selectors
to hydrate, and returns the raw DOM text for the price and weight elements.
The caller is responsible for parsing those strings via
:mod:`app.scraper.parser`.

Design decisions:

* Browser + context + page are created per call and torn down in a
  ``finally`` block — short-lived, CAPTCHA-resistant, and proxy-session
  isolated.
* Timeouts are never ``time.sleep()`` based; every wait is anchored to a
  selector, a network-idle condition, or the explicit Playwright timeout
  kwargs.
* CAPTCHA detection runs BEFORE selector waits, so a challenge doesn't
  masquerade as a timeout.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any, Optional, TypedDict

from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

from app.core.config import get_settings
from app.scraper.exceptions import CaptchaDetectedError, ScrapeTimeoutError

logger = logging.getLogger(__name__)

_DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Selectors + URL-fragment markers that, if present, indicate the page is a
# human-verification challenge rather than the product detail we requested.
_CAPTCHA_SELECTORS: tuple[str, ...] = (
    'iframe[src*="recaptcha"]',
    'iframe[src*="hcaptcha"]',
    'iframe[src*="challenges.cloudflare.com"]',
    "#captcha",
    "div#challenge-form",
    'div[class*="captcha" i]',
)
_CAPTCHA_URL_HINTS: tuple[str, ...] = ("captcha", "challenge", "verify")
_CAPTCHA_TITLE_HINTS: tuple[str, ...] = (
    "captcha",
    "just a moment",
    "attention required",
    "verify",
    "robot",
)


class FetchResult(TypedDict):
    """Structured return type of :func:`fetch_asset_data`."""

    url: str
    fetched_at: datetime
    price_text: str
    weight_text: Optional[str]
    final_url: str


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _resolve_proxy(explicit: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """Materialize a Playwright proxy-settings dict from kwargs or env.

    Playwright accepts a plain dict matching its ``ProxySettings`` shape:
    ``{"server": "...", "username": "...", "password": "..."}``.
    """
    if explicit:
        return explicit

    settings = get_settings()
    server = settings.proxy_server
    if not server:
        return None

    proxy: dict[str, Any] = {"server": server}
    if settings.proxy_username:
        proxy["username"] = settings.proxy_username
    if settings.proxy_password is not None:
        proxy["password"] = settings.proxy_password.get_secret_value()
    return proxy


async def _detect_captcha(page: Page) -> None:
    """Raise :class:`CaptchaDetectedError` if the current page looks hostile."""
    url_lower = (page.url or "").lower()
    if any(hint in url_lower for hint in _CAPTCHA_URL_HINTS):
        raise CaptchaDetectedError(f"URL suggests a challenge page: {page.url}")

    try:
        title = (await page.title()).lower()
    except Exception:  # pragma: no cover - title failures are non-fatal here
        title = ""
    if any(hint in title for hint in _CAPTCHA_TITLE_HINTS):
        raise CaptchaDetectedError(f"Page title suggests a challenge: {title!r}")

    for selector in _CAPTCHA_SELECTORS:
        # `count()` is cheap and does not wait; it just queries the DOM state
        # at this instant. A non-zero count means the challenge markup is
        # already present on the page.
        try:
            if await page.locator(selector).count() > 0:
                raise CaptchaDetectedError(
                    f"CAPTCHA markup detected on {page.url} via selector {selector!r}"
                )
        except CaptchaDetectedError:
            raise
        except Exception:  # pragma: no cover - transient locator failures
            continue


async def _extract_text(page: Page, selector: str, timeout_ms: int) -> str:
    """Wait for ``selector`` to be attached + visible, then return its text."""
    try:
        await page.wait_for_selector(selector, state="visible", timeout=timeout_ms)
    except PlaywrightTimeoutError as exc:
        raise ScrapeTimeoutError(
            f"Selector {selector!r} did not appear within {timeout_ms}ms"
        ) from exc

    text = await page.locator(selector).first.inner_text()
    text = (text or "").strip()
    if not text:
        raise ScrapeTimeoutError(
            f"Selector {selector!r} rendered empty text (likely partial hydration)"
        )
    return text


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
async def fetch_asset_data(
    url: str,
    price_selector: str,
    weight_selector: Optional[str] = None,
    *,
    proxy: Optional[dict[str, Any]] = None,
    headless: Optional[bool] = None,
    timeout_ms: Optional[int] = None,
    extra_wait_selectors: Iterable[str] = (),
    user_agent: str = _DEFAULT_USER_AGENT,
) -> FetchResult:
    """Scrape a single product page and return the raw price / weight text.

    Parameters
    ----------
    url:
        Product detail page to load.
    price_selector:
        CSS selector whose inner text contains the price.
    weight_selector:
        CSS selector whose inner text contains the net weight / volume.
        May be ``None`` when the caller intends to rely on a static
        ``fallback_net_weight`` from :mod:`app.scraper.assets`.
    proxy:
        Optional dict in Playwright's ``ProxySettings`` shape. If omitted,
        the proxy configured via ``PROXY_SERVER`` / ``PROXY_USERNAME`` /
        ``PROXY_PASSWORD`` env vars is used (or no proxy at all).
    headless:
        Overrides ``SCRAPER_HEADLESS``. Default: env-driven (True in prod).
    timeout_ms:
        Overrides ``SCRAPER_TIMEOUT_MS``. Applies to every explicit wait.
    extra_wait_selectors:
        Additional selectors that must be present before the price/weight
        extraction runs — useful for platforms that render container
        scaffolding before the data we care about.

    Raises
    ------
    ScrapeTimeoutError
        A required selector did not hydrate in time.
    CaptchaDetectedError
        The remote surface served a human-verification challenge.
    """
    settings = get_settings()
    resolved_headless = settings.scraper_headless if headless is None else headless
    resolved_timeout = settings.scraper_timeout_ms if timeout_ms is None else timeout_ms
    resolved_proxy = _resolve_proxy(proxy)

    browser: Optional[Browser] = None
    context: Optional[BrowserContext] = None
    page: Optional[Page] = None

    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(
                headless=resolved_headless,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ],
            )
            context = await browser.new_context(
                proxy=resolved_proxy,
                user_agent=user_agent,
                locale="th-TH",
                timezone_id="Asia/Bangkok",
                viewport={"width": 1366, "height": 900},
                ignore_https_errors=False,
            )
            # A single, consistent default timeout for every implicit wait.
            context.set_default_timeout(resolved_timeout)
            context.set_default_navigation_timeout(resolved_timeout)

            page = await context.new_page()

            logger.info("scraping %s (proxy=%s)", url, bool(resolved_proxy))
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=resolved_timeout)
            except PlaywrightTimeoutError as exc:
                raise ScrapeTimeoutError(
                    f"Navigation to {url} timed out after {resolved_timeout}ms"
                ) from exc

            # Detect a challenge BEFORE we start waiting on product selectors,
            # otherwise a CAPTCHA looks identical to a plain timeout.
            await _detect_captcha(page)

            for extra in extra_wait_selectors:
                try:
                    await page.wait_for_selector(
                        extra, state="attached", timeout=resolved_timeout
                    )
                except PlaywrightTimeoutError as exc:
                    raise ScrapeTimeoutError(
                        f"Scaffolding selector {extra!r} did not attach in time"
                    ) from exc

            price_text = await _extract_text(page, price_selector, resolved_timeout)
            weight_text: Optional[str] = None
            if weight_selector:
                try:
                    weight_text = await _extract_text(
                        page, weight_selector, resolved_timeout
                    )
                except ScrapeTimeoutError:
                    # Weight is optional at the DOM level — the pipeline can
                    # fall back to ``fallback_net_weight`` if the site hides
                    # it. Re-check for CAPTCHA just in case the missing
                    # selector is symptomatic of a post-nav challenge.
                    await _detect_captcha(page)
                    weight_text = None

            return FetchResult(
                url=url,
                fetched_at=datetime.now(tz=timezone.utc),
                price_text=price_text,
                weight_text=weight_text,
                final_url=page.url,
            )
        finally:
            # Tear down in reverse order so we never leak a browser process.
            if page is not None:
                try:
                    await page.close()
                except Exception:  # pragma: no cover
                    pass
            if context is not None:
                try:
                    await context.close()
                except Exception:  # pragma: no cover
                    pass
            if browser is not None:
                try:
                    await browser.close()
                except Exception:  # pragma: no cover
                    pass
