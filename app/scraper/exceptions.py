"""Scraper exception hierarchy.

All scraper-originated errors inherit from :class:`ScraperError` so the
pipeline orchestrator can branch on a single base type. Operational concerns
(timeouts) and adversarial ones (CAPTCHA) are split so retry policies can
treat them differently.
"""

from __future__ import annotations


class ScraperError(Exception):
    """Base class for every error raised by the scraping subsystem."""


class ScrapeTimeoutError(ScraperError):
    """A selector did not hydrate within the configured timeout window.

    Transient by nature; the pipeline retries these a small, bounded number
    of times with exponential backoff.
    """


class CaptchaDetectedError(ScraperError):
    """The remote surface served a human-verification challenge.

    Deliberately NOT transient: retrying through the same proxy / IP will
    usually make the situation worse. Callers should surface this as a hard
    failure and rotate upstream proxies out-of-band.
    """


class ScrapeParseError(ScraperError):
    """Raw text was extracted but could not be coerced into price/weight."""
