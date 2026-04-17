"""Robust text → (price, weight) extraction.

The scraper's raw output is messy: prices can be rendered as "฿42", "42.00 ฿",
"THB 42.00", or "42 บาท", and weights show up as "55 g", "55g", "55 กรัม", or
even "1 kg" that needs converting to grams. This module isolates that mess
behind two deterministic, pure functions that return typed values or raise
:class:`ScrapeParseError`.
"""

from __future__ import annotations

import re
import unicodedata
from decimal import Decimal, InvalidOperation
from typing import Optional

from app.scraper.exceptions import ScrapeParseError

# A numeric literal with optional thousands separators and decimals.
_PRICE_NUMBER = re.compile(r"(?P<num>\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?)")

# Weight / volume token with unit. Order in the alternation matters — longer
# Thai tokens must be tried before shorter prefixes to avoid partial matches.
_WEIGHT_TOKEN = re.compile(
    r"(?P<num>\d+(?:[.,]\d+)?)\s*"
    r"(?P<unit>"
    r"มิลลิลิตร|กรัม|ลิตร|kilograms?|kilos?|grams?|millilit(?:er|re)s?|lit(?:er|re)s?|kg|mg|ml|cl|l|g"
    r")\b",
    re.IGNORECASE,
)

# Map every accepted unit to (canonical_unit, multiplier_to_canonical).
#   * Grams:       canonical 'g'  (1 g = 1 g; 1 kg = 1000 g; 1 mg = 0.001 g)
#   * Millilitres: canonical 'ml' (1 ml = 1 ml; 1 l = 1000 ml; 1 cl = 10 ml)
_UNIT_TABLE: dict[str, tuple[str, Decimal]] = {
    # Grams family
    "g": ("g", Decimal("1")),
    "gram": ("g", Decimal("1")),
    "grams": ("g", Decimal("1")),
    "กรัม": ("g", Decimal("1")),
    "kg": ("g", Decimal("1000")),
    "kilo": ("g", Decimal("1000")),
    "kilos": ("g", Decimal("1000")),
    "kilogram": ("g", Decimal("1000")),
    "kilograms": ("g", Decimal("1000")),
    "mg": ("g", Decimal("0.001")),
    # Millilitres family
    "ml": ("ml", Decimal("1")),
    "milliliter": ("ml", Decimal("1")),
    "milliliters": ("ml", Decimal("1")),
    "millilitre": ("ml", Decimal("1")),
    "millilitres": ("ml", Decimal("1")),
    "มิลลิลิตร": ("ml", Decimal("1")),
    "cl": ("ml", Decimal("10")),
    "l": ("ml", Decimal("1000")),
    "liter": ("ml", Decimal("1000")),
    "liters": ("ml", Decimal("1000")),
    "litre": ("ml", Decimal("1000")),
    "litres": ("ml", Decimal("1000")),
    "ลิตร": ("ml", Decimal("1000")),
}


def _normalize(raw: str) -> str:
    """Strip zero-width chars, normalize Unicode, collapse whitespace."""
    cleaned = unicodedata.normalize("NFKC", raw).replace("\u200b", "")
    return " ".join(cleaned.split())


def parse_price(raw: str) -> Decimal:
    """Extract a monetary amount from a noisy DOM string.

    Examples:
        '฿42.00'        -> Decimal('42.00')
        'THB 1,250'     -> Decimal('1250')
        '42 บาท'        -> Decimal('42')

    Raises :class:`ScrapeParseError` if no numeric token can be found.
    """
    text = _normalize(raw)
    match = _PRICE_NUMBER.search(text)
    if not match:
        raise ScrapeParseError(f"No numeric price token in: {raw!r}")
    token = match.group("num").replace(",", "")
    try:
        value = Decimal(token)
    except InvalidOperation as exc:  # pragma: no cover - regex guards this
        raise ScrapeParseError(f"Unparseable price {token!r} in {raw!r}") from exc
    if value < 0:
        raise ScrapeParseError(f"Negative price parsed from {raw!r}")
    return value


def parse_weight(raw: str, expected_unit: Optional[str] = None) -> tuple[Decimal, str]:
    """Extract ``(net_weight, unit)`` from a noisy DOM string.

    The weight is returned in the **canonical** unit for its family
    (``'g'`` or ``'ml'``), with input units like ``kg`` or ``l`` converted
    automatically. If ``expected_unit`` is provided, the parsed canonical
    unit must match it — otherwise :class:`ScrapeParseError` is raised.
    """
    text = _normalize(raw)
    match = _WEIGHT_TOKEN.search(text)
    if not match:
        raise ScrapeParseError(f"No weight/volume token in: {raw!r}")

    num_token = match.group("num").replace(",", ".")
    unit_token = match.group("unit").lower()

    try:
        raw_value = Decimal(num_token)
    except InvalidOperation as exc:  # pragma: no cover - regex guards this
        raise ScrapeParseError(f"Unparseable weight {num_token!r} in {raw!r}") from exc

    if unit_token not in _UNIT_TABLE:
        raise ScrapeParseError(f"Unknown unit {unit_token!r} in {raw!r}")

    canonical_unit, multiplier = _UNIT_TABLE[unit_token]
    value = raw_value * multiplier

    if value <= 0:
        raise ScrapeParseError(f"Non-positive weight parsed from {raw!r}")

    if expected_unit is not None and expected_unit != canonical_unit:
        raise ScrapeParseError(
            f"Unit mismatch: expected {expected_unit!r}, "
            f"got {canonical_unit!r} (source: {raw!r})"
        )

    return value, canonical_unit
