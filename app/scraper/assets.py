"""Static configuration for the five TSSI basket assets.

The TSSI micro-basket follows the Thai Street Survival Index brief:
five factory-sealed, non-substitutable Thai staples mapped to the NSO
COICOP 2018 household expenditure categories. Weights sum to 1.00:

    Mahboonkrong Jasmine Rice 5kg    25%  (01.1.1 Cereals)
    Mama Tom Yum Koong pack          15%  (01.1.1 Cereals)
    7-Eleven EZYGO Kaphrao box       35%  (01.1.9 Ready-made food)
    Crystal Drinking Water 600ml     15%  (01.2.2 Non-alcoholic beverages)
    M-150 Energy Drink 150ml         10%  (01.2.2 Non-alcoholic beverages)

These records are intentionally declarative: one row per (asset,
platform) target, with CSS selectors for the price and the weight /
volume. URLs are placeholders pointing at the correct retail surfaces
-- replace them with real SKU pages once the retail partnerships are
live. The ``fallback_net_weight`` lets the scraper still emit a PPU
if the on-page weight DOM is missing (shrinkflation is detected via
price changes even with a static reference weight, though a scraped
weight is always preferred).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Optional


class AssetId(StrEnum):
    """Canonical identifiers for basket members.

    The string values match the Appendix B API response keys verbatim
    and are persisted into ``tssi_raw_data.asset_name`` /
    ``observations.csv`` -- DO NOT rename without a data migration.
    """

    MAHBOONKRONG_RICE_5KG = "mahboonkrong_rice_5kg"
    MAMA_TOMYUM_PACK = "mama_tomyum_pack"
    EZYGO_KAPHRAO_BOX = "ezygo_kaphrao_box"
    CRYSTAL_WATER_600ML = "crystal_water_600ml"
    M150_BOTTLE = "m150_bottle"


@dataclass(frozen=True, slots=True)
class AssetConfig:
    """A single scrape target."""

    asset_id: AssetId
    display_name: str
    platform_source: str
    url: str
    price_selector: str
    weight_selector: Optional[str]
    unit_type: str  # 'g' or 'ml' -- must match the CHECK constraint in the DB.
    weight: Decimal  # Basket weight (must sum to 1.0 across registry).
    coicop_category: str  # NSO COICOP 2018 sub-category label.
    rationale: str  # One-line justification from the brief §3.1.
    fallback_net_weight: Optional[Decimal] = None
    extra_selectors_to_wait: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.unit_type not in {"g", "ml"}:
            raise ValueError(
                f"unit_type must be 'g' or 'ml', got {self.unit_type!r} "
                f"for {self.asset_id}"
            )
        if self.weight <= 0 or self.weight > 1:
            raise ValueError(
                f"Basket weight must be in (0, 1], got {self.weight} for {self.asset_id}"
            )


# -----------------------------------------------------------------------------
# Registry -- the canonical five-asset basket.
#
# NOTE: Selectors below mirror the typical DOM shape of each platform's
# product page. Swap them for the real stable selectors once the retail
# integrations are confirmed; the pipeline and tests do not depend on the
# exact CSS strings.
# -----------------------------------------------------------------------------
ASSET_REGISTRY: dict[AssetId, AssetConfig] = {
    AssetId.MAHBOONKRONG_RICE_5KG: AssetConfig(
        asset_id=AssetId.MAHBOONKRONG_RICE_5KG,
        display_name="Mahboonkrong Jasmine Rice 5kg",
        platform_source="lazada",
        # Placeholder: replace with the real SKU slug on Lazada / CP Freshmart.
        url="https://www.lazada.co.th/products/mahboonkrong-jasmine-rice-5kg-REPLACE.html",
        price_selector="span.pdp-price_type_normal",
        weight_selector="div.pdp-mod-specification li:has-text('Weight') .key-value",
        unit_type="g",
        weight=Decimal("0.25"),
        coicop_category="01.1.1 Cereals & Cereal Products",
        rationale=(
            "The absolute fundamental staple of the Thai diet. Tracking the "
            "5kg bag provides a vital anchor for un-subsidized, raw caloric cost."
        ),
        fallback_net_weight=Decimal("5000.0"),
        extra_selectors_to_wait=("div.pdp-block__main-info",),
    ),
    AssetId.MAMA_TOMYUM_PACK: AssetConfig(
        asset_id=AssetId.MAMA_TOMYUM_PACK,
        display_name="Mama Tom Yum Koong Instant Noodles",
        platform_source="lazada",
        # Placeholder: replace with the real SKU slug.
        url="https://www.lazada.co.th/products/mama-instant-noodles-tom-yum-koong-55g-REPLACE.html",
        price_selector="span.pdp-price_type_normal",
        weight_selector="div.pdp-mod-specification li:has-text('Net') .key-value",
        unit_type="g",
        weight=Decimal("0.15"),
        coicop_category="01.1.1 Cereals & Cereal Products",
        rationale=(
            "Extreme nominal price rigidity (6 baht pegged 14 years) makes "
            "this pack a highly sensitive gauge of underlying raw material inflation."
        ),
        fallback_net_weight=Decimal("55.0"),
        extra_selectors_to_wait=("div.pdp-block__main-info",),
    ),
    AssetId.EZYGO_KAPHRAO_BOX: AssetConfig(
        asset_id=AssetId.EZYGO_KAPHRAO_BOX,
        display_name="7-Eleven EZYGO Kaphrao Chilled Box",
        platform_source="7eleven",
        # Placeholder: 7-Eleven TH's e-commerce front.
        url="https://www.7eleven.co.th/eleven-delivery/REPLACE-ezygo-kaphrao",
        price_selector="div.product-price .amount",
        weight_selector="div.product-specs li:has-text('Net Weight')",
        unit_type="g",
        weight=Decimal("0.35"),
        coicop_category="01.1.9 Ready-made Food / Prepared",
        rationale=(
            "CP All's 14k-store retail footprint makes EZYGO pricing a de facto "
            "national standard for prepared protein, stripping vendor portion variability."
        ),
        fallback_net_weight=Decimal("210.0"),
        extra_selectors_to_wait=("section.product-detail",),
    ),
    AssetId.CRYSTAL_WATER_600ML: AssetConfig(
        asset_id=AssetId.CRYSTAL_WATER_600ML,
        display_name="Crystal Drinking Water 600ml",
        platform_source="shopee",
        # Placeholder: replace with the real SKU slug on Shopee / Lazada.
        url="https://shopee.co.th/Crystal-Drinking-Water-600ml-i.REPLACE.REPLACE",
        price_selector="div.flex.items-center section div[class*='pqTWkA']",
        weight_selector="div.product-detail section:has-text('Net Volume')",
        unit_type="ml",
        weight=Decimal("0.15"),
        coicop_category="01.2.2 Non-alcoholic Beverages",
        rationale=(
            "Bottled drinking water is a core survival necessity. A factory-sealed "
            "600ml PET bottle captures fundamental liquid intake costs without luxury markups."
        ),
        fallback_net_weight=Decimal("600.0"),
        extra_selectors_to_wait=("div.product-briefing",),
    ),
    AssetId.M150_BOTTLE: AssetConfig(
        asset_id=AssetId.M150_BOTTLE,
        display_name="M-150 Energy Drink 150ml",
        platform_source="shopee",
        # Placeholder: replace with the real SKU slug.
        url="https://shopee.co.th/M-150-Energy-Drink-150ml-i.REPLACE.REPLACE",
        price_selector="div.flex.items-center section div[class*='pqTWkA']",
        weight_selector="div.product-detail section:has-text('Net Volume')",
        unit_type="ml",
        weight=Decimal("0.10"),
        coicop_category="01.2.2 Non-alcoholic Beverages",
        rationale=(
            "The standard 150ml brown-glass bottle is a pristine proxy for the "
            "cost of sustaining blue-collar Thai labor."
        ),
        fallback_net_weight=Decimal("150.0"),
        extra_selectors_to_wait=("div.product-briefing",),
    ),
}


def _validate_registry(registry: dict[AssetId, AssetConfig]) -> None:
    total = sum((cfg.weight for cfg in registry.values()), start=Decimal("0"))
    if total != Decimal("1.00"):
        raise RuntimeError(
            f"TSSI basket weights must sum to 1.00, got {total}. "
            "Check app/scraper/assets.py::ASSET_REGISTRY."
        )


_validate_registry(ASSET_REGISTRY)
