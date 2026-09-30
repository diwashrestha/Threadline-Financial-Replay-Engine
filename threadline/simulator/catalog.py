"""Deterministic catalog and order-item generation for Threadline."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from hashlib import sha256
from random import Random
from typing import TypeVar


CATALOG_VERSION = "catalog-v1"


@dataclass(frozen=True, slots=True)
class Style:
    product_id: str
    category: str
    name: str
    price_gross_cents: int
    demand_weight: int
    # (code, display name, selection weight)
    colors: tuple[tuple[str, str, int], ...]
    # (size, selection weight)
    sizes: tuple[tuple[str, int], ...]


@dataclass(frozen=True, slots=True)
class Sku:
    sku_id: str
    product_id: str
    category: str
    name: str
    color: str
    size: str
    price_gross_cents: int
    selection_weight: int


@dataclass(frozen=True, slots=True)
class OrderItem:
    item_id: str
    order_id: str
    sku_id: str
    quantity: int
    unit_price_gross_cents: int
    line_total_gross_cents: int


@dataclass(frozen=True, slots=True)
class OrderDraft:
    order_id: str
    catalog_version: str
    items: tuple[OrderItem, ...]
    order_total_gross_cents: int

    @property
    def order_total_eur(self) -> str:
        return cents_to_eur(self.order_total_gross_cents)


# The weights are explicit fictional assumptions. Each style's color
# weights and size weights sum to 100.
STYLES = (
    Style(
        product_id="TL-TEE",
        category="TOPS",
        name="Everyday T-shirt",
        price_gross_cents=2490,
        demand_weight=40,
        colors=(("BLK", "Black", 60), ("WHT", "White", 40)),
        sizes=(("XS", 8), ("S", 22), ("M", 34), ("L", 25), ("XL", 11)),
    ),
    Style(
        product_id="TL-HOODIE",
        category="TOPS",
        name="Cotton Hoodie",
        price_gross_cents=6490,
        demand_weight=20,
        colors=(("BLK", "Black", 70), ("NVY", "Navy", 30)),
        sizes=(("XS", 8), ("S", 22), ("M", 34), ("L", 25), ("XL", 11)),
    ),
    Style(
        product_id="TL-JEANS",
        category="DENIM",
        name="Straight Jeans",
        price_gross_cents=7990,
        demand_weight=20,
        colors=(("BLU", "Blue", 70), ("BLK", "Black", 30)),
        sizes=(("28", 10), ("30", 25), ("32", 35), ("34", 22), ("36", 8)),
    ),
    Style(
        product_id="TL-JACKET",
        category="OUTERWEAR",
        name="Lightweight Jacket",
        price_gross_cents=12990,
        demand_weight=10,
        colors=(("BLK", "Black", 65), ("OLV", "Olive", 35)),
        sizes=(("XS", 8), ("S", 22), ("M", 34), ("L", 25), ("XL", 11)),
    ),
    Style(
        product_id="TL-SOCKS",
        category="ACCESSORIES",
        name="Three-pack Socks",
        price_gross_cents=1490,
        demand_weight=10,
        colors=(("BLK", "Black", 70), ("WHT", "White", 30)),
        sizes=(("36-39", 30), ("40-43", 50), ("44-47", 20)),
    ),
)


def build_catalog() -> tuple[Sku, ...]:
    skus = []

    for style in STYLES:
        for color_code, color_name, color_weight in style.colors:
            for size, size_weight in style.sizes:
                skus.append(
                    Sku(
                        sku_id=(
                            f"{style.product_id}-{color_code}-{size}"
                        ),
                        product_id=style.product_id,
                        category=style.category,
                        name=style.name,
                        color=color_name,
                        size=size,
                        price_gross_cents=style.price_gross_cents,
                        selection_weight=(
                            style.demand_weight
                            * color_weight
                            * size_weight
                        ),
                    )
                )

    catalog = tuple(sorted(skus, key=lambda sku: sku.sku_id))

    if len({sku.sku_id for sku in catalog}) != len(catalog):
        raise ValueError("Catalog contains duplicate SKU IDs")

    return catalog


T = TypeVar("T")


def _weighted_choice(
    rng: Random,
    options: Sequence[T],
    weights: Sequence[int],
) -> T:
    if not options or len(options) != len(weights):
        raise ValueError("Options and weights must have equal length")

    if any(weight <= 0 for weight in weights):
        raise ValueError("Weights must be positive")

    ticket = rng.randrange(sum(weights))

    for option, weight in zip(options, weights, strict=True):
        if ticket < weight:
            return option
        ticket -= weight

    raise AssertionError("Weighted choice failed")


def cents_to_eur(cents: int) -> str:
    if cents < 0:
        raise ValueError("Step 1 prices must be non-negative")

    euros, remainder = divmod(cents, 100)
    return f"{euros}.{remainder:02d}"


def generate_order_draft(
    *,
    order_id: str,
    seed: int,
    catalog: Sequence[Sku],
) -> OrderDraft:
    if not order_id:
        raise ValueError("order_id is required")
    if seed < 0:
        raise ValueError("seed must be non-negative")
    if len(catalog) < 4:
        raise ValueError("Catalog needs at least four SKUs")

    # Do not use Python's hash(): its value varies between processes.
    material = f"{CATALOG_VERSION}|{seed}|{order_id}".encode("utf-8")
    order_seed = int.from_bytes(sha256(material).digest(), "big")
    rng = Random(order_seed)

    line_count = _weighted_choice(
        rng,
        options=(1, 2, 3, 4),
        weights=(55, 30, 12, 3),
    )

    available = list(sorted(catalog, key=lambda sku: sku.sku_id))
    items = []

    for line_number in range(1, line_count + 1):
        sku = _weighted_choice(
            rng,
            options=available,
            weights=[
                candidate.selection_weight
                for candidate in available
            ],
        )
        available.remove(sku)

        quantity = _weighted_choice(
            rng,
            options=(1, 2, 3),
            weights=(90, 9, 1),
        )

        items.append(
            OrderItem(
                item_id=f"{order_id}-L{line_number:02d}",
                order_id=order_id,
                sku_id=sku.sku_id,
                quantity=quantity,
                unit_price_gross_cents=sku.price_gross_cents,
                line_total_gross_cents=(
                    sku.price_gross_cents * quantity
                ),
            )
        )

    return OrderDraft(
        order_id=order_id,
        catalog_version=CATALOG_VERSION,
        items=tuple(items),
        order_total_gross_cents=sum(
            item.line_total_gross_cents for item in items
        ),
    )