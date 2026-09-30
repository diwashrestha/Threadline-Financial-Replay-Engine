from threadline.simulator.catalog import (
    build_catalog,
    cents_to_eur,
    generate_order_draft,
)


def test_catalog_has_stable_unique_skus():
    first = build_catalog()
    second = build_catalog()

    assert first == second
    assert len(first) == 46
    assert len({sku.sku_id for sku in first}) == len(first)
    assert all(sku.price_gross_cents > 0 for sku in first)
    assert cents_to_eur(2490) == "24.90"


def test_order_items_are_reproducible_and_balance():
    catalog = build_catalog()
    prices = {
        sku.sku_id: sku.price_gross_cents
        for sku in catalog
    }

    first_run = [
        generate_order_draft(
            order_id=f"ORD-{number:06d}",
            seed=20260914,
            catalog=catalog,
        )
        for number in range(100)
    ]
    second_run = [
        generate_order_draft(
            order_id=f"ORD-{number:06d}",
            seed=20260914,
            catalog=catalog,
        )
        for number in range(100)
    ]

    assert first_run == second_run

    for order in first_run:
        assert 1 <= len(order.items) <= 4
        assert len({item.sku_id for item in order.items}) == len(
            order.items
        )

        for item in order.items:
            assert 1 <= item.quantity <= 3
            assert item.unit_price_gross_cents == prices[item.sku_id]
            assert item.line_total_gross_cents == (
                item.unit_price_gross_cents * item.quantity
            )

        assert order.order_total_gross_cents == sum(
            item.line_total_gross_cents
            for item in order.items
        )
        assert order.order_total_gross_cents > 0