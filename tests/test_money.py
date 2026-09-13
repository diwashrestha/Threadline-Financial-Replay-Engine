"""Contract tests for Threadline money and MockPay fee calculations."""

from decimal import Decimal

import pytest

from threadline.money import (
    MAX_MONEY,
    InvalidMoneyError,
    UnsupportedCurrencyError,
    UnsupportedPaymentMethodError,
    calculate_fee,
    format_money,
    parse_money,
    round_eur,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("0.00", Decimal("0.00")),
        ("19.99", Decimal("19.99")),
        ("9999999999999999.99", MAX_MONEY),
    ],
)
def test_parse_money_accepts_canonical_non_negative_values(raw, expected):
    assert parse_money(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "1",
        "1.0",
        "1.000",
        ".99",
        "01.00",
        "+1.00",
        " 1.00",
        "1.00 ",
        "NaN",
        "Infinity",
        "10000000000000000.00",
    ],
)
def test_parse_money_rejects_non_canonical_or_out_of_range_strings(raw):
    with pytest.raises(InvalidMoneyError):
        parse_money(raw)


@pytest.mark.parametrize("raw", [1, 1.0, Decimal("1.00"), None, True])
def test_parse_money_rejects_non_string_inputs(raw):
    with pytest.raises(TypeError):
        parse_money(raw)  # type: ignore[arg-type]


def test_negative_money_requires_explicit_settlement_opt_in():
    with pytest.raises(InvalidMoneyError):
        parse_money("-30.00")

    assert parse_money("-30.00", allow_negative=True) == Decimal("-30.00")


def test_negative_zero_is_normalized():
    assert parse_money("-0.00", allow_negative=True).as_tuple().sign == 0


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1.004", "1.00"),
        ("1.005", "1.01"),
        ("-1.005", "-1.01"),
    ],
)
def test_round_eur_uses_half_up(raw, expected):
    assert round_eur(Decimal(raw)) == Decimal(expected)


@pytest.mark.parametrize("raw", [1, 1.005, "1.005"])
def test_round_eur_rejects_implicit_numeric_coercion(raw):
    with pytest.raises(TypeError):
        round_eur(raw)  # type: ignore[arg-type]


def test_card_fee_matches_golden_rounding_scenario():
    assert calculate_fee("CARD", Decimal("19.99")) == Decimal("0.56")


@pytest.mark.parametrize(
    ("method", "amount", "expected"),
    [
        ("CARD", "100.00", "2.00"),
        ("WALLET", "100.00", "2.25"),
        ("CARD", "0.00", "0.20"),
        ("WALLET", "0.00", "0.25"),
    ],
)
def test_calculate_fee_uses_mockpay_v1_schedule(method, amount, expected):
    assert calculate_fee(method, Decimal(amount)) == Decimal(expected)


def test_calculate_fee_rejects_unknown_method():
    with pytest.raises(UnsupportedPaymentMethodError):
        calculate_fee("BANK_TRANSFER", Decimal("100.00"))


def test_calculate_fee_rejects_non_eur_currency():
    with pytest.raises(UnsupportedCurrencyError):
        calculate_fee("CARD", Decimal("100.00"), currency="USD")


def test_calculate_fee_rejects_negative_amount():
    with pytest.raises(InvalidMoneyError):
        calculate_fee("CARD", Decimal("-0.01"))


def test_format_money_is_stable_and_two_decimal():
    assert format_money(Decimal("98")) == "98.00"
    assert format_money(Decimal("19.995")) == "20.00"
