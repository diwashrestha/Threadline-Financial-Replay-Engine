"""Money parsing, rounding, formatting, and MockPay fee calculation.

The financial contract stores money as exact EUR decimal values.  This module
deliberately rejects floats so binary floating-point values cannot enter the
reconciliation calculations unnoticed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from types import MappingProxyType
from typing import Final, Mapping


EUR: Final = "EUR"
CENT: Final = Decimal("0.01")
ZERO_EUR: Final = Decimal("0.00")
MAX_MONEY: Final = Decimal("9999999999999999.99")

_MONEY_PATTERN: Final = re.compile(r"-?(?:0|[1-9]\d*)\.\d{2}\Z")


class MoneyError(ValueError):
    """Base class for money-contract violations."""


class InvalidMoneyError(MoneyError):
    """Raised when a monetary value violates the Threadline contract."""


class UnsupportedCurrencyError(MoneyError):
    """Raised when a Stage 2 calculation receives a currency other than EUR."""


class UnsupportedPaymentMethodError(MoneyError):
    """Raised when no MockPay fee rule exists for a payment method."""


@dataclass(frozen=True, slots=True)
class FeeRule:
    """One fixed-plus-percentage processing-fee rule."""

    fixed: Decimal
    rate: Decimal


FEE_RULES: Final[Mapping[str, FeeRule]] = MappingProxyType(
    {
        "CARD": FeeRule(fixed=Decimal("0.20"), rate=Decimal("0.018")),
        "WALLET": FeeRule(fixed=Decimal("0.25"), rate=Decimal("0.020")),
    }
)


def _require_decimal(value: Decimal) -> Decimal:
    """Return a finite Decimal and reject implicit numeric coercion."""
    if not isinstance(value, Decimal):
        raise TypeError("money calculations require decimal.Decimal values")
    if not value.is_finite():
        raise InvalidMoneyError("money must be finite")
    return value


def _require_supported_range(value: Decimal) -> Decimal:
    """Enforce the NUMERIC(18, 2) magnitude used by the warehouse contract."""
    if value.copy_abs() > MAX_MONEY:
        raise InvalidMoneyError(
            f"money exceeds the supported NUMERIC(18, 2) range: {value}"
        )
    return value


def parse_money(value: str, *, allow_negative: bool = False) -> Decimal:
    """Parse an exact two-decimal money string.

    Domain amounts such as orders, payments, refunds, and fees are non-negative.
    Callers handling signed settlement movements must opt in with
    ``allow_negative=True``.
    """
    if not isinstance(value, str):
        raise TypeError("money must be provided as a string")
    if not _MONEY_PATTERN.fullmatch(value):
        raise InvalidMoneyError(
            "money must use canonical decimal notation with exactly two decimals"
        )

    try:
        amount = Decimal(value)
    except InvalidOperation as exc:  # Defensive: the regular expression is strict.
        raise InvalidMoneyError(f"invalid money value: {value!r}") from exc

    _require_supported_range(amount)
    if amount < ZERO_EUR and not allow_negative:
        raise InvalidMoneyError("negative money is not allowed for this field")

    # Decimal preserves a negative sign on -0.00; canonical output does not.
    return ZERO_EUR if amount == ZERO_EUR else amount


def round_eur(value: Decimal) -> Decimal:
    """Round a finite Decimal to euro cents using ROUND_HALF_UP."""
    amount = _require_decimal(value)
    try:
        rounded = amount.quantize(CENT, rounding=ROUND_HALF_UP)
    except InvalidOperation as exc:
        raise InvalidMoneyError(f"cannot round money value: {value}") from exc
    return _require_supported_range(rounded)


def format_money(value: Decimal) -> str:
    """Return a deterministic two-decimal representation for JSON output."""
    return format(round_eur(value), ".2f")


def calculate_fee(
    method: str,
    amount: Decimal,
    *,
    currency: str = EUR,
) -> Decimal:
    """Calculate the MockPay v1 processing fee for one captured payment.

    Formula: ``round_eur(fixed_fee + captured_amount * percentage_rate)``.
    """
    if currency != EUR:
        raise UnsupportedCurrencyError(
            f"Stage 2 supports {EUR} only; received {currency!r}"
        )
    if method not in FEE_RULES:
        raise UnsupportedPaymentMethodError(
            f"unsupported payment method {method!r}; expected one of "
            f"{', '.join(sorted(FEE_RULES))}"
        )

    captured_amount = _require_decimal(amount)
    _require_supported_range(captured_amount)
    if captured_amount < ZERO_EUR:
        raise InvalidMoneyError("captured payment amount cannot be negative")

    rule = FEE_RULES[method]
    return round_eur(rule.fixed + captured_amount * rule.rate)
