"""
Unit tests for Pydantic domain models in src/models.py.
Verifies strict Decimal precision, rejection of float, integer cents computation, and enums.
"""

from decimal import Decimal
import pytest
from pydantic import ValidationError

from src.models import (
    AvailabilityStatus,
    NotificationStatus,
    ProductPriceState,
    ScrapePayload,
    compute_cents,
    to_decimal_18_4,
)


def test_rejection_of_float_prices():
    """Floats must be strictly forbidden to avoid precision drift."""
    with pytest.raises(ValueError, match="Floating-point monetary value"):
        to_decimal_18_4(199.99)

    with pytest.raises(ValidationError):
        ScrapePayload(
            domain="competitor.com",
            canonical_url="https://competitor.com/item1",
            sku="SKU-1",
            product_title="Item 1",
            currency_code="BRL",
            current_price=199.99,  # Float must be rejected
            availability_status=AvailabilityStatus.IN_STOCK,
        )


def test_decimal_precision_quantization():
    """Decimal values must quantize to 4 decimal places."""
    d = to_decimal_18_4(Decimal("2499.9"))
    assert str(d) == "2499.9000"

    d_str = to_decimal_18_4("499.9999")
    assert str(d_str) == "499.9999"


def test_compute_cents_round_half_up():
    """Cents must round half up to exact integer cents."""
    assert compute_cents(Decimal("2499.9000")) == 249990
    assert compute_cents(Decimal("145.7500")) == 14575
    assert compute_cents(Decimal("10.0050")) == 1001
    assert compute_cents(Decimal("10.0049")) == 1000


def test_negative_prices_forbidden():
    """Negative prices must be blocked."""
    with pytest.raises(ValueError, match="cannot be negative"):
        to_decimal_18_4(Decimal("-10.00"))


def test_currency_code_validation():
    """Currency code must be 3-letter ISO code."""
    payload = ScrapePayload(
        domain="competitor.com",
        canonical_url="https://competitor.com/item1",
        sku="SKU-1",
        product_title="Item 1",
        currency_code="usd ",
        current_price=Decimal("100.0000"),
        availability_status=AvailabilityStatus.IN_STOCK,
    )
    assert payload.currency_code == "USD"

    with pytest.raises(ValidationError):
        ScrapePayload(
            domain="competitor.com",
            canonical_url="https://competitor.com/item1",
            sku="SKU-1",
            product_title="Item 1",
            currency_code="INVALID",
            current_price=Decimal("100.0000"),
            availability_status=AvailabilityStatus.IN_STOCK,
        )


def test_availability_status_closed_domain():
    """Availability must belong to closed enum domain."""
    valid = ScrapePayload(
        domain="competitor.com",
        canonical_url="https://competitor.com/item1",
        sku="SKU-1",
        product_title="Item 1",
        currency_code="BRL",
        current_price=Decimal("50.0000"),
        availability_status=AvailabilityStatus.BACKORDER,
    )
    assert valid.availability_status == AvailabilityStatus.BACKORDER

    with pytest.raises(ValidationError):
        ScrapePayload(
            domain="competitor.com",
            canonical_url="https://competitor.com/item1",
            sku="SKU-1",
            product_title="Item 1",
            currency_code="BRL",
            current_price=Decimal("50.0000"),
            availability_status="SOMETHING_ELSE",
        )


def test_product_price_state_cents_parity_check():
    """ProductPriceState must enforce ROUND(current_price * 100) == current_price_cents."""
    # Matching
    state = ProductPriceState(
        entity_key="a" * 64,
        state_hash="b" * 64,
        domain="competitor.com",
        canonical_url="https://competitor.com/p1",
        sku="SKU-1",
        product_title="Title",
        currency_code="BRL",
        current_price=Decimal("123.4500"),
        current_price_cents=12345,
        availability_status=AvailabilityStatus.IN_STOCK,
    )
    assert state.current_price_cents == 12345

    # Mismatching cents
    with pytest.raises(ValidationError, match="does not match"):
        ProductPriceState(
            entity_key="a" * 64,
            state_hash="b" * 64,
            domain="competitor.com",
            canonical_url="https://competitor.com/p1",
            sku="SKU-1",
            product_title="Title",
            currency_code="BRL",
            current_price=Decimal("123.4500"),
            current_price_cents=99999,  # Intentional discrepancy
            availability_status=AvailabilityStatus.IN_STOCK,
        )


def test_product_price_state_negative_deltas_permitted():
    """Price drops produce negative price_delta and price_delta_percent, which must be permitted."""
    state = ProductPriceState(
        entity_key="c" * 64,
        state_hash="d" * 64,
        domain="competitor.com",
        canonical_url="https://competitor.com/p2",
        sku="SKU-2",
        product_title="Product On Sale",
        currency_code="BRL",
        current_price=Decimal("2500.0000"),
        current_price_cents=250000,
        previous_price=Decimal("2700.0000"),
        price_delta=Decimal("-200.0000"),
        price_delta_percent=Decimal("-7.4074"),
        availability_status=AvailabilityStatus.IN_STOCK,
    )
    assert state.price_delta == Decimal("-200.0000")
    assert state.price_delta_percent == Decimal("-7.4074")


def test_product_price_state_negative_current_price_rejected():
    """Current price and previous price can never be negative."""
    with pytest.raises(ValidationError):
        ProductPriceState(
            entity_key="e" * 64,
            state_hash="f" * 64,
            domain="competitor.com",
            canonical_url="https://competitor.com/p3",
            sku="SKU-3",
            product_title="Negative Item",
            currency_code="BRL",
            current_price=Decimal("-10.0000"),
            current_price_cents=-1000,
            availability_status=AvailabilityStatus.IN_STOCK,
        )
