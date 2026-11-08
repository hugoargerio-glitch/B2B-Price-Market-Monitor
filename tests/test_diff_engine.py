"""
Unit tests for Dual-Hash Diff Engine in src/diff_engine.py.
Verifies entity_key determinism, state_hash transient exclusion, and delta calculations.
"""

from decimal import Decimal
import pytest

from src.diff_engine import (
    calculate_price_delta,
    compute_entity_key,
    compute_state_hash,
    evaluate_diff,
)
from src.models import AvailabilityStatus, OutboxEventType, ProductPriceState, ScrapePayload


def test_entity_key_determinism():
    """Entity key must be case-normalized and deterministic."""
    ek1 = compute_entity_key("Competitor-A.com.br ", " sku-prod-100 ")
    ek2 = compute_entity_key("competitor-a.com.br", "SKU-PROD-100")
    assert ek1 == ek2
    assert len(ek1) == 64


def test_state_hash_excludes_transient_metadata():
    """State hash must strictly exclude transient metadata (timestamps, latencies)."""
    sh1 = compute_state_hash(249990, "BRL", "IN_STOCK", promo_badge="FRETE_GRATIS")
    sh2 = compute_state_hash(249990, "BRL", AvailabilityStatus.IN_STOCK, promo_badge="FRETE_GRATIS")
    assert sh1 == sh2

    # Promo badge difference changes state hash
    sh3 = compute_state_hash(249990, "BRL", "IN_STOCK", promo_badge=None)
    assert sh1 != sh3


def test_price_delta_calculation_cases():
    """Price delta and guarded percentage formula checks."""
    # Case 1: Initial capture (previous_price is None)
    d1, pct1 = calculate_price_delta(Decimal("100.0000"), None)
    assert d1 == Decimal("0.0000")
    assert pct1 == Decimal("0.0000")

    # Case 2: Guarded against zero previous price
    d2, pct2 = calculate_price_delta(Decimal("100.0000"), Decimal("0.0000"))
    assert d2 == Decimal("100.0000")
    assert pct2 == Decimal("0.0000")

    # Case 3: Price decrease (-10%)
    d3, pct3 = calculate_price_delta(Decimal("90.0000"), Decimal("100.0000"))
    assert d3 == Decimal("-10.0000")
    assert pct3 == Decimal("-10.0000")

    # Case 4: Price increase (+25%)
    d4, pct4 = calculate_price_delta(Decimal("125.0000"), Decimal("100.0000"))
    assert d4 == Decimal("25.0000")
    assert pct4 == Decimal("25.0000")


def test_evaluate_diff_new_entity():
    """New entity produces INITIAL_CAPTURE diff result."""
    payload = ScrapePayload(
        domain="competitor.com",
        canonical_url="https://competitor.com/item1",
        sku="SKU-1",
        product_title="Item 1",
        currency_code="BRL",
        current_price=Decimal("199.9000"),
        availability_status=AvailabilityStatus.IN_STOCK,
    )
    diff = evaluate_diff(payload, existing=None)
    assert diff.is_new is True
    assert diff.state_changed is True
    assert diff.event_type == OutboxEventType.INITIAL_CAPTURE
    assert diff.previous_price is None
    assert diff.current_price == Decimal("199.9000")


def test_evaluate_diff_heartbeat_zero_change():
    """Unchanged state produces state_changed=False and event_type=None (Zero Outbox)."""
    payload = ScrapePayload(
        domain="competitor.com",
        canonical_url="https://competitor.com/item1",
        sku="SKU-1",
        product_title="Item 1",
        currency_code="BRL",
        current_price=Decimal("199.9000"),
        availability_status=AvailabilityStatus.IN_STOCK,
    )
    ek = compute_entity_key(payload.domain, payload.sku)
    sh = compute_state_hash(payload.current_price_cents, payload.currency_code, payload.availability_status)

    existing = ProductPriceState(
        entity_key=ek,
        state_hash=sh,
        domain=payload.domain,
        canonical_url=payload.canonical_url,
        sku=payload.sku,
        product_title=payload.product_title,
        currency_code=payload.currency_code,
        current_price=payload.current_price,
        current_price_cents=payload.current_price_cents,
        availability_status=payload.availability_status,
    )

    diff = evaluate_diff(payload, existing=existing)
    assert diff.is_new is False
    assert diff.state_changed is False
    assert diff.event_type is None


def test_evaluate_diff_price_mutation():
    """Price mutation produces PRICE_CHANGED diff result."""
    payload = ScrapePayload(
        domain="competitor.com",
        canonical_url="https://competitor.com/item1",
        sku="SKU-1",
        product_title="Item 1",
        currency_code="BRL",
        current_price=Decimal("179.9000"),  # Dropped from 199.90
        availability_status=AvailabilityStatus.IN_STOCK,
    )
    ek = compute_entity_key(payload.domain, payload.sku)
    old_sh = compute_state_hash(19990, "BRL", AvailabilityStatus.IN_STOCK)

    existing = ProductPriceState(
        entity_key=ek,
        state_hash=old_sh,
        domain=payload.domain,
        canonical_url=payload.canonical_url,
        sku=payload.sku,
        product_title=payload.product_title,
        currency_code="BRL",
        current_price=Decimal("199.9000"),
        current_price_cents=19990,
        availability_status=AvailabilityStatus.IN_STOCK,
    )

    diff = evaluate_diff(payload, existing=existing)
    assert diff.is_new is False
    assert diff.state_changed is True
    assert diff.event_type == OutboxEventType.PRICE_CHANGED
    assert diff.previous_price == Decimal("199.9000")
    assert diff.current_price == Decimal("179.9000")
    assert diff.price_delta == Decimal("-20.0000")
