"""
Dual-Hash Diff Engine for Profile 1 (Lean App/CDC).

Calculates:
1. entity_key: Deterministic SHA-256 identity key based on domain + SKU.
2. state_hash: Business state signature hash strictly excluding transient metadata
   (scrape timestamps, request latencies, session tokens) to ensure ZERO false positives.
"""

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
import hashlib
from typing import Optional

from src.models import AvailabilityStatus, OutboxEventType, ProductPriceState, ScrapePayload


def compute_entity_key(
    domain: str,
    sku_or_url: str,
    separator: str = "::",
    upper_sku: bool = True,
) -> str:
    """Compute deterministic SHA-256 entity key.

    Default formula (Section 5 / Worker mandate):
      SHA-256(LOWER(TRIM(competitor_domain)) || '::' || UPPER(TRIM(competitor_sku)))
    """
    cleaned_domain = domain.strip().lower()
    cleaned_sku = sku_or_url.strip().upper() if upper_sku else sku_or_url.strip().lower()
    raw = f"{cleaned_domain}{separator}{cleaned_sku}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def compute_state_hash(
    current_price_cents: int,
    currency_code: str,
    availability_status: str | AvailabilityStatus,
    promo_badge: Optional[str] = None,
    include_promo_badge: bool = True,
) -> str:
    """Compute deterministic SHA-256 state signature.

    Formula (Worker mandate):
      SHA-256(CAST(current_price_cents AS TEXT) || '|' || UPPER(TRIM(currency_code)) || '|' || UPPER(TRIM(availability_status)) || '|' || COALESCE(TRIM(promo_badge), ''))

    Transient metadata (timestamps, proxy IPs, latencies) are strictly excluded.
    """
    avail = (
        availability_status.value
        if isinstance(availability_status, AvailabilityStatus)
        else str(availability_status).strip().upper()
    )
    curr = currency_code.strip().upper()

    if include_promo_badge:
        promo = (promo_badge or "").strip()
        raw = f"{current_price_cents}|{curr}|{avail}|{promo}"
    else:
        raw = f"{current_price_cents}|{curr}|{avail}"

    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def calculate_price_delta(
    current_price: Decimal,
    previous_price: Optional[Decimal],
) -> tuple[Optional[Decimal], Optional[Decimal]]:
    """Calculate absolute and percentage price deltas with zero-division guard.

    Formulas:
      price_delta = current_price - COALESCE(previous_price, current_price)
      price_delta_percent:
        CASE WHEN previous_price IS NULL OR previous_price = 0
             THEN 0.0000
             ELSE CAST(((current_price - previous_price) / previous_price) * 100.0000 AS DECIMAL(18,4))
        END
    """
    if previous_price is None:
        return Decimal("0.0000"), Decimal("0.0000")

    delta = (current_price - previous_price).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    if previous_price == Decimal("0"):
        delta_pct = Decimal("0.0000")
    else:
        raw_pct = ((current_price - previous_price) / previous_price) * Decimal("100.0000")
        delta_pct = raw_pct.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    return delta, delta_pct


@dataclass(frozen=True)
class DiffResult:
    is_new: bool
    state_changed: bool
    entity_key: str
    new_state_hash: str
    previous_price: Optional[Decimal]
    current_price: Decimal
    current_price_cents: int
    price_delta: Optional[Decimal]
    price_delta_percent: Optional[Decimal]
    event_type: Optional[OutboxEventType]


def evaluate_diff(
    payload: ScrapePayload,
    existing: Optional[ProductPriceState] = None,
) -> DiffResult:
    """Evaluate incoming scraped payload against existing state.

    Returns a DiffResult dictating whether to:
    - Insert new entity + INITIAL_CAPTURE outbox
    - Update heartbeat ONLY (zero outbox events)
    - Mutate state + PRICE_CHANGED / AVAILABILITY_CHANGED outbox
    """
    entity_key = compute_entity_key(payload.domain, payload.sku)
    new_state_hash = compute_state_hash(
        current_price_cents=payload.current_price_cents,
        currency_code=payload.currency_code,
        availability_status=payload.availability_status,
        promo_badge=payload.promo_badge,
    )

    if existing is None:
        delta, delta_pct = calculate_price_delta(payload.current_price, None)
        return DiffResult(
            is_new=True,
            state_changed=True,
            entity_key=entity_key,
            new_state_hash=new_state_hash,
            previous_price=None,
            current_price=payload.current_price,
            current_price_cents=payload.current_price_cents,
            price_delta=delta,
            price_delta_percent=delta_pct,
            event_type=OutboxEventType.INITIAL_CAPTURE,
        )

    if existing.state_hash == new_state_hash:
        # Zero state change: Heartbeat update ONLY
        return DiffResult(
            is_new=False,
            state_changed=False,
            entity_key=entity_key,
            new_state_hash=new_state_hash,
            previous_price=existing.previous_price,
            current_price=existing.current_price,
            current_price_cents=existing.current_price_cents,
            price_delta=existing.price_delta,
            price_delta_percent=existing.price_delta_percent,
            event_type=None,
        )

    # State transition occurred
    delta, delta_pct = calculate_price_delta(payload.current_price, existing.current_price)

    if payload.current_price_cents != existing.current_price_cents:
        event_type = OutboxEventType.PRICE_CHANGED
    else:
        event_type = OutboxEventType.AVAILABILITY_CHANGED

    return DiffResult(
        is_new=False,
        state_changed=True,
        entity_key=entity_key,
        new_state_hash=new_state_hash,
        previous_price=existing.current_price,
        current_price=payload.current_price,
        current_price_cents=payload.current_price_cents,
        price_delta=delta,
        price_delta_percent=delta_pct,
        event_type=event_type,
    )
