"""
Pydantic v2 domain models for Profile 1 (Lean App/CDC).
Strictly enforces monetary precision via Decimal(18,4) and integer cents.
Floating-point primitives (float/double) are strictly forbidden to prevent precision drift.
"""

from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class AvailabilityStatus(str, Enum):
    IN_STOCK = "IN_STOCK"
    OUT_OF_STOCK = "OUT_OF_STOCK"
    DISCONTINUED = "DISCONTINUED"
    BACKORDER = "BACKORDER"


class NotificationStatus(str, Enum):
    PENDING = "PENDING"
    DISPATCHED = "DISPATCHED"
    FAILED = "FAILED"
    DEAD_LETTER = "DEAD_LETTER"


class QuarantineResolutionStatus(str, Enum):
    UNRESOLVED = "UNRESOLVED"
    RESOLVED = "RESOLVED"
    IGNORED = "IGNORED"


class OutboxEventType(str, Enum):
    INITIAL_CAPTURE = "INITIAL_CAPTURE"
    PRICE_CHANGED = "PRICE_CHANGED"
    AVAILABILITY_CHANGED = "AVAILABILITY_CHANGED"


def to_decimal_18_4(val: Any, allow_negative: bool = False) -> Decimal:
    """Validate and convert value to Decimal with 4 fractional places, forbidding float."""
    if isinstance(val, float):
        raise ValueError(
            f"Floating-point monetary value {val!r} is strictly forbidden. "
            "Pass monetary values as Decimal, integer cents, or string (e.g. '199.90')."
        )
    if not isinstance(val, Decimal):
        try:
            val = Decimal(str(val))
        except Exception as e:
            raise ValueError(f"Cannot convert {val!r} to Decimal: {e}") from e

    if not allow_negative and val < Decimal("0"):
        raise ValueError(f"Monetary value cannot be negative: {val}")

    return val.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def compute_cents(price: Decimal) -> int:
    """Convert Decimal price to integer cents using ROUND_HALF_UP."""
    return int((price * Decimal("100")).to_integral_value(rounding=ROUND_HALF_UP))


class ScrapePayload(BaseModel):
    """Raw scraped entity payload presented to the Diff Engine."""

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    domain: str = Field(..., min_length=1, max_length=255)
    canonical_url: str = Field(..., min_length=1)
    sku: str = Field(..., min_length=1, max_length=128)
    product_title: str = Field(..., min_length=1, max_length=512)
    currency_code: str = Field(..., min_length=3, max_length=3)
    current_price: Decimal = Field(...)
    availability_status: AvailabilityStatus = Field(...)
    promo_badge: str | None = Field(default=None, max_length=128)
    scraped_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("current_price", mode="before")
    @classmethod
    def validate_price(cls, v: Any) -> Decimal:
        return to_decimal_18_4(v)

    @field_validator("currency_code")
    @classmethod
    def validate_currency(cls, v: str) -> str:
        code = v.strip().upper()
        if len(code) != 3 or not code.isalpha():
            raise ValueError(f"Invalid ISO 4217 currency code: {v!r}")
        return code

    @property
    def current_price_cents(self) -> int:
        return compute_cents(self.current_price)


class ProductPriceState(BaseModel):
    """Persistent product price SCD Type 1 state entity."""

    model_config = ConfigDict(extra="ignore")

    entity_key: str = Field(..., min_length=64, max_length=64)
    state_hash: str = Field(..., min_length=64, max_length=64)
    domain: str = Field(..., max_length=255)
    canonical_url: str
    sku: str = Field(..., max_length=128)
    product_title: str = Field(..., max_length=512)
    currency_code: str = Field(..., min_length=3, max_length=3)
    current_price_cents: int = Field(..., ge=0)
    current_price: Decimal = Field(..., ge=Decimal("0"))
    previous_price: Decimal | None = Field(default=None, ge=Decimal("0"))
    price_delta: Decimal | None = Field(default=None)
    price_delta_percent: Decimal | None = Field(default=None)
    availability_status: AvailabilityStatus = Field(...)
    notification_status: NotificationStatus = Field(default=NotificationStatus.PENDING)
    retry_count: int = Field(default=0, ge=0, le=5)
    last_notification_error: str | None = Field(default=None, max_length=512)
    last_checked_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_changed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("current_price", "previous_price", mode="before")
    @classmethod
    def validate_positive_prices(cls, v: Any) -> Decimal | None:
        if v is None:
            return None
        return to_decimal_18_4(v, allow_negative=False)

    @field_validator("price_delta", "price_delta_percent", mode="before")
    @classmethod
    def validate_deltas(cls, v: Any) -> Decimal | None:
        if v is None:
            return None
        return to_decimal_18_4(v, allow_negative=True)

    @model_validator(mode="after")
    def verify_cents_parity(self) -> "ProductPriceState":
        expected_cents = compute_cents(self.current_price)
        if self.current_price_cents != expected_cents:
            raise ValueError(
                f"current_price_cents ({self.current_price_cents}) does not match "
                f"ROUND(current_price * 100) ({expected_cents})"
            )
        return self


class PriceChangeOutbox(BaseModel):
    """Transactional outbox event for state changes requiring downstream notification."""

    model_config = ConfigDict(extra="ignore")

    outbox_id: str = Field(default_factory=lambda: str(uuid4()))
    entity_key: str = Field(..., min_length=64, max_length=64)
    event_type: OutboxEventType
    payload: str
    notification_status: NotificationStatus = Field(default=NotificationStatus.PENDING)
    retry_count: int = Field(default=0, ge=0, le=5)
    last_notification_error: str | None = Field(default=None, max_length=512)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class QuarantineRecord(BaseModel):
    """Dead-letter quarantine record isolating malformed or unparseable scrape payloads."""

    model_config = ConfigDict(extra="ignore")

    quarantine_id: str = Field(default_factory=lambda: str(uuid4()))
    domain: str = Field(..., max_length=255)
    raw_url: str
    raw_payload: str | None = None
    failure_reason: str = Field(..., max_length=128)
    quarantined_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    resolution_status: QuarantineResolutionStatus = Field(default=QuarantineResolutionStatus.UNRESOLVED)
