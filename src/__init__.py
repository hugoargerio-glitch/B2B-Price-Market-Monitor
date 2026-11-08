"""
B2B Price Monitor - Profile 1 (Lean App/CDC).
"""

from src.config import DatabaseConfig, CircuitBreakerConfig
from src.diff_engine import compute_entity_key, compute_state_hash, calculate_price_delta, evaluate_diff
from src.models import (
    AvailabilityStatus,
    NotificationStatus,
    OutboxEventType,
    ProductPriceState,
    PriceChangeOutbox,
    QuarantineRecord,
    ScrapePayload,
)
from src.outbox import OutboxDispatcher
from src.pipeline import PriceMonitorPipeline
from src.storage import get_connection, init_db, transaction

__all__ = [
    "DatabaseConfig",
    "CircuitBreakerConfig",
    "compute_entity_key",
    "compute_state_hash",
    "calculate_price_delta",
    "evaluate_diff",
    "AvailabilityStatus",
    "NotificationStatus",
    "OutboxEventType",
    "ProductPriceState",
    "PriceChangeOutbox",
    "QuarantineRecord",
    "ScrapePayload",
    "OutboxDispatcher",
    "PriceMonitorPipeline",
    "get_connection",
    "init_db",
    "transaction",
]
