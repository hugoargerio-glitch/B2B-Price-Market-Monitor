"""
End-to-end Ingestion Pipeline for Profile 1 (Lean App/CDC).
Coordinates Pydantic shift-left validation, Dual-Hash Diff Engine,
atomic SQLite transactions, DLQ quarantine routing, and Domain Circuit Breakers.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
import json
import logging
import sqlite3
from typing import Any, Optional
from uuid import uuid4

from pydantic import ValidationError

from src.config import CircuitBreakerConfig
from src.diff_engine import (
    calculate_price_delta,
    compute_entity_key,
    compute_state_hash,
    evaluate_diff,
)
from src.models import (
    AvailabilityStatus,
    NotificationStatus,
    OutboxEventType,
    ProductPriceState,
    QuarantineRecord,
    ScrapePayload,
    to_decimal_18_4,
)
from src.storage import row_to_product_price_state, transaction

logger = logging.getLogger("b2b_price_monitor.pipeline")


@dataclass
class IngestionSummary:
    total_received: int = 0
    inserted_new: int = 0
    updated_heartbeat_only: int = 0
    updated_state_change: int = 0
    quarantined: int = 0
    tripped_circuit_breakers: list[str] = field(default_factory=list)

    @property
    def total_processed(self) -> int:
        return (
            self.inserted_new
            + self.updated_heartbeat_only
            + self.updated_state_change
            + self.quarantined
        )


class PriceMonitorPipeline:
    """Atomic transaction pipeline for B2B competitor price monitoring."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        circuit_breaker_config: Optional[CircuitBreakerConfig] = None,
    ):
        self.conn = conn
        self.cb_config = circuit_breaker_config or CircuitBreakerConfig()

    def quarantine_payload(
        self,
        domain: str,
        raw_url: str,
        raw_payload: str | None,
        failure_reason: str,
    ) -> QuarantineRecord:
        """Atomically persist a malformed or invalid payload to the Dead Letter Queue."""
        record = QuarantineRecord(
            quarantine_id=str(uuid4()),
            domain=domain or "UNKNOWN_DOMAIN",
            raw_url=raw_url or "UNKNOWN_URL",
            raw_payload=raw_payload,
            failure_reason=failure_reason[:128],
            quarantined_at=datetime.now(timezone.utc),
        )

        with transaction(self.conn) as cur:
            cur.execute(
                """
                INSERT INTO b2b_price_monitor_quarantine (
                    quarantine_id, domain, raw_url, raw_payload,
                    failure_reason, quarantined_at, resolution_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    record.quarantine_id,
                    record.domain,
                    record.raw_url,
                    record.raw_payload,
                    record.failure_reason,
                    record.quarantined_at.isoformat(),
                    record.resolution_status.value,
                ),
            )
        return record

    def get_existing_state(self, entity_key: str) -> Optional[ProductPriceState]:
        """Lookup existing entity state by entity_key using SQLite B-Tree index."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """
                SELECT entity_key, state_hash, domain, canonical_url, sku,
                       product_title, currency_code, current_price_cents,
                       current_price, previous_price, price_delta,
                       price_delta_percent, availability_status,
                       last_checked_at, last_changed_at, created_at
                FROM product_price_state
                WHERE entity_key = ?;
                """,
                (entity_key,),
            )
            row = cursor.fetchone()
            if row:
                return row_to_product_price_state(row)
            return None
        finally:
            cursor.close()

    def process_item(
        self,
        raw_item: dict[str, Any],
        trip_domains: set[str],
    ) -> tuple[str, Optional[str]]:
        """Process a single raw scraped item through the validation and diff engine.

        Returns (result_category, entity_key_or_quarantine_id).
        Categories: 'INSERTED', 'HEARTBEAT', 'STATE_CHANGE', 'QUARANTINED'.
        """
        domain = raw_item.get("domain", "")
        raw_url = raw_item.get("canonical_url", "")
        raw_json = json.dumps(raw_item, default=str)

        # Shift-Left Edge Validation: In-memory Pydantic validation
        try:
            payload = ScrapePayload.model_validate(raw_item)
        except ValidationError as ve:
            reason = f"VALIDATION_ERROR: {ve.errors()[0]['msg']}"
            q = self.quarantine_payload(domain, raw_url, raw_json, reason)
            return "QUARANTINED", q.quarantine_id
        except Exception as e:
            reason = f"PARSE_EXCEPTION: {e}"
            q = self.quarantine_payload(domain, raw_url, raw_json, reason)
            return "QUARANTINED", q.quarantine_id

        entity_key = compute_entity_key(payload.domain, payload.sku)
        existing = self.get_existing_state(entity_key)
        diff = evaluate_diff(payload, existing)
        now_dt = datetime.now(timezone.utc)
        now_iso = now_dt.isoformat()

        # Check circuit breaker for domain
        domain_blocked = payload.domain in trip_domains

        with transaction(self.conn) as cur:
            if diff.is_new:
                # Case A: Brand-new product entity
                cur.execute(
                    """
                    INSERT INTO product_price_state (
                        entity_key, state_hash, domain, canonical_url, sku,
                        product_title, currency_code, current_price_cents,
                        current_price, previous_price, price_delta,
                        price_delta_percent, price_change_pct, availability_status,
                        last_checked_at, last_seen_at_utc, last_changed_at,
                        updated_at_utc, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        diff.entity_key,
                        diff.new_state_hash,
                        payload.domain,
                        payload.canonical_url,
                        payload.sku,
                        payload.product_title,
                        payload.currency_code,
                        diff.current_price_cents,
                        str(diff.current_price),
                        str(diff.previous_price) if diff.previous_price is not None else None,
                        str(diff.price_delta) if diff.price_delta is not None else None,
                        str(diff.price_delta_percent) if diff.price_delta_percent is not None else None,
                        str(diff.price_delta_percent) if diff.price_delta_percent is not None else None,
                        payload.availability_status.value,
                        now_iso,
                        now_iso,
                        now_iso,
                        now_iso,
                        now_iso,
                    ),
                )

                # Contract-aligned physical table b2b_price_monitor
                cur.execute(
                    """
                    INSERT INTO b2b_price_monitor (
                        entity_key, state_hash, domain, canonical_url, sku,
                        product_title, currency_code, current_price_cents,
                        current_price, previous_price, price_delta,
                        price_delta_percent, availability_status, notification_status,
                        retry_count, last_notification_error, last_checked_at,
                        last_changed_at, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        diff.entity_key,
                        diff.new_state_hash,
                        payload.domain,
                        payload.canonical_url,
                        payload.sku,
                        payload.product_title,
                        payload.currency_code,
                        diff.current_price_cents,
                        str(diff.current_price),
                        str(diff.previous_price) if diff.previous_price is not None else None,
                        str(diff.price_delta) if diff.price_delta is not None else None,
                        str(diff.price_delta_percent) if diff.price_delta_percent is not None else None,
                        payload.availability_status.value,
                        NotificationStatus.PENDING.value if not domain_blocked else NotificationStatus.FAILED.value,
                        0,
                        None if not domain_blocked else "HALTED_BY_CIRCUIT_BREAKER",
                        now_iso,
                        now_iso,
                        now_iso,
                    ),
                )

                # ACID Transactional Outbox insertion
                outbox_payload = json.dumps(
                    {
                        "entity_key": diff.entity_key,
                        "event_type": OutboxEventType.INITIAL_CAPTURE.value,
                        "sku": payload.sku,
                        "domain": payload.domain,
                        "canonical_url": payload.canonical_url,
                        "product_title": payload.product_title,
                        "current_price": str(diff.current_price),
                        "currency": payload.currency_code,
                        "availability": payload.availability_status.value,
                    }
                )
                cur.execute(
                    """
                    INSERT INTO price_change_outbox (
                        outbox_id, entity_key, event_type, payload,
                        notification_status, retry_count, last_notification_error,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        str(uuid4()),
                        diff.entity_key,
                        OutboxEventType.INITIAL_CAPTURE.value,
                        outbox_payload,
                        NotificationStatus.PENDING.value if not domain_blocked else NotificationStatus.FAILED.value,
                        0,
                        None if not domain_blocked else "HALTED_BY_CIRCUIT_BREAKER",
                        now_iso,
                        now_iso,
                    ),
                )
                return "INSERTED", diff.entity_key

            elif not diff.state_changed:
                # Case B: Unchanged state signature -> UPDATE Heartbeat ONLY. Zero outbox events!
                cur.execute(
                    """
                    UPDATE product_price_state
                    SET last_checked_at = ?,
                        last_seen_at_utc = ?
                    WHERE entity_key = ?;
                    """,
                    (now_iso, now_iso, diff.entity_key),
                )
                cur.execute(
                    """
                    UPDATE b2b_price_monitor
                    SET last_checked_at = ?
                    WHERE entity_key = ?;
                    """,
                    (now_iso, diff.entity_key),
                )
                return "HEARTBEAT", diff.entity_key

            else:
                # Case C: Business state changed -> Mutate state and emit outbox event atomically
                cur.execute(
                    """
                    UPDATE product_price_state
                    SET state_hash = ?,
                        previous_price = ?,
                        current_price = ?,
                        current_price_cents = ?,
                        price_delta = ?,
                        price_delta_percent = ?,
                        price_change_pct = ?,
                        availability_status = ?,
                        product_title = ?,
                        canonical_url = ?,
                        last_changed_at = ?,
                        updated_at_utc = ?,
                        last_checked_at = ?,
                        last_seen_at_utc = ?
                    WHERE entity_key = ?;
                    """,
                    (
                        diff.new_state_hash,
                        str(diff.previous_price),
                        str(diff.current_price),
                        diff.current_price_cents,
                        str(diff.price_delta),
                        str(diff.price_delta_percent),
                        str(diff.price_delta_percent),
                        payload.availability_status.value,
                        payload.product_title,
                        payload.canonical_url,
                        now_iso,
                        now_iso,
                        now_iso,
                        now_iso,
                        diff.entity_key,
                    ),
                )

                cur.execute(
                    """
                    UPDATE b2b_price_monitor
                    SET state_hash = ?,
                        previous_price = ?,
                        current_price = ?,
                        current_price_cents = ?,
                        price_delta = ?,
                        price_delta_percent = ?,
                        availability_status = ?,
                        product_title = ?,
                        canonical_url = ?,
                        notification_status = ?,
                        retry_count = 0,
                        last_notification_error = ?,
                        last_changed_at = ?,
                        last_checked_at = ?
                    WHERE entity_key = ?;
                    """,
                    (
                        diff.new_state_hash,
                        str(diff.previous_price),
                        str(diff.current_price),
                        diff.current_price_cents,
                        str(diff.price_delta),
                        str(diff.price_delta_percent),
                        payload.availability_status.value,
                        payload.product_title,
                        payload.canonical_url,
                        NotificationStatus.PENDING.value if not domain_blocked else NotificationStatus.FAILED.value,
                        None if not domain_blocked else "HALTED_BY_CIRCUIT_BREAKER",
                        now_iso,
                        now_iso,
                        diff.entity_key,
                    ),
                )

                event_type_str = diff.event_type.value if diff.event_type else OutboxEventType.PRICE_CHANGED.value
                outbox_payload = json.dumps(
                    {
                        "entity_key": diff.entity_key,
                        "event_type": event_type_str,
                        "sku": payload.sku,
                        "domain": payload.domain,
                        "canonical_url": payload.canonical_url,
                        "product_title": payload.product_title,
                        "previous_price": str(diff.previous_price),
                        "current_price": str(diff.current_price),
                        "price_delta": str(diff.price_delta),
                        "price_delta_percent": str(diff.price_delta_percent),
                        "currency": payload.currency_code,
                        "availability": payload.availability_status.value,
                    }
                )
                cur.execute(
                    """
                    INSERT INTO price_change_outbox (
                        outbox_id, entity_key, event_type, payload,
                        notification_status, retry_count, last_notification_error,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        str(uuid4()),
                        diff.entity_key,
                        event_type_str,
                        outbox_payload,
                        NotificationStatus.PENDING.value if not domain_blocked else NotificationStatus.FAILED.value,
                        0,
                        None if not domain_blocked else "HALTED_BY_CIRCUIT_BREAKER",
                        now_iso,
                        now_iso,
                    ),
                )
                return "STATE_CHANGE", diff.entity_key

    def ingest_batch(self, batch: list[dict[str, Any]]) -> IngestionSummary:
        """Process a batch of scraped entities with Circuit Breaker detection."""
        summary = IngestionSummary(total_received=len(batch))
        if not batch:
            return summary

        # 1. Pre-scan for domain failure / quarantine rates to trip circuit breakers
        domain_counts: dict[str, int] = {}
        domain_failures: dict[str, int] = {}

        for item in batch:
            dom = item.get("domain", "UNKNOWN_DOMAIN") if isinstance(item, dict) else "UNKNOWN_DOMAIN"
            domain_counts[dom] = domain_counts.get(dom, 0) + 1
            try:
                ScrapePayload.model_validate(item)
            except Exception:
                domain_failures[dom] = domain_failures.get(dom, 0) + 1

        tripped_domains: set[str] = set()
        for dom, total in domain_counts.items():
            fail_count = domain_failures.get(dom, 0)
            if total > 0 and (fail_count / total) > self.cb_config.max_quarantine_rate:
                tripped_domains.add(dom)
                logger.warning(
                    "CIRCUIT_BREAKER_TRIPPED: Domain %s failure rate %.2f exceeds %.2f threshold. "
                    "Halt customer-facing notifications (TEMPLATE_DRIFT_DETECTED).",
                    dom,
                    fail_count / total,
                    self.cb_config.max_quarantine_rate,
                )

        summary.tripped_circuit_breakers = list(tripped_domains)

        # 2. Process each item
        for item in batch:
            category, _ = self.process_item(item, tripped_domains)
            if category == "INSERTED":
                summary.inserted_new += 1
            elif category == "HEARTBEAT":
                summary.updated_heartbeat_only += 1
            elif category == "STATE_CHANGE":
                summary.updated_state_change += 1
            elif category == "QUARANTINED":
                summary.quarantined += 1

        return summary
