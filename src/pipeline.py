"""
End-to-end Ingestion Pipeline for Profile 1 (Lean App/CDC).
Coordinates Pydantic shift-left validation, Dual-Hash Diff Engine,
atomic SQLite transactions, DLQ quarantine routing, and Domain Circuit Breakers.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import logging
import sqlite3
from typing import Any, Callable, Optional
from uuid import uuid4

from pydantic import ValidationError

from src.config import CircuitBreakerConfig
from src.diff_engine import (
    compute_entity_key,
    evaluate_diff,
)
from src.models import (
    NotificationStatus,
    OutboxEventType,
    ProductPriceState,
    QuarantineRecord,
    ScrapePayload,
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
        admin_notifier: Optional[Callable[[dict[str, Any]], Any]] = None,
        notifier: Optional[Any] = None,
    ):
        self.conn = conn
        self.cb_config = circuit_breaker_config or CircuitBreakerConfig()
        self.admin_notifier = admin_notifier if admin_notifier is not None else notifier

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

    def get_existing_state(
        self,
        entity_key: str,
        cursor: Optional[sqlite3.Cursor] = None,
    ) -> Optional[ProductPriceState]:
        """Lookup existing entity state by entity_key using SQLite B-Tree index."""
        own_cursor = cursor is None
        cur = self.conn.cursor() if own_cursor else cursor
        try:
            cur.execute(
                """
                SELECT entity_key, state_hash, domain, canonical_url, sku,
                       product_title, currency_code, current_price_cents,
                       current_price, previous_price, price_delta,
                       price_delta_percent, availability_status, promo_badge,
                       notification_status, retry_count, last_notification_error,
                       last_checked_at, last_changed_at, created_at
                FROM b2b_price_monitor
                WHERE entity_key = ?;
                """,
                (entity_key,),
            )
            row = cur.fetchone()
            if row:
                return row_to_product_price_state(row)
            return None
        finally:
            if own_cursor:
                cur.close()

    def process_item(
        self,
        raw_item: dict[str, Any],
        trip_domains: set[str],
    ) -> tuple[str, Optional[str]]:
        """Process a single raw scraped item through the validation and diff engine.

        Returns (result_category, entity_key_or_quarantine_id).
        Categories: 'INSERTED', 'HEARTBEAT', 'STATE_CHANGE', 'QUARANTINED'.
        """
        if not isinstance(raw_item, dict):
            q = self.quarantine_payload(
                "UNKNOWN_DOMAIN",
                "UNKNOWN_URL",
                str(raw_item),
                "VALIDATION_ERROR: Payload must be a JSON object",
            )
            return "QUARANTINED", q.quarantine_id

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
        domain_blocked = payload.domain in trip_domains
        notif_status = (
            NotificationStatus.CIRCUIT_HALTED.value
            if domain_blocked
            else NotificationStatus.PENDING.value
        )
        notif_error = "HALTED_BY_CIRCUIT_BREAKER" if domain_blocked else None

        # Check-and-write inside BEGIN IMMEDIATE to prevent TOCTOU races
        with transaction(self.conn) as cur:
            existing = self.get_existing_state(entity_key, cursor=cur)
            diff = evaluate_diff(payload, existing)
            now_dt = datetime.now(timezone.utc)
            now_iso = now_dt.isoformat()

            if diff.is_new:
                # Case A: Brand-new product entity
                cur.execute(
                    """
                    INSERT INTO b2b_price_monitor (
                        entity_key, state_hash, domain, canonical_url, sku,
                        product_title, currency_code, current_price_cents,
                        current_price, previous_price, price_delta,
                        price_delta_percent, availability_status, promo_badge,
                        notification_status, retry_count, last_notification_error,
                        last_checked_at, last_changed_at, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
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
                        payload.promo_badge,
                        notif_status,
                        0,
                        notif_error,
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
                        "promo_badge": payload.promo_badge,
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
                        notif_status,
                        0,
                        notif_error,
                        now_iso,
                        now_iso,
                    ),
                )
                return "INSERTED", diff.entity_key

            elif not diff.state_changed:
                # Case B: Unchanged state signature -> UPDATE Heartbeat ONLY. Zero outbox events!
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
                    UPDATE b2b_price_monitor
                    SET state_hash = ?,
                        currency_code = ?,
                        previous_price = ?,
                        current_price = ?,
                        current_price_cents = ?,
                        price_delta = ?,
                        price_delta_percent = ?,
                        availability_status = ?,
                        promo_badge = ?,
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
                        payload.currency_code,
                        str(diff.previous_price) if diff.previous_price is not None else None,
                        str(diff.current_price),
                        diff.current_price_cents,
                        str(diff.price_delta) if diff.price_delta is not None else None,
                        str(diff.price_delta_percent) if diff.price_delta_percent is not None else None,
                        payload.availability_status.value,
                        payload.promo_badge,
                        payload.product_title,
                        payload.canonical_url,
                        notif_status,
                        notif_error,
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
                        "previous_price": str(diff.previous_price) if diff.previous_price is not None else None,
                        "current_price": str(diff.current_price),
                        "price_delta": str(diff.price_delta) if diff.price_delta is not None else None,
                        "price_delta_percent": str(diff.price_delta_percent) if diff.price_delta_percent is not None else None,
                        "currency": payload.currency_code,
                        "availability": payload.availability_status.value,
                        "promo_badge": payload.promo_badge,
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
                        notif_status,
                        0,
                        notif_error,
                        now_iso,
                        now_iso,
                    ),
                )
                return "STATE_CHANGE", diff.entity_key

    def _emit_admin_alert(self, alert_payload: dict[str, Any]) -> None:
        """Send administrative TEMPLATE_DRIFT_DETECTED alert via configured callback."""
        if self.admin_notifier is None:
            return
        try:
            if hasattr(self.admin_notifier, "send_admin_alert"):
                self.admin_notifier.send_admin_alert(alert_payload)
            elif callable(self.admin_notifier):
                self.admin_notifier(alert_payload)
        except Exception as exc:
            logger.error("Failed to dispatch admin alert for domain %s: %s", alert_payload.get("domain"), exc)

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
            if (
                total >= self.cb_config.min_sample_size
                and (fail_count / total) > self.cb_config.max_quarantine_rate
            ):
                tripped_domains.add(dom)
                failure_rate = fail_count / total
                logger.warning(
                    "CIRCUIT_BREAKER_TRIPPED: Domain %s failure rate %.2f (%d/%d, min_sample=%d) exceeds %.2f threshold. "
                    "Halt customer-facing notifications (TEMPLATE_DRIFT_DETECTED).",
                    dom,
                    failure_rate,
                    fail_count,
                    total,
                    self.cb_config.min_sample_size,
                    self.cb_config.max_quarantine_rate,
                )
                self._emit_admin_alert(
                    {
                        "event_type": "TEMPLATE_DRIFT_DETECTED",
                        "alert_type": "TEMPLATE_DRIFT_DETECTED",
                        "severity": "CRITICAL",
                        "domain": dom,
                        "failure_count": fail_count,
                        "total_count": total,
                        "failure_rate": round(failure_rate, 4),
                        "threshold": self.cb_config.max_quarantine_rate,
                        "min_sample_size": self.cb_config.min_sample_size,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "message": (
                            f"TEMPLATE_DRIFT_DETECTED on domain {dom}: "
                            f"{fail_count}/{total} ({failure_rate:.1%}) failed validation "
                            f"(threshold={self.cb_config.max_quarantine_rate:.1%}). "
                            "Customer-facing alerts halted."
                        ),
                    }
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
