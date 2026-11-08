"""
Transactional Outbox Engine & State Machine for Profile 1 (Lean App/CDC).
Manages notification dispatch with strict state transitions:
  PENDING -> DISPATCHED | FAILED -> DEAD_LETTER
"""

from datetime import datetime, timedelta, timezone
import json
import random
import sqlite3
from typing import Callable, Optional

from src.models import NotificationStatus, PriceChangeOutbox
from src.storage import transaction


def calculate_backoff_seconds(
    retry_count: int,
    base_seconds: float = 1.0,
    max_seconds: float = 300.0,
    with_jitter: bool = True,
) -> float:
    """Calculate Exponential Backoff with Full Jitter:
    t_wait = min(t_max, t_base * 2^retry_count) + jitter
    """
    if retry_count <= 0:
        return 0.0
    exp_wait = min(max_seconds, base_seconds * (2 ** retry_count))
    jitter = random.uniform(0.0, base_seconds) if with_jitter else 0.0
    return min(max_seconds, exp_wait + jitter)


class OutboxDispatcher:
    """Dispatches pending outbox events and transitions states under ACID transactions."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        max_retries: int = 5,
        base_backoff_seconds: float = 1.0,
        max_backoff_seconds: float = 300.0,
    ):
        self.conn = conn
        self.max_retries = max_retries
        self.base_backoff_seconds = base_backoff_seconds
        self.max_backoff_seconds = max_backoff_seconds

    def fetch_pending_events(
        self,
        limit: int = 50,
        enforce_backoff: bool = False,
        now_utc: Optional[datetime] = None,
    ) -> list[PriceChangeOutbox]:
        """Fetch outbox records that require dispatch (PENDING or FAILED with retry_count < max_retries)."""
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                """
                SELECT outbox_id, entity_key, event_type, payload,
                       notification_status, retry_count, last_notification_error,
                       created_at, updated_at
                FROM price_change_outbox
                WHERE notification_status IN ('PENDING', 'FAILED')
                  AND retry_count < ?
                ORDER BY created_at ASC
                LIMIT ?;
                """,
                (self.max_retries, limit),
            )
            rows = cursor.fetchall()
            events = [
                PriceChangeOutbox(
                    outbox_id=row["outbox_id"],
                    entity_key=row["entity_key"],
                    event_type=row["event_type"],
                    payload=row["payload"],
                    notification_status=NotificationStatus(row["notification_status"]),
                    retry_count=row["retry_count"],
                    last_notification_error=row["last_notification_error"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
                for row in rows
            ]
            if not enforce_backoff:
                return events

            ref_now = now_utc or datetime.now(timezone.utc)
            ready_events: list[PriceChangeOutbox] = []
            for ev in events:
                if ev.notification_status == NotificationStatus.PENDING or ev.retry_count == 0:
                    ready_events.append(ev)
                else:
                    wait_sec = calculate_backoff_seconds(
                        ev.retry_count,
                        base_seconds=self.base_backoff_seconds,
                        max_seconds=self.max_backoff_seconds,
                        with_jitter=False,
                    )
                    updated_dt = ev.updated_at if ev.updated_at.tzinfo else ev.updated_at.replace(tzinfo=timezone.utc)
                    if ref_now >= updated_dt + timedelta(seconds=wait_sec):
                        ready_events.append(ev)
            return ready_events
        finally:
            cursor.close()

    def mark_dispatched(self, outbox_id: str, entity_key: str) -> None:
        """Mark event and corresponding b2b_price_monitor row as DISPATCHED."""
        now_iso = datetime.now(timezone.utc).isoformat()
        with transaction(self.conn) as cur:
            cur.execute(
                """
                UPDATE price_change_outbox
                SET notification_status = 'DISPATCHED',
                    updated_at = ?
                WHERE outbox_id = ?;
                """,
                (now_iso, outbox_id),
            )
            cur.execute(
                """
                UPDATE b2b_price_monitor
                SET notification_status = 'DISPATCHED'
                WHERE entity_key = ?;
                """,
                (entity_key,),
            )

    def mark_failed(self, outbox_id: str, entity_key: str, error_message: str) -> NotificationStatus:
        """Increment retry count and transition to FAILED or DEAD_LETTER if retries exhausted."""
        now_iso = datetime.now(timezone.utc).isoformat()
        with transaction(self.conn) as cur:
            cur.execute(
                """
                SELECT retry_count FROM price_change_outbox WHERE outbox_id = ?;
                """,
                (outbox_id,),
            )
            row = cur.fetchone()
            current_retries = row["retry_count"] if row else 0
            new_retries = min(self.max_retries, current_retries + 1)
            new_status = (
                NotificationStatus.DEAD_LETTER
                if new_retries >= self.max_retries
                else NotificationStatus.FAILED
            )

            truncated_err = error_message[:512]

            cur.execute(
                """
                UPDATE price_change_outbox
                SET notification_status = ?,
                    retry_count = ?,
                    last_notification_error = ?,
                    updated_at = ?
                WHERE outbox_id = ?;
                """,
                (new_status.value, new_retries, truncated_err, now_iso, outbox_id),
            )
            cur.execute(
                """
                UPDATE b2b_price_monitor
                SET notification_status = ?,
                    retry_count = ?,
                    last_notification_error = ?
                WHERE entity_key = ?;
                """,
                (new_status.value, new_retries, truncated_err, entity_key),
            )
            return new_status

    def dispatch_batch(
        self,
        sender: Callable[[PriceChangeOutbox], bool],
        limit: int = 50,
    ) -> dict[str, int]:
        """Dispatch a batch of pending events using the provided sender callable."""
        events = self.fetch_pending_events(limit=limit)
        stats = {"dispatched": 0, "failed": 0, "dead_letter": 0}

        for ev in events:
            try:
                success = sender(ev)
                if success:
                    self.mark_dispatched(ev.outbox_id, ev.entity_key)
                    stats["dispatched"] += 1
                else:
                    status = self.mark_failed(ev.outbox_id, ev.entity_key, "Sender returned failure status")
                    if status == NotificationStatus.DEAD_LETTER:
                        stats["dead_letter"] += 1
                    else:
                        stats["failed"] += 1
            except Exception as e:
                status = self.mark_failed(ev.outbox_id, ev.entity_key, str(e))
                if status == NotificationStatus.DEAD_LETTER:
                    stats["dead_letter"] += 1
                else:
                    stats["failed"] += 1

        return stats
