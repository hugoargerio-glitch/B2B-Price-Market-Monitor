"""
Unit tests for Transactional Outbox State Machine in src/outbox.py.
Verifies PENDING -> DISPATCHED, retry increments, and DEAD_LETTER transitions.
"""

from datetime import datetime, timezone
import pytest

from src.models import NotificationStatus, OutboxEventType
from src.outbox import OutboxDispatcher
from src.storage import get_connection, init_db, transaction


@pytest.fixture
def outbox_db(tmp_path):
    conn = get_connection(tmp_path / "outbox.db")
    init_db(conn)

    now_iso = datetime.now(timezone.utc).isoformat()
    test_ek = "a" * 64
    test_sh = "b" * 64
    # Seed an entity and outbox events
    with transaction(conn) as cur:
        cur.execute(
            """
            INSERT INTO product_price_state (
                entity_key, state_hash, domain, canonical_url, sku,
                product_title, currency_code, current_price_cents,
                current_price, availability_status, last_checked_at,
                last_changed_at, created_at
            ) VALUES (?, ?, 'competitor.com', 'url', 'SKU-TEST',
                      'Title', 'USD', 1000, '10.0000', 'IN_STOCK', ?, ?, ?);
            """,
            (test_ek, test_sh, now_iso, now_iso, now_iso),
        )
        cur.execute(
            """
            INSERT INTO b2b_price_monitor (
                entity_key, state_hash, domain, canonical_url, sku,
                product_title, currency_code, current_price_cents,
                current_price, availability_status, notification_status,
                retry_count, last_checked_at, last_changed_at, created_at
            ) VALUES (?, ?, 'competitor.com', 'url', 'SKU-TEST',
                      'Title', 'USD', 1000, '10.0000', 'IN_STOCK', 'PENDING',
                      0, ?, ?, ?);
            """,
            (test_ek, test_sh, now_iso, now_iso, now_iso),
        )
        cur.execute(
            """
            INSERT INTO price_change_outbox (
                outbox_id, entity_key, event_type, payload,
                notification_status, retry_count, created_at, updated_at
            ) VALUES ('ob-1', ?, 'INITIAL_CAPTURE', '{"price": 10.0}',
                      'PENDING', 0, ?, ?);
            """,
            (test_ek, now_iso, now_iso),
        )
    yield conn
    conn.close()


def test_outbox_mark_dispatched(outbox_db):
    """Successful dispatch transitions outbox and monitor row to DISPATCHED."""
    dispatcher = OutboxDispatcher(outbox_db)
    pending = dispatcher.fetch_pending_events()
    assert len(pending) == 1
    assert pending[0].outbox_id == "ob-1"

    test_ek = "a" * 64
    dispatcher.mark_dispatched("ob-1", test_ek)

    cur = outbox_db.cursor()
    cur.execute("SELECT notification_status FROM price_change_outbox WHERE outbox_id = 'ob-1';")
    assert cur.fetchone()[0] == "DISPATCHED"

    cur.execute("SELECT notification_status FROM b2b_price_monitor WHERE entity_key = ?;", (test_ek,))
    assert cur.fetchone()[0] == "DISPATCHED"


def test_outbox_failure_retry_and_dead_letter(outbox_db):
    """Failures increment retry_count up to 5, then transition to DEAD_LETTER."""
    dispatcher = OutboxDispatcher(outbox_db, max_retries=5)
    test_ek = "a" * 64

    # Attempts 1 to 4: status should be FAILED
    for attempt in range(1, 5):
        status = dispatcher.mark_failed("ob-1", test_ek, f"Error attempt {attempt}")
        assert status == NotificationStatus.FAILED

        cur = outbox_db.cursor()
        cur.execute("SELECT retry_count, notification_status FROM price_change_outbox WHERE outbox_id = 'ob-1';")
        r_count, n_stat = cur.fetchone()
        assert r_count == attempt
        assert n_stat == "FAILED"

    # Attempt 5: max_retries reached -> DEAD_LETTER
    status = dispatcher.mark_failed("ob-1", test_ek, "Error attempt 5")
    assert status == NotificationStatus.DEAD_LETTER

    cur = outbox_db.cursor()
    cur.execute("SELECT retry_count, notification_status, last_notification_error FROM price_change_outbox WHERE outbox_id = 'ob-1';")
    r_count, n_stat, err = cur.fetchone()
    assert r_count == 5
    assert n_stat == "DEAD_LETTER"
    assert "attempt 5" in err


def test_outbox_dispatch_batch(outbox_db):
    """dispatch_batch invokes sender and records metrics."""
    dispatcher = OutboxDispatcher(outbox_db)

    # Test with successful sender
    stats = dispatcher.dispatch_batch(sender=lambda ev: True)
    assert stats["dispatched"] == 1
    assert stats["failed"] == 0
    assert stats["dead_letter"] == 0

    # No more pending events
    assert len(dispatcher.fetch_pending_events()) == 0
