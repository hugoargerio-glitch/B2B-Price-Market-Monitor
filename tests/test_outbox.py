"""
Unit tests for Transactional Outbox State Machine in src/outbox.py.
Verifies PENDING -> DISPATCHED, retry increments, DEAD_LETTER transitions,
CIRCUIT_HALTED non-leakage, and enforced backoff/jitter.
"""

from datetime import datetime, timedelta, timezone
import pytest

from src.models import NotificationStatus, OutboxEventType
from src.outbox import OutboxDispatcher, calculate_backoff_seconds
from src.storage import get_connection, init_db, transaction


@pytest.fixture
def outbox_db(tmp_path):
    conn = get_connection(tmp_path / "outbox.db")
    init_db(conn)

    now_iso = datetime.now(timezone.utc).isoformat()
    test_ek = "a" * 64
    test_sh = "b" * 64

    # Seed an entity and outbox events in canonical b2b_price_monitor table
    with transaction(conn) as cur:
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
            ) VALUES ('ob-1', ?, 'INITIAL_CAPTURE', '{"price": "10.0000"}',
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
    """dispatch_batch invokes sender and records metrics with enforced backoff by default."""
    dispatcher = OutboxDispatcher(outbox_db)

    # Test with successful sender
    stats = dispatcher.dispatch_batch(sender=lambda ev: True)
    assert stats["dispatched"] == 1
    assert stats["failed"] == 0
    assert stats["dead_letter"] == 0

    # No more pending events
    assert len(dispatcher.fetch_pending_events()) == 0


def test_calculate_backoff_exponential_and_jitter():
    """Backoff grows exponentially (t_base * 2^retry_count) capped at max_seconds, plus jitter."""
    assert calculate_backoff_seconds(0) == 0.0
    assert calculate_backoff_seconds(1, base_seconds=2.0, max_seconds=300.0, with_jitter=False) == 4.0
    assert calculate_backoff_seconds(3, base_seconds=2.0, max_seconds=300.0, with_jitter=False) == 16.0
    assert calculate_backoff_seconds(10, base_seconds=2.0, max_seconds=60.0, with_jitter=False) == 60.0

    # With jitter in [0, base_seconds]
    val = calculate_backoff_seconds(2, base_seconds=2.0, max_seconds=300.0, with_jitter=True)
    assert 8.0 <= val <= 10.0


def test_fetch_pending_events_enforces_backoff(outbox_db):
    """When enforce_backoff=True (default), FAILED events must wait until updated_at + backoff window."""
    dispatcher = OutboxDispatcher(outbox_db, max_retries=5, base_backoff_seconds=2.0)
    test_ek = "a" * 64

    # Mark ob-1 failed once -> retry_count = 1 -> wait = 2.0 * 2^1 = 4.0s
    dispatcher.mark_failed("ob-1", test_ek, "HTTP 429 Too Many Requests")

    cur = outbox_db.cursor()
    cur.execute("SELECT updated_at FROM price_change_outbox WHERE outbox_id = 'ob-1';")
    updated_at_dt = datetime.fromisoformat(cur.fetchone()[0])

    # 1 second after failure (< 4.0s wait): should be filtered out when enforce_backoff=True
    early_now = updated_at_dt + timedelta(seconds=1)
    assert len(dispatcher.fetch_pending_events(now_utc=early_now)) == 0
    assert len(dispatcher.fetch_pending_events(enforce_backoff=True, now_utc=early_now)) == 0

    # 5 seconds after failure (>= 4.0s wait): should be eligible
    ready_now = updated_at_dt + timedelta(seconds=5)
    assert len(dispatcher.fetch_pending_events(now_utc=ready_now)) == 1


def test_circuit_halted_rows_never_fetched_for_dispatch(outbox_db):
    """AUD-001: CIRCUIT_HALTED events must NEVER leak to customer webhook dispatch."""
    dispatcher = OutboxDispatcher(outbox_db)
    test_ek = "a" * 64
    now_iso = datetime.now(timezone.utc).isoformat()

    with transaction(outbox_db) as cur:
        # Event marked CIRCUIT_HALTED
        cur.execute(
            """
            INSERT INTO price_change_outbox (
                outbox_id, entity_key, event_type, payload,
                notification_status, retry_count, last_notification_error, created_at, updated_at
            ) VALUES ('ob-ch1', ?, 'PRICE_CHANGED', '{"price": "10.0000"}',
                      'CIRCUIT_HALTED', 0, 'HALTED_BY_CIRCUIT_BREAKER', ?, ?);
            """,
            (test_ek, now_iso, now_iso),
        )
        # Event with FAILED status but HALTED_BY_CIRCUIT_BREAKER error
        cur.execute(
            """
            INSERT INTO price_change_outbox (
                outbox_id, entity_key, event_type, payload,
                notification_status, retry_count, last_notification_error, created_at, updated_at
            ) VALUES ('ob-ch2', ?, 'PRICE_CHANGED', '{"price": "10.0000"}',
                      'FAILED', 0, 'HALTED_BY_CIRCUIT_BREAKER', ?, ?);
            """,
            (test_ek, now_iso, now_iso),
        )

    # Fetch pending events must strictly exclude ob-ch1 and ob-ch2
    pending = dispatcher.fetch_pending_events(enforce_backoff=False)
    pending_ids = [ev.outbox_id for ev in pending]
    assert "ob-ch1" not in pending_ids
    assert "ob-ch2" not in pending_ids
    assert pending_ids == ["ob-1"]


def test_error_message_token_scrubbing(outbox_db):
    """Webhook tokens must be scrubbed when saving last_notification_error."""
    dispatcher = OutboxDispatcher(outbox_db)
    test_ek = "a" * 64

    sensitive_error = "HTTP 404 from https://discord.com/api/webhooks/123456/SECRET_TOKEN_XYZ"
    dispatcher.mark_failed("ob-1", test_ek, sensitive_error)

    cur = outbox_db.cursor()
    cur.execute("SELECT last_notification_error FROM price_change_outbox WHERE outbox_id = 'ob-1';")
    err = cur.fetchone()[0]
    assert "SECRET_TOKEN_XYZ" not in err
    assert "***" in err


def test_dispatch_batch_preserves_sender_last_error_with_scrubbing(outbox_db):
    """AUD-019: dispatch_batch captures notifier.last_error and scrubs sensitive tokens."""
    dispatcher = OutboxDispatcher(outbox_db)

    class MockFailingNotifier:
        def __init__(self):
            self.last_error = "HTTP 401 Unauthorized from https://discord.com/api/webhooks/999/LEAKED_WEBHOOK_SECRET_KEY"

        def send(self, ev):
            return False

        def __call__(self, ev):
            return self.send(ev)

    notifier = MockFailingNotifier()
    stats = dispatcher.dispatch_batch(sender=notifier.send, enforce_backoff=False)
    assert stats["failed"] == 1

    cur = outbox_db.cursor()
    cur.execute("SELECT last_notification_error FROM price_change_outbox WHERE outbox_id = 'ob-1';")
    err = cur.fetchone()[0]
    assert err is not None
    assert "LEAKED_WEBHOOK_SECRET_KEY" not in err
    assert "***" in err or "401" in err


def test_older_outbox_event_does_not_overwrite_newer_monitor_state(outbox_db):
    """AUD-019: Processing an older outbox event must not overwrite a newer event's status on b2b_price_monitor."""
    dispatcher = OutboxDispatcher(outbox_db)
    test_ek = "a" * 64

    # ob-1 is already in DB from fixture at T1
    # Add ob-2 at T2 (newer)
    t2_iso = "2026-11-15T12:00:00+00:00"
    with transaction(outbox_db) as cur:
        cur.execute(
            """
            INSERT INTO price_change_outbox (
                outbox_id, entity_key, event_type, payload,
                notification_status, retry_count, created_at, updated_at
            ) VALUES ('ob-2-newer', ?, 'PRICE_CHANGED', '{"price": "12.0000"}',
                      'PENDING', 0, ?, ?);
            """,
            (test_ek, t2_iso, t2_iso),
        )
        cur.execute(
            """
            UPDATE b2b_price_monitor
            SET notification_status = 'PENDING'
            WHERE entity_key = ?;
            """,
            (test_ek,),
        )

    # When older event ob-1 fails, it must NOT overwrite b2b_price_monitor to FAILED
    dispatcher.mark_failed("ob-1", test_ek, "Stale failure for ob-1")

    cur = outbox_db.cursor()
    cur.execute("SELECT notification_status FROM b2b_price_monitor WHERE entity_key = ?;", (test_ek,))
    assert cur.fetchone()[0] == "PENDING"  # Protected against stale overwrite!

    # When older event ob-1 is marked dispatched, it must NOT overwrite b2b_price_monitor
    dispatcher.mark_dispatched("ob-1", test_ek)
    cur.execute("SELECT notification_status FROM b2b_price_monitor WHERE entity_key = ?;", (test_ek,))
    assert cur.fetchone()[0] == "PENDING"

    # But when newest event ob-2 is marked dispatched, b2b_price_monitor MUST update
    dispatcher.mark_dispatched("ob-2-newer", test_ek)
    cur.execute("SELECT notification_status FROM b2b_price_monitor WHERE entity_key = ?;", (test_ek,))
    assert cur.fetchone()[0] == "DISPATCHED"

