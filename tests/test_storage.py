"""
Unit tests for SQLite storage layer in src/storage.py.
Verifies WAL mode, PRAGMAs, single table DDL, TEXT decimal storage,
indexes, and primary key uniqueness.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import sqlite3
import pytest

from src.config import DatabaseConfig
from src.diff_engine import compute_entity_key, compute_state_hash
from src.storage import (
    checkpoint_and_vacuum,
    get_connection,
    init_db,
    purge_dispatched_outbox,
    transaction,
)


@pytest.fixture
def temp_db(tmp_path):
    db_file = tmp_path / "test_monitor.db"
    conn = get_connection(db_file)
    init_db(conn)
    yield conn
    conn.close()


def test_sqlite_wal_mode_and_pragmas(temp_db):
    """SQLite must be configured in WAL mode with correct pragmas."""
    cur = temp_db.cursor()
    cur.execute("PRAGMA journal_mode;")
    assert cur.fetchone()[0].lower() == "wal"

    cur.execute("PRAGMA busy_timeout;")
    assert cur.fetchone()[0] == 5000

    cur.execute("PRAGMA foreign_keys;")
    assert cur.fetchone()[0] == 1


def test_table_creation(temp_db):
    """Canonical tables must exist and legacy product_price_state must be removed (AUD-007)."""
    cur = temp_db.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = {r[0] for r in cur.fetchall()}
    assert "b2b_price_monitor" in tables
    assert "price_change_outbox" in tables
    assert "b2b_price_monitor_quarantine" in tables
    assert "product_price_state" not in tables


def test_primary_key_uniqueness(temp_db):
    """entity_key primary key uniqueness must be strictly enforced on b2b_price_monitor."""
    ek = compute_entity_key("domain.com", "SKU-001")
    sh = compute_state_hash(1000, "USD", "IN_STOCK")

    with transaction(temp_db) as cur:
        cur.execute(
            """
            INSERT INTO b2b_price_monitor (
                entity_key, state_hash, domain, canonical_url, sku,
                product_title, currency_code, current_price_cents,
                current_price, availability_status, last_checked_at,
                last_changed_at, created_at
            ) VALUES (?, ?, 'domain.com', 'url', 'SKU-001', 'title', 'USD', 1000, '10.0000', 'IN_STOCK', '2026-11-08T00:00:00Z', '2026-11-08T00:00:00Z', '2026-11-08T00:00:00Z');
            """,
            (ek, sh),
        )

    # Attempt duplicate insert must fail with IntegrityError
    with pytest.raises(sqlite3.IntegrityError):
        with transaction(temp_db) as cur:
            cur.execute(
                """
                INSERT INTO b2b_price_monitor (
                    entity_key, state_hash, domain, canonical_url, sku,
                    product_title, currency_code, current_price_cents,
                    current_price, availability_status, last_checked_at,
                    last_changed_at, created_at
                ) VALUES (?, ?, 'domain.com', 'url', 'SKU-001', 'title', 'USD', 1000, '10.0000', 'IN_STOCK', '2026-11-08T00:00:00Z', '2026-11-08T00:00:00Z', '2026-11-08T00:00:00Z');
                """,
                (ek, sh),
            )


def test_decimal_storage_as_text(temp_db):
    """AUD-006: Monetary decimals in b2b_price_monitor must be stored as TEXT."""
    ek = compute_entity_key("domain.com", "SKU-DEC-01")
    sh = compute_state_hash(249990, "BRL", "IN_STOCK", promo_badge="DESCONTO")

    with transaction(temp_db) as cur:
        cur.execute(
            """
            INSERT INTO b2b_price_monitor (
                entity_key, state_hash, domain, canonical_url, sku,
                product_title, currency_code, current_price_cents,
                current_price, previous_price, price_delta, price_delta_percent,
                availability_status, promo_badge, last_checked_at, last_changed_at, created_at
            ) VALUES (?, ?, 'domain.com', 'url', 'SKU-DEC-01', 'title', 'BRL', 249990,
                      '2499.9000', '2699.9000', '-200.0000', '-7.4074',
                      'IN_STOCK', 'DESCONTO', '2026-11-08T00:00:00Z', '2026-11-08T00:00:00Z', '2026-11-08T00:00:00Z');
            """,
            (ek, sh),
        )

    cur = temp_db.cursor()
    cur.execute(
        """
        SELECT typeof(current_price), typeof(previous_price), typeof(price_delta), typeof(price_delta_percent),
               promo_badge
        FROM b2b_price_monitor
        WHERE entity_key = ?;
        """,
        (ek,),
    )
    row = cur.fetchone()
    assert row[0] == "text"
    assert row[1] == "text"
    assert row[2] == "text"
    assert row[3] == "text"
    assert row[4] == "DESCONTO"


def test_foreign_key_to_b2b_price_monitor(temp_db):
    """AUD-007: price_change_outbox FK must enforce presence in b2b_price_monitor."""
    non_existent_ek = "f" * 64
    with pytest.raises(sqlite3.IntegrityError):
        with transaction(temp_db) as cur:
            cur.execute(
                """
                INSERT INTO price_change_outbox (
                    outbox_id, entity_key, event_type, payload,
                    notification_status, retry_count, created_at, updated_at
                ) VALUES ('ob-fail', ?, 'INITIAL_CAPTURE', '{}', 'PENDING', 0, '2026-11-08T00:00:00Z', '2026-11-08T00:00:00Z');
                """,
                (non_existent_ek,),
            )


def test_transaction_rollback_on_error(temp_db):
    """Failed operations inside transaction must rollback cleanly."""
    try:
        with transaction(temp_db) as cur:
            cur.execute(
                """
                INSERT INTO b2b_price_monitor_quarantine (
                    quarantine_id, domain, raw_url, raw_payload,
                    failure_reason, quarantined_at, resolution_status
                ) VALUES ('q1', 'test.com', 'http://test', NULL, 'TEST', '2026-11-08T00:00:00Z', 'UNRESOLVED');
                """
            )
            # Simulate unexpected failure
            raise RuntimeError("Simulated failure")
    except RuntimeError:
        pass

    cur = temp_db.cursor()
    cur.execute("SELECT COUNT(*) FROM b2b_price_monitor_quarantine WHERE quarantine_id = 'q1';")
    assert cur.fetchone()[0] == 0


def test_purge_dispatched_outbox_and_vacuum(temp_db):
    """Housekeeping purges old DISPATCHED outbox records while keeping recent or PENDING records."""
    ek = compute_entity_key("domain.com", "SKU-HK")
    sh = compute_state_hash(1000, "USD", "IN_STOCK")
    old_iso = "2020-01-01T00:00:00+00:00"
    recent_iso = "2099-01-01T00:00:00+00:00"

    with transaction(temp_db) as cur:
        cur.execute(
            """
            INSERT INTO b2b_price_monitor (
                entity_key, state_hash, domain, canonical_url, sku,
                product_title, currency_code, current_price_cents,
                current_price, availability_status, last_checked_at,
                last_changed_at, created_at
            ) VALUES (?, ?, 'domain.com', 'url', 'SKU-HK', 'title', 'USD', 1000, '10.0000', 'IN_STOCK', ?, ?, ?);
            """,
            (ek, sh, recent_iso, recent_iso, recent_iso),
        )
        # Old DISPATCHED record (should be purged)
        cur.execute(
            """
            INSERT INTO price_change_outbox (
                outbox_id, entity_key, event_type, payload,
                notification_status, retry_count, created_at, updated_at
            ) VALUES ('ob-old', ?, 'PRICE_CHANGE', '{}', 'DISPATCHED', 0, ?, ?);
            """,
            (ek, old_iso, old_iso),
        )
        # Old PENDING record (must NOT be purged)
        cur.execute(
            """
            INSERT INTO price_change_outbox (
                outbox_id, entity_key, event_type, payload,
                notification_status, retry_count, created_at, updated_at
            ) VALUES ('ob-pending', ?, 'PRICE_CHANGE', '{}', 'PENDING', 0, ?, ?);
            """,
            (ek, old_iso, old_iso),
        )

    deleted = purge_dispatched_outbox(temp_db, retention_days=30)
    assert deleted == 1

    cur = temp_db.cursor()
    cur.execute("SELECT outbox_id FROM price_change_outbox;")
    remaining = [r[0] for r in cur.fetchall()]
    assert remaining == ["ob-pending"]

    # Verify checkpoint_and_vacuum executes cleanly
    checkpoint_and_vacuum(temp_db)


@pytest.mark.parametrize(
    "offset_days,offset_hours,tz_suffix,status,should_purge",
    [
        (29, 0, "+00:00", "DISPATCHED", False),   # 29d: within retention window, keep
        (29, 23, "Z", "DISPATCHED", False),       # 29d23h: within retention window, keep
        (30, 2, "+00:00", "DISPATCHED", True),    # 30d2h: older than 30d cutoff, purge
        (30, 2, "Z", "DISPATCHED", True),         # 30d2h with 'Z': older than 30d cutoff, purge
        (31, 0, "+00:00", "DISPATCHED", True),    # 31d: older than 30d cutoff, purge
        (31, 0, "Z", "DISPATCHED", True),         # 31d with 'Z': older than 30d cutoff, purge
        (45, 0, "+00:00", "FAILED", False),       # FAILED: never purged
        (45, 0, "Z", "DEAD_LETTER", False),       # DEAD_LETTER: never purged
        (45, 0, "+00:00", "CIRCUIT_HALTED", False),# CIRCUIT_HALTED: never purged
        (45, 0, "Z", "PENDING", False),           # PENDING: never purged
    ],
)
def test_purge_dispatched_outbox_boundary_and_tz_formats(
    temp_db, offset_days, offset_hours, tz_suffix, status, should_purge
):
    """AUD-021: Parameterized boundary tests for 29d, 30d, 31d, Z vs +00:00, and non-DISPATCHED safety."""
    ek = compute_entity_key("domain.com", f"SKU-BOUND-{offset_days}-{offset_hours}-{status}-{tz_suffix}")
    sh = compute_state_hash(1000, "USD", "IN_STOCK")
    now = datetime.now(timezone.utc)
    ts = now - timedelta(days=offset_days, hours=offset_hours)

    if tz_suffix == "Z":
        formatted_ts = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        formatted_ts = ts.isoformat()

    now_iso = now.isoformat()
    outbox_id = f"ob-{offset_days}d-{offset_hours}h-{status}-{tz_suffix}"

    with transaction(temp_db) as cur:
        cur.execute(
            """
            INSERT INTO b2b_price_monitor (
                entity_key, state_hash, domain, canonical_url, sku,
                product_title, currency_code, current_price_cents,
                current_price, availability_status, last_checked_at,
                last_changed_at, created_at
            ) VALUES (?, ?, 'domain.com', 'url', 'SKU-TEST', 'title', 'USD', 1000, '10.0000', 'IN_STOCK', ?, ?, ?);
            """,
            (ek, sh, now_iso, now_iso, now_iso),
        )
        cur.execute(
            """
            INSERT INTO price_change_outbox (
                outbox_id, entity_key, event_type, payload,
                notification_status, retry_count, created_at, updated_at
            ) VALUES (?, ?, 'PRICE_CHANGED', '{}', ?, 0, ?, ?);
            """,
            (outbox_id, ek, status, formatted_ts, formatted_ts),
        )

    deleted = purge_dispatched_outbox(temp_db, retention_days=30)
    expected_deleted = 1 if should_purge else 0
    assert deleted == expected_deleted

    cur = temp_db.cursor()
    cur.execute("SELECT COUNT(*) FROM price_change_outbox WHERE outbox_id = ?;", (outbox_id,))
    count = cur.fetchone()[0]
    expected_count = 0 if should_purge else 1
    assert count == expected_count

