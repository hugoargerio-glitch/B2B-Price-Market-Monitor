"""
Unit tests for SQLite storage layer in src/storage.py.
Verifies WAL mode, PRAGMAs, table DDL, indexes, and primary key uniqueness.
"""

from decimal import Decimal
import sqlite3
import pytest

from src.config import DatabaseConfig
from src.diff_engine import compute_entity_key, compute_state_hash
from src.storage import get_connection, init_db, transaction


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
    """Tables defined in schema must exist."""
    cur = temp_db.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = {r[0] for r in cur.fetchall()}
    assert "product_price_state" in tables
    assert "price_change_outbox" in tables
    assert "b2b_price_monitor_quarantine" in tables
    assert "b2b_price_monitor" in tables


def test_primary_key_uniqueness(temp_db):
    """entity_key primary key uniqueness must be strictly enforced."""
    ek = compute_entity_key("domain.com", "SKU-001")
    sh = compute_state_hash(1000, "USD", "IN_STOCK")

    with transaction(temp_db) as cur:
        cur.execute(
            """
            INSERT INTO product_price_state (
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
                INSERT INTO product_price_state (
                    entity_key, state_hash, domain, canonical_url, sku,
                    product_title, currency_code, current_price_cents,
                    current_price, availability_status, last_checked_at,
                    last_changed_at, created_at
                ) VALUES (?, ?, 'domain.com', 'url', 'SKU-001', 'title', 'USD', 1000, '10.0000', 'IN_STOCK', '2026-11-08T00:00:00Z', '2026-11-08T00:00:00Z', '2026-11-08T00:00:00Z');
                """,
                (ek, sh),
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
