"""
Direct verification of all ODCS v3.2.0 quality checks and SQL invariants
against the physical SQLite database at /home/Hugo/b2b-price-monitor/b2b_monitor.db.
"""

from decimal import Decimal
import sqlite3
import pytest

from src.config import DEFAULT_DB_PATH
from src.storage import get_connection


@pytest.fixture(scope="module")
def prod_db():
    conn = get_connection(DEFAULT_DB_PATH)
    yield conn
    conn.close()


def test_contract_row_count_greater_than_zero(prod_db):
    """Quality rule: The active state store must contain at least one monitored product."""
    cur = prod_db.cursor()
    cur.execute("SELECT COUNT(*) FROM b2b_price_monitor;")
    count = cur.fetchone()[0]
    assert count > 0, f"Expected row count > 0, got {count}"


def test_contract_entity_key_uniqueness(prod_db):
    """Quality rule: Deterministic guarantee of entity_key uniqueness (one row per SKU/product)."""
    cur = prod_db.cursor()
    cur.execute("SELECT COUNT(*), COUNT(DISTINCT entity_key) FROM b2b_price_monitor;")
    total, distinct = cur.fetchone()
    assert total == distinct, f"Duplicate entity_keys found: total={total}, distinct={distinct}"


def test_contract_price_cents_reconciliation(prod_db):
    """Quality rule: Consistency check between integer cents and decimal price."""
    cur = prod_db.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM b2b_price_monitor
        WHERE CAST(ROUND(current_price * 100, 0) AS BIGINT) <> current_price_cents;
        """
    )
    mismatches = cur.fetchone()[0]
    assert mismatches == 0, f"Found {mismatches} rows with cents reconciliation mismatch"


def test_contract_non_negative_prices(prod_db):
    """Quality rule: Domain invariant - non-negative prices."""
    cur = prod_db.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM b2b_price_monitor
        WHERE current_price < 0 OR current_price_cents < 0;
        """
    )
    negatives = cur.fetchone()[0]
    assert negatives == 0, f"Found {negatives} rows with negative price"


def test_contract_availability_status_domain(prod_db):
    """Quality rule: Enforce closed domain of valid availability statuses."""
    cur = prod_db.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM b2b_price_monitor
        WHERE availability_status NOT IN ('IN_STOCK', 'OUT_OF_STOCK', 'DISCONTINUED', 'BACKORDER');
        """
    )
    invalids = cur.fetchone()[0]
    assert invalids == 0, f"Found {invalids} rows with invalid availability_status"


def test_contract_notification_status_domain(prod_db):
    """Quality rule: Enforce closed domain of outbox states."""
    cur = prod_db.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM b2b_price_monitor
        WHERE notification_status NOT IN ('PENDING', 'DISPATCHED', 'FAILED', 'DEAD_LETTER');
        """
    )
    invalids = cur.fetchone()[0]
    assert invalids == 0, f"Found {invalids} rows with invalid notification_status"


def test_contract_retry_count_limits(prod_db):
    """Quality rule: Retry count must be non-negative and not exceed 5 attempts."""
    cur = prod_db.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM b2b_price_monitor
        WHERE retry_count < 0 OR retry_count > 5;
        """
    )
    invalids = cur.fetchone()[0]
    assert invalids == 0, f"Found {invalids} rows with invalid retry_count"


def test_contract_quarantine_integrity(prod_db):
    """Quality rule: Quarantine table must have unique quarantine_id and non-null required fields."""
    cur = prod_db.cursor()
    cur.execute("SELECT COUNT(*), COUNT(DISTINCT quarantine_id) FROM b2b_price_monitor_quarantine;")
    total, distinct = cur.fetchone()
    assert total == distinct, f"Duplicate quarantine_id found: total={total}, distinct={distinct}"

    cur.execute(
        """
        SELECT COUNT(*)
        FROM b2b_price_monitor_quarantine
        WHERE quarantine_id IS NULL
           OR domain IS NULL
           OR raw_url IS NULL
           OR failure_reason IS NULL
           OR quarantined_at IS NULL
           OR resolution_status IS NULL;
        """
    )
    nulls = cur.fetchone()[0]
    assert nulls == 0, f"Found {nulls} quarantine rows with null mandatory fields"

    cur.execute(
        """
        SELECT COUNT(*)
        FROM b2b_price_monitor_quarantine
        WHERE resolution_status NOT IN ('UNRESOLVED', 'RESOLVED', 'IGNORED');
        """
    )
    invalid_res = cur.fetchone()[0]
    assert invalid_res == 0, f"Found {invalid_res} rows with invalid resolution_status"
