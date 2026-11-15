"""
Hermetic verification of all ODCS v3.2.0 quality checks and SQL invariants
against an isolated temporary SQLite database populated via seed_database.
"""

from decimal import Decimal
import sqlite3
import pytest

from src.seed import seed_database
from src.storage import get_connection, init_db


@pytest.fixture
def prod_db(tmp_path):
    """Hermetic SQLite database seeded according to ODCS v3.2.0 contracts."""
    db_file = tmp_path / "hermetic_contract_test.db"
    seed_database(db_file)
    conn = get_connection(db_file)
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
        WHERE CAST(ROUND(CAST(current_price AS REAL) * 100, 0) AS BIGINT) <> current_price_cents;
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
        WHERE CAST(current_price AS REAL) < 0 OR current_price_cents < 0;
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
        WHERE notification_status NOT IN ('PENDING', 'DISPATCHED', 'FAILED', 'DEAD_LETTER', 'CIRCUIT_HALTED');
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


def test_contract_text_decimal_storage(prod_db):
    """AUD-006: Monetary decimals in SQLite must be stored physically as TEXT."""
    cur = prod_db.cursor()
    cur.execute(
        """
        SELECT typeof(current_price), typeof(previous_price), typeof(price_delta), typeof(price_delta_percent)
        FROM b2b_price_monitor
        WHERE previous_price IS NOT NULL
        LIMIT 1;
        """
    )
    row = cur.fetchone()
    assert row is not None
    curr_type, prev_type, delta_type, pct_type = row
    assert curr_type == "text"
    assert prev_type == "text"
    assert delta_type == "text"
    assert pct_type == "text"
