"""
SQLite Storage Layer for Profile 1 (Lean App/CDC).
Configures WAL mode, B-Tree indexes, and ACID transactional guarantees.
"""

from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
import sqlite3
from typing import Generator, Optional

from src.config import DatabaseConfig, DEFAULT_DB_PATH
from src.models import (
    AvailabilityStatus,
    NotificationStatus,
    ProductPriceState,
)


DDL_PRODUCT_PRICE_STATE = """
CREATE TABLE IF NOT EXISTS product_price_state (
    entity_key CHAR(64) PRIMARY KEY,
    state_hash CHAR(64) NOT NULL,
    domain VARCHAR(255) NOT NULL,
    canonical_url TEXT NOT NULL,
    sku VARCHAR(128) NOT NULL,
    product_title VARCHAR(512) NOT NULL,
    currency_code CHAR(3) NOT NULL,
    current_price_cents BIGINT NOT NULL,
    current_price DECIMAL(18,4) NOT NULL,
    previous_price DECIMAL(18,4),
    price_delta DECIMAL(18,4),
    price_delta_percent DECIMAL(18,4),
    price_change_pct DECIMAL(18,4),
    availability_status VARCHAR(32) NOT NULL,
    last_checked_at TIMESTAMP WITH TIME ZONE NOT NULL,
    last_seen_at_utc TIMESTAMP WITH TIME ZONE,
    last_changed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at_utc TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);
"""

DDL_PRICE_CHANGE_OUTBOX = """
CREATE TABLE IF NOT EXISTS price_change_outbox (
    outbox_id VARCHAR(64) PRIMARY KEY,
    entity_key CHAR(64) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    payload TEXT NOT NULL,
    notification_status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_notification_error VARCHAR(512),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    FOREIGN KEY (entity_key) REFERENCES product_price_state(entity_key)
);
"""

DDL_QUARANTINE = """
CREATE TABLE IF NOT EXISTS b2b_price_monitor_quarantine (
    quarantine_id VARCHAR(64) PRIMARY KEY,
    domain VARCHAR(255) NOT NULL,
    raw_url TEXT NOT NULL,
    raw_payload TEXT,
    failure_reason VARCHAR(128) NOT NULL,
    quarantined_at TIMESTAMP WITH TIME ZONE NOT NULL,
    resolution_status VARCHAR(32) NOT NULL DEFAULT 'UNRESOLVED'
);
"""

DDL_B2B_PRICE_MONITOR = """
CREATE TABLE IF NOT EXISTS b2b_price_monitor (
    entity_key CHAR(64) PRIMARY KEY,
    state_hash CHAR(64) NOT NULL,
    domain VARCHAR(255) NOT NULL,
    canonical_url TEXT NOT NULL,
    sku VARCHAR(128) NOT NULL,
    product_title VARCHAR(512) NOT NULL,
    currency_code CHAR(3) NOT NULL,
    current_price_cents BIGINT NOT NULL,
    current_price DECIMAL(18,4) NOT NULL,
    previous_price DECIMAL(18,4),
    price_delta DECIMAL(18,4),
    price_delta_percent DECIMAL(18,4),
    availability_status VARCHAR(32) NOT NULL,
    notification_status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_notification_error VARCHAR(512),
    last_checked_at TIMESTAMP WITH TIME ZONE NOT NULL,
    last_changed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);
"""

DDL_INDEXES = [
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_pps_entity_key ON product_price_state(entity_key);",
    "CREATE INDEX IF NOT EXISTS idx_pps_domain ON product_price_state(domain);",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_bpm_entity_key ON b2b_price_monitor(entity_key);",
    "CREATE INDEX IF NOT EXISTS idx_bpm_domain ON b2b_price_monitor(domain);",
    "CREATE INDEX IF NOT EXISTS idx_outbox_status ON price_change_outbox(notification_status);",
    "CREATE INDEX IF NOT EXISTS idx_outbox_entity ON price_change_outbox(entity_key);",
    "CREATE INDEX IF NOT EXISTS idx_quarantine_domain ON b2b_price_monitor_quarantine(domain);",
]


def get_connection(
    db_path: Path | str | None = None,
    config: Optional[DatabaseConfig] = None,
) -> sqlite3.Connection:
    """Open SQLite connection and apply required PRAGMAs for concurrency and durability."""
    target_path = Path(db_path) if db_path else (config.db_path if config else DEFAULT_DB_PATH)
    target_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(
        str(target_path),
        timeout=(config.busy_timeout_ms / 1000.0) if config else 5.0,
        isolation_level=None,  # Autocommit mode for explicit transaction management
    )
    conn.row_factory = sqlite3.Row

    # Apply PRAGMAs
    cfg = config or DatabaseConfig()
    cursor = conn.cursor()
    for pragma in cfg.pragma_statements:
        cursor.execute(pragma)
    cursor.close()

    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Execute DDL statements to create tables and indexes."""
    cursor = conn.cursor()
    cursor.execute("BEGIN IMMEDIATE;")
    try:
        cursor.execute(DDL_PRODUCT_PRICE_STATE)
        cursor.execute(DDL_PRICE_CHANGE_OUTBOX)
        cursor.execute(DDL_QUARANTINE)
        cursor.execute(DDL_B2B_PRICE_MONITOR)
        for idx in DDL_INDEXES:
            cursor.execute(idx)
        cursor.execute("COMMIT;")
    except Exception:
        cursor.execute("ROLLBACK;")
        raise
    finally:
        cursor.close()


@contextmanager
def transaction(conn: sqlite3.Connection) -> Generator[sqlite3.Cursor, None, None]:
    """Provide an ACID transaction context manager using SQLite BEGIN IMMEDIATE."""
    cursor = conn.cursor()
    cursor.execute("BEGIN IMMEDIATE;")
    try:
        yield cursor
        cursor.execute("COMMIT;")
    except Exception:
        cursor.execute("ROLLBACK;")
        raise
    finally:
        cursor.close()


def row_to_product_price_state(row: sqlite3.Row) -> ProductPriceState:
    """Convert an SQLite Row into a ProductPriceState Pydantic model."""
    return ProductPriceState(
        entity_key=row["entity_key"],
        state_hash=row["state_hash"],
        domain=row["domain"],
        canonical_url=row["canonical_url"],
        sku=row["sku"],
        product_title=row["product_title"],
        currency_code=row["currency_code"],
        current_price_cents=row["current_price_cents"],
        current_price=Decimal(str(row["current_price"])),
        previous_price=Decimal(str(row["previous_price"])) if row["previous_price"] is not None else None,
        price_delta=Decimal(str(row["price_delta"])) if row["price_delta"] is not None else None,
        price_delta_percent=Decimal(str(row["price_delta_percent"])) if row["price_delta_percent"] is not None else None,
        availability_status=AvailabilityStatus(row["availability_status"]),
        notification_status=NotificationStatus(row["notification_status"]) if "notification_status" in row.keys() else NotificationStatus.PENDING,
        retry_count=row["retry_count"] if "retry_count" in row.keys() else 0,
        last_notification_error=row["last_notification_error"] if "last_notification_error" in row.keys() else None,
        last_checked_at=row["last_checked_at"],
        last_changed_at=row["last_changed_at"],
        created_at=row["created_at"],
    )
