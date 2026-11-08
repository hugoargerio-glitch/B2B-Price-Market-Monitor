"""
Integration tests for end-to-end Ingestion Pipeline in src/pipeline.py.
Verifies atomic state transitions, heartbeat-only updates, quarantine routing,
and Domain Circuit Breakers.
"""

from decimal import Decimal
import pytest

from src.config import CircuitBreakerConfig
from src.diff_engine import compute_entity_key
from src.models import AvailabilityStatus, NotificationStatus, OutboxEventType
from src.pipeline import PriceMonitorPipeline
from src.storage import get_connection, init_db


@pytest.fixture
def pipeline_db(tmp_path):
    conn = get_connection(tmp_path / "pipeline.db")
    init_db(conn)
    yield conn
    conn.close()


def test_pipeline_ingest_new_entity(pipeline_db):
    """New entity insertion must write to product_price_state and outbox atomically."""
    pipeline = PriceMonitorPipeline(pipeline_db)

    batch = [
        {
            "domain": "competitor-a.com",
            "canonical_url": "https://competitor-a.com/item-1",
            "sku": "SKU-001",
            "product_title": "Product Title 1",
            "currency_code": "BRL",
            "current_price": Decimal("199.9000"),
            "availability_status": AvailabilityStatus.IN_STOCK.value,
        }
    ]

    summary = pipeline.ingest_batch(batch)
    assert summary.total_received == 1
    assert summary.inserted_new == 1
    assert summary.quarantined == 0

    cur = pipeline_db.cursor()
    cur.execute("SELECT COUNT(*) FROM product_price_state;")
    assert cur.fetchone()[0] == 1

    cur.execute("SELECT event_type, notification_status FROM price_change_outbox;")
    ev_type, notif_status = cur.fetchone()
    assert ev_type == OutboxEventType.INITIAL_CAPTURE.value
    assert notif_status == NotificationStatus.PENDING.value


def test_pipeline_heartbeat_zero_outbox_events(pipeline_db):
    """Unchanged state must update heartbeat only and generate zero outbox events."""
    pipeline = PriceMonitorPipeline(pipeline_db)

    item = {
        "domain": "competitor-a.com",
        "canonical_url": "https://competitor-a.com/item-1",
        "sku": "SKU-001",
        "product_title": "Product Title 1",
        "currency_code": "BRL",
        "current_price": Decimal("199.9000"),
        "availability_status": AvailabilityStatus.IN_STOCK.value,
    }

    # First run: initial insertion
    pipeline.ingest_batch([item])

    cur = pipeline_db.cursor()
    cur.execute("SELECT COUNT(*) FROM price_change_outbox;")
    assert cur.fetchone()[0] == 1

    cur.execute("SELECT last_checked_at, last_changed_at FROM product_price_state;")
    first_checked, first_changed = cur.fetchone()

    # Second run: identical state
    summary = pipeline.ingest_batch([item])
    assert summary.updated_heartbeat_only == 1
    assert summary.inserted_new == 0
    assert summary.updated_state_change == 0

    # Outbox count remains exactly 1 (Zero new events generated!)
    cur.execute("SELECT COUNT(*) FROM price_change_outbox;")
    assert cur.fetchone()[0] == 1

    # last_changed_at must not be mutated
    cur.execute("SELECT last_changed_at FROM product_price_state;")
    assert cur.fetchone()[0] == first_changed


def test_pipeline_price_mutation_and_outbox(pipeline_db):
    """Mutated price updates state, calculates delta, and emits PRICE_CHANGED event."""
    pipeline = PriceMonitorPipeline(pipeline_db)

    item = {
        "domain": "competitor-a.com",
        "canonical_url": "https://competitor-a.com/item-1",
        "sku": "SKU-001",
        "product_title": "Product Title 1",
        "currency_code": "BRL",
        "current_price": Decimal("200.0000"),
        "availability_status": AvailabilityStatus.IN_STOCK.value,
    }
    pipeline.ingest_batch([item])

    # Price drops to 150.00 (-25%)
    item_mutated = {
        **item,
        "current_price": Decimal("150.0000"),
    }
    summary = pipeline.ingest_batch([item_mutated])
    assert summary.updated_state_change == 1

    cur = pipeline_db.cursor()
    cur.execute(
        """
        SELECT previous_price, current_price, price_delta, price_delta_percent
        FROM product_price_state;
        """
    )
    prev, curr, delta, pct = cur.fetchone()
    assert Decimal(str(prev)) == Decimal("200.0000")
    assert Decimal(str(curr)) == Decimal("150.0000")
    assert Decimal(str(delta)) == Decimal("-50.0000")
    assert Decimal(str(pct)) == Decimal("-25.0000")

    # Verify PRICE_CHANGED outbox event
    cur.execute("SELECT event_type FROM price_change_outbox ORDER BY created_at DESC LIMIT 1;")
    assert cur.fetchone()[0] == OutboxEventType.PRICE_CHANGED.value


def test_pipeline_shift_left_quarantine_without_batch_abortion(pipeline_db):
    """Malformed item is quarantined to DLQ while valid items in same batch succeed."""
    pipeline = PriceMonitorPipeline(pipeline_db)

    batch = [
        {
            "domain": "valid-site.com",
            "canonical_url": "https://valid-site.com/p1",
            "sku": "SKU-VALID",
            "product_title": "Valid Product",
            "currency_code": "USD",
            "current_price": Decimal("49.9900"),
            "availability_status": AvailabilityStatus.IN_STOCK.value,
        },
        {
            # Malformed item: float price forbidden
            "domain": "invalid-site.com",
            "canonical_url": "https://invalid-site.com/p2",
            "sku": "SKU-INVALID",
            "product_title": "Invalid Float Price",
            "currency_code": "USD",
            "current_price": 49.99,  # FLOAT!
            "availability_status": AvailabilityStatus.IN_STOCK.value,
        },
        {
            # Malformed item: negative price forbidden
            "domain": "invalid-site.com",
            "canonical_url": "https://invalid-site.com/p3",
            "sku": "SKU-NEGATIVE",
            "product_title": "Negative Price",
            "currency_code": "USD",
            "current_price": Decimal("-10.0000"),
            "availability_status": AvailabilityStatus.IN_STOCK.value,
        },
    ]

    summary = pipeline.ingest_batch(batch)
    assert summary.total_received == 3
    assert summary.inserted_new == 1
    assert summary.quarantined == 2

    cur = pipeline_db.cursor()
    # Valid item saved
    cur.execute("SELECT COUNT(*) FROM product_price_state;")
    assert cur.fetchone()[0] == 1

    # Malformed items routed to DLQ
    cur.execute("SELECT COUNT(*), failure_reason FROM b2b_price_monitor_quarantine GROUP BY failure_reason;")
    rows = cur.fetchall()
    assert sum(r[0] for r in rows) == 2


def test_pipeline_domain_circuit_breaker(pipeline_db):
    """When a domain has >= min_sample_size items and >20% failure rate, circuit breaker trips and halts alerts."""
    pipeline = PriceMonitorPipeline(
        pipeline_db,
        circuit_breaker_config=CircuitBreakerConfig(max_quarantine_rate=0.20, min_sample_size=10),
    )

    # Batch with 10 items for bad-domain.com: 4 valid, 6 invalid (60% failure rate, total=10 >= 10)
    batch = [
        {
            "domain": "bad-domain.com",
            "canonical_url": f"https://bad-domain.com/p{i}",
            "sku": f"SKU-{i}",
            "product_title": f"Product {i}",
            "currency_code": "BRL",
            "current_price": Decimal("100.0000") if i < 4 else 99.99,  # Float for i >= 4
            "availability_status": AvailabilityStatus.IN_STOCK.value,
        }
        for i in range(10)
    ]

    summary = pipeline.ingest_batch(batch)
    assert "bad-domain.com" in summary.tripped_circuit_breakers
    assert summary.quarantined == 6
    assert summary.inserted_new == 4

    cur = pipeline_db.cursor()
    cur.execute("SELECT notification_status, last_notification_error FROM price_change_outbox WHERE entity_key IN (SELECT entity_key FROM product_price_state WHERE domain = 'bad-domain.com');")
    for notif_stat, notif_err in cur.fetchall():
        assert notif_stat == NotificationStatus.FAILED.value
        assert notif_err == "HALTED_BY_CIRCUIT_BREAKER"


def test_pipeline_circuit_breaker_ignores_small_sample_size(pipeline_db):
    """When a domain has < min_sample_size items (e.g., 3 items with 1 failure = 33%), circuit breaker MUST NOT trip."""
    pipeline = PriceMonitorPipeline(
        pipeline_db,
        circuit_breaker_config=CircuitBreakerConfig(max_quarantine_rate=0.20, min_sample_size=10),
    )

    # 3 items: 2 valid, 1 invalid (33.3% > 20%, but total=3 < min_sample_size=10)
    batch = [
        {
            "domain": "boutique-domain.com",
            "canonical_url": f"https://boutique-domain.com/p{i}",
            "sku": f"BOUTIQUE-{i}",
            "product_title": f"Boutique Product {i}",
            "currency_code": "BRL",
            "current_price": Decimal("150.0000") if i < 2 else 149.99,
            "availability_status": AvailabilityStatus.IN_STOCK.value,
        }
        for i in range(3)
    ]

    summary = pipeline.ingest_batch(batch)
    assert "boutique-domain.com" not in summary.tripped_circuit_breakers
    assert summary.quarantined == 1
    assert summary.inserted_new == 2

    cur = pipeline_db.cursor()
    cur.execute(
        "SELECT notification_status FROM price_change_outbox WHERE entity_key IN (SELECT entity_key FROM product_price_state WHERE domain = 'boutique-domain.com');"
    )
    for (notif_stat,) in cur.fetchall():
        assert notif_stat == NotificationStatus.PENDING.value
