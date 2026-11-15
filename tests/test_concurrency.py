"""
Concurrency and multi-threading resilience tests for Profile 1 SQLite WAL mode.
Verifies non-blocking WAL readers during active writes and multi-threaded pipeline
ingest/process_item resilience using ThreadPoolExecutor.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
import time
import pytest

from src.diff_engine import compute_entity_key, compute_state_hash
from src.models import AvailabilityStatus
from src.pipeline import PriceMonitorPipeline
from src.storage import get_connection, init_db, transaction


@pytest.fixture
def concurrency_db_path(tmp_path):
    db_file = tmp_path / "concurrency_test.db"
    conn = get_connection(db_file)
    init_db(conn)

    # Insert baseline item
    ek = compute_entity_key("concurrent-supplier.com", "SKU-BASE-01")
    sh = compute_state_hash(10000, "BRL", "IN_STOCK")
    with transaction(conn) as cur:
        cur.execute(
            """
            INSERT INTO b2b_price_monitor (
                entity_key, state_hash, domain, canonical_url, sku,
                product_title, currency_code, current_price_cents,
                current_price, availability_status, last_checked_at,
                last_changed_at, created_at
            ) VALUES (?, ?, 'concurrent-supplier.com', 'https://concurrent-supplier.com/p1',
                      'SKU-BASE-01', 'Concurrent Item 1', 'BRL', 10000, '100.0000',
                      'IN_STOCK', '2026-11-15T00:00:00Z', '2026-11-15T00:00:00Z', '2026-11-15T00:00:00Z');
            """,
            (ek, sh),
        )
    conn.close()
    return db_file


def test_concurrent_wal_readers_during_active_writes(concurrency_db_path):
    """SQLite WAL mode guarantees readers never block writers and writers never block readers."""
    writer_started = False
    stop_readers = False
    reader_results = []
    errors = []

    def writer_task():
        nonlocal writer_started
        conn = get_connection(concurrency_db_path)
        try:
            with transaction(conn) as cur:
                writer_started = True
                # Perform multiple write statements inside active transaction with deliberate delay
                for i in range(1, 15):
                    ek = compute_entity_key("concurrent-supplier.com", f"SKU-WRITE-{i}")
                    sh = compute_state_hash(i * 1000, "BRL", "IN_STOCK")
                    cur.execute(
                        """
                        INSERT INTO b2b_price_monitor (
                            entity_key, state_hash, domain, canonical_url, sku,
                            product_title, currency_code, current_price_cents,
                            current_price, availability_status, last_checked_at,
                            last_changed_at, created_at
                        ) VALUES (?, ?, 'concurrent-supplier.com', 'url', ?, 'Title',
                                  'BRL', ?, '10.0000', 'IN_STOCK', '2026-11-15T00:00:00Z',
                                  '2026-11-15T00:00:00Z', '2026-11-15T00:00:00Z');
                        """,
                        (ek, sh, f"SKU-WRITE-{i}", i * 1000),
                    )
                    time.sleep(0.01)
        finally:
            conn.close()

    def reader_task(reader_id: int):
        conn = get_connection(concurrency_db_path)
        reads = 0
        try:
            # Poll reads continuously while writer runs
            while not stop_readers:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM b2b_price_monitor;")
                count = cur.fetchone()[0]
                assert count >= 1
                reads += 1
                time.sleep(0.005)
            return reads
        except Exception as exc:
            errors.append((reader_id, exc))
            return 0
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=6) as executor:
        writer_future = executor.submit(writer_task)

        # Launch 5 concurrent reader threads
        reader_futures = [executor.submit(reader_task, r_id) for r_id in range(5)]

        # Wait for writer to finish
        writer_future.result(timeout=10)
        stop_readers = True

        for rf in reader_futures:
            reader_results.append(rf.result(timeout=5))

    assert len(errors) == 0, f"Concurrent readers failed: {errors}"
    assert all(r > 0 for r in reader_results)

    # Post-check: final row count in database
    conn = get_connection(concurrency_db_path)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM b2b_price_monitor;")
    assert cur.fetchone()[0] == 15  # 1 initial + 14 inserted
    conn.close()


def test_multithreaded_pipeline_ingest_resilience(tmp_path):
    """Multiple threads ingesting batches concurrently with busy_timeout serialize cleanly."""
    db_file = tmp_path / "mt_pipeline.db"
    conn = get_connection(db_file)
    init_db(conn)
    conn.close()

    num_threads = 4
    items_per_thread = 5

    def worker_ingest(worker_idx: int):
        thread_conn = get_connection(db_file)
        pipeline = PriceMonitorPipeline(thread_conn)
        batch = [
            {
                "domain": f"vendor-{worker_idx}.com",
                "canonical_url": f"https://vendor-{worker_idx}.com/p/{item_idx}",
                "sku": f"SKU-W{worker_idx}-{item_idx}",
                "product_title": f"Product W{worker_idx} #{item_idx}",
                "currency_code": "USD",
                "current_price": Decimal(f"{100 + item_idx}.0000"),
                "availability_status": AvailabilityStatus.IN_STOCK.value,
            }
            for item_idx in range(items_per_thread)
        ]
        try:
            summary = pipeline.ingest_batch(batch)
            return summary.inserted_new
        finally:
            thread_conn.close()

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker_ingest, i) for i in range(num_threads)]
        results = [f.result(timeout=15) for f in as_completed(futures)]

    assert sum(results) == num_threads * items_per_thread

    # Verify atomic persistence in SQLite
    final_conn = get_connection(db_file)
    cur = final_conn.cursor()
    cur.execute("SELECT COUNT(*) FROM b2b_price_monitor;")
    assert cur.fetchone()[0] == num_threads * items_per_thread

    cur.execute("SELECT COUNT(*) FROM price_change_outbox;")
    assert cur.fetchone()[0] == num_threads * items_per_thread
    final_conn.close()


def test_multithreaded_process_item_state_resilience(tmp_path):
    """Concurrent updates to identical entity via process_item maintain state consistency."""
    db_file = tmp_path / "mt_process_item.db"
    conn = get_connection(db_file)
    init_db(conn)

    # Initial insert
    init_pipeline = PriceMonitorPipeline(conn)
    init_item = {
        "domain": "shared-competitor.com",
        "canonical_url": "https://shared-competitor.com/shared-product",
        "sku": "SKU-SHARED-99",
        "product_title": "Shared Product",
        "currency_code": "BRL",
        "current_price": Decimal("100.0000"),
        "availability_status": AvailabilityStatus.IN_STOCK.value,
    }
    init_pipeline.ingest_batch([init_item])
    conn.close()

    # Launch multiple threads updating price
    def update_worker(worker_id: int):
        w_conn = get_connection(db_file)
        w_pipeline = PriceMonitorPipeline(w_conn)
        item = {
            "domain": "shared-competitor.com",
            "canonical_url": "https://shared-competitor.com/shared-product",
            "sku": "SKU-SHARED-99",
            "product_title": "Shared Product",
            "currency_code": "BRL",
            "current_price": Decimal(f"{100 + worker_id * 10}.0000"),
            "availability_status": AvailabilityStatus.IN_STOCK.value,
        }
        try:
            category, ek = w_pipeline.process_item(item, set())
            return category
        finally:
            w_conn.close()

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(update_worker, i) for i in range(1, 5)]
        categories = [f.result(timeout=10) for f in as_completed(futures)]

    # All mutations must be recognized as STATE_CHANGE or HEARTBEAT
    assert all(c in ("STATE_CHANGE", "HEARTBEAT") for c in categories)

    check_conn = get_connection(db_file)
    cur = check_conn.cursor()
    cur.execute("SELECT COUNT(*) FROM b2b_price_monitor WHERE sku = 'SKU-SHARED-99';")
    # Must remain strictly 1 row per entity_key!
    assert cur.fetchone()[0] == 1
    check_conn.close()
