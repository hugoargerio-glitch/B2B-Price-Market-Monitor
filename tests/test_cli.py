"""
Integration tests for CLI entrypoint in src/main.py.
Verifies CLI subcommands: simulate, status, dispatch (--dry-run), ingest, and run.
"""

from decimal import Decimal
import json
import pytest

from src.main import main
from src.storage import get_connection, init_db


@pytest.fixture
def cli_db_path(tmp_path):
    db_file = tmp_path / "cli_test.db"
    conn = get_connection(db_file)
    init_db(conn)
    conn.close()
    return db_file


def test_cli_simulate(cli_db_path, capsys):
    """simulate subcommand ingests simulated items and displays summary table."""
    exit_code = main(["--db-path", str(cli_db_path), "simulate", "--items", "5"])
    assert exit_code == 0

    captured = capsys.readouterr()
    assert "SIMULATION INGESTION SUMMARY" in captured.out
    assert "Total Payloads Received   : 5" in captured.out

    # Check database was populated
    conn = get_connection(cli_db_path)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM b2b_price_monitor;")
    assert cur.fetchone()[0] > 0
    conn.close()


def test_cli_simulate_with_quarantine(cli_db_path, capsys):
    """simulate with --include-quarantine routes malformed items to DLQ."""
    exit_code = main(["--db-path", str(cli_db_path), "simulate", "--items", "5", "--include-quarantine"])
    assert exit_code == 0

    captured = capsys.readouterr()
    assert "Quarantined (DLQ)" in captured.out

    conn = get_connection(cli_db_path)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM b2b_price_monitor_quarantine;")
    assert cur.fetchone()[0] >= 1
    conn.close()


def test_cli_status(cli_db_path, capsys):
    """status subcommand displays executive dashboard."""
    # First seed 2 items via simulate
    main(["--db-path", str(cli_db_path), "simulate", "--items", "3"])

    exit_code = main(["--db-path", str(cli_db_path), "status"])
    assert exit_code == 0

    captured = capsys.readouterr()
    assert "B2B PRICE & MARKET MONITOR — EXECUTIVE DASHBOARD" in captured.out
    assert "Active Tracked SKUs" in captured.out
    assert "TRANSACTIONAL OUTBOX QUEUE" in captured.out


def test_cli_dispatch_dry_run(cli_db_path, capsys):
    """dispatch --dry-run prints outbox alerts without failing."""
    # First seed via simulate
    main(["--db-path", str(cli_db_path), "simulate", "--items", "3"])

    exit_code = main(["--db-path", str(cli_db_path), "dispatch", "--dry-run"])
    assert exit_code == 0

    captured = capsys.readouterr()
    assert "OUTBOX DISPATCH SUMMARY" in captured.out
    assert "Dispatched (Success)" in captured.out


def test_cli_ingest_file(cli_db_path, tmp_path, capsys):
    """ingest --file reads a valid JSON file and updates SQLite."""
    payload_file = tmp_path / "test_scrape.json"
    data = [
        {
            "domain": "supplier-x.com",
            "canonical_url": "https://supplier-x.com/item/100",
            "sku": "SKU-INGEST-1",
            "product_title": "Product Ingest 1",
            "currency_code": "USD",
            "current_price": "299.9900",
            "availability_status": "IN_STOCK",
        }
    ]
    payload_file.write_text(json.dumps(data), encoding="utf-8")

    exit_code = main(["--db-path", str(cli_db_path), "ingest", "--file", str(payload_file)])
    assert exit_code == 0

    captured = capsys.readouterr()
    assert "FILE INGESTION SUMMARY" in captured.out
    assert "Total Received            : 1" in captured.out
    assert "Inserted (New Entities)   : 1" in captured.out


def test_cli_run_end_to_end(cli_db_path, capsys):
    """run executes simulate -> dispatch -> status in sequence."""
    exit_code = main([
        "--db-path", str(cli_db_path),
        "run",
        "--items", "4",
        "--dry-run",
    ])
    assert exit_code == 0

    captured = capsys.readouterr()
    assert "STARTING END-TO-END FLOW" in captured.out
    assert "SIMULATION INGESTION SUMMARY" in captured.out
    assert "OUTBOX DISPATCH SUMMARY" in captured.out
    assert "EXECUTIVE DASHBOARD" in captured.out
