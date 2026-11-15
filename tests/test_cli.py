"""
Integration tests for CLI entrypoint in src/main.py.
Verifies CLI subcommands: simulate, status, dispatch (--dry-run), ingest, and run,
plus B2B_DB_PATH env resolution and non-blocking cmd_status execution.
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


def test_cli_status_non_blocking_uninitialized(tmp_path, capsys):
    """AUD-008: cmd_status must exit cleanly without creating DB file or acquiring write locks."""
    non_existent = tmp_path / "never_created.db"
    assert not non_existent.exists()

    exit_code = main(["--db-path", str(non_existent), "status"])
    assert exit_code == 0

    captured = capsys.readouterr()
    assert "does not exist yet" in captured.out
    # DB file should still not exist!
    assert not non_existent.exists()


def test_cli_b2b_db_path_env_var(cli_db_path, monkeypatch, capsys):
    """AUD-002: CLI defaults --db-path dynamically from B2B_DB_PATH when omitted."""
    # Seed data
    main(["--db-path", str(cli_db_path), "simulate", "--items", "2"])

    monkeypatch.setenv("B2B_DB_PATH", str(cli_db_path))
    exit_code = main(["status"])
    assert exit_code == 0

    captured = capsys.readouterr()
    assert "B2B PRICE & MARKET MONITOR — EXECUTIVE DASHBOARD" in captured.out
    assert "Active Tracked SKUs" in captured.out


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


def test_cli_clean_outbox_and_vacuum(cli_db_path, capsys):
    """clean-outbox and vacuum CLI subcommands execute cleanly."""
    exit_clean = main(["--db-path", str(cli_db_path), "clean-outbox", "--days", "30"])
    assert exit_clean == 0
    captured_clean = capsys.readouterr()
    assert "Purged" in captured_clean.out

    exit_vac = main(["--db-path", str(cli_db_path), "vacuum"])
    assert exit_vac == 0
    captured_vac = capsys.readouterr()
    assert "PRAGMA wal_checkpoint(TRUNCATE)" in captured_vac.out


@pytest.mark.parametrize("subcommand", ["status", "vacuum"])
def test_cli_db_path_ordering_before_and_after_subcommand(cli_db_path, subcommand, capsys):
    """AUD-025: --db-path works identically whether passed before or after subcommand verb."""
    # 1. Passed BEFORE subcommand: `main(["--db-path", path, verb])`
    exit_before = main(["--db-path", str(cli_db_path), subcommand])
    assert exit_before == 0
    captured_before = capsys.readouterr()

    # 2. Passed AFTER subcommand: `main([verb, "--db-path", path])`
    exit_after = main([subcommand, "--db-path", str(cli_db_path)])
    assert exit_after == 0
    captured_after = capsys.readouterr()

    if subcommand == "status":
        assert "EXECUTIVE DASHBOARD" in captured_before.out
        assert "EXECUTIVE DASHBOARD" in captured_after.out
    elif subcommand == "vacuum":
        assert "PRAGMA wal_checkpoint" in captured_before.out
        assert "PRAGMA wal_checkpoint" in captured_after.out


def test_cli_db_path_ordering_with_flags_after_subcommand(cli_db_path, capsys):
    """AUD-025: --db-path after subcommand with flags: simulate and clean-outbox."""
    # simulate with --db-path after verb
    exit_sim = main(["simulate", "--db-path", str(cli_db_path), "--items", "2"])
    assert exit_sim == 0
    captured_sim = capsys.readouterr()
    assert "SIMULATION INGESTION SUMMARY" in captured_sim.out

    # clean-outbox with --db-path after verb
    exit_clean = main(["clean-outbox", "--days", "30", "--db-path", str(cli_db_path)])
    assert exit_clean == 0
    captured_clean = capsys.readouterr()
    assert "Purged" in captured_clean.out

