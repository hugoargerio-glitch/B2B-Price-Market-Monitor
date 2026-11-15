"""
Unit and integration tests for SQLite seed script in src/seed.py.
Verifies programmatic seed_database execution and CLI invocation (python -m src.seed).
"""

from decimal import Decimal
import subprocess
import sys
import pytest

from src.seed import SEED_PRODUCTS, SEED_QUARANTINE, build_parser, seed_database
from src.storage import get_connection


def test_seed_database_programmatic(tmp_path):
    """seed_database initializes tables and populates verified seed catalog."""
    db_file = tmp_path / "seed_test.db"
    assert not db_file.exists()

    seed_database(db_file)
    assert db_file.exists()

    conn = get_connection(db_file)
    cur = conn.cursor()

    # Verify canonical tables populated
    cur.execute("SELECT COUNT(*) FROM b2b_price_monitor;")
    bpm_count = cur.fetchone()[0]
    assert bpm_count == len(SEED_PRODUCTS)
    assert bpm_count >= 5

    cur.execute("SELECT COUNT(*) FROM price_change_outbox;")
    outbox_count = cur.fetchone()[0]
    assert outbox_count == len(SEED_PRODUCTS)

    cur.execute("SELECT COUNT(*) FROM b2b_price_monitor_quarantine;")
    quarantine_count = cur.fetchone()[0]
    assert quarantine_count == len(SEED_QUARANTINE)
    assert quarantine_count >= 1

    # Verify exact SKUs present
    cur.execute("SELECT sku FROM b2b_price_monitor;")
    skus_in_db = {r[0] for r in cur.fetchall()}
    for product in SEED_PRODUCTS:
        assert product["sku"] in skus_in_db

    # Verify monetary decimal physical text storage
    cur.execute(
        """
        SELECT typeof(current_price), typeof(current_price_cents)
        FROM b2b_price_monitor
        LIMIT 1;
        """
    )
    p_type, cents_type = cur.fetchone()
    assert p_type == "text"
    assert cents_type == "integer"

    conn.close()


def test_seed_database_cli_invocation(tmp_path):
    """CLI execution via python -m src.seed --db-path seeds target database cleanly."""
    cli_db = tmp_path / "seed_cli.db"
    assert not cli_db.exists()

    cmd = [
        sys.executable,
        "-m",
        "src.seed",
        "--db-path",
        str(cli_db),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    assert res.returncode == 0
    assert "Successfully seeded" in res.stdout
    assert cli_db.exists()

    conn = get_connection(cli_db)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM b2b_price_monitor;")
    assert cur.fetchone()[0] == len(SEED_PRODUCTS)
    conn.close()


def test_seed_build_parser():
    """build_parser correctly parses --db-path argument."""
    parser = build_parser()
    args = parser.parse_args(["--db-path", "/tmp/custom_seed.db"])
    assert args.db_path == "/tmp/custom_seed.db"
