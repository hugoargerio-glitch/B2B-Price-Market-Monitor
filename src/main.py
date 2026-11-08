"""
CLI Entrypoint for B2B Price & Market Monitor (Profile 1: Lean App/CDC).
Subcommands:
  - simulate [--items N] [--include-quarantine]
  - ingest --file PATH
  - dispatch [--limit N] [--dry-run]
  - status
  - run (end-to-end: simulate -> dispatch -> status)
"""

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import json
import logging
from pathlib import Path
import sys
from typing import Optional

from src.config import DEFAULT_DB_PATH
from src.notifications import WebhookNotifier
from src.outbox import OutboxDispatcher
from src.pipeline import PriceMonitorPipeline
from src.scraper import MarketSimulator
from src.storage import get_connection, init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("b2b_price_monitor.cli")


def cmd_simulate(args: argparse.Namespace) -> int:
    """Run market drift simulation and ingest batch into SQLite pipeline."""
    db_path = getattr(args, "db_path", DEFAULT_DB_PATH)
    items_count = getattr(args, "items", 5)
    include_quarantine = getattr(args, "include_quarantine", False)

    conn = get_connection(db_path)
    init_db(conn)

    print(f"\n🚀 Running Market Drift Simulation ({items_count} items, include_quarantine={include_quarantine})...")
    simulator = MarketSimulator()
    batch = simulator.generate_batch(items_count=items_count, include_quarantine=include_quarantine)

    pipeline = PriceMonitorPipeline(conn)
    summary = pipeline.ingest_batch(batch)
    conn.close()

    print("\n" + "=" * 65)
    print("           SIMULATION INGESTION SUMMARY")
    print("=" * 65)
    print(f"  Total Payloads Received   : {summary.total_received}")
    print(f"  Inserted (New Entities)   : {summary.inserted_new}")
    print(f"  Heartbeats (Zero Change)  : {summary.updated_heartbeat_only}")
    print(f"  State Changes (Mutations) : {summary.updated_state_change}")
    print(f"  Quarantined (DLQ)         : {summary.quarantined}")
    if summary.tripped_circuit_breakers:
        print(f"  ⚠️ Tripped Circuit Breakers: {', '.join(summary.tripped_circuit_breakers)}")
    print("=" * 65 + "\n")

    return 0


def cmd_ingest(args: argparse.Namespace) -> int:
    """Ingest external JSON file containing scrape payloads."""
    db_path = getattr(args, "db_path", DEFAULT_DB_PATH)
    file_path = Path(args.file)

    if not file_path.exists():
        print(f"❌ Error: File not found at '{file_path}'", file=sys.stderr)
        return 1

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = json.load(f)
    except Exception as e:
        print(f"❌ Error parsing JSON file: {e}", file=sys.stderr)
        return 1

    batch = content if isinstance(content, list) else [content]

    conn = get_connection(db_path)
    init_db(conn)

    print(f"\n📥 Ingesting {len(batch)} items from {file_path.name}...")
    pipeline = PriceMonitorPipeline(conn)
    summary = pipeline.ingest_batch(batch)
    conn.close()

    print("\n" + "=" * 65)
    print("             FILE INGESTION SUMMARY")
    print("=" * 65)
    print(f"  Total Received            : {summary.total_received}")
    print(f"  Inserted (New Entities)   : {summary.inserted_new}")
    print(f"  Heartbeats (Zero Change)  : {summary.updated_heartbeat_only}")
    print(f"  State Changes (Mutations) : {summary.updated_state_change}")
    print(f"  Quarantined (DLQ)         : {summary.quarantined}")
    if summary.tripped_circuit_breakers:
        print(f"  ⚠️ Tripped Circuit Breakers: {', '.join(summary.tripped_circuit_breakers)}")
    print("=" * 65 + "\n")

    return 0


def cmd_dispatch(args: argparse.Namespace) -> int:
    """Fetch pending outbox events and dispatch via WebhookNotifier."""
    db_path = getattr(args, "db_path", DEFAULT_DB_PATH)
    limit = getattr(args, "limit", 50)
    dry_run = getattr(args, "dry_run", False)

    conn = get_connection(db_path)
    init_db(conn)

    notifier = WebhookNotifier(dry_run=dry_run)
    dispatcher = OutboxDispatcher(conn)

    print(f"\n📤 Dispatching Outbox Alerts (Limit: {limit}, Platform: {notifier.platform}, Dry-Run: {dry_run})...")
    stats = dispatcher.dispatch_batch(sender=notifier.send, limit=limit)
    conn.close()

    print("\n" + "=" * 65)
    print("             OUTBOX DISPATCH SUMMARY")
    print("=" * 65)
    print(f"  Dispatched (Success)      : {stats['dispatched']}")
    print(f"  Failed (Will Retry)       : {stats['failed']}")
    print(f"  Dead-Letter (Exhausted)   : {stats['dead_letter']}")
    print("=" * 65 + "\n")

    return 0


def cmd_status(args: argparse.Namespace) -> int:
    """Display an executive terminal status dashboard."""
    db_path = getattr(args, "db_path", DEFAULT_DB_PATH)
    conn = get_connection(db_path)
    init_db(conn)

    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*), COUNT(DISTINCT domain) FROM b2b_price_monitor;")
        total_skus, total_domains = cur.fetchone()

        cur.execute(
            """
            SELECT COUNT(*) FROM b2b_price_monitor
            WHERE previous_price IS NOT NULL AND current_price <> previous_price;
            """
        )
        total_changes = cur.fetchone()[0]

        cur.execute(
            """
            SELECT notification_status, COUNT(*)
            FROM price_change_outbox
            GROUP BY notification_status;
            """
        )
        outbox_counts = {r[0]: r[1] for r in cur.fetchall()}

        cur.execute("SELECT COUNT(*) FROM b2b_price_monitor_quarantine;")
        total_quarantine = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM b2b_price_monitor_quarantine WHERE resolution_status = 'UNRESOLVED';")
        unresolved_quarantine = cur.fetchone()[0]

    finally:
        cur.close()
        conn.close()

    dispatched = outbox_counts.get("DISPATCHED", 0)
    pending = outbox_counts.get("PENDING", 0)
    failed = outbox_counts.get("FAILED", 0)
    dead_letter = outbox_counts.get("DEAD_LETTER", 0)

    print("\n" + "=" * 78)
    print("            B2B PRICE & MARKET MONITOR — EXECUTIVE DASHBOARD")
    print("=" * 78)
    print(f"  Database Path      : {db_path}")
    print("  Storage Engine     : SQLite 3 (WAL Mode, synchronous=NORMAL)")
    print("  Execution Profile  : Profile 1 (Lean App / CDC Engine)")
    print("  Contract Spec      : ODCS v3.2.0 (competitor_intelligence)")
    print("-" * 78)
    print("  📊 MONITORED INVENTORY")
    print(f"     • Active Tracked SKUs        : {total_skus}")
    print(f"     • Competitor Domains         : {total_domains}")
    print(f"     • State Drift Mutations      : {total_changes}")
    print("-" * 78)
    print("  📬 TRANSACTIONAL OUTBOX QUEUE")
    print(f"     • Dispatched Alerts          : {dispatched}")
    print(f"     • Pending Outbox Alerts      : {pending}")
    print(f"     • Retrying Failures          : {failed}")
    print(f"     • Dead-Letter (Max Retries)  : {dead_letter}")
    print("-" * 78)
    print("  🛡️ QUARANTINE DLQ (SHIFT-LEFT EDGE VALIDATION)")
    print(f"     • Total Quarantined Events   : {total_quarantine}")
    print(f"     • Unresolved Incidents       : {unresolved_quarantine}")
    print("=" * 78 + "\n")

    return 0


def cmd_run(args: argparse.Namespace) -> int:
    """Execute complete end-to-end flow: simulate -> dispatch -> status."""
    print("==============================================================================")
    print("          STARTING END-TO-END FLOW (SIMULATE -> DISPATCH -> STATUS)           ")
    print("==============================================================================")

    res_sim = cmd_simulate(args)
    if res_sim != 0:
        return res_sim

    res_disp = cmd_dispatch(args)
    if res_disp != 0:
        return res_disp

    return cmd_status(args)


def build_parser() -> argparse.ArgumentParser:
    """Construct command-line interface arguments."""
    parser = argparse.ArgumentParser(
        prog="b2b-price-monitor",
        description="B2B Competitor Price & Market Monitor CLI (ODCS v3.2.0 Profile 1)",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to SQLite database (default: {DEFAULT_DB_PATH})",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")

    # simulate
    sub_sim = subparsers.add_parser("simulate", help="Generate simulated market drift and ingest batch")
    sub_sim.add_argument("--items", "-n", type=int, default=5, help="Number of items to simulate (default: 5)")
    sub_sim.add_argument(
        "--include-quarantine",
        "-q",
        action="store_true",
        help="Include malformed payloads to exercise quarantine DLQ",
    )

    # ingest
    sub_ingest = subparsers.add_parser("ingest", help="Ingest external JSON payload file")
    sub_ingest.add_argument("--file", "-f", type=str, required=True, help="Path to JSON file")

    # dispatch
    sub_dispatch = subparsers.add_parser("dispatch", help="Dispatch pending outbox notifications")
    sub_dispatch.add_argument("--limit", "-l", type=int, default=50, help="Max pending events to dispatch")
    sub_dispatch.add_argument(
        "--dry-run",
        action="store_true",
        help="Log notification payload without sending real HTTP request",
    )

    # status
    subparsers.add_parser("status", help="Display executive status dashboard")

    # run
    sub_run = subparsers.add_parser("run", help="Run end-to-end simulate -> dispatch -> status")
    sub_run.add_argument("--items", "-n", type=int, default=5, help="Number of items to simulate")
    sub_run.add_argument(
        "--include-quarantine",
        "-q",
        action="store_true",
        help="Include malformed payloads in simulation",
    )
    sub_run.add_argument("--limit", "-l", type=int, default=50, help="Max pending events to dispatch")
    sub_run.add_argument(
        "--dry-run",
        action="store_true",
        help="Log notifications without HTTP request",
    )

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    """CLI entrypoint callable."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 0

    commands = {
        "simulate": cmd_simulate,
        "ingest": cmd_ingest,
        "dispatch": cmd_dispatch,
        "status": cmd_status,
        "run": cmd_run,
    }

    handler = commands.get(args.command)
    if not handler:
        parser.print_help()
        return 1

    return handler(args)


if __name__ == "__main__":
    sys.exit(main())
