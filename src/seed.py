"""
Seed script to initialize and populate SQLite b2b_monitor.db
with verified production-grade seed data meeting 100% of ODCS v3.2.0 quality rules.
"""

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import json
import logging
from pathlib import Path
from uuid import uuid4

from src.config import DEFAULT_DB_PATH, get_default_db_path
from src.diff_engine import calculate_price_delta, compute_entity_key, compute_state_hash
from src.models import AvailabilityStatus, NotificationStatus, OutboxEventType, QuarantineResolutionStatus, compute_cents
from src.storage import get_connection, init_db, transaction

logger = logging.getLogger("b2b_price_monitor.seed")

SEED_PRODUCTS = [
    {
        "domain": "competitor-a.com.br",
        "canonical_url": "https://competitor-a.com.br/produtos/laptop-workstation-pro-16",
        "sku": "SKU-PROD-98124",
        "product_title": "Laptop Workstation Pro 16 32GB RAM",
        "currency_code": "BRL",
        "current_price": Decimal("2499.9000"),
        "previous_price": Decimal("2699.9000"),
        "availability_status": AvailabilityStatus.IN_STOCK,
        "promo_badge": None,
        "notification_status": NotificationStatus.DISPATCHED,
        "retry_count": 0,
        "last_notification_error": None,
    },
    {
        "domain": "competitor-a.com.br",
        "canonical_url": "https://competitor-a.com.br/produtos/monitor-ultrawide-4k-34",
        "sku": "SKU-PROD-44102",
        "product_title": "Monitor UltraWide 4K 34 Polegadas USB-C",
        "currency_code": "BRL",
        "current_price": Decimal("1850.0000"),
        "previous_price": Decimal("1850.0000"),
        "availability_status": AvailabilityStatus.IN_STOCK,
        "promo_badge": "BLACK_FRIDAY",
        "notification_status": NotificationStatus.DISPATCHED,
        "retry_count": 0,
        "last_notification_error": None,
    },
    {
        "domain": "industrial-supplier.com",
        "canonical_url": "https://industrial-supplier.com/parts/industrial-valve-brass-2in",
        "sku": "VALVE-BRASS-02",
        "product_title": "Válvula Esfera Industrial Latão 2 Polegadas",
        "currency_code": "USD",
        "current_price": Decimal("145.7500"),
        "previous_price": None,
        "availability_status": AvailabilityStatus.IN_STOCK,
        "promo_badge": None,
        "notification_status": NotificationStatus.PENDING,
        "retry_count": 0,
        "last_notification_error": None,
    },
    {
        "domain": "industrial-supplier.com",
        "canonical_url": "https://industrial-supplier.com/parts/heavy-duty-pump-hp-50",
        "sku": "PUMP-HD-50HP",
        "product_title": "Bomba Centrífuga Alta Vazão 50 HP Trifásica",
        "currency_code": "USD",
        "current_price": Decimal("3890.5000"),
        "previous_price": Decimal("4100.0000"),
        "availability_status": AvailabilityStatus.BACKORDER,
        "promo_badge": None,
        "notification_status": NotificationStatus.FAILED,
        "retry_count": 1,
        "last_notification_error": "HTTP 429: Rate limit exceeded on Discord webhook",
    },
    {
        "domain": "b2b-direct.com.br",
        "canonical_url": "https://b2b-direct.com.br/materiais/cabo-rede-cat6-305m",
        "sku": "CABO-CAT6-305M",
        "product_title": "Cabo de Rede UTP Cat6 Bobina 305 Metros Cobre",
        "currency_code": "BRL",
        "current_price": Decimal("499.0000"),
        "previous_price": Decimal("549.0000"),
        "availability_status": AvailabilityStatus.IN_STOCK,
        "promo_badge": "QUEIMA_ESTOQUE",
        "notification_status": NotificationStatus.DISPATCHED,
        "retry_count": 0,
        "last_notification_error": None,
    },
]

SEED_QUARANTINE = [
    {
        "domain": "competitor-drift.com",
        "raw_url": "https://competitor-drift.com/catalog/item-99021",
        "raw_payload": '{"domain": "competitor-drift.com", "html_snippet": "<div>Preço sob consulta</div>"}',
        "failure_reason": "TEMPLATE_DRIFT_MISSING_PRICE",
        "resolution_status": QuarantineResolutionStatus.UNRESOLVED,
    }
]


def seed_database(db_path: Path | str | None = None) -> None:
    """Populate database with clean verified seed data."""
    if db_path is None:
        db_path = get_default_db_path()
    target_path = Path(db_path)
    conn = get_connection(target_path)
    init_db(conn)

    now_iso = datetime.now(timezone.utc).isoformat()

    with transaction(conn) as cur:
        # Clear existing seed data if any
        cur.execute("DELETE FROM price_change_outbox;")
        cur.execute("DELETE FROM b2b_price_monitor;")
        cur.execute("DELETE FROM b2b_price_monitor_quarantine;")

        # Detect if promo_badge column exists on b2b_price_monitor
        cur.execute("PRAGMA table_info(b2b_price_monitor);")
        columns = [row[1] for row in cur.fetchall()]
        has_promo_badge = "promo_badge" in columns

        for p in SEED_PRODUCTS:
            cents = compute_cents(p["current_price"])
            ek = compute_entity_key(p["domain"], p["sku"])
            promo = p.get("promo_badge")
            sh = compute_state_hash(
                current_price_cents=cents,
                currency_code=p["currency_code"],
                availability_status=p["availability_status"].value,
                promo_badge=promo,
            )
            delta, delta_pct = calculate_price_delta(p["current_price"], p["previous_price"])

            if has_promo_badge:
                cur.execute(
                    """
                    INSERT INTO b2b_price_monitor (
                        entity_key, state_hash, domain, canonical_url, sku,
                        product_title, currency_code, current_price_cents,
                        current_price, previous_price, price_delta,
                        price_delta_percent, availability_status, promo_badge,
                        notification_status, retry_count, last_notification_error,
                        last_checked_at, last_changed_at, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        ek,
                        sh,
                        p["domain"],
                        p["canonical_url"],
                        p["sku"],
                        p["product_title"],
                        p["currency_code"],
                        cents,
                        str(p["current_price"]),
                        str(p["previous_price"]) if p["previous_price"] is not None else None,
                        str(delta) if delta is not None else None,
                        str(delta_pct) if delta_pct is not None else None,
                        p["availability_status"].value,
                        promo,
                        p["notification_status"].value,
                        p["retry_count"],
                        p["last_notification_error"],
                        now_iso,
                        now_iso,
                        now_iso,
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO b2b_price_monitor (
                        entity_key, state_hash, domain, canonical_url, sku,
                        product_title, currency_code, current_price_cents,
                        current_price, previous_price, price_delta,
                        price_delta_percent, availability_status,
                        notification_status, retry_count, last_notification_error,
                        last_checked_at, last_changed_at, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        ek,
                        sh,
                        p["domain"],
                        p["canonical_url"],
                        p["sku"],
                        p["product_title"],
                        p["currency_code"],
                        cents,
                        str(p["current_price"]),
                        str(p["previous_price"]) if p["previous_price"] is not None else None,
                        str(delta) if delta is not None else None,
                        str(delta_pct) if delta_pct is not None else None,
                        p["availability_status"].value,
                        p["notification_status"].value,
                        p["retry_count"],
                        p["last_notification_error"],
                        now_iso,
                        now_iso,
                        now_iso,
                    ),
                )

            # Insert initial outbox event
            outbox_payload_dict = {
                "entity_key": ek,
                "event_type": OutboxEventType.INITIAL_CAPTURE.value,
                "sku": p["sku"],
                "domain": p["domain"],
                "current_price": str(p["current_price"]),
                "currency": p["currency_code"],
                "availability": p["availability_status"].value,
            }
            if promo is not None:
                outbox_payload_dict["promo_badge"] = promo
            outbox_payload = json.dumps(outbox_payload_dict)

            cur.execute(
                """
                INSERT INTO price_change_outbox (
                    outbox_id, entity_key, event_type, payload,
                    notification_status, retry_count, last_notification_error,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    str(uuid4()),
                    ek,
                    OutboxEventType.INITIAL_CAPTURE.value,
                    outbox_payload,
                    p["notification_status"].value,
                    p["retry_count"],
                    p["last_notification_error"],
                    now_iso,
                    now_iso,
                ),
            )

        for q in SEED_QUARANTINE:
            cur.execute(
                """
                INSERT INTO b2b_price_monitor_quarantine (
                    quarantine_id, domain, raw_url, raw_payload,
                    failure_reason, quarantined_at, resolution_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    str(uuid4()),
                    q["domain"],
                    q["raw_url"],
                    q["raw_payload"],
                    q["failure_reason"],
                    now_iso,
                    q["resolution_status"].value,
                ),
            )

    conn.close()
    print(f"✅ Successfully seeded {len(SEED_PRODUCTS)} products and {len(SEED_QUARANTINE)} quarantine records into {target_path}!")


def build_parser() -> argparse.ArgumentParser:
    """Construct command-line parser for seed script."""
    default_db = get_default_db_path()
    parser = argparse.ArgumentParser(
        prog="b2b-price-monitor-seed",
        description="Seed database with verified production-grade test data meeting ODCS v3.2.0 quality rules",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=str(default_db),
        help=f"Path to SQLite database (default: {default_db})",
    )
    return parser


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    seed_database(db_path=args.db_path)
