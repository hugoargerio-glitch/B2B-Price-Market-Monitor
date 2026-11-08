"""
Market Drift Simulator and HTTP Scraper for Profile 1 (Lean App/CDC).
Generates realistic drift scenarios (Price Drop, Price Increase, Stock Outage,
Heartbeat, and Malformed Payload for Quarantine) and provides basic HTTP extraction.
"""

from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import json
import logging
from typing import Any, Optional
import urllib.error
import urllib.parse
import urllib.request

from src.models import AvailabilityStatus, to_decimal_18_4

logger = logging.getLogger("b2b_price_monitor.scraper")

DEFAULT_BASE_CATALOG = [
    {
        "domain": "competitor-a.com.br",
        "canonical_url": "https://competitor-a.com.br/produtos/laptop-workstation-pro-16",
        "sku": "SKU-PROD-98124",
        "product_title": "Laptop Workstation Pro 16 32GB RAM",
        "currency_code": "BRL",
        "current_price": Decimal("2499.9000"),
        "availability_status": AvailabilityStatus.IN_STOCK,
        "promo_badge": None,
    },
    {
        "domain": "competitor-a.com.br",
        "canonical_url": "https://competitor-a.com.br/produtos/monitor-ultrawide-4k-34",
        "sku": "SKU-PROD-44102",
        "product_title": "Monitor UltraWide 4K 34 Polegadas USB-C",
        "currency_code": "BRL",
        "current_price": Decimal("1850.0000"),
        "availability_status": AvailabilityStatus.IN_STOCK,
        "promo_badge": None,
    },
    {
        "domain": "industrial-supplier.com",
        "canonical_url": "https://industrial-supplier.com/parts/industrial-valve-brass-2in",
        "sku": "VALVE-BRASS-02",
        "product_title": "Válvula Esfera Industrial Latão 2 Polegadas",
        "currency_code": "USD",
        "current_price": Decimal("145.7500"),
        "availability_status": AvailabilityStatus.IN_STOCK,
        "promo_badge": None,
    },
    {
        "domain": "industrial-supplier.com",
        "canonical_url": "https://industrial-supplier.com/parts/heavy-duty-pump-hp-50",
        "sku": "PUMP-HD-50HP",
        "product_title": "Bomba Centrífuga Alta Vazão 50 HP Trifásica",
        "currency_code": "USD",
        "current_price": Decimal("3890.5000"),
        "availability_status": AvailabilityStatus.BACKORDER,
        "promo_badge": None,
    },
    {
        "domain": "b2b-direct.com.br",
        "canonical_url": "https://b2b-direct.com.br/materiais/cabo-rede-cat6-305m",
        "sku": "CABO-CAT6-305M",
        "product_title": "Cabo de Rede UTP Cat6 Bobina 305 Metros Cobre",
        "currency_code": "BRL",
        "current_price": Decimal("499.0000"),
        "availability_status": AvailabilityStatus.IN_STOCK,
        "promo_badge": None,
    },
    {
        "domain": "b2b-direct.com.br",
        "canonical_url": "https://b2b-direct.com.br/materiais/switch-poe-24p",
        "sku": "SWITCH-POE-24P",
        "product_title": "Switch Gerenciável 24 Portas PoE Gigabit",
        "currency_code": "BRL",
        "current_price": Decimal("1280.0000"),
        "availability_status": AvailabilityStatus.IN_STOCK,
        "promo_badge": None,
    },
    {
        "domain": "competitor-b.com.br",
        "canonical_url": "https://competitor-b.com.br/hardware/nobreak-senoidal-3kva",
        "sku": "UPS-SENOIDAL-3K",
        "product_title": "Nobreak Senoidal Online Dupla Conversão 3000VA",
        "currency_code": "BRL",
        "current_price": Decimal("3150.0000"),
        "availability_status": AvailabilityStatus.IN_STOCK,
        "promo_badge": None,
    },
]


class MarketSimulator:
    """Generates realistic market drift scenarios for monitored competitor SKUs."""

    def __init__(self, catalog: Optional[list[dict[str, Any]]] = None):
        self.catalog = list(catalog) if catalog else list(DEFAULT_BASE_CATALOG)

    def simulate_price_drop(
        self,
        base_item: dict[str, Any],
        discount_pct: Decimal = Decimal("10.0000"),
        promo_badge: str = "PROMO_DISCOUNT_10",
    ) -> dict[str, Any]:
        """Scenario 1: Price drop (5% to 20% discount with promo badge)."""
        original_price = Decimal(str(base_item["current_price"]))
        multiplier = Decimal("1.0000") - (discount_pct / Decimal("100.0000"))
        new_price = (original_price * multiplier).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

        return {
            "domain": base_item["domain"],
            "canonical_url": base_item["canonical_url"],
            "sku": base_item["sku"],
            "product_title": base_item["product_title"],
            "currency_code": base_item["currency_code"],
            "current_price": new_price,
            "availability_status": (
                base_item["availability_status"].value
                if hasattr(base_item["availability_status"], "value")
                else str(base_item["availability_status"])
            ),
            "promo_badge": promo_badge,
            "scraped_at": datetime.now(timezone.utc),
        }

    def simulate_price_increase(
        self,
        base_item: dict[str, Any],
        increase_pct: Decimal = Decimal("5.0000"),
    ) -> dict[str, Any]:
        """Scenario 2: Price increase (3% to 10%)."""
        original_price = Decimal(str(base_item["current_price"]))
        multiplier = Decimal("1.0000") + (increase_pct / Decimal("100.0000"))
        new_price = (original_price * multiplier).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

        return {
            "domain": base_item["domain"],
            "canonical_url": base_item["canonical_url"],
            "sku": base_item["sku"],
            "product_title": base_item["product_title"],
            "currency_code": base_item["currency_code"],
            "current_price": new_price,
            "availability_status": (
                base_item["availability_status"].value
                if hasattr(base_item["availability_status"], "value")
                else str(base_item["availability_status"])
            ),
            "promo_badge": None,
            "scraped_at": datetime.now(timezone.utc),
        }

    def simulate_stock_outage(self, base_item: dict[str, Any]) -> dict[str, Any]:
        """Scenario 3: Stock outage (OUT_OF_STOCK)."""
        return {
            "domain": base_item["domain"],
            "canonical_url": base_item["canonical_url"],
            "sku": base_item["sku"],
            "product_title": base_item["product_title"],
            "currency_code": base_item["currency_code"],
            "current_price": Decimal(str(base_item["current_price"])),
            "availability_status": AvailabilityStatus.OUT_OF_STOCK.value,
            "promo_badge": base_item.get("promo_badge"),
            "scraped_at": datetime.now(timezone.utc),
        }

    def simulate_heartbeat(self, base_item: dict[str, Any]) -> dict[str, Any]:
        """Scenario 4: Heartbeat with zero price change."""
        return {
            "domain": base_item["domain"],
            "canonical_url": base_item["canonical_url"],
            "sku": base_item["sku"],
            "product_title": base_item["product_title"],
            "currency_code": base_item["currency_code"],
            "current_price": Decimal(str(base_item["current_price"])),
            "availability_status": (
                base_item["availability_status"].value
                if hasattr(base_item["availability_status"], "value")
                else str(base_item["availability_status"])
            ),
            "promo_badge": base_item.get("promo_badge"),
            "scraped_at": datetime.now(timezone.utc),
        }

    def simulate_malformed_payload(
        self,
        base_item: dict[str, Any],
        reason_type: str = "FLOAT_PRICE",
    ) -> dict[str, Any]:
        """Scenario 5: Malformed payload (for testing quarantine routing)."""
        if reason_type == "FLOAT_PRICE":
            return {
                "domain": base_item["domain"],
                "canonical_url": base_item["canonical_url"],
                "sku": base_item["sku"],
                "product_title": base_item["product_title"],
                "currency_code": base_item["currency_code"],
                "current_price": float(base_item["current_price"]),  # Forbidden float
                "availability_status": "IN_STOCK",
            }
        elif reason_type == "MISSING_PRICE":
            return {
                "domain": base_item["domain"],
                "canonical_url": base_item["canonical_url"],
                "sku": base_item["sku"],
                "product_title": base_item["product_title"],
                "currency_code": base_item["currency_code"],
                "current_price": "NOT_A_PRICE",
                "availability_status": "IN_STOCK",
            }
        else:
            return {
                "domain": base_item["domain"],
                "canonical_url": base_item["canonical_url"],
                "sku": base_item["sku"],
                "product_title": base_item["product_title"],
                "currency_code": "INVALID_CODE",
                "current_price": Decimal("-50.0000"),
                "availability_status": "UNKNOWN_STATUS",
            }

    def generate_batch(
        self,
        items_count: int = 5,
        include_quarantine: bool = False,
    ) -> list[dict[str, Any]]:
        """Generate a simulated batch of scraped payloads spanning drift scenarios."""
        batch: list[dict[str, Any]] = []
        catalog_len = len(self.catalog)

        # Ensure reasonable bounds
        total = max(1, items_count)

        for i in range(total):
            base = self.catalog[i % catalog_len]
            scenario_idx = i % 5

            if scenario_idx == 0:
                # Scenario 1: Price drop (10%)
                item = self.simulate_price_drop(base, discount_pct=Decimal("12.5000"), promo_badge="PROMO_FLASH_SALE")
            elif scenario_idx == 1:
                # Scenario 2: Price increase (5%)
                item = self.simulate_price_increase(base, increase_pct=Decimal("6.0000"))
            elif scenario_idx == 2:
                # Scenario 3: Stock outage
                item = self.simulate_stock_outage(base)
            elif scenario_idx == 3:
                # Scenario 4: Heartbeat
                item = self.simulate_heartbeat(base)
            else:
                # Scenario 5
                if include_quarantine:
                    item = self.simulate_malformed_payload(base, reason_type="FLOAT_PRICE")
                else:
                    item = self.simulate_price_drop(base, discount_pct=Decimal("7.5000"), promo_badge="WEEKEND_DEAL")

            batch.append(item)

        # If include_quarantine is requested but wasn't hit by scenario_idx, force the last item to be malformed
        if include_quarantine and not any(isinstance(it.get("current_price"), float) or it.get("currency_code") == "INVALID_CODE" for it in batch):
            batch[-1] = self.simulate_malformed_payload(self.catalog[0], reason_type="FLOAT_PRICE")

        return batch


class HttpScraper:
    """Basic HTTP JSON/HTML fetcher using urllib.request with timeout and User-Agent handling."""

    def __init__(
        self,
        user_agent: str = "B2B-Price-Monitor/1.0 (+https://b2b-monitor.internal/crawler)",
        timeout: int = 10,
    ):
        self.user_agent = user_agent
        self.timeout = timeout

    def fetch_url(self, url: str, headers: Optional[dict[str, str]] = None) -> str:
        """Fetch content from an HTTP/HTTPS URL returning decoded UTF-8 string."""
        req_headers = {
            "User-Agent": self.user_agent,
            "Accept": "application/json, text/html, */*",
        }
        if headers:
            req_headers.update(headers)

        req = urllib.request.Request(url, headers=req_headers, method="GET")
        with urllib.request.urlopen(req, timeout=self.timeout) as response:
            return response.read().decode("utf-8")

    def normalize_payload(self, raw_dict: dict[str, Any], url: str = "") -> dict[str, Any]:
        """Normalize parsed dictionary into a structure compatible with ScrapePayload."""
        parsed_url = urllib.parse.urlparse(url)
        domain = raw_dict.get("domain") or parsed_url.netloc or "unknown-domain.com"
        canonical_url = raw_dict.get("canonical_url") or url or f"https://{domain}/product"
        sku = raw_dict.get("sku") or raw_dict.get("id") or "UNKNOWN_SKU"
        title = raw_dict.get("product_title") or raw_dict.get("title") or sku
        currency = raw_dict.get("currency_code") or raw_dict.get("currency") or "BRL"
        raw_price = raw_dict.get("current_price") or raw_dict.get("price") or "0.00"

        # Guard: convert to Decimal
        try:
            price = to_decimal_18_4(raw_price)
        except Exception:
            price = raw_price  # Pass through to trigger quarantine if malformed

        raw_avail = raw_dict.get("availability_status") or raw_dict.get("availability") or "IN_STOCK"

        return {
            "domain": domain,
            "canonical_url": canonical_url,
            "sku": sku,
            "product_title": title,
            "currency_code": currency,
            "current_price": price,
            "availability_status": raw_avail,
            "promo_badge": raw_dict.get("promo_badge"),
            "scraped_at": datetime.now(timezone.utc),
        }

    def scrape_json(self, url: str) -> dict[str, Any]:
        """Fetch JSON endpoint and return normalized payload dict."""
        body = self.fetch_url(url, headers={"Accept": "application/json"})
        data = json.loads(body)
        if isinstance(data, list) and len(data) > 0:
            data = data[0]
        return self.normalize_payload(data, url=url)
