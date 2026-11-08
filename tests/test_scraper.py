"""
Unit tests for Market Drift Simulator and HTTP Scraper in src/scraper.py.
Verifies realistic drift scenarios (Price Drop, Price Increase, Stock Outage,
Heartbeat, Malformed Payloads) and HttpScraper normalization.
"""

from decimal import Decimal
import json
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from src.models import AvailabilityStatus, ScrapePayload
from src.scraper import DEFAULT_BASE_CATALOG, HttpScraper, MarketSimulator


@pytest.fixture
def base_product():
    return {
        "domain": "competitor-a.com.br",
        "canonical_url": "https://competitor-a.com.br/produtos/laptop-workstation-pro-16",
        "sku": "SKU-PROD-98124",
        "product_title": "Laptop Workstation Pro 16",
        "currency_code": "BRL",
        "current_price": Decimal("2500.0000"),
        "availability_status": AvailabilityStatus.IN_STOCK,
        "promo_badge": None,
    }


def test_simulate_price_drop(base_product):
    """Price drop calculates discount strictly via Decimal and attaches promo badge."""
    sim = MarketSimulator()
    res = sim.simulate_price_drop(base_product, discount_pct=Decimal("20.0000"), promo_badge="FLASH_20")

    assert isinstance(res["current_price"], Decimal)
    assert res["current_price"] == Decimal("2000.0000")
    assert res["promo_badge"] == "FLASH_20"

    # Validates against ScrapePayload
    payload = ScrapePayload.model_validate(res)
    assert payload.current_price == Decimal("2000.0000")


def test_simulate_price_increase(base_product):
    """Price increase calculates increase via Decimal and leaves promo_badge None."""
    sim = MarketSimulator()
    res = sim.simulate_price_increase(base_product, increase_pct=Decimal("10.0000"))

    assert isinstance(res["current_price"], Decimal)
    assert res["current_price"] == Decimal("2750.0000")
    assert res["promo_badge"] is None

    payload = ScrapePayload.model_validate(res)
    assert payload.current_price == Decimal("2750.0000")


def test_simulate_stock_outage(base_product):
    """Stock outage scenario mutates status to OUT_OF_STOCK."""
    sim = MarketSimulator()
    res = sim.simulate_stock_outage(base_product)

    assert res["availability_status"] == AvailabilityStatus.OUT_OF_STOCK.value
    payload = ScrapePayload.model_validate(res)
    assert payload.availability_status == AvailabilityStatus.OUT_OF_STOCK


def test_simulate_heartbeat(base_product):
    """Heartbeat retains exact price, currency, and availability."""
    sim = MarketSimulator()
    res = sim.simulate_heartbeat(base_product)

    assert res["current_price"] == Decimal("2500.0000")
    assert res["availability_status"] == base_product["availability_status"].value
    payload = ScrapePayload.model_validate(res)
    assert payload.current_price == Decimal("2500.0000")


def test_simulate_malformed_payload(base_product):
    """Malformed payload triggers validation error on ScrapePayload."""
    sim = MarketSimulator()
    malformed = sim.simulate_malformed_payload(base_product, reason_type="FLOAT_PRICE")

    assert isinstance(malformed["current_price"], float)
    with pytest.raises(ValidationError):
        ScrapePayload.model_validate(malformed)


def test_generate_batch_clean():
    """Batch without quarantine must have 100% valid ScrapePayload items."""
    sim = MarketSimulator()
    batch = sim.generate_batch(items_count=10, include_quarantine=False)

    assert len(batch) == 10
    for item in batch:
        payload = ScrapePayload.model_validate(item)
        assert isinstance(payload.current_price, Decimal)
        assert payload.current_price > Decimal("0")


def test_generate_batch_with_quarantine():
    """Batch with quarantine must include at least one malformed item."""
    sim = MarketSimulator()
    batch = sim.generate_batch(items_count=10, include_quarantine=True)

    assert len(batch) == 10
    malformed_count = 0
    for item in batch:
        try:
            ScrapePayload.model_validate(item)
        except Exception:
            malformed_count += 1

    assert malformed_count >= 1


def test_http_scraper_normalize_payload():
    """HttpScraper normalizes price strings to Decimal without floating-point errors."""
    scraper = HttpScraper()
    raw = {
        "domain": "competitor.com",
        "canonical_url": "https://competitor.com/prod/1",
        "sku": "SKU-HTTP-1",
        "product_title": "Product Title",
        "currency_code": "BRL",
        "current_price": "199.9900",
        "availability_status": "IN_STOCK",
    }
    normalized = scraper.normalize_payload(raw)
    assert isinstance(normalized["current_price"], Decimal)
    assert normalized["current_price"] == Decimal("199.9900")
    assert normalized["sku"] == "SKU-HTTP-1"


def test_http_scraper_scrape_json():
    """HttpScraper fetches URL and returns normalized payload."""
    scraper = HttpScraper()
    mock_json = json.dumps(
        {
            "domain": "test-competitor.com",
            "canonical_url": "https://test-competitor.com/item/42",
            "sku": "SKU-42",
            "product_title": "Test Item 42",
            "currency_code": "USD",
            "current_price": "89.50",
            "availability_status": "IN_STOCK",
        }
    )

    mock_resp = MagicMock()
    mock_resp.read.return_value = mock_json.encode("utf-8")
    mock_resp.__enter__.return_value = mock_resp

    with patch("urllib.request.urlopen", return_value=mock_resp):
        payload = scraper.scrape_json("https://test-competitor.com/api/product/42")
        assert payload["sku"] == "SKU-42"
        assert payload["current_price"] == Decimal("89.5000")
        assert payload["domain"] == "test-competitor.com"
