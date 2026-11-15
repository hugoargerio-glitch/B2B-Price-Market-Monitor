"""
Unit tests for Webhook Notifier in src/notifications.py.
Verifies Discord embeds (colors for drop/increase/availability), Slack Block Kit,
console fallback, platform auto-detection, B2B_WEBHOOK_URL env resolution,
and dispatch success/failure handling.
"""

from datetime import datetime, timezone
from decimal import Decimal
import json
import os
from unittest.mock import MagicMock, patch
import urllib.error

import pytest

from src.models import NotificationStatus, OutboxEventType, PriceChangeOutbox
from src.notifications import (
    DISCORD_COLOR_AMBER,
    DISCORD_COLOR_BLUE,
    DISCORD_COLOR_GREEN,
    DISCORD_COLOR_RED,
    WebhookNotifier,
)


@pytest.fixture
def sample_outbox_event():
    payload_data = {
        "entity_key": "a" * 64,
        "event_type": OutboxEventType.PRICE_CHANGED.value,
        "sku": "SKU-PROD-98124",
        "domain": "competitor-a.com.br",
        "canonical_url": "https://competitor-a.com.br/produtos/laptop-workstation-pro-16",
        "product_title": "Laptop Workstation Pro 16",
        "previous_price": "2699.9000",
        "current_price": "2499.9000",
        "price_delta": "-200.0000",
        "price_delta_percent": "-7.4077",
        "currency": "BRL",
        "availability": "IN_STOCK",
        "promo_badge": "FRETE_GRATIS",
    }
    return PriceChangeOutbox(
        outbox_id="test-outbox-uuid-1",
        entity_key="a" * 64,
        event_type=OutboxEventType.PRICE_CHANGED,
        payload=json.dumps(payload_data),
        notification_status=NotificationStatus.PENDING,
        created_at=datetime(2026, 11, 8, 12, 0, 0, tzinfo=timezone.utc),
    )


def test_platform_auto_detection():
    """Auto-detects discord from discord.com, slack from hooks.slack.com, and console otherwise."""
    notifier_discord = WebhookNotifier(webhook_url="https://discord.com/api/webhooks/123/abc")
    assert notifier_discord.platform == "discord"

    notifier_slack = WebhookNotifier(webhook_url="https://hooks.slack.com/services/T00/B00/X00")
    assert notifier_slack.platform == "slack"

    notifier_console = WebhookNotifier(webhook_url="")
    assert notifier_console.platform == "console"

    notifier_mock = WebhookNotifier(webhook_url="mock://console")
    assert notifier_mock.platform == "console"

    # Explicit override takes precedence
    notifier_override = WebhookNotifier(
        webhook_url="https://discord.com/api/webhooks/123/abc",
        platform="console",
    )
    assert notifier_override.platform == "console"


def test_b2b_webhook_url_env_resolution(monkeypatch):
    """AUD-003: WebhookNotifier resolves B2B_WEBHOOK_URL, then ALERT_WEBHOOK_URL fallback."""
    # When B2B_WEBHOOK_URL is set
    monkeypatch.setenv("B2B_WEBHOOK_URL", "https://discord.com/api/webhooks/b2b/test")
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    notifier1 = WebhookNotifier()
    assert notifier1.webhook_url == "https://discord.com/api/webhooks/b2b/test"
    assert notifier1.platform == "discord"

    # When B2B_WEBHOOK_URL is empty, fall back to ALERT_WEBHOOK_URL
    monkeypatch.delenv("B2B_WEBHOOK_URL", raising=False)
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://hooks.slack.com/services/alert/test")
    notifier2 = WebhookNotifier()
    assert notifier2.webhook_url == "https://hooks.slack.com/services/alert/test"
    assert notifier2.platform == "slack"

    # Explicit webhook_url override takes highest precedence
    notifier3 = WebhookNotifier(webhook_url="https://discord.com/api/webhooks/explicit")
    assert notifier3.webhook_url == "https://discord.com/api/webhooks/explicit"


def test_discord_formatting_price_decrease(sample_outbox_event):
    """PRICE_DECREASE (opportunity) must use GREEN color and include key fields."""
    notifier = WebhookNotifier(platform="discord")
    card = notifier.format_discord(sample_outbox_event)

    assert "embeds" in card
    embed = card["embeds"][0]
    assert embed["color"] == DISCORD_COLOR_GREEN
    assert "PRICE DROP" in embed["title"]
    assert embed["url"] == "https://competitor-a.com.br/produtos/laptop-workstation-pro-16"

    fields = {f["name"]: f["value"] for f in embed["fields"]}
    assert fields["SKU"] == "`SKU-PROD-98124`"
    assert fields["Competitor"] == "competitor-a.com.br"
    assert "2699.9000" in fields["Old Price"]
    assert "2499.9000" in fields["New Price"]
    assert "-200.0000" in fields["Delta ($ / %)"]


def test_discord_formatting_price_increase():
    """PRICE_INCREASE must use RED color."""
    payload_data = {
        "sku": "SKU-PROD-98124",
        "domain": "competitor-a.com.br",
        "previous_price": "2000.0000",
        "current_price": "2200.0000",
        "price_delta": "200.0000",
        "price_delta_percent": "10.0000",
        "event_type": OutboxEventType.PRICE_CHANGED.value,
    }
    event = PriceChangeOutbox(
        outbox_id="test-2",
        entity_key="b" * 64,
        event_type=OutboxEventType.PRICE_CHANGED,
        payload=json.dumps(payload_data),
    )
    notifier = WebhookNotifier(platform="discord")
    card = notifier.format_discord(event)
    assert card["embeds"][0]["color"] == DISCORD_COLOR_RED
    assert "PRICE INCREASE" in card["embeds"][0]["title"]


def test_discord_formatting_availability_change():
    """AVAILABILITY_CHANGED must use AMBER color."""
    payload_data = {
        "sku": "SKU-PROD-98124",
        "domain": "competitor-a.com.br",
        "current_price": "2000.0000",
        "availability": "OUT_OF_STOCK",
        "event_type": OutboxEventType.AVAILABILITY_CHANGED.value,
    }
    event = PriceChangeOutbox(
        outbox_id="test-3",
        entity_key="c" * 64,
        event_type=OutboxEventType.AVAILABILITY_CHANGED,
        payload=json.dumps(payload_data),
    )
    notifier = WebhookNotifier(platform="discord")
    card = notifier.format_discord(event)
    assert card["embeds"][0]["color"] == DISCORD_COLOR_AMBER
    assert "STOCK STATUS CHANGED" in card["embeds"][0]["title"]


def test_slack_formatting(sample_outbox_event):
    """Slack format must produce blocks with header, fields, and mrkdwn links."""
    notifier = WebhookNotifier(platform="slack")
    data = notifier.format_slack(sample_outbox_event)

    assert "blocks" in data
    blocks = data["blocks"]
    assert any(b.get("type") == "header" for b in blocks)
    section = next(b for b in blocks if b.get("type") == "section" and "fields" in b)
    field_texts = [f["text"] for f in section["fields"]]

    assert any("SKU-PROD-98124" in t for t in field_texts)
    assert any("competitor-a.com.br" in t for t in field_texts)


def test_console_formatting_ascii_box(sample_outbox_event):
    """Console mock produces bordered ASCII box without exceptions."""
    notifier = WebhookNotifier(platform="console")
    ascii_card = notifier.format_console(sample_outbox_event)

    assert "+----------------" in ascii_card
    assert "SKU-PROD-98124" in ascii_card
    assert "competitor-a.com.br" in ascii_card
    assert "BRL 2499.9000" in ascii_card


def test_send_console_fallback_succeeds(sample_outbox_event, capsys):
    """Console / mock mode logs and returns True without network requests."""
    notifier = WebhookNotifier(platform="console", dry_run=True)
    success = notifier.send(sample_outbox_event)
    assert success is True

    captured = capsys.readouterr()
    assert "SKU-PROD-98124" in captured.out


def test_send_http_success(sample_outbox_event):
    """HTTP 200/204 response from webhook returns True."""
    notifier = WebhookNotifier(
        webhook_url="https://discord.com/api/webhooks/test/123",
        platform="discord",
    )

    mock_resp = MagicMock()
    mock_resp.getcode.return_value = 204
    mock_resp.__enter__.return_value = mock_resp

    with patch("urllib.request.urlopen", return_value=mock_resp):
        success = notifier.send(sample_outbox_event)
        assert success is True


def test_send_http_failure_handling(sample_outbox_event):
    """HTTP 429 Rate Limit error is handled cleanly, returning False and logging error."""
    notifier = WebhookNotifier(
        webhook_url="https://discord.com/api/webhooks/test/123",
        platform="discord",
    )

    http_error = urllib.error.HTTPError(
        url="https://discord.com/api/webhooks/test/123",
        code=429,
        msg="Too Many Requests",
        hdrs={},
        fp=None,
    )

    with patch("urllib.request.urlopen", side_effect=http_error):
        success = notifier.send(sample_outbox_event)
        assert success is False
        assert notifier.last_error is not None
        assert "429" in notifier.last_error


def test_send_url_error_handling(sample_outbox_event):
    """Network connection failure (URLError) returns False."""
    notifier = WebhookNotifier(
        webhook_url="https://discord.com/api/webhooks/test/123",
        platform="discord",
    )

    url_error = urllib.error.URLError("Connection refused")
    with patch("urllib.request.urlopen", side_effect=url_error):
        success = notifier.send(sample_outbox_event)
        assert success is False
        assert "Connection refused" in notifier.last_error


@pytest.mark.parametrize(
    "ssrf_target",
    [
        "file:///etc/passwd",
        "http://127.0.0.1:9000/webhook",
        "http://localhost:8080/hook",
        "http://169.254.169.254/latest/meta-data/",
        "http://10.0.0.5:5000/alert",
        "http://192.168.0.100/notify",
    ],
)
def test_webhook_notifier_ssrf_blocking(sample_outbox_event, ssrf_target):
    """AUD-015: WebhookNotifier blocks file:// and private/loopback targets before dispatch."""
    notifier = WebhookNotifier(webhook_url=ssrf_target, platform="discord")
    try:
        success = notifier.send(sample_outbox_event)
        assert success is False
        assert notifier.last_error is not None
        assert any(
            term in notifier.last_error.lower()
            for term in ["ssrf", "private", "loopback", "blocked", "scheme", "http", "address"]
        )
    except ValueError as exc:
        assert any(
            term in str(exc).lower()
            for term in ["ssrf", "private", "loopback", "blocked", "scheme", "http", "address"]
        )


def test_webhook_notifier_promo_changed_formatting():
    """AUD-024: WebhookNotifier handles OutboxEventType.PROMO_CHANGED explicitly without fallback."""
    payload_data = {
        "sku": "SKU-PROMO-99",
        "domain": "deal-site.com.br",
        "canonical_url": "https://deal-site.com.br/deal-item",
        "product_title": "Promo Product",
        "current_price": "99.9000",
        "previous_price": "99.9000",
        "price_delta": "0.0000",
        "price_delta_percent": "0.0000",
        "currency": "BRL",
        "availability": "IN_STOCK",
        "promo_badge": "BLACK_FRIDAY_SPECIAL",
        "event_type": OutboxEventType.PROMO_CHANGED.value,
    }
    event = PriceChangeOutbox(
        outbox_id="test-promo-id",
        entity_key="p" * 64,
        event_type=OutboxEventType.PROMO_CHANGED,
        payload=json.dumps(payload_data),
        notification_status=NotificationStatus.PENDING,
    )

    notifier = WebhookNotifier(platform="discord")
    discord_card = notifier.format_discord(event)
    assert "PROMO" in discord_card["embeds"][0]["title"]
    assert "NEW PRODUCT MONITORED" not in discord_card["embeds"][0]["title"]

    notifier_slack = WebhookNotifier(platform="slack")
    slack_data = notifier_slack.format_slack(event)
    assert "PROMO" in slack_data["text"]
    assert "NEW PRODUCT MONITORED" not in slack_data["text"]

    notifier_console = WebhookNotifier(platform="console")
    console_text = notifier_console.format_console(event)
    assert "deal-site.com.br" in console_text
    assert "SKU-PROMO-99" in console_text


def test_webhook_notifier_price_changed_null_delta_fallback():
    """AUD-024: PRICE_CHANGED with price_delta=None must not fall through to NEW PRODUCT MONITORED."""
    payload_data = {
        "sku": "SKU-NULL-DELTA",
        "domain": "competitor-x.com",
        "canonical_url": "https://competitor-x.com/item",
        "product_title": "Null Delta Item",
        "current_price": "500.0000",
        "previous_price": None,
        "price_delta": None,
        "price_delta_percent": None,
        "currency": "USD",
        "availability": "IN_STOCK",
        "event_type": OutboxEventType.PRICE_CHANGED.value,
    }
    event = PriceChangeOutbox(
        outbox_id="test-null-delta",
        entity_key="d" * 64,
        event_type=OutboxEventType.PRICE_CHANGED,
        payload=json.dumps(payload_data),
        notification_status=NotificationStatus.PENDING,
    )

    notifier = WebhookNotifier(platform="discord")
    discord_card = notifier.format_discord(event)
    assert "NEW PRODUCT MONITORED" not in discord_card["embeds"][0]["title"]
    assert "PRICE" in discord_card["embeds"][0]["title"]

    notifier_slack = WebhookNotifier(platform="slack")
    slack_data = notifier_slack.format_slack(event)
    assert "NEW PRODUCT MONITORED" not in slack_data["text"]

