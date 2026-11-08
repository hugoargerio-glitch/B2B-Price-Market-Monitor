"""
Transactional Webhook Notifier for Profile 1 (Lean App/CDC).
Dispatches rich notifications (Discord, Slack, Console) with zero external HTTP dependencies
in test/mock/dry-run mode, and strictly handles ACID outbox status transitions.
"""

from datetime import datetime, timezone
from decimal import Decimal
import json
import logging
import os
from pathlib import Path
from typing import Any, Optional
import urllib.error
import urllib.request

from src.models import NotificationStatus, OutboxEventType, PriceChangeOutbox

# Attempt to load .env if present
try:
    from dotenv import load_dotenv

    env_path = Path("/home/Hugo/b2b-price-monitor/.env")
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    pass

logger = logging.getLogger("b2b_price_monitor.notifications")

# Discord Embed Color Constants
DISCORD_COLOR_GREEN = 3066993      # 0x2ECC71 - PRICE_DECREASE (Opportunity)
DISCORD_COLOR_RED = 15158332       # 0xE74C3C - PRICE_INCREASE
DISCORD_COLOR_AMBER = 16744448     # 0xFFA500 - AVAILABILITY_CHANGED
DISCORD_COLOR_BLUE = 3447003       # 0x3498DB - INITIAL_CAPTURE


class WebhookNotifier:
    """Dispatches price monitoring alerts to webhooks (Discord, Slack) or console."""

    def __init__(
        self,
        webhook_url: Optional[str] = None,
        platform: Optional[str] = None,
        dry_run: bool = False,
        timeout: int = 10,
    ):
        self.webhook_url = (
            webhook_url
            if webhook_url is not None
            else os.getenv("ALERT_WEBHOOK_URL", "")
        ).strip()
        configured_platform = (
            platform
            if platform is not None
            else os.getenv("ALERT_PLATFORM", "auto")
        ).strip().lower()

        self.platform = self._resolve_platform(self.webhook_url, configured_platform)
        self.dry_run = dry_run
        self.timeout = timeout
        self.last_error: Optional[str] = None

    @staticmethod
    def _resolve_platform(webhook_url: str, configured_platform: str) -> str:
        """Resolve alert platform: auto-detect from URL or explicit platform."""
        if configured_platform in ("discord", "slack", "console"):
            return configured_platform
        url_lower = webhook_url.lower()
        if "discord.com" in url_lower:
            return "discord"
        if "hooks.slack.com" in url_lower:
            return "slack"
        return "console"

    @staticmethod
    def classify_change(payload_dict: dict[str, Any]) -> str:
        """Classify change direction: PRICE_DECREASE, PRICE_INCREASE, AVAILABILITY_CHANGED, INITIAL_CAPTURE."""
        event_type = payload_dict.get("event_type", OutboxEventType.PRICE_CHANGED.value)
        if event_type == OutboxEventType.INITIAL_CAPTURE.value:
            return "INITIAL_CAPTURE"
        if event_type == OutboxEventType.AVAILABILITY_CHANGED.value:
            return "AVAILABILITY_CHANGED"

        delta_raw = payload_dict.get("price_delta")
        if delta_raw is not None:
            try:
                delta = Decimal(str(delta_raw))
                if delta < Decimal("0"):
                    return "PRICE_DECREASE"
                elif delta > Decimal("0"):
                    return "PRICE_INCREASE"
            except Exception:
                pass
        return "PRICE_CHANGED"

    def format_discord(self, outbox_event: PriceChangeOutbox) -> dict[str, Any]:
        """Format event as Discord Rich Embed Card."""
        try:
            p = json.loads(outbox_event.payload)
        except Exception:
            p = {"raw_payload": outbox_event.payload}

        change_type = self.classify_change(p)
        sku = p.get("sku", "UNKNOWN")
        domain = p.get("domain", "Unknown Competitor")
        currency = p.get("currency", "BRL")
        curr_price = p.get("current_price", "0.0000")
        prev_price = p.get("previous_price")
        delta = p.get("price_delta")
        delta_pct = p.get("price_delta_percent")
        avail = p.get("availability", "IN_STOCK")
        canonical_url = p.get("canonical_url") or f"https://{domain}/products/{sku}"

        if change_type == "PRICE_DECREASE":
            color = DISCORD_COLOR_GREEN
            title = f"🟢 PRICE DROP DETECTED: {sku}"
            action_desc = f"Competitor price discount detected on **{domain}**!"
        elif change_type == "PRICE_INCREASE":
            color = DISCORD_COLOR_RED
            title = f"🔴 PRICE INCREASE: {sku}"
            action_desc = f"Competitor increased unit price on **{domain}**."
        elif change_type == "AVAILABILITY_CHANGED":
            color = DISCORD_COLOR_AMBER
            title = f"🟡 STOCK STATUS CHANGED: {sku}"
            action_desc = f"Availability status mutated to **{avail}** on **{domain}**."
        else:
            color = DISCORD_COLOR_BLUE
            title = f"ℹ️ NEW PRODUCT MONITORED: {sku}"
            action_desc = f"Initial baseline established on **{domain}**."

        iso_ts = outbox_event.created_at.isoformat()

        fields = [
            {"name": "SKU", "value": f"`{sku}`", "inline": True},
            {"name": "Competitor", "value": domain, "inline": True},
            {"name": "Status", "value": avail, "inline": True},
            {
                "name": "Old Price",
                "value": f"{currency} {prev_price}" if prev_price is not None else "N/A (Baseline)",
                "inline": True,
            },
            {"name": "New Price", "value": f"{currency} {curr_price}", "inline": True},
            {
                "name": "Delta ($ / %)",
                "value": f"{delta} ({delta_pct}%)" if delta is not None and delta_pct is not None else "0.00 (0.00%)",
                "inline": True,
            },
            {"name": "Product URL", "value": f"<{canonical_url}>", "inline": False},
        ]

        return {
            "embeds": [
                {
                    "title": title,
                    "description": action_desc,
                    "url": canonical_url,
                    "color": color,
                    "fields": fields,
                    "timestamp": iso_ts,
                    "footer": {
                        "text": f"B2B Price Monitor • Event {outbox_event.outbox_id[:8]}"
                    },
                }
            ]
        }

    def format_slack(self, outbox_event: PriceChangeOutbox) -> dict[str, Any]:
        """Format event as Slack Block Kit JSON."""
        try:
            p = json.loads(outbox_event.payload)
        except Exception:
            p = {"raw_payload": outbox_event.payload}

        change_type = self.classify_change(p)
        sku = p.get("sku", "UNKNOWN")
        domain = p.get("domain", "Unknown Competitor")
        currency = p.get("currency", "BRL")
        curr_price = p.get("current_price", "0.0000")
        prev_price = p.get("previous_price")
        delta = p.get("price_delta")
        delta_pct = p.get("price_delta_percent")
        avail = p.get("availability", "IN_STOCK")
        canonical_url = p.get("canonical_url") or f"https://{domain}/products/{sku}"

        emoji = "🟢" if change_type == "PRICE_DECREASE" else ("🔴" if change_type == "PRICE_INCREASE" else "🟡")
        header_text = f"{emoji} B2B Alert: {change_type} ({sku})"

        old_str = f"{currency} {prev_price}" if prev_price is not None else "N/A"
        new_str = f"{currency} {curr_price}"
        delta_str = f"{delta} ({delta_pct}%)" if delta is not None and delta_pct is not None else "0.00 (0.00%)"

        return {
            "text": header_text,
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": header_text,
                        "emoji": True,
                    },
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*SKU:*\n{sku}"},
                        {"type": "mrkdwn", "text": f"*Competitor:*\n{domain}"},
                        {"type": "mrkdwn", "text": f"*Old Price:*\n{old_str}"},
                        {"type": "mrkdwn", "text": f"*New Price:*\n{new_str}"},
                        {"type": "mrkdwn", "text": f"*Delta:*\n{delta_str}"},
                        {"type": "mrkdwn", "text": f"*Availability:*\n{avail}"},
                    ],
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Product Link:* <{canonical_url}|Open on {domain}>",
                    },
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"Event ID: `{outbox_event.outbox_id}` | Time: {outbox_event.created_at.isoformat()}",
                        }
                    ],
                },
            ],
        }

    def format_console(self, outbox_event: PriceChangeOutbox) -> str:
        """Format event as an ASCII box for CLI / logger mock output."""
        try:
            p = json.loads(outbox_event.payload)
        except Exception:
            p = {"raw_payload": outbox_event.payload}

        change_type = self.classify_change(p)
        sku = p.get("sku", "UNKNOWN")
        domain = p.get("domain", "Unknown Competitor")
        currency = p.get("currency", "BRL")
        curr_price = p.get("current_price", "0.0000")
        prev_price = p.get("previous_price")
        delta = p.get("price_delta", "0.0000")
        delta_pct = p.get("price_delta_percent", "0.0000")
        avail = p.get("availability", "IN_STOCK")
        canonical_url = p.get("canonical_url") or f"https://{domain}/products/{sku}"

        emoji = "🟢" if change_type == "PRICE_DECREASE" else ("🔴" if change_type == "PRICE_INCREASE" else "🟡")
        title_line = f"{emoji} B2B PRICE ALERT: {change_type} ({sku})"

        old_str = f"{currency} {prev_price}" if prev_price is not None else "N/A"
        new_str = f"{currency} {curr_price}"
        price_flow = f"{old_str} -> {new_str} (Delta: {delta} / {delta_pct}%)"

        lines = [
            "+--------------------------------------------------------------------------------+",
            f"| {title_line:<78} |",
            "+--------------------------------------------------------------------------------+",
            f"| Competitor : {domain:<65} |",
            f"| SKU        : {sku:<65} |",
            f"| Price Flow : {price_flow:<65} |",
            f"| Status     : {avail:<65} |",
            f"| URL        : {canonical_url[:65]:<65} |",
            f"| Event ID   : {outbox_event.outbox_id:<65} |",
            f"| Timestamp  : {outbox_event.created_at.isoformat():<65} |",
            "+--------------------------------------------------------------------------------+",
        ]
        return "\n".join(lines)

    def send(self, outbox_event: PriceChangeOutbox) -> bool:
        """Send notification via configured platform webhook or fallback console mock."""
        self.last_error = None

        # Format according to platform
        if self.platform == "discord":
            payload_data = self.format_discord(outbox_event)
        elif self.platform == "slack":
            payload_data = self.format_slack(outbox_event)
        else:
            payload_data = None

        # If console mode, mock, dry-run, or URL not configured, output to console/logger and succeed
        is_mock_env = (
            self.dry_run
            or self.platform == "console"
            or not self.webhook_url
            or "console" in self.webhook_url.lower()
            or "mock" in self.webhook_url.lower()
            or self.webhook_url.startswith("test://")
        )

        if is_mock_env:
            console_card = self.format_console(outbox_event)
            logger.info("\n%s", console_card)
            print(console_card)
            return True

        # Real HTTP Webhook Dispatch
        try:
            raw_bytes = json.dumps(payload_data).encode("utf-8")
            req = urllib.request.Request(
                self.webhook_url,
                data=raw_bytes,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "B2B-Price-Monitor/1.0 (+https://internal.b2b-price-monitor.local)",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                code = resp.getcode()
                if code in (200, 201, 204):
                    logger.info("Successfully dispatched outbox %s to %s (HTTP %s)", outbox_event.outbox_id, self.platform, code)
                    return True
                self.last_error = f"HTTP {code}: Webhook endpoint returned unexpected status"
                logger.warning("Webhook dispatch failed: %s", self.last_error)
                return False

        except urllib.error.HTTPError as he:
            self.last_error = f"HTTP {he.code}: {he.reason}"
            logger.error("HTTP error dispatching outbox %s: %s", outbox_event.outbox_id, self.last_error)
            return False
        except urllib.error.URLError as ue:
            self.last_error = f"URLError: {ue.reason}"
            logger.error("Network error dispatching outbox %s: %s", outbox_event.outbox_id, self.last_error)
            return False
        except Exception as exc:
            self.last_error = f"DispatchException: {exc}"
            logger.error("Unexpected error dispatching outbox %s: %s", outbox_event.outbox_id, self.last_error)
            return False
