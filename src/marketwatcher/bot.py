"""Telegram bot listener for watchlist management.

Long-polling bot that handles:
- /watch <symbol> [coingecko_id] — add a CEX token
- /watchdex <symbol> <chain> <address> — add a DEX token
- /unwatch <symbol> — remove a token
- /watchlist — show current watchlist
- /watchlists — list all watchlists
"""

import time
from typing import Any

import httpx

from marketwatcher.logging_config import get_logger
from marketwatcher.watchlist import (
    add_token,
    remove_token,
    get_watchlist,
    list_watchlists,
    set_token_alerts,
    clear_token_alerts,
)

logger = get_logger("bot")


class TelegramBot:
    """Simple long-polling Telegram bot."""

    def __init__(self, bot_token: str, allowed_chat_ids: list[str] | None = None):
        self.bot_token = bot_token
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self._client = httpx.Client(timeout=60.0)
        self._offset = 0
        self.allowed_chat_ids = set(allowed_chat_ids) if allowed_chat_ids else None

    def _request(self, method: str, data: dict | None = None) -> dict:
        url = f"{self.base_url}/{method}"
        try:
            response = self._client.post(url, json=data or {})
            response.raise_for_status()
            result = response.json()
            if not result.get("ok"):
                logger.error(f"API error: {result.get('description')}")
                return {}
            return result.get("result", {})
        except Exception as e:
            logger.error(f"Request failed: {e}")
            return {}

    def _send(self, chat_id: str | int, text: str) -> None:
        self._request("sendMessage", {
            "chat_id": str(chat_id),
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        })

    def _get_updates(self, timeout: int = 30) -> list[dict]:
        result = self._request("getUpdates", {
            "offset": self._offset,
            "timeout": timeout,
        })
        if isinstance(result, list):
            return result
        return []

    def _is_allowed(self, chat_id: int | str) -> bool:
        if self.allowed_chat_ids is None:
            return True
        return str(chat_id) in self.allowed_chat_ids

    def _handle_message(self, message: dict) -> None:
        chat_id = message.get("chat", {}).get("id")
        text = (message.get("text") or "").strip()

        if not chat_id or not text:
            return

        if not self._is_allowed(chat_id):
            logger.warning(f"Ignoring message from unauthorized chat {chat_id}")
            return

        if not text.startswith("/"):
            return

        parts = text.split()
        command = parts[0].lower().split("@")[0]  # Strip @botname suffix
        cmd_args = parts[1:]

        if command == "/watch":
            self._cmd_watch(chat_id, cmd_args)
        elif command == "/watchdex":
            self._cmd_watchdex(chat_id, cmd_args)
        elif command == "/unwatch":
            self._cmd_unwatch(chat_id, cmd_args)
        elif command == "/watchlist":
            self._cmd_watchlist(chat_id, cmd_args)
        elif command == "/watchlists":
            self._cmd_watchlists(chat_id)
        elif command == "/alert":
            self._cmd_alert(chat_id, cmd_args)
        elif command == "/alerts":
            self._cmd_alerts(chat_id, cmd_args)
        elif command == "/help" or command == "/start":
            self._cmd_help(chat_id)

    def _is_contract_address(self, text: str) -> bool:
        """Check if text looks like a contract address."""
        # EVM: 0x + 40 hex chars; Solana: base58, typically 32-44 chars
        if text.startswith("0x") and len(text) == 42:
            return True
        if len(text) >= 32 and text.isalnum():
            return True
        return False

    def _cmd_watch(self, chat_id: int, args: list[str]) -> None:
        """Handle /watch — auto-detects contract addresses vs CEX symbols."""
        if len(args) < 1:
            self._send(chat_id, "Usage:\n/watch BTC [coingecko_id]\n/watch 0xCONTRACT_ADDRESS\nExample: /watch BTC bitcoin")
            return

        # Auto-detect contract address
        if self._is_contract_address(args[0]):
            address = args[0]
            watchlist_id = args[1] if len(args) > 1 else "main"
            self._send(chat_id, f"Looking up <i>{address[:10]}...</i>")

            from marketwatcher.providers.geckoterminal import GeckoTerminalProvider
            gt = GeckoTerminalProvider()
            try:
                result = gt.search_token(address)
            finally:
                gt.close()

            if not result:
                self._send(chat_id, "Token not found on any chain.")
                return

            symbol = result["symbol"].upper()
            chain = result["chain"]
            name = result.get("name", "")

            ok = add_token(watchlist_id, symbol, token_type="dex", chain=chain, address=address)
            if ok:
                self._send(chat_id, f"Added <b>{symbol}</b> ({name}) on {chain} to '{watchlist_id}'")
                logger.info(f"Bot: auto-added {symbol} (dex/{chain}) to {watchlist_id}")
            else:
                self._send(chat_id, f"<b>{symbol}</b> already in watchlist '{watchlist_id}'")
            return

        # CEX token path
        symbol = args[0].upper()
        coingecko_id = args[1].lower() if len(args) > 1 else symbol.lower()
        watchlist_id = args[2] if len(args) > 2 else "main"

        ok = add_token(watchlist_id, symbol, token_type="cex", coingecko_id=coingecko_id)
        if ok:
            self._send(chat_id, f"Added <b>{symbol}</b> to watchlist '{watchlist_id}'")
            logger.info(f"Bot: added {symbol} (cex) to {watchlist_id}")
        else:
            self._send(chat_id, f"<b>{symbol}</b> already in watchlist '{watchlist_id}'")

    def _cmd_watchdex(self, chat_id: int, args: list[str]) -> None:
        """Handle /watchdex <symbol> <chain> <address> [watchlist_id]"""
        if len(args) < 3:
            self._send(chat_id, "Usage: /watchdex SYMBOL CHAIN ADDRESS [watchlist_id]\nExample: /watchdex BONK solana DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263")
            return

        symbol = args[0].upper()
        chain = args[1].lower()
        address = args[2]
        watchlist_id = args[3] if len(args) > 3 else "main"

        ok = add_token(watchlist_id, symbol, token_type="dex", chain=chain, address=address)
        if ok:
            self._send(chat_id, f"Added <b>{symbol}</b> (DEX: {chain}) to watchlist '{watchlist_id}'")
            logger.info(f"Bot: added {symbol} (dex/{chain}) to {watchlist_id}")
        else:
            self._send(chat_id, f"<b>{symbol}</b> already in watchlist '{watchlist_id}'")

    def _cmd_unwatch(self, chat_id: int, args: list[str]) -> None:
        """Handle /unwatch <symbol> [watchlist_id]"""
        if len(args) < 1:
            self._send(chat_id, "Usage: /unwatch SYMBOL [watchlist_id]\nExample: /unwatch BTC")
            return

        symbol = args[0].upper()
        watchlist_id = args[1] if len(args) > 1 else "main"

        ok = remove_token(watchlist_id, symbol)
        if ok:
            self._send(chat_id, f"Removed <b>{symbol}</b> from watchlist '{watchlist_id}'")
            logger.info(f"Bot: removed {symbol} from {watchlist_id}")
        else:
            self._send(chat_id, f"<b>{symbol}</b> not found in watchlist '{watchlist_id}'")

    def _cmd_watchlist(self, chat_id: int, args: list[str]) -> None:
        """Handle /watchlist [watchlist_id]"""
        watchlist_id = args[0] if args else "main"
        wl = get_watchlist(watchlist_id)
        tokens = wl.get("tokens", [])

        lines = [f"<b>{wl.get('name', watchlist_id)}</b> ({len(tokens)} tokens)"]

        if not tokens:
            lines.append("\n<i>Empty — use /watch to add tokens</i>")
        else:
            for t in tokens:
                sym = t.get("symbol", "???")
                if t.get("type") == "dex":
                    chain = t.get("chain", "?")
                    addr = t.get("address", "?")[:10]
                    lines.append(f"  {sym} <i>(DEX: {chain}/{addr}...)</i>")
                else:
                    lines.append(f"  {sym} <i>(CEX: {t.get('coingecko_id', '?')})</i>")

        self._send(chat_id, "\n".join(lines))

    def _cmd_watchlists(self, chat_id: int) -> None:
        """Handle /watchlists"""
        wls = list_watchlists()
        if not wls:
            self._send(chat_id, "No watchlists yet. Use /watch to create one.")
            return

        lines = ["<b>Watchlists</b>"]
        for wl in wls:
            lines.append(f"  <b>{wl['name']}</b> <i>({wl['id']}, {wl['count']} tokens)</i>")

        self._send(chat_id, "\n".join(lines))

    def _cmd_alert(self, chat_id: int, args: list[str]) -> None:
        """Handle /alert SYMBOL above|below|pct|pctup|pctdown|off VALUE [watchlist_id]"""
        if len(args) < 2:
            self._send(chat_id, (
                "Usage:\n"
                "/alert BTC above 100000\n"
                "/alert BTC below 60000\n"
                "/alert BTC pct 5\n"
                "/alert BTC pctup 4\n"
                "/alert BTC pctdown 6\n"
                "/alert BTC off"
            ))
            return

        symbol = args[0].upper()
        action = args[1].lower()

        if action == "off":
            watchlist_id = args[2] if len(args) > 2 else "main"
            ok = clear_token_alerts(watchlist_id, symbol)
            if ok:
                self._send(chat_id, f"Cleared alerts for <b>{symbol}</b>")
            else:
                self._send(chat_id, f"<b>{symbol}</b> not found in watchlist '{watchlist_id}'")
            return

        if len(args) < 3:
            self._send(chat_id, f"Missing value. Example: /alert {symbol} {action} 100000")
            return

        try:
            value = float(args[2])
        except ValueError:
            self._send(chat_id, f"Invalid number: {args[2]}")
            return

        watchlist_id = args[3] if len(args) > 3 else "main"

        if action == "above":
            ok = set_token_alerts(watchlist_id, symbol, alert_above=value)
        elif action == "below":
            ok = set_token_alerts(watchlist_id, symbol, alert_below=value)
        elif action == "pct":
            ok = set_token_alerts(watchlist_id, symbol, alert_pct=value)
        elif action == "pctup":
            ok = set_token_alerts(watchlist_id, symbol, alert_pct_up=value)
        elif action == "pctdown":
            ok = set_token_alerts(watchlist_id, symbol, alert_pct_down=value)
        else:
            self._send(chat_id, f"Unknown action '{action}'. Use: above, below, pct, pctup, pctdown, off")
            return

        if ok:
            self._send(chat_id, f"Set <b>{symbol}</b> alert: {action} {value}")
            logger.info(f"Bot: set alert {symbol} {action} {value} in {watchlist_id}")
        else:
            self._send(chat_id, f"<b>{symbol}</b> not found in watchlist '{watchlist_id}'")

    def _cmd_alerts(self, chat_id: int, args: list[str]) -> None:
        """Handle /alerts [watchlist_id] — list all active alerts."""
        watchlist_id = args[0] if args else "main"
        wl = get_watchlist(watchlist_id)
        tokens = wl.get("tokens", [])

        alert_lines = [f"<b>\U0001f6a8 Alert Config: {wl.get('name', watchlist_id)}</b>"]

        # Watchlist-level default
        wl_pct = wl.get("alert_pct")
        wl_pct_up = wl.get("alert_pct_up")
        wl_pct_down = wl.get("alert_pct_down")
        if wl_pct_up is not None or wl_pct_down is not None:
            up_text = f"+{wl_pct_up}%" if wl_pct_up is not None else "off"
            down_text = f"-{wl_pct_down}%" if wl_pct_down is not None else "off"
            alert_lines.append(f"<i>Default momentum thresholds: up {up_text}, down {down_text}</i>")
        elif wl_pct:
            alert_lines.append(f"<i>Default momentum threshold: \u00b1{wl_pct}%</i>")

        wl_alert_chat = wl.get("alert_chat_id")
        if wl_alert_chat:
            alert_lines.append(f"<i>Alert channel override: {wl_alert_chat}</i>")
        else:
            alert_lines.append("<i>Alert channel: default</i>")

        has_alerts = False
        for t in tokens:
            parts = []
            if "alert_above" in t:
                parts.append(f"\u2b06 above ${t['alert_above']:,.2f}")
            if "alert_below" in t:
                parts.append(f"\u2b07 below ${t['alert_below']:,.2f}")
            if "alert_pct_up" in t:
                parts.append(f"\U0001f4c8 +{t['alert_pct_up']}%")
            if "alert_pct_down" in t:
                parts.append(f"\U0001f4c9 -{t['alert_pct_down']}%")
            if "alert_pct" in t and "alert_pct_up" not in t and "alert_pct_down" not in t:
                parts.append(f"\U0001f4c9/\U0001f4c8 \u00b1{t['alert_pct']}%")
            if parts:
                has_alerts = True
                alert_lines.append(f"\u2022 <b>{t.get('symbol', '?')}</b>: {', '.join(parts)}")

        has_wl_defaults = (
            wl_pct is not None
            or wl_pct_up is not None
            or wl_pct_down is not None
        )
        if not has_alerts and not has_wl_defaults:
            alert_lines.append("\n<i>No alerts set. Use /alert SYMBOL above|below|pct|pctup|pctdown VALUE</i>")

        self._send(chat_id, "\n".join(alert_lines))

    def _cmd_help(self, chat_id: int) -> None:
        """Handle /help"""
        self._send(chat_id, (
            "<b>MarketWatcher Bot</b>\n\n"
            "<b>Watchlist</b>\n"
            "/watch SYMBOL [coingecko_id] [watchlist_id]\n"
            "/watchdex SYMBOL CHAIN ADDRESS [watchlist_id]\n"
            "/unwatch SYMBOL [watchlist_id]\n"
            "/watchlist [watchlist_id]\n"
            "/watchlists\n\n"
            "<b>Alerts</b>\n"
            "/alert SYMBOL above PRICE\n"
            "/alert SYMBOL below PRICE\n"
            "/alert SYMBOL pct PERCENT\n"
            "/alert SYMBOL pctup PERCENT\n"
            "/alert SYMBOL pctdown PERCENT\n"
            "/alert SYMBOL off\n"
            "/alerts [watchlist_id]"
        ))

    def run(self) -> None:
        """Start the long-polling loop."""
        logger.info("Bot starting long-poll loop...")
        while True:
            try:
                updates = self._get_updates(timeout=30)
                for update in updates:
                    self._offset = update.get("update_id", 0) + 1
                    msg = update.get("message")
                    if msg:
                        self._handle_message(msg)
            except KeyboardInterrupt:
                logger.info("Bot stopped by user")
                break
            except Exception as e:
                logger.error(f"Poll error: {e}")
                time.sleep(5)

    def close(self) -> None:
        self._client.close()
