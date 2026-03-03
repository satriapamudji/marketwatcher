"""Watchlist report formatter for Telegram."""

import html
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from marketwatcher.logging_config import get_logger

logger = get_logger("formatters")

TEMPLATE_DIR = Path(__file__).parent / "templates"
MAX_MESSAGE_LENGTH = 4096


def escape_html(text: str) -> str:
    return html.escape(str(text))


def render_watchlist_report(report_data: dict, config: Any = None) -> str:
    """Render watchlist report to Telegram HTML message."""
    try:
        env = Environment(
            loader=FileSystemLoader(str(TEMPLATE_DIR)),
            autoescape=select_autoescape(["html", "xml"]),
        )
        template = env.get_template("telegram_watchlist_v1.html.j2")
        message = template.render(config=config, **report_data)
    except Exception:
        logger.exception("Failed rendering watchlist template, using fallback")
        message = render_watchlist_fallback(report_data)

    if len(message) > MAX_MESSAGE_LENGTH:
        logger.warning("Message too long (%s chars), truncating", len(message))
        message = message[: MAX_MESSAGE_LENGTH - 50] + "\n\n[truncated]"

    return message


def render_watchlist_fallback(report_data: dict) -> str:
    """Fallback renderer if Jinja template fails."""
    ts = report_data.get("timestamp")
    ts_line = f"<i>{ts.strftime('%Y-%m-%d %H:%M UTC')}</i>" if ts else ""
    divider = report_data.get("divider_line", "--------------------------------")

    lines = [
        ts_line,
        f"<b><u>Watchlist: {escape_html(report_data.get('watchlist_name', 'Main'))}</u></b>",
        divider,
    ]

    for token in report_data.get("tokens", []):
        sym = escape_html(token.get("symbol", "???"))
        if token.get("type") == "cex":
            lines.append(f"\n<b>{sym}:</b> {token.get('price', 'N/A')} ({token.get('change_24h', 'N/A')})")
            lines.append(f"<b>╰➤ 7D:</b> {token.get('change_7d', 'N/A')} | "
                         f"<b>{token.get('mcap_label', 'MCAP')}:</b> {token.get('mcap', 'N/A')}")
        else:
            lines.append(f"\n<b>{sym}:</b> {token.get('change_24h', 'N/A')}")
            lines.append(f"<b>╰➤</b> Vol {token.get('volume', 'N/A')} · "
                         f"Liq {token.get('liquidity', 'N/A')} · "
                         f"<b>{token.get('mcap_label', 'FDV')}:</b> {token.get('mcap', 'N/A')}")

    lines.extend(["", divider])
    return "\n".join(lines)
