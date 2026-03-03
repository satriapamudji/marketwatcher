"""On-chain report formatter for Telegram."""

import html
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from marketwatcher.logging_config import get_logger

logger = get_logger("formatters")

TEMPLATE_DIR = Path(__file__).parent / "templates"
MAX_MESSAGE_LENGTH = 4096


def escape_html(text: str) -> str:
    """Escape HTML special characters."""
    return html.escape(str(text))


def render_onchain_report(report_data: dict, config: Any = None) -> str:
    """Render on-chain report to Telegram HTML message."""
    divider_style = getattr(config, "divider_style", None) if config is not None else None
    divider_line = (divider_style * 10) if divider_style else "--------------------------------"
    try:
        env = Environment(
            loader=FileSystemLoader(str(TEMPLATE_DIR)),
            autoescape=select_autoescape(["html", "xml"]),
        )
        template = env.get_template("telegram_onchain_v1.html.j2")
        message = template.render(config=config, divider_line=divider_line, **report_data)
    except Exception:
        logger.exception("Failed rendering on-chain template, using fallback")
        message = render_onchain_fallback(report_data, divider_line=divider_line)

    if len(message) > MAX_MESSAGE_LENGTH:
        logger.warning("Message too long (%s chars), truncating", len(message))
        message = message[: MAX_MESSAGE_LENGTH - 50] + "\n\n[truncated]"

    return message


def _render_section_lines(
    network: str, title: str, emoji: str, rows: list[dict], streak_emoji: str = "\U0001f525",
) -> list[str]:
    lines: list[str] = ["", f"{emoji} <b>{title}</b>"]
    for idx, row in enumerate(rows, 1):
        symbol = escape_html(row.get("symbol") or row.get("name", ""))
        change = escape_html(row.get("change", "N/A"))
        pool_addr = escape_html(row.get("address", ""))
        volume = escape_html(row.get("volume", ""))
        liquidity = escape_html(row.get("liquidity", ""))
        mcap = escape_html(row.get("mcap", ""))
        mcap_label = escape_html(row.get("mcap_label", "MCAP"))
        streak = row.get("streak", 1)
        change_h1 = escape_html(row.get("change_h1", "N/A"))
        change_h6 = escape_html(row.get("change_h6", "N/A"))
        link = f'<a href="https://www.geckoterminal.com/{network}/pools/{pool_addr}">{symbol}</a>'
        if streak >= 2:
            link = f"{link} ({streak_emoji}{streak})"
        lines.append(f"")
        lines.append(f"{idx}. {link}")
        lines.append(f"\u2570\u27a4 1H: {change_h1} \u00b7 6H: {change_h6} \u00b7 1D: {change}")
        detail_parts: list[str] = []
        if volume:
            detail_parts.append(f"Vol {volume}")
        if liquidity:
            detail_parts.append(f"Liq {liquidity}")
        if mcap:
            detail_parts.append(f"{mcap_label} {mcap}")
        if detail_parts:
            lines.append(f"\u2570\u27a4 {' \u00b7 '.join(detail_parts)}")
    return lines


def render_onchain_fallback(report_data: dict, divider_line: str = "--------------------------------") -> str:
    """Fallback renderer if Jinja template fails."""
    network = escape_html(report_data.get("network", ""))
    network_name = escape_html(report_data.get("network_name", network))
    ts = report_data.get("timestamp")
    ts_line = f"<i>{ts.strftime('%Y-%m-%d %H:%M UTC')}</i>" if ts else ""
    lines: list[str] = [ts_line, f"<b><u>On-Chain: {network_name}</u></b>", divider_line]

    gainers = report_data.get("top_gainers", [])
    losers = report_data.get("top_losers", [])

    if gainers:
        lines.extend(_render_section_lines(
            network, "Top Gainers", "\U0001f4c8", gainers[:8], streak_emoji="\U0001f525",
        ))

    if gainers:
        lines.extend(["", divider_line])

    if losers:
        lines.extend(_render_section_lines(
            network, "Top Losers", "\U0001f4c9", losers[:8], streak_emoji="\U0001f9ca",
        ))

    lines.extend(["", divider_line])
    return "\n".join(lines)
