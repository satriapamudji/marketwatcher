"""Global on-chain report formatter for Telegram."""

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


def render_global_onchain_report(report_data: dict, config: Any = None) -> str:
    """Render global on-chain report to Telegram HTML message."""
    divider_style = getattr(config, "divider_style", None) if config is not None else None
    divider_line = (divider_style * 10) if divider_style else "--------------------------------"

    try:
        env = Environment(
            loader=FileSystemLoader(str(TEMPLATE_DIR)),
            autoescape=select_autoescape(["html", "xml"]),
        )
        template = env.get_template("telegram_global_onchain_v1.html.j2")
        # report_data already contains divider_line from the report builder
        message = template.render(config=config, **report_data)
    except Exception:
        logger.exception("Failed rendering global on-chain template, using fallback")
        message = render_global_onchain_fallback(report_data, divider_line=divider_line)

    if len(message) > MAX_MESSAGE_LENGTH:
        logger.warning("Message too long (%s chars), truncating", len(message))
        message = message[: MAX_MESSAGE_LENGTH - 50] + "\n\n[truncated]"

    return message


def render_global_onchain_fallback(
    report_data: dict,
    divider_line: str = "--------------------------------",
) -> str:
    """Fallback renderer if Jinja template fails."""
    ts = report_data.get("timestamp")
    ts_line = f"<i>{ts.strftime('%Y-%m-%d %H:%M UTC')}</i>" if ts else ""

    lines: list[str] = [
        ts_line,
        "<b><u>Global On-Chain</u></b>",
        divider_line,
        "",
        f"<b>Total DeFi TVL:</b> {report_data.get('total_tvl', 'N/A')}",
        f"<b>╰➤ 1D:</b> {report_data.get('tvl_1d', 'N/A')} | "
        f"<b>7D:</b> {report_data.get('tvl_7d', 'N/A')} | "
        f"<b>14D:</b> {report_data.get('tvl_14d', 'N/A')}",
        "",
        f"<b>24h DEX Volume:</b> {report_data.get('dex_volume_24h', 'N/A')}",
        f"<b>╰➤ 1D:</b> {report_data.get('dex_change_1d', 'N/A')} | "
        f"<b>7D:</b> {report_data.get('dex_change_7d', 'N/A')}",
        "",
        f"<b>Stablecoin Supply:</b> {report_data.get('stablecoin_supply', 'N/A')}",
        "",
        divider_line,
        "",
        "<b>TVL by Chain:</b>",
    ]

    for chain in report_data.get("top_chains", []):
        change_1d = chain.get("change_1d", "N/A")
        change_7d = chain.get("change_7d", "N/A")
        lines.append(
            f"• {escape_html(chain['name'])}: {chain['tvl']} ({chain['dominance']})"
            f" | 1D: {change_1d} | 7D: {change_7d}"
        )

    lines.extend(["", divider_line])

    gainers = report_data.get("tvl_gainers", [])
    if gainers:
        lines.append("")
        lines.append("<b>TVL Gainers (7D):</b>")
        for chain in gainers:
            name = escape_html(chain["name"])
            streak = chain.get("streak", 1)
            prefix = f"{name} (\U0001f525{streak})" if streak >= 2 else name
            lines.append(f"• {prefix}: {chain['change']}")

    losers = report_data.get("tvl_losers", [])
    if losers:
        lines.append("")
        lines.append("<b>TVL Losers (7D):</b>")
        for chain in losers:
            name = escape_html(chain["name"])
            streak = chain.get("streak", 1)
            prefix = f"{name} (\U0001f9ca{streak})" if streak >= 2 else name
            lines.append(f"• {prefix}: {chain['change']}")

    lines.extend(["", divider_line])
    return "\n".join(lines)
