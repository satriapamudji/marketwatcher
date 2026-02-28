"""Telegram HTML formatter for market reports."""

import html
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

from marketwatcher.logging_config import get_logger
from marketwatcher.models import MarketSummary
from marketwatcher.config import ReportConfig

logger = get_logger("formatters")

# Template directory - looks in package templates
TEMPLATE_DIR = Path(__file__).parent / "templates"

# Max Telegram message length
MAX_MESSAGE_LENGTH = 4096


def escape_html(text: str) -> str:
    """Escape HTML special characters for Telegram."""
    return html.escape(str(text))


def format_abbreviated_mcap(value: float) -> str:
    """Format market cap with abbreviation."""
    if value >= 1_000_000_000_000:
        return f"${value / 1_000_000_000_000:.2f}T"
    elif value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    elif value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    elif value >= 1_000:
        return f"${value / 1_000:.2f}K"
    else:
        return f"${value:.2f}"


def format_pct(value: float | None, decimals: int = 2) -> str:
    """Format percentage."""
    if value is None:
        return "N/A"
    return f"{value:.{decimals}f}%"


def format_signed_pct(value: float | None, decimals: int = 2) -> str:
    """Format percentage with sign."""
    if value is None:
        return "N/A"
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.{decimals}f}%"


def create_jinja_env() -> Environment:
    """Create Jinja2 environment with custom filters."""
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=select_autoescape(["html", "xml"]),
    )

    # Add custom filters
    env.filters["escape_html"] = escape_html
    env.filters["fmt_abbrev"] = format_abbreviated_mcap
    env.filters["fmt_pct"] = format_pct
    env.filters["fmt_signed_pct"] = format_signed_pct

    return env


def render_market_report(
    summary: MarketSummary,
    config: ReportConfig,
) -> str:
    """Render market summary to Telegram HTML message.

    Args:
        summary: Market summary data
        config: Report configuration

    Returns:
        Rendered HTML message string
    """
    env = create_jinja_env()

    try:
        template = env.get_template("telegram_market_v1.html.j2")
    except Exception as e:
        logger.warning(f"Template not found, using fallback: {e}")
        return render_fallback(summary, config)

    # Render template
    try:
        message = template.render(
            summary=summary,
            config=config,
        )
    except Exception as e:
        logger.error(f"Template render failed: {e}")
        return render_fallback(summary, config)

    # Truncate if too long
    if len(message) > MAX_MESSAGE_LENGTH:
        logger.warning(f"Message too long ({len(message)} chars), truncating")
        message = message[:MAX_MESSAGE_LENGTH - 50] + "\n\n[truncated...]"

    return message


def render_fallback(summary: MarketSummary, config: ReportConfig) -> str:
    """Fallback renderer when template not available."""
    divider = config.divider_style * 10

    # Format timestamp
    ts = summary.timestamp.strftime(config.timestamp_format)

    lines = [
        f"<i>{ts}</i>",
        divider,
        "",
        f"<b>Global MCAP:</b> {summary.formatted_global_mcap}",
        f"<b>╰➤ 1D:</b> {summary.formatted_global_mcap_1d} | <b>7D:</b> {summary.formatted_global_mcap_7d} | <b>14D:</b> {summary.formatted_global_mcap_14d}",
        "",
        f"<b>BTC Dominance:</b> {summary.formatted_btc_dominance}",
        f"<b>╰➤ 1D:</b> {summary.formatted_btc_dominance_1d} | <b>7D:</b> {summary.formatted_btc_dominance_7d} | <b>14D:</b> {summary.formatted_btc_dominance_14d}",
        divider,
        "",
        "<b>Top Gainers (24h):</b>",
    ]

    for i, cat in enumerate(summary.top_gainers, 1):
        change = format_signed_pct(cat.pct_change_24h, config.decimals)
        name = escape_html(cat.category_name)
        url = f"https://www.coingecko.com/en/categories/{cat.category_id}"
        lines.append(f"• <a href=\"{url}\">{name}</a> ({change})")

    lines.append("<b>Top Losers (24h):</b>")

    for i, cat in enumerate(summary.top_losers, 1):
        change = format_signed_pct(cat.pct_change_24h, config.decimals)
        name = escape_html(cat.category_name)
        url = f"https://www.coingecko.com/en/categories/{cat.category_id}"
        lines.append(f"• <a href=\"{url}\">{name}</a> ({change})")

    lines.append(divider)

    return "\n".join(lines)
