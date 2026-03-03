"""Global macro report formatter for Telegram."""

from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from marketwatcher.logging_config import get_logger

logger = get_logger("formatters.macro")

TEMPLATE_DIR = Path(__file__).parent / "templates"
MAX_MESSAGE_LENGTH = 4096


def render_macro_report(report_data: dict[str, Any], config: Any = None) -> str:
    """Render global macro report to Telegram HTML message."""
    divider_style = getattr(config, "divider_style", None) if config is not None else None
    divider_line = (divider_style * 10) if divider_style else "————————————————————"

    try:
        env = Environment(
            loader=FileSystemLoader(str(TEMPLATE_DIR)),
            autoescape=select_autoescape(["html", "xml"]),
        )
        template = env.get_template("telegram_macro_v1.html.j2")
        message = template.render(config=config, divider_line=divider_line, **report_data)
    except Exception:
        logger.exception("Failed rendering macro template, using fallback")
        message = render_macro_fallback(report_data, divider_line=divider_line)

    if len(message) > MAX_MESSAGE_LENGTH:
        logger.warning("Message too long (%s chars), truncating", len(message))
        message = message[:MAX_MESSAGE_LENGTH - 50] + "\n\n[truncated]"

    return message


def render_macro_fallback(report_data: dict[str, Any], divider_line: str = "——" * 10) -> str:
    """Fallback renderer when template not available."""
    d = report_data
    lines = [
        f"<i>{d['timestamp'].strftime('%Y-%m-%d %H:%M UTC')}</i>",
        "<b><u>Global Macro</u></b>",
        divider_line,
        "",
        f"<b>DXY:</b>  {d['dxy_price']}  ({d['dxy_change']})",
        "",
        divider_line,
        "",
        f"<b>Fed Funds:</b>  {d['fed_funds_rate']}",
        f"<b>US 5Y:</b>  {d['us_5y_yield']}  ({d['us_5y_change']})",
        f"<b>US 10Y:</b>  {d['us_10y_yield']}  ({d['us_10y_change']})",
        f"<b>US 30Y:</b>  {d['us_30y_yield']}  ({d['us_30y_change']})",
        "",
        divider_line,
        "",
        f"<b>S&amp;P 500:</b>  {d['sp500_price']}  ({d['sp500_change']})",
        f"<b>NASDAQ:</b>  {d['nasdaq_price']}  ({d['nasdaq_change']})",
        f"<b>VIX:</b>  {d['vix_price']}  ({d['vix_change']})",
        "",
        divider_line,
        "",
        f"<b>Gold:</b>  ${d['gold_price']}  ({d['gold_change']})",
        f"<b>Oil (WTI):</b>  ${d['oil_price']}  ({d['oil_change']})",
        f"<b>Copper:</b>  ${d['copper_price']}  ({d['copper_change']})",
        "",
        divider_line,
        "",
        f"<b>USD/JPY:</b>  {d['usdjpy_price']}  ({d['usdjpy_change']})",
        f"<b>EUR/USD:</b>  {d['eurusd_price']}  ({d['eurusd_change']})",
        "",
        divider_line,
    ]
    return "\n".join(lines)
