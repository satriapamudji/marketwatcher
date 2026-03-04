"""Terminal UI for MarketWatcher.

Simplified operator console for:
- Running reports (global market, on-chain)
- Managing scheduled jobs
- Checking system health
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table
from rich.text import Text
import yaml

from marketwatcher import config
from marketwatcher.logging_config import get_logger
from marketwatcher.storage.sqlite import Storage
from marketwatcher.timezones import normalize_timezone_label, parse_timezone

logger = get_logger("tui")


class QuitTUI(Exception):
    """Raised when user types 'q' at any prompt."""
    pass


def _ask(prompt: str, choices: list[str] | None = None, default: str = "") -> str:
    """Prompt wrapper that raises QuitTUI on 'q'."""
    all_choices = list(choices) if choices else None
    if all_choices and "q" not in all_choices:
        all_choices.append("q")
    result = Prompt.ask(prompt, choices=all_choices, default=default)
    if result.strip().lower() == "q":
        raise QuitTUI()
    return result


def _ask_int(
    prompt: str,
    *,
    default: str,
    min_value: int | None = None,
    max_value: int | None = None,
) -> int:
    """Ask for an integer with validation and retry."""
    while True:
        raw = _ask(prompt, default=default).strip()
        try:
            value = int(raw)
        except ValueError:
            print("Invalid number. Enter an integer.")
            continue
        if min_value is not None and value < min_value:
            print(f"Value must be >= {min_value}.")
            continue
        if max_value is not None and value > max_value:
            print(f"Value must be <= {max_value}.")
            continue
        return value


def _ask_hhmm(prompt: str, *, default: str = "09:00") -> str:
    """Ask for HH:MM with 24h validation."""
    while True:
        value = _ask(prompt, default=default).strip()
        try:
            hour_str, minute_str = value.split(":")
            hour = int(hour_str)
            minute = int(minute_str)
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                return f"{hour:02d}:{minute:02d}"
        except Exception:
            pass
        print("Invalid time. Use HH:MM in 24h format.")


@dataclass
class SessionEvent:
    ts_utc: datetime
    action: str
    status: str
    detail: str = ""


@dataclass
class TuiState:
    events: list[SessionEvent] = field(default_factory=list)

    def push(self, action: str, status: str, detail: str = "") -> None:
        self.events.append(
            SessionEvent(
                ts_utc=datetime.now(timezone.utc),
                action=action,
                status=status,
                detail=detail,
            )
        )
        self.events = self.events[-12:]


def _read_last_run(cfg) -> tuple[str, str]:
    """Get last run status."""
    try:
        storage = Storage(cfg.database_path)
        row = storage.get_last_run()
        storage.close()
        if row is None:
            return ("-", "No runs")
        return (row.status or "-", row.started_at_utc or "unknown")
    except Exception:
        return ("err", "unavailable")


def _schedules_yaml_path(cfg) -> Path:
    return Path(cfg.project_root).resolve() / "config" / "schedules.yaml"


def _settings_yaml_path(cfg) -> Path:
    return Path(cfg.project_root).resolve() / "config" / "settings.yaml"


def _load_settings_yaml(cfg) -> dict:
    path = _settings_yaml_path(cfg)
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _save_settings_yaml(cfg, data: dict) -> None:
    path = _settings_yaml_path(cfg)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False)


def _write_settings_scheduler_timezone(cfg, timezone_value: str) -> None:
    """Persist scheduler timezone in config/settings.yaml."""
    data = _load_settings_yaml(cfg)
    scheduler_data = data.get("scheduler", {})
    if not isinstance(scheduler_data, dict):
        scheduler_data = {}
    scheduler_data["timezone"] = timezone_value
    data["scheduler"] = scheduler_data
    _save_settings_yaml(cfg, data)


def _write_settings_telegram_channels(cfg) -> None:
    """Persist telegram channel presets and default channel."""
    data = _load_settings_yaml(cfg)
    telegram_data = data.get("telegram", {})
    if not isinstance(telegram_data, dict):
        telegram_data = {}
    telegram_data["default_chat_id"] = cfg.telegram.chat_id
    telegram_data["channels"] = [
        {"name": ch.name, "chat_id": ch.chat_id}
        for ch in cfg.telegram.channels
    ]
    data["telegram"] = telegram_data
    _save_settings_yaml(cfg, data)


def _write_schedules_yaml(cfg) -> None:
    """Save scheduler config to YAML."""
    path = _schedules_yaml_path(cfg)
    path.parent.mkdir(parents=True, exist_ok=True)

    jobs_data = []
    for job in cfg.scheduler.jobs:
        entry: dict = {
            "id": job.id,
            "type": job.type,
            "chain": job.chain,
            "time": job.time,
            "enabled": job.enabled,
        }
        if job.interval_hours > 0:
            entry["interval_hours"] = job.interval_hours
            entry["offset_minutes"] = job.offset_minutes
        if job.chat_id:
            entry["chat_id"] = job.chat_id
        if job.watchlist_id:
            entry["watchlist_id"] = job.watchlist_id
        jobs_data.append(entry)

    payload = {
        "scheduler": {
            "timezone": cfg.scheduler.timezone,
            "jobs": jobs_data,
        }
    }
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(payload, f, sort_keys=False)


def _channel_name_map(cfg) -> dict[str, str]:
    channel_map: dict[str, str] = {}
    for ch in cfg.telegram.channels:
        if ch.chat_id and ch.chat_id not in channel_map:
            channel_map[ch.chat_id] = ch.name or ch.chat_id
    return channel_map


def _channel_label(cfg, chat_id: str) -> str:
    if not chat_id:
        if cfg.telegram.chat_id:
            return f"Default ({cfg.telegram.chat_id})"
        return "Default"
    names = _channel_name_map(cfg)
    if chat_id in names:
        return f"{names[chat_id]} ({chat_id})"
    return chat_id


def _pick_channel(
    console: Console,
    cfg,
    prompt_text: str,
    *,
    current_chat_id: str = "",
    allow_default: bool = True,
    allow_keep: bool = False,
    allow_off: bool = False,
) -> str:
    """Prompt user to pick from preconfigured channels."""
    options: list[tuple[str, str]] = []
    if allow_keep:
        options.append(("__KEEP__", f"Keep current ({_channel_label(cfg, current_chat_id)})"))
    if allow_default:
        options.append(("", _channel_label(cfg, "")))

    for chat_id, name in _channel_name_map(cfg).items():
        if chat_id not in [val for val, _ in options]:
            options.append((chat_id, f"{name} ({chat_id})"))

    if current_chat_id and current_chat_id not in [val for val, _ in options]:
        options.append((current_chat_id, f"Current custom ({current_chat_id})"))

    if allow_off:
        options.append(("__OFF__", "Clear override"))

    if not options:
        return current_chat_id

    default_value = "__KEEP__" if allow_keep else options[0][0]
    if not allow_keep and current_chat_id:
        for value, _ in options:
            if value == current_chat_id:
                default_value = value
                break

    return _pick_option(
        console,
        prompt_text,
        options,
        current_value=default_value,
    )


def _select_channel_index(console: Console, cfg, prompt_text: str = "Select Channel") -> int:
    """Pick channel preset index (0-based), -1 if cancelled."""
    channels = cfg.telegram.channels
    if not channels:
        return -1
    options = [(str(i), f"{ch.name} ({ch.chat_id})") for i, ch in enumerate(channels)]
    choice = _pick_option(console, prompt_text, options, current_value="0")
    idx = int(choice)
    if idx < 0 or idx >= len(channels):
        return -1
    return idx


def _channels_menu(console: Console, cfg, state: TuiState):
    """Manage telegram channel presets."""
    action_cursor = "add"
    while True:
        console.clear()
        console.rule("[bold]Channels[/bold]")

        table = Table(show_header=True, header_style="bold")
        table.add_column("#", width=3, style="dim")
        table.add_column("Name", width=18)
        table.add_column("Chat ID", width=20)
        table.add_column("Default", width=8)

        if cfg.telegram.channels:
            for idx, ch in enumerate(cfg.telegram.channels, 1):
                is_default = "Yes" if ch.chat_id == cfg.telegram.chat_id else ""
                table.add_row(str(idx), ch.name, ch.chat_id, is_default)
        else:
            table.add_row("-", "[dim]No channels[/dim]", "[dim]-[/dim]", "")
        console.print(table)
        console.print(f"\n  Current default: [cyan]{cfg.telegram.chat_id or '(not set)'}[/cyan]")

        console.print("[dim]Enter option number (q to go back)[/dim]")

        try:
            choice = _pick_option(
                console,
                "Channels Action",
                [
                    ("add", "Add channel"),
                    ("edit", "Edit channel"),
                    ("delete", "Delete channel"),
                    ("default", "Set default channel"),
                ],
                current_value=action_cursor,
            )
            action_cursor = choice
        except QuitTUI:
            return cfg

        try:
            if choice == "add":
                name = _ask("Channel name").strip()
                chat_id = _ask("Channel chat_id").strip()
                if not name or not chat_id:
                    console.print("[yellow]Name and chat_id are required[/yellow]")
                elif any(ch.chat_id == chat_id for ch in cfg.telegram.channels):
                    console.print("[yellow]chat_id already exists in presets[/yellow]")
                else:
                    from marketwatcher.config import TelegramChannelConfig

                    cfg.telegram.channels.append(TelegramChannelConfig(name=name, chat_id=chat_id))
                    _write_settings_telegram_channels(cfg)
                    cfg = config.reload_config()
                    state.push("Add Channel", "OK", name)
                    console.print(f"[green]Added channel:[/green] {name}")

            elif choice == "edit":
                idx = _select_channel_index(console, cfg, "Edit Channel")
                if idx >= 0:
                    ch = cfg.telegram.channels[idx]
                    new_name = _ask("Name", default=ch.name).strip()
                    new_chat_id = _ask("Chat ID", default=ch.chat_id).strip()
                    if not new_name or not new_chat_id:
                        console.print("[yellow]Name and chat_id are required[/yellow]")
                    else:
                        dup = any(
                            i != idx and c.chat_id == new_chat_id
                            for i, c in enumerate(cfg.telegram.channels)
                        )
                        if dup:
                            console.print("[yellow]chat_id already exists in presets[/yellow]")
                        else:
                            old_chat_id = ch.chat_id
                            ch.name = new_name
                            ch.chat_id = new_chat_id
                            if cfg.telegram.chat_id == old_chat_id:
                                cfg.telegram.chat_id = new_chat_id
                            _write_settings_telegram_channels(cfg)
                            cfg = config.reload_config()
                            state.push("Edit Channel", "OK", new_name)
                            console.print(f"[green]Updated channel:[/green] {new_name}")

            elif choice == "delete":
                idx = _select_channel_index(console, cfg, "Delete Channel")
                if idx >= 0:
                    ch = cfg.telegram.channels[idx]
                    confirm = _ask(f"Delete {ch.name}?", choices=["y", "n"], default="n")
                    if confirm == "y":
                        removed = cfg.telegram.channels.pop(idx)
                        if cfg.telegram.chat_id == removed.chat_id:
                            cfg.telegram.chat_id = cfg.telegram.channels[0].chat_id if cfg.telegram.channels else ""
                        _write_settings_telegram_channels(cfg)
                        cfg = config.reload_config()
                        state.push("Delete Channel", "OK", removed.name)
                        console.print(f"[green]Deleted channel:[/green] {removed.name}")

            elif choice == "default":
                idx = _select_channel_index(console, cfg, "Set Default Channel")
                if idx >= 0:
                    selected = cfg.telegram.channels[idx]
                    cfg.telegram.chat_id = selected.chat_id
                    _write_settings_telegram_channels(cfg)
                    cfg = config.reload_config()
                    state.push("Default Channel", "OK", selected.name)
                    console.print(f"[green]Default channel set:[/green] {selected.name}")
        except QuitTUI:
            continue

        Prompt.ask("\n[dim]Press Enter[/dim]", default="")


def _timezone_options(cfg) -> list[str]:
    """Get validated timezone preset options for TUI picker."""
    raw = list(cfg.scheduler.timezone_presets or [])
    if not raw:
        raw = ["UTC", "UTC+08:00", "UTC-05:00"]

    options: list[str] = []
    seen: set[str] = set()
    for value in raw:
        try:
            parse_timezone(value)
        except Exception:
            continue
        label = normalize_timezone_label(value)
        if label not in seen:
            seen.add(label)
            options.append(label)

    current = normalize_timezone_label(cfg.scheduler.timezone)
    if current not in seen:
        try:
            parse_timezone(current)
        except Exception:
            current = "UTC"
        if current not in seen:
            options.insert(0, current)

    if not options:
        options = ["UTC"]
    return options


def _read_single_key() -> str:
    """Read one keypress and normalize to up/down/enter/q."""
    if not sys.stdin.isatty():
        return ""
    try:
        import termios
        import tty
    except Exception:
        return ""

    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        ch = sys.stdin.read(1)
        if ch in ("\r", "\n"):
            return "enter"
        if ch.lower() == "q":
            return "q"
        if ch.lower() == "j":
            return "down"
        if ch.lower() == "k":
            return "up"
        if ch == "\x1b":
            second = sys.stdin.read(1)
            if second == "[":
                third = sys.stdin.read(1)
                if third == "A":
                    return "up"
                if third == "B":
                    return "down"
            return "esc"
        return ""
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)


def _spin_pick_option(console: Console, title: str, options: list[str], current_index: int = 0) -> int:
    """Slot-style picker where current option stays centered."""
    if not options:
        return 0

    idx = max(0, min(current_index, len(options) - 1))
    while True:
        console.clear()
        console.rule(f"[bold]{title}[/bold]")
        console.print("[dim]Use ↑/↓ (or j/k), Enter to select, q to cancel[/dim]\n")

        if len(options) == 1:
            console.print(f"  [bold cyan]▶ {options[0]} ◀[/bold cyan]")
        else:
            prev_label = options[(idx - 1) % len(options)]
            curr_label = options[idx]
            next_label = options[(idx + 1) % len(options)]
            console.print(f"  [dim]  {prev_label}[/dim]")
            console.print(f"  [bold cyan]▶ {curr_label} ◀[/bold cyan]")
            console.print(f"  [dim]  {next_label}[/dim]")

        key = _read_single_key()
        if key == "up":
            idx = (idx - 1) % len(options)
        elif key == "down":
            idx = (idx + 1) % len(options)
        elif key == "enter":
            return idx
        elif key == "q":
            raise QuitTUI()
        else:
            # Fallback for terminals where raw key reading may not work reliably.
            choice = _ask(
                "Pick #",
                choices=[str(i) for i in range(1, len(options) + 1)],
                default=str(idx + 1),
            )
            return int(choice) - 1


def _pick_option(
    console: Console,
    title: str,
    options: list[tuple[str, str]],
    *,
    current_value: str | None = None,
) -> str:
    """Pick one value/label pair using numbered input."""
    if not options:
        raise QuitTUI()
    current_index = 0
    if current_value is not None:
        for idx, (value, _) in enumerate(options):
            if value == current_value:
                current_index = idx
                break

    console.print(f"\n  [bold]{title}[/bold]")
    for idx, (_, label) in enumerate(options, 1):
        console.print(f"    [yellow]{idx}[/yellow]. {label}")

    choice = _ask(
        "Option",
        choices=[str(i) for i in range(1, len(options) + 1)],
        default=str(current_index + 1),
    )
    return options[int(choice) - 1][0]


def _generate_job_id(job_type: str, chain: str = "") -> str:
    """Generate a unique job ID."""
    import uuid
    suffix = uuid.uuid4().hex[:6]
    if job_type == "global":
        return f"global_{suffix}"
    elif job_type == "global_onchain":
        return f"global_onchain_{suffix}"
    elif job_type == "macro":
        return f"macro_{suffix}"
    elif job_type == "watchlist":
        return f"watchlist_{suffix}"
    elif job_type == "alerts":
        return f"alerts_{suffix}"
    else:
        return f"onchain_{chain}_{suffix}"


def _count_enabled_jobs(cfg) -> tuple[int, int]:
    """Count enabled vs total jobs."""
    jobs = cfg.scheduler.jobs
    enabled = sum(1 for j in jobs if j.enabled)
    return (enabled, len(jobs))


def _show_home(console: Console, cfg, state: TuiState) -> None:
    """Display simplified home screen."""
    console.clear()

    # Title
    title = Text("MarketWatcher", style="bold white on dark_green")
    console.print(Panel(title, border_style="green"))

    # Status line
    last_status, last_time = _read_last_run(cfg)
    enabled, total = _count_enabled_jobs(cfg)

    status_color = "green" if last_status == "ok" else "yellow" if last_status == "-" else "red"
    status_text = f"Last: [{status_color}]{last_status}[/{status_color}] @ {last_time}"
    jobs_text = f"Jobs: [cyan]{enabled}/{total} ON[/cyan]"

    console.print(f"\n  {status_text}  |  {jobs_text}\n")

    # Actions overview
    menu = Table(show_header=False, box=None, pad_edge=False)
    menu.add_column(style="bold")
    menu.add_column(style="dim")
    menu.add_row("Global Crypto", "Market summary report")
    menu.add_row("Global Macro", "Equities, rates, FX, commodities")
    menu.add_row("Global On-Chain", "DeFi TVL overview")
    menu.add_row("On-Chain", "Chain movers report")
    menu.add_row("Watchlist", "Manage & run watchlists")
    menu.add_row("Scheduler", "Manage scheduled jobs")
    menu.add_row("Config", "View settings")
    menu.add_row("Doctor", "Check system health")

    console.print(Panel(menu, title="Actions", border_style="cyan"))
    console.print("[dim]Enter option number (or q to quit)[/dim]")

    # Recent activity (last 3)
    if state.events:
        console.print("\n  [dim]Recent:[/dim]", end="")
        for ev in state.events[-3:]:
            color = "green" if ev.status == "OK" else "red"
            console.print(f" [{color}]{ev.action}[/{color}]", end="")
        console.print()


def _run_action(console: Console, state: TuiState, action_name: str, func, args: argparse.Namespace) -> int:
    """Run an action and track result."""
    console.print()
    console.rule(f"[bold]{action_name}[/bold]")
    try:
        rc = func(args)
        state.push(action_name, "OK" if rc == 0 else "ERR", f"rc={rc}")
        return rc
    except KeyboardInterrupt:
        state.push(action_name, "INT", "cancelled")
        console.print("[yellow]Cancelled[/yellow]")
        return 130
    except Exception as exc:
        state.push(action_name, "ERR", str(exc))
        logger.exception("TUI action failed")
        console.print(f"[red]Error:[/red] {exc}")
        return 1


def _ask_preview_or_send(console: Console) -> bool:
    """Ask user if they want to preview or send. Returns True for send."""
    choice = _pick_option(
        console,
        "Preview or Send",
        [("preview", "Preview only"), ("send", "Send to Telegram")],
        current_value="preview",
    )
    return choice == "send"


def _run_doctor(console: Console, state: TuiState) -> int:
    from marketwatcher.cli import doctor
    return _run_action(console, state, "Doctor", doctor, argparse.Namespace())


def _job_chat_id(job_type: str, chain: str = "", watchlist_id: str = "") -> str:
    """Look up chat_id from the matching scheduler job."""
    cfg = config.get_config()
    for j in cfg.scheduler.jobs:
        if j.type == job_type:
            if job_type == "onchain" and j.chain != chain:
                continue
            if job_type == "watchlist" and (j.watchlist_id or "main") != watchlist_id:
                continue
            return j.chat_id
    return ""


def _run_global(console: Console, state: TuiState) -> int:
    """Run global report with preview/send choice."""
    send_it = _ask_preview_or_send(console)
    chat_id = _job_chat_id("global")

    if send_it:
        from marketwatcher.cli import send
        return _run_action(console, state, "Global Send", send, argparse.Namespace(dry_run=False, chat_id=chat_id))
    else:
        from marketwatcher.cli import render
        return _run_action(console, state, "Global Preview", render, argparse.Namespace(dry_run=True, chat_id=chat_id))


def _select_chain(console: Console) -> str:
    """Interactive chain selection with search."""
    from marketwatcher.chains import POPULAR_CHAINS, find_chain, get_cache_age_days

    # Show cache status
    cache_age = get_cache_age_days()
    if cache_age is None:
        console.print("  [dim]Chain list: no cache (will fetch)[/dim]")
    elif cache_age > 14:
        console.print(f"  [yellow]Chain list: {cache_age}d old (refresh recommended)[/yellow]")
    else:
        console.print(f"  [dim]Chain list: {cache_age}d old[/dim]")

    # Show popular chains
    console.print("\n  [bold]Popular chains:[/bold]")
    for idx, (cid, name) in enumerate(POPULAR_CHAINS, 1):
        console.print(f"    [yellow]{idx}[/yellow]. {name} [dim]({cid})[/dim]")
    console.print("    [yellow]s[/yellow]. [cyan]Search other chains...[/cyan]")

    chain_choices = [str(i) for i in range(1, len(POPULAR_CHAINS) + 1)] + ["s"]
    choice = _ask("\n  Chain", choices=chain_choices, default="1")

    if choice != "s":
        return POPULAR_CHAINS[int(choice) - 1][0]

    # Search mode
    console.print("\n  [bold]Search chains[/bold] (type to filter, Enter to select)")

    query = ""
    while True:
        # Get matching chains
        matches = find_chain(query) if query else find_chain("")

        # Show top matches
        console.print("\n  [dim]Matches:[/dim]")
        for idx, chain in enumerate(matches[:8], 1):
            console.print(f"    [yellow]{idx}[/yellow]. {chain['name']} [dim]({chain['id']})[/dim]")

        if len(matches) > 8:
            console.print(f"    [dim]... and {len(matches) - 8} more[/dim]")

        query = _ask("\n  Search (or number to select)", default="").strip().lower()

        if not query:
            # Empty input, use first match
            if matches:
                return matches[0]["id"]
            return "solana"

        if query.isdigit():
            num = int(query)
            if 1 <= num <= min(8, len(matches)):
                return matches[num - 1]["id"]

        # Check for exact match
        for chain in matches:
            if chain["id"] == query:
                return query

        # Continue searching with this query
        if len(matches) == 1:
            # Only one match, use it
            return matches[0]["id"]


def _run_onchain(console: Console, state: TuiState) -> int:
    """Run on-chain report with chain selection and preview/send choice."""
    chain = _select_chain(console)

    # Ask preview or send
    send_it = _ask_preview_or_send(console)

    from marketwatcher.cli import onchain
    chat_id = _job_chat_id("onchain", chain=chain)
    action_name = f"On-Chain {chain} {'Send' if send_it else 'Preview'}"
    return _run_action(console, state, action_name, onchain,
                       argparse.Namespace(network=chain, dry_run=not send_it, chat_id=chat_id))


def _run_global_onchain(console: Console, state: TuiState) -> int:
    """Run global on-chain report with preview/send choice."""
    send_it = _ask_preview_or_send(console)

    from marketwatcher.cli import global_onchain
    chat_id = _job_chat_id("global_onchain")
    action_name = f"Glb On-Chain {'Send' if send_it else 'Preview'}"
    return _run_action(console, state, action_name, global_onchain,
                       argparse.Namespace(dry_run=not send_it, chat_id=chat_id))


def _run_macro(console: Console, state: TuiState) -> int:
    """Run global macro report with preview/send choice."""
    send_it = _ask_preview_or_send(console)

    from marketwatcher.cli import macro
    chat_id = _job_chat_id("macro")
    action_name = f"Macro {'Send' if send_it else 'Preview'}"
    return _run_action(console, state, action_name, macro,
                       argparse.Namespace(dry_run=not send_it, chat_id=chat_id))


def _select_watchlist(console: Console, current_id: str | None = None) -> str | None:
    """Let user pick a watchlist. Returns watchlist_id or None if cancelled."""
    from marketwatcher.watchlist import list_watchlists

    wls = list_watchlists()
    if not wls:
        console.print("  [dim]No watchlists yet[/dim]")
        return None

    options = [
        (wl["id"], f"{wl['name']} ({wl['id']}, {wl['count']} tokens)")
        for wl in wls
    ]
    picked = _pick_option(
        console,
        "Select Watchlist",
        options,
        current_value=current_id or options[0][0],
    )
    if not picked:
        return None
    return picked


def _watchlist_menu(console: Console, cfg, state: TuiState) -> None:
    """Watchlist management and run menu."""
    from marketwatcher.watchlist import (
        add_token, remove_token, get_watchlist,
        list_watchlists, create_watchlist, delete_watchlist,
        set_token_alerts, clear_token_alerts, set_watchlist_alerts,
    )

    active_id = "main"
    action_cursor = "add"

    while True:
        console.clear()
        console.rule("[bold]Watchlist[/bold]")

        # Show all watchlists summary
        all_wls = list_watchlists()
        if all_wls:
            console.print("\n  [bold]Watchlists:[/bold]")
            for wl in all_wls:
                marker = " [cyan]<[/cyan]" if wl["id"] == active_id else ""
                console.print(f"    {wl['name']} [dim]({wl['count']} tokens)[/dim]{marker}")
        console.print()

        # Show tokens in active watchlist
        wl = get_watchlist(active_id)
        tokens = wl.get("tokens", [])
        console.print(f"  [bold]{wl.get('name', active_id)}[/bold] ({len(tokens)} tokens)")
        if tokens:
            for t in tokens:
                sym = t.get("symbol", "???")
                # Alert indicator
                has_alert = any(
                    k in t for k in ("alert_above", "alert_below", "alert_pct", "alert_pct_up", "alert_pct_down")
                )
                alert_tag = " [yellow]![/yellow]" if has_alert else ""
                if t.get("type") == "dex":
                    console.print(f"    {sym}{alert_tag} [dim](DEX: {t.get('chain', '?')}/{t.get('address', '?')[:10]}...)[/dim]")
                else:
                    console.print(f"    {sym}{alert_tag} [dim](CEX: {t.get('coingecko_id', '?')})[/dim]")
        else:
            console.print("    [dim]No tokens yet[/dim]")
        console.print()

        console.print("[dim]Enter option number (q to go back)[/dim]")

        try:
            choice = _pick_option(
                console,
                "Watchlist Action",
                [
                    ("add", "Add token"),
                    ("remove", "Remove token"),
                    ("run", "Run watchlist report"),
                    ("switch", "Switch watchlist"),
                    ("new", "New watchlist"),
                    ("delete", "Delete watchlist"),
                    ("token_alert", "Set token alert"),
                    ("defaults", "Watchlist alert defaults"),
                ],
                current_value=action_cursor,
            )
            action_cursor = choice
        except QuitTUI:
            return

        if choice == "add":
            try:
                t_choice = _pick_option(
                    console,
                    "Token Type",
                    [("cex", "CEX (CoinGecko-listed)"), ("dex", "DEX (on-chain by contract)")],
                    current_value="cex",
                )

                symbol = _ask("Symbol (e.g. BTC)").strip().upper()
                if not symbol:
                    continue

                if t_choice == "cex":
                    cg_id = _ask("CoinGecko ID (e.g. bitcoin)").strip().lower()
                    if cg_id:
                        ok = add_token(active_id, symbol, token_type="cex", coingecko_id=cg_id)
                        if ok:
                            state.push("Add Token", "OK", symbol)
                            console.print(f"[green]Added {symbol}[/green]")
                        else:
                            console.print(f"[yellow]{symbol} already exists[/yellow]")
                else:
                    chain = _select_chain(console)
                    address = _ask("Contract address").strip()
                    if address:
                        ok = add_token(active_id, symbol, token_type="dex", chain=chain, address=address)
                        if ok:
                            state.push("Add Token", "OK", symbol)
                            console.print(f"[green]Added {symbol}[/green]")
                        else:
                            console.print(f"[yellow]{symbol} already exists[/yellow]")
            except QuitTUI:
                continue

        elif choice == "remove":
            try:
                if not tokens:
                    console.print("[yellow]No tokens to remove[/yellow]")
                else:
                    symbol = _ask("Symbol to remove").strip().upper()
                    if symbol:
                        ok = remove_token(active_id, symbol)
                        if ok:
                            state.push("Remove Token", "OK", symbol)
                            console.print(f"[green]Removed {symbol}[/green]")
                        else:
                            console.print(f"[yellow]{symbol} not found[/yellow]")
            except QuitTUI:
                continue

        elif choice == "run":
            try:
                send_it = _ask_preview_or_send(console)
                from marketwatcher.cli import watchlist_cmd
                chat_id = _job_chat_id("watchlist", watchlist_id=active_id)
                action_name = f"Watchlist {'Send' if send_it else 'Preview'}"
                _run_action(console, state, action_name, watchlist_cmd,
                            argparse.Namespace(dry_run=not send_it, watchlist_id=active_id, chat_id=chat_id))
            except QuitTUI:
                continue

        elif choice == "switch":
            try:
                picked = _select_watchlist(console, active_id)
                if picked:
                    active_id = picked
                    state.push("Switch WL", "OK", active_id)
                    continue  # Skip "Press Enter" — redraw immediately
            except QuitTUI:
                continue

        elif choice == "new":
            try:
                new_id = _ask("Watchlist ID (e.g. defi, memes)").strip().lower()
                if new_id:
                    new_name = _ask("Display name", default=new_id.title()).strip()
                    ok = create_watchlist(new_id, new_name)
                    if ok:
                        active_id = new_id
                        state.push("New WL", "OK", new_id)
                        console.print(f"[green]Created {new_name}[/green]")
                    else:
                        console.print(f"[yellow]{new_id} already exists[/yellow]")
            except QuitTUI:
                continue

        elif choice == "delete":
            try:
                if active_id == "main" and len(all_wls) <= 1:
                    console.print("[yellow]Cannot delete the only watchlist[/yellow]")
                else:
                    target = _select_watchlist(console, active_id)
                    if target:
                        confirm = _ask(f"Delete '{target}'?", choices=["y", "n"], default="n")
                        if confirm == "y":
                            delete_watchlist(target)
                            state.push("Del WL", "OK", target)
                            console.print(f"[green]Deleted {target}[/green]")
                            if active_id == target:
                                remaining = list_watchlists()
                                active_id = remaining[0]["id"] if remaining else "main"
            except QuitTUI:
                continue

        elif choice == "token_alert":
            try:
                if not tokens:
                    console.print("[yellow]No tokens to set alerts on[/yellow]")
                else:
                    token_options: list[tuple[str, str]] = []
                    for t in tokens:
                        sym = t.get("symbol", "???")
                        alerts_info = []
                        if "alert_above" in t:
                            alerts_info.append(f"above ${t['alert_above']:,.0f}")
                        if "alert_below" in t:
                            alerts_info.append(f"below ${t['alert_below']:,.0f}")
                        if "alert_pct_up" in t:
                            alerts_info.append(f"up +{t['alert_pct_up']}%")
                        if "alert_pct_down" in t:
                            alerts_info.append(f"down -{t['alert_pct_down']}%")
                        if "alert_pct" in t:
                            alerts_info.append(f"pct \u00b1{t['alert_pct']}%")
                        info = f" ({', '.join(alerts_info)})" if alerts_info else ""
                        token_options.append((sym, f"{sym}{info}"))

                    picked_symbol = _pick_option(
                        console,
                        "Select Token Alert Target",
                        token_options,
                        current_value=token_options[0][0],
                    )
                    token = next((t for t in tokens if t.get("symbol") == picked_symbol), tokens[0])
                    sym = token.get("symbol", "???")

                    console.print(f"\n  Setting alerts for [bold]{sym}[/bold]")
                    console.print("  Leave blank to skip, 'off' to remove")

                    above_input = _ask("  Price above (USD)", default="").strip()
                    below_input = _ask("  Price below (USD)", default="").strip()
                    pct_up_input = _ask("  Change % up threshold", default="").strip()
                    pct_down_input = _ask("  Change % down threshold", default="").strip()

                    if (
                        above_input.lower() == "off"
                        or below_input.lower() == "off"
                        or pct_up_input.lower() == "off"
                        or pct_down_input.lower() == "off"
                    ):
                        clear_token_alerts(active_id, sym)
                        state.push("Clear Alert", "OK", sym)
                        console.print(f"[green]Cleared alerts for {sym}[/green]")
                    else:
                        def _parse_optional_float(raw: str) -> float | None:
                            if not raw:
                                return None
                            return float(raw)

                        try:
                            a_above = _parse_optional_float(above_input)
                            a_below = _parse_optional_float(below_input)
                            a_pct_up = _parse_optional_float(pct_up_input)
                            a_pct_down = _parse_optional_float(pct_down_input)
                        except ValueError:
                            console.print("[red]Invalid number format in alert fields[/red]")
                            continue

                        if (
                            a_above is not None
                            or a_below is not None
                            or a_pct_up is not None
                            or a_pct_down is not None
                        ):
                            set_token_alerts(
                                active_id,
                                sym,
                                alert_above=a_above,
                                alert_below=a_below,
                                alert_pct_up=a_pct_up,
                                alert_pct_down=a_pct_down,
                            )
                            state.push("Set Alert", "OK", sym)
                            console.print(f"[green]Alerts updated for {sym}[/green]")
                        else:
                            console.print("[dim]No changes[/dim]")
            except QuitTUI:
                continue

        elif choice == "defaults":
            try:
                console.print(f"\n  [bold]Watchlist alert defaults for '{active_id}'[/bold]")
                current_pct_up = wl.get("alert_pct_up")
                current_pct_down = wl.get("alert_pct_down")
                current_pct_legacy = wl.get("alert_pct")
                current_chat = str(wl.get("alert_chat_id", ""))
                if current_pct_up is not None or current_pct_down is not None:
                    up_desc = f"+{current_pct_up}%" if current_pct_up is not None else "off"
                    down_desc = f"-{current_pct_down}%" if current_pct_down is not None else "off"
                    pct_desc = f"up={up_desc}, down={down_desc}"
                else:
                    pct_desc = f"\u00b1{current_pct_legacy}%" if current_pct_legacy is not None else "not set"
                console.print(f"  Current: pct={pct_desc}, channel={_channel_label(cfg, current_chat)}")

                pct_up_input = _ask("  Default pct up threshold (blank to skip)", default="").strip()
                pct_down_input = _ask("  Default pct down threshold (blank to skip)", default="").strip()
                picked_channel = _pick_channel(
                    console,
                    cfg,
                    "Alert channel",
                    current_chat_id=current_chat,
                    allow_default=True,
                    allow_keep=True,
                    allow_off=True,
                )

                try:
                    wl_pct_up = float(pct_up_input) if pct_up_input else None
                    wl_pct_down = float(pct_down_input) if pct_down_input else None
                except ValueError:
                    console.print("[red]Invalid percent value[/red]")
                    continue
                if picked_channel == "__KEEP__":
                    wl_chat = None
                elif picked_channel == "__OFF__":
                    wl_chat = ""
                else:
                    wl_chat = picked_channel

                if wl_pct_up is not None or wl_pct_down is not None or wl_chat is not None:
                    set_watchlist_alerts(
                        active_id,
                        alert_pct_up=wl_pct_up,
                        alert_pct_down=wl_pct_down,
                        alert_chat_id=wl_chat,
                    )
                    state.push("WL Alerts", "OK", active_id)
                    console.print("[green]Watchlist alert defaults updated[/green]")
                else:
                    console.print("[dim]No changes[/dim]")
            except QuitTUI:
                continue

        Prompt.ask("\n[dim]Press Enter[/dim]", default="")


def _show_jobs_table(console: Console, cfg) -> None:
    """Display scheduled jobs."""
    if not cfg.scheduler.jobs:
        console.print("  [dim]No jobs configured[/dim]")
        return

    tz_label = normalize_timezone_label(cfg.scheduler.timezone)
    table = Table(show_header=True, header_style="bold")
    table.add_column("#", style="dim", width=3)
    table.add_column("Job")
    table.add_column("Type", width=12)
    table.add_column("Schedule", width=22)
    table.add_column("Channel", width=30)
    table.add_column("Status", width=6)

    for idx, job in enumerate(cfg.scheduler.jobs, 1):
        status = "[green]ON[/green]" if job.enabled else "[dim]OFF[/dim]"
        if job.type == "global":
            job_type = "Global"
        elif job.type == "global_onchain":
            job_type = "Glb On-Chain"
        elif job.type == "macro":
            job_type = "Macro"
        elif job.type == "watchlist":
            job_type = "Watchlist"
        elif job.type == "alerts":
            job_type = "Alerts"
        else:
            job_type = "On-Chain"
        name = job.display_name()
        channel = _channel_label(cfg, job.chat_id)
        schedule = job.schedule_display()
        if schedule != "--:--":
            schedule = f"{schedule} {tz_label}"
        table.add_row(str(idx), name, job_type, schedule, channel, status)

    console.print(table)


def _add_job(console: Console, cfg, state: TuiState):
    """Add a new job."""
    from marketwatcher.config import JobConfig

    job_type = _pick_option(
        console,
        "Job Type",
        [
            ("global", "Global (market summary)"),
            ("onchain", "On-Chain (chain movers)"),
            ("global_onchain", "Global On-Chain (DeFi TVL overview)"),
            ("macro", "Global Macro (equities, rates, FX, commodities)"),
            ("watchlist", "Watchlist (tracked tokens + alerts)"),
        ],
        current_value="global",
    )
    chain = ""
    watchlist_id = ""

    if job_type == "onchain":
        chain = _select_chain(console)
    elif job_type == "watchlist":
        watchlist_id = _ask("Watchlist ID", default="main").strip()

    # Ask for schedule mode
    sched_choice = _pick_option(
        console,
        "Schedule Mode",
        [("daily", "Daily at specific time"), ("interval", "Every N hours (interval)")],
        current_value="daily",
    )

    time_input = "09:00"
    interval_hours = 0
    offset_minutes = 0

    tz_label = normalize_timezone_label(cfg.scheduler.timezone)

    if sched_choice == "interval":
        interval_hours = _ask_int("Interval (hours)", default="4", min_value=1, max_value=168)
        offset_minutes = _ask_int(
            "Offset (minutes, to stagger jobs)",
            default="0",
            min_value=0,
            max_value=1439,
        )
        time_input = ""
    else:
        time_input = _ask_hhmm(f"Time (HH:MM, {tz_label})", default="09:00")

    # Channel override from preconfigured channel list
    chat_id = _pick_channel(console, cfg, "Channel", current_chat_id="", allow_default=True)

    # Create job
    job = JobConfig(
        id=_generate_job_id(job_type, chain),
        type=job_type,
        chain=chain,
        time=time_input,
        enabled=True,
        interval_hours=interval_hours,
        offset_minutes=offset_minutes,
        chat_id=chat_id,
        watchlist_id=watchlist_id,
    )

    cfg.scheduler.jobs.append(job)
    _write_schedules_yaml(cfg)
    new_cfg = config.reload_config()
    state.push("Add Job", "OK", job.display_name())
    console.print(f"[green]Added:[/green] {job.display_name()} ({job.schedule_display()})")
    return new_cfg


def _select_job_index(console: Console, cfg, prompt_text: str = "Select Job") -> int:
    """Let user select a job by number. Returns -1 if cancelled."""
    if not cfg.scheduler.jobs:
        console.print("[yellow]No jobs to select[/yellow]")
        return -1

    tz_label = normalize_timezone_label(cfg.scheduler.timezone)
    options: list[tuple[str, str]] = []
    for idx, job in enumerate(cfg.scheduler.jobs):
        status = "ON" if job.enabled else "OFF"
        schedule = job.schedule_display()
        if schedule != "--:--":
            schedule = f"{schedule} {tz_label}"
        options.append((str(idx), f"{job.display_name()} | {schedule} | {status}"))

    choice = _pick_option(console, prompt_text, options, current_value="0")
    idx = int(choice)
    if idx >= len(cfg.scheduler.jobs):
        return -1
    return idx


def _edit_job(console: Console, cfg, state: TuiState):
    """Edit a job's schedule, chat_id, and watchlist_id."""
    idx = _select_job_index(console, cfg, "Job # to edit")
    if idx < 0:
        return cfg

    job = cfg.scheduler.jobs[idx]
    console.print(f"\n  Editing: [bold]{job.display_name()}[/bold] (current: {job.schedule_display()})")

    # Schedule
    default_mode = "interval" if job.interval_hours > 0 else "daily"
    sched_choice = _pick_option(
        console,
        "Schedule Mode",
        [("daily", "Daily at specific time"), ("interval", "Every N hours (interval)")],
        current_value=default_mode,
    )

    tz_label = normalize_timezone_label(cfg.scheduler.timezone)

    if sched_choice == "interval":
        job.interval_hours = _ask_int(
            "Interval (hours)",
            default=str(job.interval_hours or 4),
            min_value=1,
            max_value=168,
        )
        job.offset_minutes = _ask_int(
            "Offset (minutes)",
            default=str(job.offset_minutes),
            min_value=0,
            max_value=1439,
        )
        job.time = ""
    else:
        new_time = _ask_hhmm(f"Time (HH:MM, {tz_label})", default=job.time or "09:00")
        job.time = new_time
        job.interval_hours = 0
        job.offset_minutes = 0

    # Channel override
    job.chat_id = _pick_channel(
        console,
        cfg,
        "Channel",
        current_chat_id=job.chat_id,
        allow_default=True,
    )

    # Watchlist ID (only for watchlist jobs)
    if job.type == "watchlist":
        new_wl = _ask("Watchlist ID", default=job.watchlist_id or "main").strip()
        job.watchlist_id = new_wl

    _write_schedules_yaml(cfg)
    new_cfg = config.reload_config()
    state.push("Edit Job", "OK", f"{job.display_name()} {job.schedule_display()}")
    console.print(f"[green]Updated:[/green] {job.display_name()} ({job.schedule_display()})")
    return new_cfg


def _toggle_job(console: Console, cfg, state: TuiState):
    """Toggle a job on/off."""
    idx = _select_job_index(console, cfg, "Job # to toggle")
    if idx < 0:
        return cfg

    job = cfg.scheduler.jobs[idx]
    job.enabled = not job.enabled
    status = "ON" if job.enabled else "OFF"

    _write_schedules_yaml(cfg)
    new_cfg = config.reload_config()
    state.push("Toggle Job", "OK", f"{job.display_name()}={status}")
    console.print(f"[green]Toggled:[/green] {job.display_name()} is now {status}")
    return new_cfg


def _delete_job(console: Console, cfg, state: TuiState):
    """Delete a job."""
    idx = _select_job_index(console, cfg, "Job # to delete")
    if idx < 0:
        return cfg

    job = cfg.scheduler.jobs[idx]
    confirm = _ask(f"Delete {job.display_name()}?", choices=["y", "n"], default="n")

    if confirm == "y":
        cfg.scheduler.jobs.pop(idx)
        _write_schedules_yaml(cfg)
        new_cfg = config.reload_config()
        state.push("Delete Job", "OK", job.display_name())
        console.print(f"[green]Deleted:[/green] {job.display_name()}")
        return new_cfg

    return cfg


def _run_scheduler(console: Console, state: TuiState):
    """Run the scheduler loop."""
    from marketwatcher.cli import scheduler as scheduler_cmd

    console.print("\n[yellow]Starting scheduler... (Ctrl+C to stop)[/yellow]\n")
    return _run_action(console, state, "Scheduler", scheduler_cmd, argparse.Namespace())


def _set_scheduler_timezone(console: Console, cfg, state: TuiState):
    """Set scheduler timezone used for scheduler input/display."""
    current = normalize_timezone_label(cfg.scheduler.timezone)
    options = _timezone_options(cfg)
    selected = _pick_option(
        console,
        "Scheduler Timezone",
        [(tz_label, tz_label) for tz_label in options],
        current_value=current,
    )
    tz_input = selected

    cfg.scheduler.timezone = tz_input
    _write_schedules_yaml(cfg)
    _write_settings_scheduler_timezone(cfg, tz_input)
    new_cfg = config.reload_config()
    state.push("Scheduler TZ", "OK", normalize_timezone_label(new_cfg.scheduler.timezone))
    console.print(f"[green]Scheduler timezone set to {normalize_timezone_label(new_cfg.scheduler.timezone)}[/green]")
    return new_cfg


def _scheduler_menu(console: Console, cfg, state: TuiState):
    """Scheduler management menu."""
    action_cursor = "add"
    while True:
        console.clear()
        tz_label = normalize_timezone_label(cfg.scheduler.timezone)
        console.rule(f"[bold]Scheduler ({tz_label})[/bold]")
        console.print()
        _show_jobs_table(console, cfg)
        console.print()

        console.print("[dim]Enter option number (q to go back)[/dim]")

        try:
            choice = _pick_option(
                console,
                "Scheduler Action",
                [
                    ("add", "Add job"),
                    ("edit", "Edit schedule"),
                    ("toggle", "Toggle on/off"),
                    ("delete", "Delete job"),
                    ("run", "Run scheduler"),
                    ("timezone", "Set timezone"),
                ],
                current_value=action_cursor,
            )
            action_cursor = choice
        except QuitTUI:
            return cfg

        try:
            if choice == "add":
                cfg = _add_job(console, cfg, state)
            elif choice == "edit":
                cfg = _edit_job(console, cfg, state)
            elif choice == "toggle":
                cfg = _toggle_job(console, cfg, state)
            elif choice == "delete":
                cfg = _delete_job(console, cfg, state)
            elif choice == "run":
                _run_scheduler(console, state)
                continue
            elif choice == "timezone":
                cfg = _set_scheduler_timezone(console, cfg, state)
        except QuitTUI:
            continue

        if choice != "run":
            Prompt.ask("\n[dim]Press Enter[/dim]", default="")


def _show_config(console: Console, cfg) -> None:
    """Display config summary."""
    from marketwatcher.chains import get_cache_age_days, get_chains

    console.print()
    console.rule("[bold]Config[/bold]")
    console.print()

    table = Table.grid(padding=(0, 3))
    table.add_column(style="bold cyan")
    table.add_column()

    table.add_row("Telegram", "[green]Configured[/green]" if cfg.telegram.bot_token else "[red]Missing token[/red]")
    table.add_row("Chat ID", cfg.telegram.chat_id or "[red]NOT SET[/red]")
    table.add_row("Sched TZ", normalize_timezone_label(cfg.scheduler.timezone))
    if cfg.telegram.channels:
        channels = ", ".join(ch.name for ch in cfg.telegram.channels)
        table.add_row("Channels", channels)
    else:
        table.add_row("Channels", "[yellow]No presets[/yellow]")
    table.add_row("Database", cfg.database_path)

    # Chain cache status
    chain_age = get_cache_age_days()
    chain_count = len(get_chains())
    if chain_age is None:
        chain_status = "[yellow]No cache[/yellow]"
    elif chain_age > 14:
        chain_status = f"[yellow]{chain_age}d old[/yellow]"
    else:
        chain_status = f"[green]{chain_age}d old[/green]"
    table.add_row("Chain list", f"{chain_count} chains ({chain_status})")

    console.print(table)

    # Schedule summary
    console.print("\n[bold]Scheduled Jobs:[/bold]")
    tz_label = normalize_timezone_label(cfg.scheduler.timezone)
    if cfg.scheduler.jobs:
        for job in cfg.scheduler.jobs:
            status = "[green]ON[/green]" if job.enabled else "[dim]OFF[/dim]"
            schedule = job.schedule_display()
            if schedule != "--:--":
                schedule = f"{schedule} {tz_label}"
            console.print(f"  {job.display_name()}: {schedule} {status}")
    else:
        console.print("  [dim]No jobs configured[/dim]")

    # On-chain settings
    console.print("\n[bold]On-Chain Filters:[/bold]")
    console.print(f"  Min liquidity: ${cfg.onchain.min_liquidity_usd:,.0f}")
    console.print(f"  Min volume: ${cfg.onchain.min_volume_usd:,.0f}")
    console.print(f"  Show: {cfg.onchain.top_gainers_count} gainers, {cfg.onchain.top_losers_count} losers")


def _config_menu(console: Console, cfg, state: TuiState):
    """Config menu."""
    action_cursor = "channels"
    while True:
        console.clear()
        _show_config(console, cfg)
        console.print()

        console.print("[dim]Enter option number (q to go back)[/dim]")

        try:
            choice = _pick_option(
                console,
                "Config Action",
                [("channels", "Manage channels"), ("reload", "Reload config")],
                current_value=action_cursor,
            )
            action_cursor = choice
        except QuitTUI:
            return cfg

        if choice == "channels":
            cfg = _channels_menu(console, cfg, state)
            continue

        if choice == "reload":
            try:
                cfg = config.reload_config()
                state.push("Reload Config", "OK")
                console.print("[green]Config reloaded[/green]")
            except Exception as exc:
                state.push("Reload Config", "ERR", str(exc))
                console.print(f"[red]Reload failed:[/red] {exc}")
            Prompt.ask("\n[dim]Press Enter[/dim]", default="")


def run_tui() -> int:
    """Run the TUI application."""
    console = Console()
    state = TuiState()

    try:
        cfg = config.get_config()
    except Exception as exc:
        console.print(f"[red]Failed to load config:[/red] {exc}")
        return 1

    home_cursor = "global"
    while True:
        _show_home(console, cfg, state)

        try:
            choice = _pick_option(
                console,
                "Main Menu",
                [
                    ("global", "Global report"),
                    ("macro", "Macro report"),
                    ("global_onchain", "Global On-Chain report"),
                    ("onchain", "On-Chain report"),
                    ("watchlist", "Watchlist menu"),
                    ("scheduler", "Scheduler menu"),
                    ("config", "Config/settings"),
                    ("doctor", "Doctor"),
                ],
                current_value=home_cursor,
            )
            home_cursor = choice
        except QuitTUI:
            console.print("[yellow]Bye[/yellow]")
            return 0

        try:
            if choice == "global":
                _run_global(console, state)
            elif choice == "macro":
                _run_macro(console, state)
            elif choice == "global_onchain":
                _run_global_onchain(console, state)
            elif choice == "onchain":
                _run_onchain(console, state)
            elif choice == "watchlist":
                _watchlist_menu(console, cfg, state)
            elif choice == "scheduler":
                cfg = _scheduler_menu(console, cfg, state)
            elif choice == "config":
                cfg = _config_menu(console, cfg, state)
            elif choice == "doctor":
                _run_doctor(console, state)
        except QuitTUI:
            continue

        if choice not in ["watchlist", "scheduler"]:
            Prompt.ask("\n[dim]Press Enter[/dim]", default="")


__all__ = ["run_tui"]
