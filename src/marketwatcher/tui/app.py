"""Terminal UI for MarketWatcher.

Simplified operator console for:
- Running reports (global market, on-chain)
- Managing scheduled jobs
- Checking system health
"""

from __future__ import annotations

import argparse
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

    # Actions menu
    menu = Table(show_header=False, box=None, pad_edge=False)
    menu.add_column(style="bold yellow", width=4)
    menu.add_column(style="bold")
    menu.add_column(style="dim")
    menu.add_row("1.", "Global Crypto", "Market summary report")
    menu.add_row("2.", "Global Macro", "Equities, rates, FX, commodities")
    menu.add_row("3.", "Global On-Chain", "DeFi TVL overview")
    menu.add_row("4.", "On-Chain", "Chain movers report")
    menu.add_row("5.", "Watchlist", "Manage & run watchlists")
    menu.add_row("6.", "Scheduler", "Manage scheduled jobs")
    menu.add_row("7.", "Config", "View settings")
    menu.add_row("8.", "Doctor", "Check system health")
    menu.add_row("q.", "Quit", "")

    console.print(Panel(menu, title="Actions", border_style="cyan"))

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


def _ask_preview_or_send() -> bool:
    """Ask user if they want to preview or send. Returns True for send."""
    choice = _ask("Preview or Send", choices=["p", "s"], default="p")
    return choice == "s"


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
    send_it = _ask_preview_or_send()
    chat_id = _job_chat_id("global")

    if send_it:
        from marketwatcher.cli import send
        return _run_action(console, state, "Global Send", send, argparse.Namespace(dry_run=False, chat_id=chat_id))
    else:
        from marketwatcher.cli import render
        return _run_action(console, state, "Global Preview", render, argparse.Namespace(dry_run=True, chat_id=chat_id))


def _select_chain(console: Console) -> str:
    """Interactive chain selection with search."""
    from marketwatcher.chains import POPULAR_CHAINS, find_chain, get_cache_age_days, refresh_chains

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
    console.print(f"    [yellow]0[/yellow]. [cyan]Search other chains...[/cyan]")

    choice = _ask("\n  Chain", default="1")

    # Quick select from popular
    if choice.isdigit():
        num = int(choice)
        if 1 <= num <= len(POPULAR_CHAINS):
            return POPULAR_CHAINS[num - 1][0]
        elif num == 0:
            pass  # Fall through to search
        else:
            console.print(f"  [red]Invalid choice, using solana[/red]")
            return "solana"
    else:
        # Direct input - try to match
        return choice.strip().lower() or "solana"

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
    send_it = _ask_preview_or_send()

    from marketwatcher.cli import onchain
    chat_id = _job_chat_id("onchain", chain=chain)
    action_name = f"On-Chain {chain} {'Send' if send_it else 'Preview'}"
    return _run_action(console, state, action_name, onchain,
                       argparse.Namespace(network=chain, dry_run=not send_it, chat_id=chat_id))


def _run_global_onchain(console: Console, state: TuiState) -> int:
    """Run global on-chain report with preview/send choice."""
    send_it = _ask_preview_or_send()

    from marketwatcher.cli import global_onchain
    chat_id = _job_chat_id("global_onchain")
    action_name = f"Glb On-Chain {'Send' if send_it else 'Preview'}"
    return _run_action(console, state, action_name, global_onchain,
                       argparse.Namespace(dry_run=not send_it, chat_id=chat_id))


def _run_macro(console: Console, state: TuiState) -> int:
    """Run global macro report with preview/send choice."""
    send_it = _ask_preview_or_send()

    from marketwatcher.cli import macro
    chat_id = _job_chat_id("macro")
    action_name = f"Macro {'Send' if send_it else 'Preview'}"
    return _run_action(console, state, action_name, macro,
                       argparse.Namespace(dry_run=not send_it, chat_id=chat_id))


def _select_watchlist(console: Console) -> str | None:
    """Let user pick a watchlist. Returns watchlist_id or None if cancelled."""
    from marketwatcher.watchlist import list_watchlists

    wls = list_watchlists()
    if not wls:
        console.print("  [dim]No watchlists yet[/dim]")
        return None

    console.print("\n  [bold]Watchlists:[/bold]")
    for idx, wl in enumerate(wls, 1):
        console.print(f"    [yellow]{idx}[/yellow]. {wl['name']} [dim]({wl['id']}, {wl['count']} tokens)[/dim]")
    console.print(f"    [yellow]0[/yellow]. Cancel")

    choices = [str(i) for i in range(len(wls) + 1)]
    choice = _ask("Watchlist", choices=choices, default="1")
    idx = int(choice) - 1
    if idx < 0 or idx >= len(wls):
        return None
    return wls[idx]["id"]


def _watchlist_menu(console: Console, cfg, state: TuiState) -> None:
    """Watchlist management and run menu."""
    from marketwatcher.watchlist import (
        add_token, remove_token, get_watchlist,
        list_watchlists, create_watchlist, delete_watchlist,
        set_token_alerts, clear_token_alerts, set_watchlist_alerts,
    )

    active_id = "main"

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
                has_alert = any(k in t for k in ("alert_above", "alert_below", "alert_pct"))
                alert_tag = " [yellow]![/yellow]" if has_alert else ""
                if t.get("type") == "dex":
                    console.print(f"    {sym}{alert_tag} [dim](DEX: {t.get('chain', '?')}/{t.get('address', '?')[:10]}...)[/dim]")
                else:
                    console.print(f"    {sym}{alert_tag} [dim](CEX: {t.get('coingecko_id', '?')})[/dim]")
        else:
            console.print("    [dim]No tokens yet[/dim]")
        console.print()

        menu = Table(show_header=False, box=None, pad_edge=False)
        menu.add_column(style="bold yellow", width=4)
        menu.add_column()
        menu.add_row("1.", "Add token")
        menu.add_row("2.", "Remove token")
        menu.add_row("3.", "Run watchlist report")
        menu.add_row("4.", "Switch watchlist")
        menu.add_row("5.", "New watchlist")
        menu.add_row("6.", "Delete watchlist")
        menu.add_row("7.", "Set token alert")
        menu.add_row("8.", "Watchlist alert defaults")
        menu.add_row("q.", "Back")
        console.print(Panel(menu, border_style="cyan"))

        try:
            choice = _ask("Option", choices=["0", "1", "2", "3", "4", "5", "6", "7", "8"], default="0")
        except QuitTUI:
            return

        if choice == "0":
            return
        elif choice == "1":
            try:
                console.print("\n  [bold]Token type:[/bold]")
                console.print("    [yellow]1[/yellow]. CEX (CoinGecko-listed)")
                console.print("    [yellow]2[/yellow]. DEX (on-chain by contract)")
                t_choice = _ask("Type", choices=["1", "2"], default="1")

                symbol = _ask("Symbol (e.g. BTC)").strip().upper()
                if not symbol:
                    continue

                if t_choice == "1":
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

        elif choice == "2":
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

        elif choice == "3":
            try:
                send_it = _ask_preview_or_send()
                from marketwatcher.cli import watchlist_cmd
                chat_id = _job_chat_id("watchlist", watchlist_id=active_id)
                action_name = f"Watchlist {'Send' if send_it else 'Preview'}"
                _run_action(console, state, action_name, watchlist_cmd,
                            argparse.Namespace(dry_run=not send_it, watchlist_id=active_id, chat_id=chat_id))
            except QuitTUI:
                continue

        elif choice == "4":
            try:
                picked = _select_watchlist(console)
                if picked:
                    active_id = picked
                    state.push("Switch WL", "OK", active_id)
                    continue  # Skip "Press Enter" — redraw immediately
            except QuitTUI:
                continue

        elif choice == "5":
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

        elif choice == "6":
            try:
                if active_id == "main" and len(all_wls) <= 1:
                    console.print("[yellow]Cannot delete the only watchlist[/yellow]")
                else:
                    target = _select_watchlist(console)
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

        elif choice == "7":
            try:
                if not tokens:
                    console.print("[yellow]No tokens to set alerts on[/yellow]")
                else:
                    console.print("\n  [bold]Select token:[/bold]")
                    for idx, t in enumerate(tokens, 1):
                        sym = t.get("symbol", "???")
                        alerts_info = []
                        if "alert_above" in t:
                            alerts_info.append(f"above ${t['alert_above']:,.0f}")
                        if "alert_below" in t:
                            alerts_info.append(f"below ${t['alert_below']:,.0f}")
                        if "alert_pct" in t:
                            alerts_info.append(f"pct {t['alert_pct']}%")
                        info = f" [yellow]({', '.join(alerts_info)})[/yellow]" if alerts_info else ""
                        console.print(f"    [yellow]{idx}[/yellow]. {sym}{info}")

                    t_choices = [str(i) for i in range(1, len(tokens) + 1)]
                    t_pick = _ask("Token #", choices=t_choices, default="1")
                    token = tokens[int(t_pick) - 1]
                    sym = token.get("symbol", "???")

                    console.print(f"\n  Setting alerts for [bold]{sym}[/bold]")
                    console.print("  Leave blank to skip, 'off' to remove")

                    above_input = _ask("  Price above (USD)", default="").strip()
                    below_input = _ask("  Price below (USD)", default="").strip()
                    pct_input = _ask("  Change % threshold", default="").strip()

                    if above_input.lower() == "off" or below_input.lower() == "off" or pct_input.lower() == "off":
                        clear_token_alerts(active_id, sym)
                        state.push("Clear Alert", "OK", sym)
                        console.print(f"[green]Cleared alerts for {sym}[/green]")
                    else:
                        a_above = float(above_input) if above_input else None
                        a_below = float(below_input) if below_input else None
                        a_pct = float(pct_input) if pct_input else None
                        if a_above is not None or a_below is not None or a_pct is not None:
                            set_token_alerts(active_id, sym, alert_above=a_above, alert_below=a_below, alert_pct=a_pct)
                            state.push("Set Alert", "OK", sym)
                            console.print(f"[green]Alerts updated for {sym}[/green]")
                        else:
                            console.print("[dim]No changes[/dim]")
            except QuitTUI:
                continue

        elif choice == "8":
            try:
                console.print(f"\n  [bold]Watchlist alert defaults for '{active_id}'[/bold]")
                current_pct = wl.get("alert_pct", "not set")
                current_chat = wl.get("alert_chat_id", "default")
                console.print(f"  Current: pct={current_pct}, channel={current_chat}")

                pct_input = _ask("  Default pct threshold (blank to skip)", default="").strip()
                chat_input = _ask("  Alert chat_id (blank to skip, 'off' to remove)", default="").strip()

                wl_pct = float(pct_input) if pct_input else None
                wl_chat = "" if chat_input.lower() == "off" else (chat_input if chat_input else None)

                if wl_pct is not None or wl_chat is not None:
                    set_watchlist_alerts(active_id, alert_pct=wl_pct, alert_chat_id=wl_chat)
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

    table = Table(show_header=True, header_style="bold")
    table.add_column("#", style="dim", width=3)
    table.add_column("Job")
    table.add_column("Type", width=12)
    table.add_column("Schedule", width=14)
    table.add_column("Channel", width=14)
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
        channel = job.chat_id[-6:] if job.chat_id else "[dim]default[/dim]"
        table.add_row(str(idx), name, job_type, job.schedule_display(), channel, status)

    console.print(table)


def _add_job(console: Console, cfg, state: TuiState):
    """Add a new job."""
    from marketwatcher.config import JobConfig

    console.print("\n  [bold]Job type:[/bold]")
    console.print("    [yellow]1[/yellow]. Global (market summary)")
    console.print("    [yellow]2[/yellow]. On-Chain (chain movers)")
    console.print("    [yellow]3[/yellow]. Global On-Chain (DeFi TVL overview)")
    console.print("    [yellow]4[/yellow]. Global Macro (equities, rates, FX, commodities)")
    console.print("    [yellow]5[/yellow]. Watchlist (tracked tokens + alerts)")

    choice = _ask("Type", choices=["1", "2", "3", "4", "5"], default="1")

    if choice == "1":
        job_type = "global"
    elif choice == "3":
        job_type = "global_onchain"
    elif choice == "4":
        job_type = "macro"
    elif choice == "5":
        job_type = "watchlist"
    else:
        job_type = "onchain"
    chain = ""
    watchlist_id = ""

    if job_type == "onchain":
        chain = _select_chain(console)
    elif job_type == "watchlist":
        watchlist_id = _ask("Watchlist ID", default="main").strip()

    # Ask for schedule mode
    console.print("\n  [bold]Schedule mode:[/bold]")
    console.print("    [yellow]1[/yellow]. Daily at specific time")
    console.print("    [yellow]2[/yellow]. Every N hours (interval)")
    sched_choice = _ask("Mode", choices=["1", "2"], default="1")

    time_input = "09:00"
    interval_hours = 0
    offset_minutes = 0

    if sched_choice == "2":
        interval_input = _ask("Interval (hours)", default="4").strip()
        interval_hours = int(interval_input)
        offset_input = _ask("Offset (minutes, to stagger jobs)", default="0").strip()
        offset_minutes = int(offset_input)
        time_input = ""
    else:
        time_input = _ask("Time (HH:MM)", default="09:00").strip()

    # Optional channel override
    chat_id = _ask("Chat ID (blank for default)", default="").strip()

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


def _select_job_index(console: Console, cfg, prompt_text: str = "Job #") -> int:
    """Let user select a job by number. Returns -1 if cancelled."""
    if not cfg.scheduler.jobs:
        console.print("[yellow]No jobs to select[/yellow]")
        return -1

    _show_jobs_table(console, cfg)
    choices = [str(i) for i in range(len(cfg.scheduler.jobs) + 1)]
    choice = _ask(prompt_text, choices=choices, default="1")

    idx = int(choice) - 1
    if idx < 0 or idx >= len(cfg.scheduler.jobs):
        return -1
    return idx


def _edit_job(console: Console, cfg, state: TuiState):
    """Edit a job's schedule, chat_id, and watchlist_id."""
    idx = _select_job_index(console, cfg, "Job # to edit (0 to cancel)")
    if idx < 0:
        return cfg

    job = cfg.scheduler.jobs[idx]
    console.print(f"\n  Editing: [bold]{job.display_name()}[/bold] (current: {job.schedule_display()})")

    # Schedule
    console.print("\n  [bold]Schedule mode:[/bold]")
    console.print("    [yellow]1[/yellow]. Daily at specific time")
    console.print("    [yellow]2[/yellow]. Every N hours (interval)")
    default_mode = "2" if job.interval_hours > 0 else "1"
    sched_choice = _ask("Mode", choices=["1", "2"], default=default_mode)

    if sched_choice == "2":
        interval_input = _ask("Interval (hours)", default=str(job.interval_hours or 4)).strip()
        job.interval_hours = int(interval_input)
        offset_input = _ask("Offset (minutes)", default=str(job.offset_minutes)).strip()
        job.offset_minutes = int(offset_input)
        job.time = ""
    else:
        new_time = _ask("Time (HH:MM)", default=job.time or "09:00").strip()
        job.time = new_time
        job.interval_hours = 0
        job.offset_minutes = 0

    # Chat ID
    current_chat = job.chat_id or "(default)"
    new_chat = _ask(f"Chat ID [dim]current: {current_chat}[/dim]", default=job.chat_id or "").strip()
    job.chat_id = new_chat

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
    idx = _select_job_index(console, cfg, "Job # to toggle (0 to cancel)")
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
    idx = _select_job_index(console, cfg, "Job # to delete (0 to cancel)")
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


def _scheduler_menu(console: Console, cfg, state: TuiState):
    """Scheduler management menu."""
    while True:
        console.clear()
        console.rule("[bold]Scheduler[/bold]")
        console.print()
        _show_jobs_table(console, cfg)
        console.print()

        menu = Table(show_header=False, box=None, pad_edge=False)
        menu.add_column(style="bold yellow", width=4)
        menu.add_column()
        menu.add_row("1.", "Add job")
        menu.add_row("2.", "Edit schedule")
        menu.add_row("3.", "Toggle on/off")
        menu.add_row("4.", "Delete job")
        menu.add_row("5.", "Run scheduler")
        menu.add_row("q.", "Back")
        console.print(Panel(menu, border_style="cyan"))

        try:
            choice = _ask("Option", choices=["0", "1", "2", "3", "4", "5"], default="0")
        except QuitTUI:
            return cfg

        if choice == "0":
            return cfg

        try:
            if choice == "1":
                cfg = _add_job(console, cfg, state)
            elif choice == "2":
                cfg = _edit_job(console, cfg, state)
            elif choice == "3":
                cfg = _toggle_job(console, cfg, state)
            elif choice == "4":
                cfg = _delete_job(console, cfg, state)
            elif choice == "5":
                _run_scheduler(console, state)
                continue
        except QuitTUI:
            continue

        if choice != "5":
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
    if cfg.scheduler.jobs:
        for job in cfg.scheduler.jobs:
            status = "[green]ON[/green]" if job.enabled else "[dim]OFF[/dim]"
            console.print(f"  {job.display_name()}: {job.schedule_display()} {status}")
    else:
        console.print("  [dim]No jobs configured[/dim]")

    # On-chain settings
    console.print("\n[bold]On-Chain Filters:[/bold]")
    console.print(f"  Min liquidity: ${cfg.onchain.min_liquidity_usd:,.0f}")
    console.print(f"  Min volume: ${cfg.onchain.min_volume_usd:,.0f}")
    console.print(f"  Show: {cfg.onchain.top_gainers_count} gainers, {cfg.onchain.top_losers_count} losers")


def _config_menu(console: Console, cfg, state: TuiState):
    """Config view and reload menu."""
    _show_config(console, cfg)
    console.print()

    try:
        choice = _ask("Reload config? (y/n)", choices=["y", "n"], default="n")
    except QuitTUI:
        return cfg

    if choice == "y":
        try:
            cfg = config.reload_config()
            state.push("Reload Config", "OK")
            console.print("[green]Config reloaded[/green]")
        except Exception as exc:
            state.push("Reload Config", "ERR", str(exc))
            console.print(f"[red]Reload failed:[/red] {exc}")

    return cfg


def run_tui() -> int:
    """Run the TUI application."""
    console = Console()
    state = TuiState()

    try:
        cfg = config.get_config()
    except Exception as exc:
        console.print(f"[red]Failed to load config:[/red] {exc}")
        return 1

    while True:
        _show_home(console, cfg, state)

        try:
            choice = _ask("\nSelect", choices=["0", "1", "2", "3", "4", "5", "6", "7", "8"], default="0")
        except QuitTUI:
            console.print("[yellow]Bye[/yellow]")
            return 0

        if choice == "0":
            console.print("[yellow]Bye[/yellow]")
            return 0

        try:
            if choice == "1":
                _run_global(console, state)
            elif choice == "2":
                _run_macro(console, state)
            elif choice == "3":
                _run_global_onchain(console, state)
            elif choice == "4":
                _run_onchain(console, state)
            elif choice == "5":
                _watchlist_menu(console, cfg, state)
            elif choice == "6":
                cfg = _scheduler_menu(console, cfg, state)
            elif choice == "7":
                cfg = _config_menu(console, cfg, state)
            elif choice == "8":
                _run_doctor(console, state)
        except QuitTUI:
            continue

        if choice not in ["0", "5", "6"]:
            Prompt.ask("\n[dim]Press Enter[/dim]", default="")


__all__ = ["run_tui"]
