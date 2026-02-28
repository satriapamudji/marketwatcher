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


def _count_enabled_jobs(cfg) -> tuple[int, int]:
    """Count enabled vs total jobs."""
    jobs = [
        cfg.scheduler.global_crypto.enabled,
        cfg.scheduler.onchain_solana.enabled,
        cfg.scheduler.onchain_base.enabled,
    ]
    enabled = sum(1 for j in jobs if j)
    return (enabled, len(jobs))


def _schedules_yaml_path(cfg) -> Path:
    return Path(cfg.project_root).resolve() / "config" / "schedules.yaml"


def _write_schedules_yaml(cfg) -> None:
    """Save scheduler config to YAML."""
    path = _schedules_yaml_path(cfg)
    path.parent.mkdir(parents=True, exist_ok=True)

    jobs_data = []
    for job in cfg.scheduler.jobs:
        jobs_data.append({
            "id": job.id,
            "type": job.type,
            "chain": job.chain,
            "time": job.time,
            "enabled": job.enabled,
        })

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
    if job_type == "global":
        return f"global_{uuid.uuid4().hex[:6]}"
    else:
        return f"onchain_{chain}_{uuid.uuid4().hex[:6]}"


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
    menu.add_row("1.", "Doctor", "Check system health")
    menu.add_row("2.", "Global", "Market summary report")
    menu.add_row("3.", "On-Chain", "Chain movers report")
    menu.add_row("4.", "Scheduler", "Manage scheduled jobs")
    menu.add_row("5.", "Config", "View settings")
    menu.add_row("0.", "Quit", "")

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
    choice = Prompt.ask("Preview or Send", choices=["p", "s"], default="p")
    return choice == "s"


def _run_doctor(console: Console, state: TuiState) -> int:
    from marketwatcher.cli import doctor
    return _run_action(console, state, "Doctor", doctor, argparse.Namespace())


def _run_global(console: Console, state: TuiState) -> int:
    """Run global report with preview/send choice."""
    send_it = _ask_preview_or_send()

    if send_it:
        from marketwatcher.cli import send
        return _run_action(console, state, "Global Send", send, argparse.Namespace(dry_run=False))
    else:
        from marketwatcher.cli import render
        return _run_action(console, state, "Global Preview", render, argparse.Namespace(dry_run=True))


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

    choice = Prompt.ask("\n  Chain", default="1")

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

        query = Prompt.ask("\n  Search (or number to select)", default="").strip().lower()

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
    action_name = f"On-Chain {chain} {'Send' if send_it else 'Preview'}"
    return _run_action(console, state, action_name, onchain,
                       argparse.Namespace(network=chain, dry_run=not send_it))


def _show_jobs_table(console: Console, cfg) -> None:
    """Display scheduled jobs."""
    if not cfg.scheduler.jobs:
        console.print("  [dim]No jobs configured[/dim]")
        return

    table = Table(show_header=True, header_style="bold")
    table.add_column("#", style="dim", width=3)
    table.add_column("Job")
    table.add_column("Type", width=10)
    table.add_column("Time", width=8)
    table.add_column("Status", width=6)

    for idx, job in enumerate(cfg.scheduler.jobs, 1):
        status = "[green]ON[/green]" if job.enabled else "[dim]OFF[/dim]"
        job_type = "Global" if job.type == "global" else f"On-Chain"
        name = job.display_name()
        table.add_row(str(idx), name, job_type, job.time or "--:--", status)

    console.print(table)


def _add_job(console: Console, cfg, state: TuiState):
    """Add a new job."""
    from marketwatcher.config import JobConfig

    console.print("\n  [bold]Job type:[/bold]")
    console.print("    [yellow]1[/yellow]. Global (market summary)")
    console.print("    [yellow]2[/yellow]. On-Chain (chain movers)")
    console.print("    [yellow]0[/yellow]. Cancel")

    choice = Prompt.ask("Type", choices=["0", "1", "2"], default="1")

    if choice == "0":
        return cfg

    job_type = "global" if choice == "1" else "onchain"
    chain = ""

    if job_type == "onchain":
        chain = _select_chain(console)

    time_input = Prompt.ask("Time (HH:MM)", default="09:00").strip()

    # Create job
    job = JobConfig(
        id=_generate_job_id(job_type, chain),
        type=job_type,
        chain=chain,
        time=time_input,
        enabled=True,
    )

    cfg.scheduler.jobs.append(job)
    _write_schedules_yaml(cfg)
    new_cfg = config.reload_config()
    state.push("Add Job", "OK", job.display_name())
    console.print(f"[green]Added:[/green] {job.display_name()} at {time_input}")
    return new_cfg


def _select_job_index(console: Console, cfg, prompt_text: str = "Job #") -> int:
    """Let user select a job by number. Returns -1 if cancelled."""
    if not cfg.scheduler.jobs:
        console.print("[yellow]No jobs to select[/yellow]")
        return -1

    _show_jobs_table(console, cfg)
    choices = [str(i) for i in range(len(cfg.scheduler.jobs) + 1)]
    choice = Prompt.ask(prompt_text, choices=choices, default="1")

    idx = int(choice) - 1
    if idx < 0 or idx >= len(cfg.scheduler.jobs):
        return -1
    return idx


def _edit_job(console: Console, cfg, state: TuiState):
    """Edit a job's time."""
    idx = _select_job_index(console, cfg, "Job # to edit (0 to cancel)")
    if idx < 0:
        return cfg

    job = cfg.scheduler.jobs[idx]
    new_time = Prompt.ask(f"New time for {job.display_name()}", default=job.time or "09:00").strip()
    job.time = new_time

    _write_schedules_yaml(cfg)
    new_cfg = config.reload_config()
    state.push("Edit Job", "OK", f"{job.display_name()}@{new_time}")
    console.print(f"[green]Updated:[/green] {job.display_name()} now at {new_time}")
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
    confirm = Prompt.ask(f"Delete {job.display_name()}?", choices=["y", "n"], default="n")

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
        menu.add_row("2.", "Edit time")
        menu.add_row("3.", "Toggle on/off")
        menu.add_row("4.", "Delete job")
        menu.add_row("5.", "Run scheduler")
        menu.add_row("0.", "Back")
        console.print(Panel(menu, border_style="cyan"))

        choice = Prompt.ask("Option", choices=["0", "1", "2", "3", "4", "5"], default="0")

        if choice == "0":
            return cfg
        elif choice == "1":
            cfg = _add_job(console, cfg, state)
        elif choice == "2":
            cfg = _edit_job(console, cfg, state)
        elif choice == "3":
            cfg = _toggle_job(console, cfg, state)
        elif choice == "4":
            cfg = _delete_job(console, cfg, state)
        elif choice == "5":
            _run_scheduler(console, state)
            # After scheduler exits, continue in menu
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
            console.print(f"  {job.display_name()}: {job.time} {status}")
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

    choice = Prompt.ask("Reload config? (y/n)", choices=["y", "n"], default="n")
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
        choice = Prompt.ask("\nSelect", choices=["0", "1", "2", "3", "4", "5"], default="0")

        if choice == "0":
            console.print("[yellow]Bye[/yellow]")
            return 0
        elif choice == "1":
            _run_doctor(console, state)
        elif choice == "2":
            _run_global(console, state)
        elif choice == "3":
            _run_onchain(console, state)
        elif choice == "4":
            cfg = _scheduler_menu(console, cfg, state)
        elif choice == "5":
            cfg = _config_menu(console, cfg, state)

        if choice not in ["0", "4"]:
            Prompt.ask("\n[dim]Press Enter[/dim]", default="")


__all__ = ["run_tui"]
