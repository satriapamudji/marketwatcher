"""CLI commands for MarketWatcher.

Commands:
- doctor: Validate configuration and connectivity
- fetch: Fetch data from CoinGecko and store in SQLite
- render: Render the report message (preview)
- send: Send the report to Telegram
- run: Full fetch -> render -> send pipeline
- tui: Launch the terminal UI
"""

import sys
from pathlib import Path

import rich.console
import rich.table

from marketwatcher import config, logging_config

console = rich.console.Console()


def _console_safe_preview(text: str) -> str:
    """Convert text to the current stdout encoding for safe dry-run previews."""
    # Keep Telegram formatting rich, but degrade unsupported glyphs for local Windows previews.
    text = text.replace("↳", "->")
    encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
    try:
        return text.encode(encoding, errors="replace").decode(encoding, errors="replace")
    except Exception:
        return text


def doctor(args) -> int:
    """Validate configuration and connectivity."""
    import sys
    from marketwatcher.logging_config import get_logger
    from marketwatcher.publishers.telegram import TelegramPublisher
    from marketwatcher.storage.sqlite import Storage

    # Fix Unicode on Windows
    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

    logger = get_logger("cli")
    logger.info("Running doctor checks...")

    # Check config
    cfg = config.get_config()

    checks_passed = 0
    checks_total = 0

    # Check 1: Config loaded
    checks_total += 1
    console.print("[bold]Check 1:[/bold] Configuration")
    if cfg.telegram.bot_token and cfg.telegram.chat_id:
        console.print("  [green]OK[/green] Telegram credentials present")
        checks_passed += 1
    else:
        console.print("  [red]FAIL[/red] Missing Telegram credentials (TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)")
    console.print()

    # Check 2: Database
    checks_total += 1
    console.print("[bold]Check 2:[/bold] Database")
    try:
        db_path = Path(cfg.database_path)
        storage = Storage(cfg.database_path)
        storage.close()
        if db_path.exists():
            console.print(f"  [green]OK[/green] Database accessible at {db_path}")
            checks_passed += 1
        else:
            console.print(f"  [green]OK[/green] Database will be created at {db_path}")
            checks_passed += 1
    except Exception as e:
        console.print(f"  [red]FAIL[/red] Database error: {e}")
    console.print()

    # Check 3: Telegram connectivity
    checks_total += 1
    console.print("[bold]Check 3:[/bold] Telegram connectivity")
    if cfg.telegram.bot_token and cfg.telegram.chat_id:
        try:
            publisher = TelegramPublisher(cfg.telegram.bot_token)
            bot_info = publisher.get_me()
            console.print(f"  [green]OK[/green] Bot connected: @{bot_info.username} (ID: {bot_info.id})")
            checks_passed += 1

            # Check chat access
            chat = publisher.get_chat(cfg.telegram.chat_id)
            console.print(f"  [green]OK[/green] Chat accessible: {chat.title or chat.type}")
        except Exception as e:
            console.print(f"  [red]FAIL[/red] Telegram error: {e}")
    else:
        console.print("  [yellow]SKIP[/yellow] Skipped (no credentials)")
    console.print()

    # Summary
    console.print(f"[bold]Doctor Summary:[/bold] {checks_passed}/{checks_total} checks passed")

    return 0 if checks_passed == checks_total else 1


def fetch(args) -> int:
    """Fetch data from CoinGecko and store in SQLite."""
    from marketwatcher.logging_config import get_logger
    from marketwatcher.providers.coingecko import CoinGeckoProvider
    from marketwatcher.storage.sqlite import Storage

    logger = get_logger("cli")

    cfg = config.get_config()
    console.print("[bold]Fetching data from CoinGecko...[/bold]")

    try:
        # Initialize storage
        storage = Storage(cfg.database_path)

        # Initialize provider
        provider = CoinGeckoProvider(
            cache_ttl=cfg.provider.cache_ttl,
            timeout=cfg.provider.timeout,
            retry_count=cfg.provider.retry_count,
            backoff_factor=cfg.provider.backoff_factor,
        )

        # Fetch global metrics
        logger.info("Fetching global metrics...")
        global_data = provider.get_global_metrics()
        console.print(f"  [green]OK[/green] Global MCAP: ${global_data['total_market_cap']:,.0f}")
        console.print(f"  [green]OK[/green] BTC Dominance: {global_data['btc_dominance']:.2f}%")

        # Store global metrics
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)

        from marketwatcher.models import MetricSnapshot
        storage.save_metric_snapshot(MetricSnapshot(
            metric_key="global_mcap_usd",
            value=global_data["total_market_cap"],
            as_of_utc=now,
            collected_at_utc=now,
            source="coingecko",
        ))
        storage.save_metric_snapshot(MetricSnapshot(
            metric_key="btc_dominance_pct",
            value=global_data["btc_dominance"],
            as_of_utc=now,
            collected_at_utc=now,
            source="coingecko",
        ))

        # Fetch categories
        logger.info("Fetching category data...")
        categories = provider.get_categories()
        console.print(f"  [green]OK[/green] Retrieved {len(categories)} categories")

        # Store categories
        from marketwatcher.models import CategorySnapshot
        for cat in categories:
            storage.save_category_snapshot(CategorySnapshot(
                category_id=cat["id"],
                category_name=cat["name"],
                market_cap_usd=cat.get("market_cap_usd", 0),
                pct_change_24h=cat.get("pct_change_24h"),
                as_of_utc=now,
                collected_at_utc=now,
                source="coingecko",
            ))

        storage.close()
        console.print("\n[bold green]Fetch complete![/bold green]")
        return 0

    except Exception as e:
        logger.error(f"Fetch failed: {e}")
        console.print(f"\n[bold red]Error:[/bold red] {e}")
        return 1


def render(args) -> int:
    """Render the report message (preview)."""
    from marketwatcher.logging_config import get_logger

    logger = get_logger("cli")

    cfg = config.get_config()
    console.print("[bold]Rendering report...[/bold]")

    try:
        # Build report
        from marketwatcher.reports.market_summary import build_market_summary
        from marketwatcher.storage.sqlite import Storage
        from marketwatcher.providers.coingecko import CoinGeckoProvider

        storage = Storage(cfg.database_path)

        # Fetch fresh data for fallback values
        api_fallback = None
        try:
            provider = CoinGeckoProvider(
                cache_ttl=cfg.provider.cache_ttl,
                timeout=cfg.provider.timeout,
                retry_count=cfg.provider.retry_count,
                backoff_factor=cfg.provider.backoff_factor,
            )
            api_fallback = provider.get_global_metrics()
            provider.close()
        except Exception as e:
            logger.warning(f"Could not fetch fresh API data: {e}")

        summary = build_market_summary(storage, cfg.report, api_fallback=api_fallback)
        storage.close()

        # Render template
        from marketwatcher.formatters.telegram_html import render_market_report
        message = render_market_report(summary, cfg.report)

        console.print("\n[bold]Preview:[/bold]\n")
        console.print(message)

        if args.dry_run:
            console.print("\n[yellow](dry-run mode - not sent)[/yellow]")

        return 0

    except Exception as e:
        logger.error(f"Render failed: {e}")
        console.print(f"\n[bold red]Error:[/bold red] {e}")
        return 1


def send(args) -> int:
    """Send the report to Telegram."""
    from marketwatcher.logging_config import get_logger

    logger = get_logger("cli")

    cfg = config.get_config()
    console.print("[bold]Sending to Telegram...[/bold]")

    try:
        # Build report
        from marketwatcher.reports.market_summary import build_market_summary
        from marketwatcher.storage.sqlite import Storage
        from marketwatcher.providers.coingecko import CoinGeckoProvider

        storage = Storage(cfg.database_path)

        # Fetch fresh data for fallback values
        api_fallback = None
        if not args.dry_run:
            try:
                provider = CoinGeckoProvider(
                    cache_ttl=cfg.provider.cache_ttl,
                    timeout=cfg.provider.timeout,
                    retry_count=cfg.provider.retry_count,
                    backoff_factor=cfg.provider.backoff_factor,
                )
                api_fallback = provider.get_global_metrics()
                provider.close()
            except Exception as e:
                logger.warning(f"Could not fetch fresh API data: {e}")

        summary = build_market_summary(storage, cfg.report, api_fallback=api_fallback)
        storage.close()

        # Render template
        from marketwatcher.formatters.telegram_html import render_market_report
        message = render_market_report(summary, cfg.report)

        # Send
        from marketwatcher.publishers.telegram import TelegramPublisher
        publisher = TelegramPublisher(cfg.telegram.bot_token)

        if args.dry_run:
            console.print("[yellow]Dry-run mode - not sending[/yellow]")
            console.print("\n[bold]Message:[/bold]\n")
            console.print(message)
        else:
            result = publisher.send_message(cfg.telegram.chat_id, message)
            console.print(f"[green]OK[/green] Message sent! Message ID: {result.message_id}")

        return 0

    except Exception as e:
        logger.error(f"Send failed: {e}")
        console.print(f"\n[bold red]Error:[/bold red] {e}")
        return 1


def run(args) -> int:
    """Full pipeline: fetch -> render -> send."""
    from marketwatcher.logging_config import get_logger

    logger = get_logger("cli")
    schedule_time = getattr(args, 'schedule', None)

    if schedule_time:
        # Scheduler mode - run daily at specific time
        import time
        from datetime import datetime, timedelta

        console.print(f"[bold]Starting daily scheduler (time: {schedule_time})...[/bold]")
        console.print("Press Ctrl+C to stop")
        console.print()

        while True:
            # Reload config each cycle
            config.reload_config()

            # Parse schedule time (e.g., "09:00" or "21:00")
            target_hour, target_minute = map(int, schedule_time.split(':'))

            now = datetime.now()
            target = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)

            # If target time has passed today, schedule for tomorrow
            if target <= now:
                target += timedelta(days=1)

            wait_seconds = (target - now).total_seconds()
            logger.info(f"Scheduler: Next run at {target}, waiting {wait_seconds/3600:.1f} hours")

            # Wait until target time
            time.sleep(wait_seconds)

            # Run pipeline
            logger.info("Scheduler: Running pipeline...")
            console.print(f"[bold]Running scheduled job...[/bold]")
            fetch_result = fetch(args)
            if fetch_result != 0:
                logger.error("Scheduler: Fetch failed")
                console.print("[red]Fetch failed[/red]")
            else:
                send_result = send(args)
                if send_result != 0:
                    logger.error("Scheduler: Send failed")
                    console.print("[red]Send failed[/red]")
                else:
                    console.print("[green]Scheduled job complete[/green]")

    # Single run mode
    logger.info("Starting full pipeline...")

    # Reload config
    config.reload_config()

    # Fetch
    result = fetch(args)
    if result != 0:
        console.print("[red]Fetch failed, aborting pipeline[/red]")
        return result

    # Send
    return send(args)


def _parse_hhmm(value: str) -> tuple[int, int]:
    hour_str, minute_str = value.split(":")
    hour = int(hour_str)
    minute = int(minute_str)
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError("time must be HH:MM in 24h format")
    return hour, minute


def scheduler(args) -> int:
    """Run multi-job scheduler using config/schedules.yaml."""
    import argparse
    import time
    from datetime import datetime, timedelta

    from marketwatcher.logging_config import get_logger

    logger = get_logger("cli")
    console.print("[bold]Starting scheduler...[/bold]")
    console.print("Press Ctrl+C to stop")
    console.print()

    while True:
        try:
            cfg = config.reload_config()

            # Get enabled jobs from new format
            enabled_jobs = [j for j in cfg.scheduler.jobs if j.enabled and j.time]

            if not enabled_jobs:
                console.print("[yellow]No enabled jobs. Sleeping 60s...[/yellow]")
                time.sleep(60)
                continue

            now = datetime.now()
            next_runs: list[tuple[datetime, object]] = []

            for job in enabled_jobs:
                try:
                    hh, mm = _parse_hhmm(job.time)
                except Exception as exc:
                    logger.error(f"Invalid time for {job.id}: {job.time} ({exc})")
                    continue

                target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
                if target <= now:
                    target += timedelta(days=1)
                next_runs.append((target, job))

            if not next_runs:
                console.print("[yellow]No valid enabled jobs. Sleeping 60s...[/yellow]")
                time.sleep(60)
                continue

            target_time, job = min(next_runs, key=lambda x: x[0])
            wait_seconds = max(0.0, (target_time - now).total_seconds())
            console.print(
                f"[dim]Next:[/dim] {job.display_name()} at {target_time:%H:%M} "
                f"[dim]in {wait_seconds/60:.0f} min[/dim]"
            )
            time.sleep(wait_seconds)

            # Reload config immediately before run
            cfg = config.reload_config()
            console.print(f"[bold cyan]Running:[/bold cyan] {job.display_name()}")

            if job.type == "global":
                rc = run(argparse.Namespace(dry_run=False, schedule=None))
            elif job.type == "onchain":
                rc = onchain(argparse.Namespace(network=job.chain, dry_run=False))
            else:
                logger.error(f"Unknown job type: {job.type}")
                rc = 1

            status = "OK" if rc == 0 else "ERR"
            console.print(f"[{'green' if rc == 0 else 'red'}]{status}[/] {job.display_name()}")

            # Small guard sleep
            time.sleep(2)

        except KeyboardInterrupt:
            console.print("\n[yellow]Scheduler stopped[/yellow]")
            return 0
        except Exception as e:
            logger.error(f"Scheduler loop error: {e}")
            console.print(f"[red]Scheduler error:[/red] {e}")
            time.sleep(30)


def onchain(args) -> int:
    """Fetch and send on-chain data from GeckoTerminal."""
    from marketwatcher.logging_config import get_logger

    logger = get_logger("cli")
    network = getattr(args, 'network', 'solana')

    cfg = config.get_config()
    console.print(f"[bold]Fetching on-chain data for {network}...[/bold]")

    try:
        from marketwatcher.providers.geckoterminal import GeckoTerminalProvider
        from marketwatcher.reports.onchain import build_onchain_report
        from marketwatcher.formatters.onchain import render_onchain_report
        from marketwatcher.publishers.telegram import TelegramPublisher

        # Fetch data
        provider = GeckoTerminalProvider(
            cache_ttl=cfg.provider.cache_ttl,
            timeout=cfg.provider.timeout,
        )
        report_data = build_onchain_report(provider, network, cfg.report, onchain_config=cfg.onchain, limit=cfg.onchain.max_tokens)
        provider.close()

        # Render
        message = render_onchain_report(report_data, cfg.report)

        if args.dry_run:
            console.print("[yellow]Dry-run mode - not sending[/yellow]")
            console.print("\n[bold]Message:[/bold]\n")
            console.print(_console_safe_preview(message))
            return 0

        # Send
        publisher = TelegramPublisher(cfg.telegram.bot_token)
        result = publisher.send_message(cfg.telegram.chat_id, message)
        console.print(f"[green]OK[/green] Message sent! Message ID: {result.message_id}")
        return 0

    except Exception as e:
        logger.error(f"On-chain fetch failed: {e}")
        console.print(f"\n[bold red]Error:[/bold red] {e}")
        return 1


def chains(args) -> int:
    """Manage chain list."""
    from marketwatcher.chains import get_chains, refresh_chains, find_chain, get_cache_age_days

    # Refresh mode
    if args.refresh:
        console.print("[bold]Refreshing chain list from GeckoTerminal...[/bold]")
        if refresh_chains():
            console.print("[green]OK[/green] Chain list updated")
            return 0
        else:
            console.print("[red]Failed to refresh chain list[/red]")
            return 1

    # Search mode
    if args.search:
        matches = find_chain(args.search)
        if not matches:
            console.print(f"[yellow]No chains matching '{args.search}'[/yellow]")
            return 0

        console.print(f"[bold]Chains matching '{args.search}':[/bold]\n")
        for chain in matches[:20]:
            console.print(f"  {chain['name']} [dim]({chain['id']})[/dim]")
        if len(matches) > 20:
            console.print(f"  [dim]... and {len(matches) - 20} more[/dim]")
        return 0

    # List mode
    cache_age = get_cache_age_days()
    if cache_age is None:
        console.print("[yellow]No cached chain list[/yellow]")
        console.print("Run [bold]marketwatcher chains --refresh[/bold] to fetch")
        return 0

    console.print(f"[bold]Cached chains[/bold] ({cache_age} days old)\n")

    # Show popular first
    console.print("[bold]Popular:[/bold]")
    for cid, name in [
        ("solana", "Solana"),
        ("base", "Base"),
        ("eth", "Ethereum"),
        ("arbitrum", "Arbitrum"),
        ("polygon_pos", "Polygon"),
        ("bsc", "BNB Chain"),
    ]:
        console.print(f"  {name} [dim]({cid})[/dim]")

    # Count total
    all_chains = get_chains()
    console.print(f"\n[bold]Total:[/bold] {len(all_chains)} chains")
    console.print("\nUse [bold]marketwatcher chains --search <query>[/bold] to find more")
    console.print("Use [bold]marketwatcher chains --refresh[/bold] to update")

    return 0


def tui(args) -> int:
    """Launch the terminal UI."""
    from marketwatcher.logging_config import get_logger

    logger = get_logger("cli")
    logger.info("Launching TUI...")

    try:
        from marketwatcher.tui.app import run_tui
        return run_tui()
    except ImportError as e:
        console.print(f"[red]TUI not available: {e}[/red]")
        return 1


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        prog="marketwatcher",
        description="Crypto market watcher - pulls data from CoinGecko and sends to Telegram",
    )
    parser.add_argument(
        "--version", action="version", version="%(prog)s 0.1.0"
    )
    parser.add_argument(
        "--config-dir", type=Path, help="Directory containing config files"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set logging level",
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Doctor command
    doctor_parser = subparsers.add_parser(
        "doctor", help="Validate configuration and connectivity"
    )
    doctor_parser.set_defaults(func=doctor)

    # Fetch command
    fetch_parser = subparsers.add_parser(
        "fetch", help="Fetch data from CoinGecko and store in SQLite"
    )
    fetch_parser.set_defaults(func=fetch)

    # Render command
    render_parser = subparsers.add_parser(
        "render", help="Render the report message (preview)"
    )
    render_parser.add_argument(
        "--dry-run", action="store_true", help="Show preview without sending"
    )
    render_parser.set_defaults(func=render)

    # Send command
    send_parser = subparsers.add_parser(
        "send", help="Send the report to Telegram"
    )
    send_parser.add_argument(
        "--dry-run", action="store_true", help="Preview without sending"
    )
    send_parser.set_defaults(func=send)

    # Run command (full pipeline)
    run_parser = subparsers.add_parser(
        "run", help="Full pipeline: fetch -> render -> send"
    )
    run_parser.add_argument(
        "--dry-run", action="store_true", help="Preview without sending"
    )
    run_parser.add_argument(
        "--schedule", type=str, metavar="HH:MM",
        help="Run daily at specific time (e.g., 09:00 or 21:00)"
    )
    run_parser.set_defaults(func=run)

    # Config-based scheduler command
    scheduler_parser = subparsers.add_parser(
        "scheduler", help="Run multi-job scheduler from config/schedules.yaml"
    )
    scheduler_parser.set_defaults(func=scheduler)

    # On-chain command
    onchain_parser = subparsers.add_parser(
        "onchain", help="Fetch on-chain data from GeckoTerminal"
    )
    onchain_parser.add_argument(
        "--network", "-n", default="solana",
        help="Network to fetch (solana, eth, base, arbitrum, etc.)"
    )
    onchain_parser.add_argument(
        "--dry-run", action="store_true", help="Preview without sending"
    )
    onchain_parser.set_defaults(func=onchain)

    # Chains command
    chains_parser = subparsers.add_parser(
        "chains", help="Manage chain list (refresh, list)"
    )
    chains_parser.add_argument(
        "--refresh", action="store_true", help="Force refresh chain list from API"
    )
    chains_parser.add_argument(
        "--search", type=str, help="Search for chains matching query"
    )
    chains_parser.set_defaults(func=chains)

    # TUI command
    tui_parser = subparsers.add_parser(
        "tui", help="Launch the terminal UI"
    )
    tui_parser.set_defaults(func=tui)

    args = parser.parse_args()

    # Load config first
    cfg = config.load_config(config_dir=args.config_dir)

    # Setup logging with config values
    logging_config.setup_logging(
        level=args.log_level or cfg.logging.level,
        jsonl_path=cfg.logging.jsonl_path if cfg.logging.jsonl_path else None,
        console=cfg.logging.console,
    )

    # Run command
    if args.command is None:
        parser.print_help()
        return 0

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
