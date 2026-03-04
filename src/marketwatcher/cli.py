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


def _get_chat_id(cfg, args=None) -> str:
    """Get the target chat ID, preferring per-job override over global default."""
    override = getattr(args, "chat_id", "") if args else ""
    return override or cfg.telegram.chat_id


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
            chat_id = _get_chat_id(cfg, args)
            result = publisher.send_message(chat_id, message)
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


def _next_interval_run(now: 'datetime', interval_hours: int, offset_minutes: int) -> 'datetime':
    """Calculate the next run time for an interval-based job.

    Slots are aligned to midnight UTC + offset, repeating every interval_hours.
    E.g. interval=4, offset=10 → 00:10, 04:10, 08:10, 12:10, 16:10, 20:10
    """
    from datetime import timedelta

    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    slot_start = midnight + timedelta(minutes=offset_minutes)

    # Find the next slot after now
    while slot_start <= now:
        slot_start += timedelta(hours=interval_hours)

    return slot_start


def alert_loop(args) -> None:
    """Independent alert checking loop (runs in its own thread).

    Checks all watchlists with alert thresholds on a fast interval
    (default 15 min). Lightweight — uses batch price fetch, not full reports.
    """
    import time

    from marketwatcher.logging_config import get_logger

    logger = get_logger("alert-loop")
    logger.info("Alert loop started")
    console.print("[bold]Alert loop started[/bold]")

    while True:
        try:
            cfg = config.reload_config()
            interval = cfg.alerts.check_interval_minutes

            from marketwatcher.watchlist import load_watchlists
            from marketwatcher.alerts import (
                watchlist_has_alerts, fetch_alert_prices,
                check_alerts, format_alerts_batch,
            )
            from marketwatcher.providers.coingecko import CoinGeckoProvider
            from marketwatcher.providers.geckoterminal import GeckoTerminalProvider
            from marketwatcher.publishers.telegram import TelegramPublisher

            data = load_watchlists()
            all_wls = data.get("watchlists", [])
            alert_wls = [wl for wl in all_wls if watchlist_has_alerts(wl)]

            if not alert_wls:
                logger.debug("No watchlists with alert thresholds, sleeping")
                time.sleep(interval * 60)
                continue

            logger.info(f"Checking alerts for {len(alert_wls)} watchlist(s)")

            cg = CoinGeckoProvider(
                cache_ttl=cfg.provider.cache_ttl,
                timeout=cfg.provider.timeout,
                retry_count=cfg.provider.retry_count,
                backoff_factor=cfg.provider.backoff_factor,
            )
            gt = GeckoTerminalProvider(
                cache_ttl=cfg.provider.cache_ttl,
                timeout=cfg.provider.timeout,
            )

            total_alerts = 0
            publisher = None

            for wl in alert_wls:
                try:
                    price_data = fetch_alert_prices(wl, cg, gt)
                    triggered = check_alerts(wl, price_data)

                    if triggered:
                        if publisher is None:
                            publisher = TelegramPublisher(cfg.telegram.bot_token)
                        message = format_alerts_batch(triggered)
                        alert_chat_id = wl.get("alert_chat_id") or cfg.telegram.chat_id
                        publisher.send_message(alert_chat_id, message)
                        total_alerts += len(triggered)
                        logger.info(f"Sent {len(triggered)} alert(s) for {wl.get('name', wl.get('id'))}")
                except Exception as e:
                    logger.error(f"Alert check failed for {wl.get('id')}: {e}")

            cg.close()
            gt.close()

            if total_alerts:
                console.print(f"[yellow]Alert loop: {total_alerts} alert(s) sent[/yellow]")
            else:
                logger.debug("Alert loop: no alerts triggered")

            time.sleep(interval * 60)

        except KeyboardInterrupt:
            logger.info("Alert loop stopped")
            return
        except Exception as e:
            logger.error(f"Alert loop error: {e}")
            time.sleep(60)


def scheduler(args) -> int:
    """Run durable queue-backed scheduler using config/schedules.yaml."""
    import argparse
    import time
    from datetime import datetime, timedelta, timezone

    from marketwatcher.logging_config import get_logger
    from marketwatcher.scheduler_queue import SchedulerQueue
    from marketwatcher.timezones import normalize_timezone_label, parse_timezone

    logger = get_logger("cli")
    console.print("[bold]Starting scheduler...[/bold]")
    console.print("Press Ctrl+C to stop")
    console.print()

    poll_seconds = 15.0
    lease_seconds = 1800
    max_attempts = 3
    max_catchup_slots = 48
    last_next_line = ""

    def _run_job(job) -> int:
        if job.type == "global":
            return run(argparse.Namespace(dry_run=False, schedule=None, chat_id=job.chat_id))
        elif job.type == "onchain":
            return onchain(argparse.Namespace(network=job.chain, dry_run=False, chat_id=job.chat_id))
        elif job.type == "global_onchain":
            return global_onchain(argparse.Namespace(dry_run=False, chat_id=job.chat_id))
        elif job.type == "macro":
            return macro(argparse.Namespace(dry_run=False, chat_id=job.chat_id))
        elif job.type == "watchlist":
            return watchlist_cmd(argparse.Namespace(
                dry_run=False,
                watchlist_id=job.watchlist_id or "main",
                chat_id=job.chat_id,
            ))
        else:
            logger.error(f"Unknown job type: {job.type}")
            return 1

    def _slot_step(job) -> timedelta:
        if job.interval_hours > 0:
            return timedelta(hours=job.interval_hours)
        return timedelta(days=1)

    def _latest_due_slot_utc(now_utc: datetime, job, scheduler_tz) -> datetime:
        now_local = now_utc.astimezone(scheduler_tz)
        if job.interval_hours > 0:
            interval_minutes = job.interval_hours * 60
            midnight = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
            minutes_since_midnight = int((now_local - midnight).total_seconds() // 60)
            slot_index = (minutes_since_midnight - job.offset_minutes) // interval_minutes
            slot_local = midnight + timedelta(minutes=job.offset_minutes + (slot_index * interval_minutes))
            return slot_local.astimezone(timezone.utc)

        hh, mm = _parse_hhmm(job.time)
        today_slot_local = now_local.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if today_slot_local <= now_local:
            return today_slot_local.astimezone(timezone.utc)
        return (today_slot_local - timedelta(days=1)).astimezone(timezone.utc)

    def _next_slot_utc(slot_utc: datetime, job) -> datetime:
        return slot_utc + _slot_step(job)

    def _next_scheduled_slot_utc(now_utc: datetime, job, scheduler_tz) -> datetime:
        latest_due_utc = _latest_due_slot_utc(now_utc, job, scheduler_tz)
        next_slot_utc = _next_slot_utc(latest_due_utc, job)
        while next_slot_utc <= now_utc:
            next_slot_utc = _next_slot_utc(next_slot_utc, job)
        return next_slot_utc

    def _retry_delay_seconds(attempt_number: int) -> int:
        return min(900, 60 * (2 ** max(0, attempt_number - 1)))

    cfg = config.get_config()
    with SchedulerQueue(cfg.database_path) as queue:
        while True:
            try:
                cfg = config.reload_config()
                now_utc = datetime.now(timezone.utc)
                try:
                    scheduler_tz = parse_timezone(cfg.scheduler.timezone)
                    scheduler_tz_label = normalize_timezone_label(cfg.scheduler.timezone)
                except Exception as exc:
                    logger.error(f"Invalid scheduler timezone '{cfg.scheduler.timezone}': {exc}; using UTC")
                    scheduler_tz = timezone.utc
                    scheduler_tz_label = "UTC"
                stored_tz_label = queue.get_meta("scheduler_timezone")
                if stored_tz_label != scheduler_tz_label:
                    if stored_tz_label is not None:
                        logger.info(
                            f"Scheduler timezone changed {stored_tz_label} -> {scheduler_tz_label}; "
                            "resetting schedule watermarks"
                        )
                    queue.clear_watermarks()
                    queue.set_meta("scheduler_timezone", scheduler_tz_label)
                recovered = queue.recover_expired_leases()
                if recovered:
                    logger.warning(f"Recovered {recovered} stale running job(s)")

                enabled_jobs = [
                    j for j in cfg.scheduler.jobs
                    if j.enabled and (j.interval_hours > 0 or j.time)
                ]

                enqueued = 0
                for job in enabled_jobs:
                    try:
                        watermark = queue.get_watermark(job.id)
                        if watermark is None:
                            watermark = _latest_due_slot_utc(now_utc, job, scheduler_tz) - _slot_step(job)

                        next_slot = _next_slot_utc(watermark, job)
                        queued_for_job = 0
                        while next_slot <= now_utc and queued_for_job < max_catchup_slots:
                            if queue.enqueue(
                                job_id=job.id,
                                job_type=job.type,
                                payload={
                                    "job_id": job.id,
                                    "type": job.type,
                                },
                                scheduled_for_utc=next_slot,
                                max_attempts=max_attempts,
                            ):
                                enqueued += 1
                            watermark = next_slot
                            next_slot = _next_slot_utc(watermark, job)
                            queued_for_job += 1

                        queue.set_watermark(job.id, watermark)
                        if queued_for_job == max_catchup_slots and next_slot <= now_utc:
                            logger.warning(
                                f"Catch-up limit reached for {job.id}; pending historical slots remain"
                            )
                    except Exception as exc:
                        logger.error(f"Failed queueing {job.id}: {exc}")

                if enqueued:
                    logger.info(f"Enqueued {enqueued} due job(s)")

                processed = 0
                while True:
                    queued_job = queue.claim_next_due(lease_seconds=lease_seconds)
                    if queued_job is None:
                        break

                    latest_cfg = config.reload_config()
                    live_jobs = {
                        j.id: j for j in latest_cfg.scheduler.jobs
                        if j.enabled and (j.interval_hours > 0 or j.time)
                    }
                    current_job = live_jobs.get(queued_job.job_id)
                    if current_job is None:
                        logger.info(f"Skipping removed/disabled queued job: {queued_job.job_id}")
                        queue.mark_done(queued_job.queue_id)
                        continue

                    attempt_number = queued_job.attempt_count + 1
                    console.print(
                        f"[bold cyan]Running:[/bold cyan] {current_job.display_name()} "
                        f"[dim](attempt {attempt_number}/{queued_job.max_attempts})[/dim]"
                    )
                    try:
                        rc = _run_job(current_job)
                    except Exception as exc:  # defensive: _run_job generally returns int
                        logger.error(f"Job {current_job.id} crashed: {exc}")
                        rc = 1

                    if rc == 0:
                        queue.mark_done(queued_job.queue_id)
                        console.print(f"[green]OK[/green] {current_job.display_name()}")
                    else:
                        delay_seconds = _retry_delay_seconds(attempt_number)
                        will_retry, next_attempt_number = queue.mark_failed(
                            queued_job,
                            error_message=f"exit_code={rc}",
                            retry_delay_seconds=delay_seconds,
                        )
                        if will_retry:
                            console.print(
                                f"[yellow]RETRY[/yellow] {current_job.display_name()} "
                                f"[dim](next attempt {next_attempt_number + 1}/{queued_job.max_attempts} "
                                f"in {delay_seconds}s)[/dim]"
                            )
                        else:
                            console.print(
                                f"[red]DEAD[/red] {current_job.display_name()} "
                                f"[dim](attempts exhausted)[/dim]"
                            )
                    processed += 1
                    time.sleep(1)

                next_candidates: list[datetime] = []
                next_pending_eta = queue.next_pending_eta()
                if next_pending_eta is not None:
                    next_candidates.append(next_pending_eta)

                for job in enabled_jobs:
                    try:
                        next_candidates.append(_next_scheduled_slot_utc(now_utc, job, scheduler_tz))
                    except Exception as exc:
                        logger.error(f"Invalid schedule for {job.id}: {exc}")

                sleep_seconds = poll_seconds
                if next_candidates:
                    next_eta = min(next_candidates)
                    seconds_until_next = (next_eta - datetime.now(timezone.utc)).total_seconds()
                    sleep_seconds = max(1.0, min(poll_seconds, seconds_until_next))
                    next_local = next_eta.astimezone(scheduler_tz)
                    next_line = (
                        f"[dim]Next queue check in {sleep_seconds:.0f}s "
                        f"(next due {next_local:%H:%M} {scheduler_tz_label})[/dim]"
                    )
                    if next_line != last_next_line:
                        console.print(next_line)
                        last_next_line = next_line
                elif not enabled_jobs and last_next_line != "NO_ENABLED":
                    console.print("[yellow]No enabled jobs configured[/yellow]")
                    last_next_line = "NO_ENABLED"

                if processed == 0:
                    time.sleep(sleep_seconds)

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
        result = publisher.send_message(_get_chat_id(cfg, args), message)
        console.print(f"[green]OK[/green] Message sent! Message ID: {result.message_id}")
        return 0

    except Exception as e:
        logger.error(f"On-chain fetch failed: {e}")
        console.print(f"\n[bold red]Error:[/bold red] {e}")
        return 1


def global_onchain(args) -> int:
    """Fetch and send global on-chain DeFi overview from DefiLlama."""
    from marketwatcher.logging_config import get_logger

    logger = get_logger("cli")

    cfg = config.get_config()
    console.print("[bold]Fetching global on-chain data from DefiLlama...[/bold]")

    try:
        from marketwatcher.providers.defillama import DefiLlamaProvider
        from marketwatcher.reports.global_onchain import build_global_onchain_report
        from marketwatcher.formatters.global_onchain import render_global_onchain_report
        from marketwatcher.publishers.telegram import TelegramPublisher

        provider = DefiLlamaProvider(
            cache_ttl=cfg.provider.cache_ttl,
            timeout=cfg.provider.timeout,
        )
        report_data = build_global_onchain_report(provider, cfg.report)
        provider.close()

        message = render_global_onchain_report(report_data, cfg.report)

        if args.dry_run:
            console.print("[yellow]Dry-run mode - not sending[/yellow]")
            console.print("\n[bold]Message:[/bold]\n")
            console.print(_console_safe_preview(message))
            return 0

        publisher = TelegramPublisher(cfg.telegram.bot_token)
        result = publisher.send_message(_get_chat_id(cfg, args), message)
        console.print(f"[green]OK[/green] Message sent! Message ID: {result.message_id}")
        return 0

    except Exception as e:
        logger.error(f"Global on-chain fetch failed: {e}")
        console.print(f"\n[bold red]Error:[/bold red] {e}")
        return 1


def macro(args) -> int:
    """Fetch and send global macro report (equities, rates, FX, commodities)."""
    from marketwatcher.logging_config import get_logger

    logger = get_logger("cli")

    cfg = config.get_config()
    console.print("[bold]Fetching global macro data...[/bold]")

    try:
        from marketwatcher.reports.macro import build_macro_report
        from marketwatcher.formatters.macro import render_macro_report
        from marketwatcher.publishers.telegram import TelegramPublisher

        report_data = build_macro_report()
        message = render_macro_report(report_data, cfg.report)

        if args.dry_run:
            console.print("[yellow]Dry-run mode - not sending[/yellow]")
            console.print("\n[bold]Message:[/bold]\n")
            console.print(_console_safe_preview(message))
            return 0

        publisher = TelegramPublisher(cfg.telegram.bot_token)
        result = publisher.send_message(_get_chat_id(cfg, args), message)
        console.print(f"[green]OK[/green] Message sent! Message ID: {result.message_id}")
        return 0

    except Exception as e:
        logger.error(f"Macro fetch failed: {e}")
        console.print(f"\n[bold red]Error:[/bold red] {e}")
        return 1


def watchlist_cmd(args) -> int:
    """Fetch and send watchlist report."""
    from marketwatcher.logging_config import get_logger

    logger = get_logger("cli")
    watchlist_id = getattr(args, "watchlist_id", "main")

    cfg = config.get_config()
    console.print(f"[bold]Fetching watchlist '{watchlist_id}'...[/bold]")

    try:
        from marketwatcher.watchlist import get_watchlist
        from marketwatcher.providers.coingecko import CoinGeckoProvider
        from marketwatcher.providers.geckoterminal import GeckoTerminalProvider
        from marketwatcher.reports.watchlist import build_watchlist_report
        from marketwatcher.formatters.watchlist import render_watchlist_report
        from marketwatcher.publishers.telegram import TelegramPublisher

        wl = get_watchlist(watchlist_id)
        if not wl.get("tokens"):
            console.print("[yellow]Watchlist is empty. Add tokens first.[/yellow]")
            return 0

        cg = CoinGeckoProvider(cache_ttl=cfg.provider.cache_ttl, timeout=cfg.provider.timeout,
                               retry_count=cfg.provider.retry_count, backoff_factor=cfg.provider.backoff_factor)
        gt = GeckoTerminalProvider(cache_ttl=cfg.provider.cache_ttl, timeout=cfg.provider.timeout)

        report_data = build_watchlist_report(wl, cg, gt, cfg.report)
        cg.close()
        gt.close()

        message = render_watchlist_report(report_data, cfg.report)

        if args.dry_run:
            console.print("[yellow]Dry-run mode - not sending[/yellow]")
            console.print("\n[bold]Message:[/bold]\n")
            console.print(_console_safe_preview(message))
            return 0

        publisher = TelegramPublisher(cfg.telegram.bot_token)
        result = publisher.send_message(_get_chat_id(cfg, args), message)
        console.print(f"[green]OK[/green] Message sent! Message ID: {result.message_id}")

        # Always check alerts on watchlist reports
        from marketwatcher.alerts import check_alerts, format_alerts_batch
        triggered = check_alerts(wl, report_data)
        if triggered:
            alert_msg = format_alerts_batch(triggered)
            alert_chat_id = wl.get("alert_chat_id") or _get_chat_id(cfg, args)
            publisher.send_message(alert_chat_id, alert_msg)
            console.print(f"[yellow]{len(triggered)} alert(s) sent![/yellow]")

        return 0

    except Exception as e:
        logger.error(f"Watchlist report failed: {e}")
        console.print(f"\n[bold red]Error:[/bold red] {e}")
        return 1


def watchlist_manage(args) -> int:
    """Manage watchlist tokens (add/remove/list)."""
    from marketwatcher.watchlist import add_token, remove_token, get_watchlist, list_watchlists

    action = args.action
    watchlist_id = getattr(args, "watchlist_id", "main")

    if action == "list":
        wl = get_watchlist(watchlist_id)
        console.print(f"\n[bold]Watchlist: {wl.get('name', watchlist_id)}[/bold]")
        tokens = wl.get("tokens", [])
        if not tokens:
            console.print("  [dim]No tokens[/dim]")
        else:
            for t in tokens:
                sym = t.get("symbol", "???")
                if t.get("type") == "dex":
                    console.print(f"  {sym} [dim](DEX: {t.get('chain', '?')}/{t.get('address', '?')[:10]}...)[/dim]")
                else:
                    console.print(f"  {sym} [dim](CEX: {t.get('coingecko_id', '?')})[/dim]")
        return 0

    elif action == "add":
        symbol = getattr(args, "symbol", None)
        if not symbol:
            console.print("[red]--symbol is required[/red]")
            return 1

        cg_id = getattr(args, "coingecko_id", "") or ""
        chain = getattr(args, "chain", "") or ""
        address = getattr(args, "address", "") or ""

        if chain and address:
            token_type = "dex"
        elif cg_id:
            token_type = "cex"
        else:
            console.print("[red]Provide --coingecko-id (CEX) or --chain + --address (DEX)[/red]")
            return 1

        ok = add_token(watchlist_id, symbol, token_type=token_type,
                       coingecko_id=cg_id, chain=chain, address=address)
        if ok:
            console.print(f"[green]Added {symbol} to {watchlist_id}[/green]")
        else:
            console.print(f"[yellow]{symbol} already in {watchlist_id}[/yellow]")
        return 0

    elif action == "remove":
        symbol = getattr(args, "symbol", None)
        if not symbol:
            console.print("[red]--symbol is required[/red]")
            return 1

        ok = remove_token(watchlist_id, symbol)
        if ok:
            console.print(f"[green]Removed {symbol} from {watchlist_id}[/green]")
        else:
            console.print(f"[yellow]{symbol} not found in {watchlist_id}[/yellow]")
        return 0

    return 0


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


def alerts_cmd(args) -> int:
    """Check watchlist alerts and send triggered ones."""
    from marketwatcher.logging_config import get_logger

    logger = get_logger("cli")
    watchlist_id = getattr(args, "watchlist_id", "main")
    dry_run = getattr(args, "dry_run", False)

    cfg = config.get_config()
    console.print(f"[bold]Checking alerts for '{watchlist_id}'...[/bold]")

    try:
        from marketwatcher.watchlist import get_watchlist
        from marketwatcher.providers.coingecko import CoinGeckoProvider
        from marketwatcher.providers.geckoterminal import GeckoTerminalProvider
        from marketwatcher.alerts import check_alerts, fetch_alert_prices, format_alerts_batch
        from marketwatcher.publishers.telegram import TelegramPublisher

        wl = get_watchlist(watchlist_id)
        if not wl.get("tokens"):
            console.print("[yellow]Watchlist is empty.[/yellow]")
            return 0

        cg = CoinGeckoProvider(cache_ttl=cfg.provider.cache_ttl, timeout=cfg.provider.timeout,
                               retry_count=cfg.provider.retry_count, backoff_factor=cfg.provider.backoff_factor)
        gt = GeckoTerminalProvider(cache_ttl=cfg.provider.cache_ttl, timeout=cfg.provider.timeout)

        report_data = fetch_alert_prices(wl, cg, gt)
        cg.close()
        gt.close()

        triggered = check_alerts(wl, report_data)

        if not triggered:
            console.print("[green]No alerts triggered.[/green]")
            return 0

        message = format_alerts_batch(triggered)
        console.print(f"[yellow]{len(triggered)} alert(s) triggered![/yellow]")

        if dry_run:
            console.print("\n[bold]Alert message:[/bold]\n")
            console.print(_console_safe_preview(message))
            return 0

        # Determine alert channel: watchlist alert_chat_id > job chat_id > global default
        alert_chat_id = wl.get("alert_chat_id") or _get_chat_id(cfg, args)
        publisher = TelegramPublisher(cfg.telegram.bot_token)
        result = publisher.send_message(alert_chat_id, message)
        console.print(f"[green]OK[/green] Alert sent! Message ID: {result.message_id}")
        return 0

    except Exception as e:
        logger.error(f"Alerts check failed: {e}")
        console.print(f"\n[bold red]Error:[/bold red] {e}")
        return 1


def bot(args) -> int:
    """Run Telegram bot listener for watchlist commands."""
    from marketwatcher.logging_config import get_logger
    from marketwatcher.bot import TelegramBot

    logger = get_logger("cli")
    cfg = config.get_config()

    if not cfg.telegram.bot_token:
        console.print("[red]TELEGRAM_BOT_TOKEN not set[/red]")
        return 1

    console.print("[bold]Starting Telegram bot listener...[/bold]")
    console.print("Commands: /watch, /watchdex, /unwatch, /watchlist, /watchlists")
    console.print("Press Ctrl+C to stop\n")

    # Restrict to the configured chat_id if set
    allowed = [cfg.telegram.chat_id] if cfg.telegram.chat_id else None

    tg_bot = TelegramBot(cfg.telegram.bot_token, allowed_chat_ids=allowed)
    try:
        tg_bot.run()
    except KeyboardInterrupt:
        console.print("\n[yellow]Bot stopped[/yellow]")
    finally:
        tg_bot.close()
    return 0


def bot_scheduler(args) -> int:
    """Run both Telegram bot and scheduler in parallel threads."""
    import threading
    from marketwatcher.logging_config import get_logger
    from marketwatcher.bot import TelegramBot

    logger = get_logger("cli")
    cfg = config.get_config()

    if not cfg.telegram.bot_token:
        console.print("[red]TELEGRAM_BOT_TOKEN not set[/red]")
        return 1

    console.print("[bold]Starting bot + scheduler + alert loop...[/bold]")
    console.print("Bot commands: /watch, /watchdex, /unwatch, /watchlist, /watchlists")
    console.print(f"Alert loop: every {cfg.alerts.check_interval_minutes}min")
    console.print("Press Ctrl+C to stop\n")

    allowed = [cfg.telegram.chat_id] if cfg.telegram.chat_id else None
    tg_bot = TelegramBot(cfg.telegram.bot_token, allowed_chat_ids=allowed)

    # Run scheduler in a daemon thread
    sched_thread = threading.Thread(
        target=scheduler,
        args=(args,),
        daemon=True,
        name="scheduler",
    )
    sched_thread.start()

    # Run alert loop in a daemon thread
    alert_thread = threading.Thread(
        target=alert_loop,
        args=(args,),
        daemon=True,
        name="alert-loop",
    )
    alert_thread.start()

    # Run bot in main thread (catches Ctrl+C)
    try:
        tg_bot.run()
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopping...[/yellow]")
    finally:
        tg_bot.close()
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

    # Global on-chain command
    global_onchain_parser = subparsers.add_parser(
        "global-onchain", help="Global DeFi on-chain overview (DefiLlama)"
    )
    global_onchain_parser.add_argument(
        "--dry-run", action="store_true", help="Preview without sending"
    )
    global_onchain_parser.set_defaults(func=global_onchain)

    # Global macro report command
    macro_parser = subparsers.add_parser(
        "macro", help="Global macro report (equities, rates, FX, commodities)"
    )
    macro_parser.add_argument(
        "--dry-run", action="store_true", help="Preview without sending"
    )
    macro_parser.set_defaults(func=macro)

    # Watchlist report command
    watchlist_parser = subparsers.add_parser(
        "watchlist", help="Run watchlist report"
    )
    watchlist_parser.add_argument("--dry-run", action="store_true", help="Preview without sending")
    watchlist_parser.add_argument("--watchlist-id", default="main", help="Watchlist ID (default: main)")
    watchlist_parser.set_defaults(func=watchlist_cmd)

    # Watchlist manage command
    wl_manage = subparsers.add_parser(
        "watchlist-manage", help="Add/remove/list watchlist tokens"
    )
    wl_manage.add_argument("action", choices=["add", "remove", "list"])
    wl_manage.add_argument("--symbol", type=str, help="Token symbol")
    wl_manage.add_argument("--coingecko-id", type=str, help="CoinGecko ID (for CEX tokens)")
    wl_manage.add_argument("--chain", type=str, help="Chain (for DEX tokens)")
    wl_manage.add_argument("--address", type=str, help="Token address (for DEX tokens)")
    wl_manage.add_argument("--watchlist-id", default="main", help="Watchlist ID (default: main)")
    wl_manage.set_defaults(func=watchlist_manage)

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

    # Alerts command
    alerts_parser = subparsers.add_parser(
        "alerts", help="Check watchlist alerts"
    )
    alerts_parser.add_argument("--dry-run", action="store_true", help="Preview without sending")
    alerts_parser.add_argument("--watchlist-id", default="main", help="Watchlist ID (default: main)")
    alerts_parser.set_defaults(func=alerts_cmd)

    # Bot command
    bot_parser = subparsers.add_parser(
        "bot", help="Run Telegram bot listener for watchlist commands"
    )
    bot_parser.set_defaults(func=bot)

    # Bot + Scheduler command
    bot_sched_parser = subparsers.add_parser(
        "bot-scheduler", help="Run bot listener and scheduler together"
    )
    bot_sched_parser.set_defaults(func=bot_scheduler)

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
