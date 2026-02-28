# Market Watcher v1 Plan (Crypto -> Telegram, TUI-Operated)

## Objective

Build a reliable bot that:

1. Pulls free crypto market data (v1: CoinGecko public endpoints).
2. Computes a small market summary (global market cap, BTC dominance, category movers).
3. Renders a configurable Telegram message (HTML parse mode).
4. Sends to a Telegram channel.
5. Exposes a TUI for preview/send/health/config workflows (teleforward-style operator UX).

This plan is written for a new repo and prioritizes reliability, easy formatting changes, and extensibility (macro later).

## Context / Assumptions

- Data source (v1): CoinGecko free/public endpoints.
- Delivery: Telegram Bot API to a channel.
- Runtime: local machine or VPS.
- Storage: SQLite (simple, durable, enough for historical daily snapshots).
- TUI style target: "teleforward-like" operational flow (menu-driven, quick actions, status visibility).
- Note: I did not find the referenced `desktop`/`teleforward` folder in this repo snapshot, so TUI design is based on your description.

## First Message Format (Target v1)

Use Telegram `HTML` parse mode (safer than MarkdownV2 for dynamic content).

```text
<i>{Date & Time}</i>
———————————————————

Global MCAP: {formatted_number}
• 1D Δ: {pct}%
• 7D Δ: {avg_pct_over_7d}%
• 14D Δ: {avg_pct_over_14d}%

Bitcoin Dominance: {formatted_pct}
• 1D Δ: {pct}%
• 7D Δ: {avg_pct_over_7d}%
• 14D Δ: {avg_pct_over_14d}%

———————————————————

| Top Gaining Categories (24h):
1. {name} [{mcap} | {pct_change}]
...

| Top Losing Categories (24h):
1. {name} [{mcap} | {pct_change}]
...

———————————————————
```

## Key Engineering Decisions (v1)

### 1) Data Model: Snapshot-first

Do not compute long-window deltas directly from a single API response.

- Persist snapshots each run (or at least daily normalized rows).
- Compute 1D/7D/14D from stored history.
- This makes reports reproducible and protects against API outages.

### 2) Delta Definitions (explicit)

To match your intent ("average of change across 7/14 days"):

- Daily percent change:
  - `pct_change(d) = ((value_d / value_(d-1)) - 1) * 100`
- `7D Δ` = arithmetic mean of the last 7 daily percent changes.
- `14D Δ` = arithmetic mean of the last 14 daily percent changes.

Same logic for:
- Global market cap
- Bitcoin dominance

### 3) Formatting Must Be Easy To Change

Formatting should live outside core logic.

- Core builds a normalized report object.
- Formatter renders from a template file.
- Start with Jinja2 templates (`templates/telegram_market_v1.html.j2`).
- Keep helper filters centralized:
  - `fmt_usd_abbrev`
  - `fmt_pct`
  - `fmt_signed_pct`
  - `escape_html`

This gives you easy template iteration without touching fetch/compute code.

### 4) Free Data / Rate Limit Strategy (CoinGecko)

Design conservatively from day one.

- Use minimal endpoint set (global + categories).
- Cache fresh responses (short TTL, e.g. 60-300s for manual preview loops).
- Client-side rate limiter (token bucket / leaky bucket).
- Retries only for transient errors (`429`, `5xx`, timeouts) with exponential backoff + jitter.
- Never hammer during TUI refresh.
- Add a `--cached` preview mode so operators don't trigger live requests repeatedly.

Practical v1 posture:
- Budget for low request volume (single report cadence, manual previews).
- Treat any `429` as normal operational behavior and recover gracefully.

## Proposed Architecture

### Components

1. `providers/coingecko.py`
- HTTP client wrapper (timeouts, retries, rate limit, response parsing).
- Endpoints:
  - global metrics
  - categories

2. `storage/sqlite.py`
- SQLite connection + schema init/migrations.
- Snapshot persistence and read queries.

3. `reports/market_summary.py`
- Builds report model from latest snapshots + historical rows.
- Computes deltas and category ranking lists.

4. `formatters/telegram_html.py`
- Template loader + render helpers + Telegram-safe escaping.
- Length guard (Telegram message cap ~4096 chars).

5. `publishers/telegram.py`
- `sendMessage` integration.
- Optional dry-run and preview-only modes.

6. `app/runner.py`
- Orchestrates `fetch -> persist -> build -> render -> send`.
- Supports scheduled loop and one-shot runs.

7. `tui/`
- Menu-driven operator UX:
  - Doctor
  - Fetch now
  - Preview message
  - Send now
  - View last run/error
  - Edit/view config

### Suggested Folder Layout

```text
marketwatcher/
  plan.md
  pyproject.toml
  .env.example
  config/
    settings.yaml
  src/marketwatcher/
    cli.py
    config.py
    models.py
    providers/
      coingecko.py
    storage/
      sqlite.py
      schema.sql
    reports/
      market_summary.py
    formatters/
      telegram_html.py
      templates/
        telegram_market_v1.html.j2
    publishers/
      telegram.py
    tui/
      app.py
    app/
      runner.py
```

## Data Schema (SQLite, v1)

Keep it simple and auditable.

### `metric_snapshots`

- `id`
- `metric_key` (`global_mcap_usd`, `btc_dominance_pct`)
- `value`
- `as_of_utc` (provider observation timestamp if available)
- `collected_at_utc` (our fetch timestamp)
- `source` (`coingecko`)
- Unique constraint on (`metric_key`, `date(as_of_utc)` or normalized day bucket)

### `category_snapshots`

- `id`
- `category_id`
- `category_name`
- `market_cap_usd`
- `pct_change_24h`
- `as_of_utc`
- `collected_at_utc`
- `source`

Indexes:
- `(as_of_utc)`
- `(category_id, as_of_utc)`

### `run_log` (ops visibility)

- `id`
- `run_type` (`fetch`, `send`, `full`)
- `started_at_utc`
- `finished_at_utc`
- `status` (`ok`, `partial`, `err`)
- `error_summary` (nullable)

## CoinGecko Mapping (v1)

Use only the endpoints needed to generate the message.

- Global data:
  - Total market cap (USD)
  - BTC dominance (%)
  - If 24h changes are available directly, use them; otherwise derive from snapshots.
- Categories:
  - Category name / id
  - Market cap
  - 24h percentage change

Rules:
- Sort gainers by `pct_change_24h` descending.
- Sort losers by `pct_change_24h` ascending.
- Exclude null/invalid percentage rows.
- Stable tie-breaker: higher market cap first, then name.

## Telegram Delivery Design

### Parse Mode

- Use `HTML`.
- Escape all dynamic text (especially category names).

### Safety / Idempotency

- `--dry-run`: render and print only.
- `--send`: posts message.
- Optional later: `--edit-last` mode (store last message id and edit instead of posting new).

### Channel Setup Risks (preempt)

- Bot not admin in channel.
- Wrong chat ID / username.
- Token typo.
- Channel permissions changed.

`doctor` command should validate these early.

## Configuration Strategy (easy ops)

### Secrets (`.env`)

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `DATABASE_PATH`
- `LOG_LEVEL`
- `REPORT_TIMEZONE` (default `UTC`)

### Non-secrets (`config/settings.yaml`)

- report cadence (later scheduler use)
- list sizes (`top_gainers_count`, `top_losers_count`)
- formatting options:
  - decimals
  - abbreviations on/off
  - divider style
  - timestamp format
- provider options:
  - cache TTL
  - timeout
  - retry count
  - backoff settings
  - rate limit budget

This split keeps secrets isolated and formatting easy to tweak.

## CLI + TUI Plan (teleforward-like ops)

Use a CLI-first core with a TUI shell on top.

### CLI Commands (first)

- `marketwatcher doctor`
- `marketwatcher fetch`
- `marketwatcher render`
- `marketwatcher send`
- `marketwatcher run`
- `marketwatcher tui`

CLI status output should be stable and grep-friendly:
- run header
- `[OK]`, `[SKIP]`, `[ERR]` step lines
- explicit retry/backoff messages for `429`

This is important for debugging rate-limit issues and unattended runs.

### TUI Approach

Phase 1 TUI: Rich-based menu (fastest path, low complexity)
- simple keyboard menu
- live preview panel
- last run status panel

Phase 2 TUI (optional): Textual
- richer navigation, forms, logs pane, background task feedback

## Failure Modes To Design Around (before coding)

1. CoinGecko returns `429`
- Backoff + jitter
- Use cached data if fresh enough
- Mark run as `partial` instead of failing entire send (when possible)

2. API payload shape changes
- Validate and fail section-by-section, not whole process
- Log raw response snippet safely for debugging

3. Missing history for 7D/14D early on
- Render `N/A` until enough snapshots exist
- Do not fake values

4. Telegram message too long
- Enforce list-size truncation
- Validate length pre-send

5. Duplicate posts from overlapping schedulers
- Lockfile / pidfile / DB lock for `run`
- Record recent run hash/timestamp to avoid duplicate send

6. Formatting breaks due to special chars
- Centralized HTML escaping
- Unit tests on category names with `&`, `<`, `>`

7. Timezone confusion
- Store everything in UTC
- Format timestamp in configured zone at render time only

## Test Strategy (minimum serious baseline)

### Unit Tests

- delta computation (1D/7D/14D)
- category sorting and filtering
- number formatting helpers
- Telegram HTML escaping
- message length truncation behavior

### Integration Tests

- CoinGecko client parsing with recorded fixtures
- SQLite persistence + read-back
- render pipeline (`snapshot rows -> final message`)

### Manual Ops Tests

- `doctor` against real Telegram bot/channel
- `fetch` and `render` with network on/off
- `send --dry-run` and real send in test channel

## Delivery Roadmap (phased)

### Phase 0: Scaffold and conventions

Deliver:
- project skeleton
- config loading
- logging
- CLI command stubs

Accept when:
- `marketwatcher --help` works
- `marketwatcher doctor --help` etc. exist

### Phase 1: CoinGecko provider + storage

Deliver:
- provider client with retry/backoff/rate limit
- SQLite schema + snapshot writes
- `fetch` command

Accept when:
- repeated `fetch` runs do not create bad duplicates
- transient failures are logged and retried correctly

### Phase 2: Report model + renderer

Deliver:
- delta calculations
- category ranking logic
- Jinja2 template rendering
- message length guard

Accept when:
- `render` prints the target layout
- `N/A` is shown correctly for insufficient history

### Phase 3: Telegram publisher + doctor

Deliver:
- Bot API send integration
- `--dry-run`
- doctor checks for auth/chat permissions

Accept when:
- message posts successfully to a test channel
- common config mistakes are detected clearly

### Phase 4: Scheduler / daemon loop

Deliver:
- `run` loop with interval
- lock protection
- run logs / last status

Accept when:
- no duplicate sends during overlapping execution attempts

### Phase 5: TUI (operator UX)

Deliver:
- menu-driven TUI (preview/send/fetch/doctor/logs/config view)

Accept when:
- operator can complete core workflow without using raw CLI commands

### Phase 6: Extensibility (macro later)

Deliver:
- provider/report registry interfaces
- add a stub macro report pipeline (no live API required yet)

Accept when:
- new report types can be added without changing core scheduler flow

## Immediate Next Build Step (recommended)

Start with:

1. `pyproject.toml` + package skeleton
2. config loader (`.env` + YAML)
3. `doctor` and `fetch` command stubs
4. CoinGecko client with rate limiting and fixtures
5. SQLite snapshot schema

This gets the risky parts (API + rate limits + persistence) stable before UI polish.

## Open Decisions (need your input before implementation)

1. Posting cadence for v1: hourly, every 4h, or daily?
2. Prefer `@channelusername` or numeric `chat_id`?
3. Should v1 always post a new message, or support edit-last from day one?
4. TUI priority: start with Rich menu (faster) vs go straight to Textual (richer)?
