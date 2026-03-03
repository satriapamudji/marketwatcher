"""Configuration loader for MarketWatcher.

Handles:
- .env file for secrets (TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DATABASE_PATH, LOG_LEVEL)
- YAML file for non-secrets (report settings, provider options)
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


@dataclass
class TelegramConfig:
    bot_token: str = ""
    chat_id: str = ""


@dataclass
class LoggingConfig:
    level: str = "INFO"
    jsonl_path: str = "logs/marketwatcher.jsonl"
    console: bool = True


@dataclass
class ProviderConfig:
    cache_ttl: int = 300
    timeout: int = 30
    retry_count: int = 3
    backoff_factor: float = 1.5
    rate_limit_calls: int = 10
    rate_limit_period: int = 60


@dataclass
class ReportConfig:
    top_gainers_count: int = 5
    top_losers_count: int = 5
    decimals: int = 2
    use_abbreviations: bool = True
    divider_style: str = "——"
    timestamp_format: str = "%Y-%m-%d %H:%M UTC"
    report_timezone: str = "UTC"


@dataclass
class OnchainConfig:
    pool_fetch_limit: int = 100
    min_liquidity_usd: float = 100000
    min_volume_usd: float = 50000
    exclude_native_tokens: bool = True
    top_gainers_count: int = 8
    top_losers_count: int = 8
    movers_candidate_count: int = 13
    name_exclude_patterns: list[str] = field(default_factory=lambda: [
        "^[^a-zA-Z]+$",
        "^(.)\\1{3,}",
        ".*[0-9]{4,}.*",
    ])
    symbol_exclude_list: list[str] = field(default_factory=list)
    dex_whitelist: list[str] = field(default_factory=list)
    top_tokens: list[str] = field(default_factory=list)
    max_tokens: int = 15


@dataclass
class AlertConfig:
    """Alert loop configuration."""
    check_interval_minutes: int = 15


@dataclass
class JobConfig:
    """A scheduled job configuration."""
    id: str = ""
    type: str = "global"  # "global", "onchain", "global_onchain", or "watchlist"
    chain: str = ""  # Only for onchain type
    time: str = "09:00"  # HH:MM (used when interval_hours is 0)
    enabled: bool = True
    interval_hours: int = 0  # 0 = daily at time, >0 = every N hours
    offset_minutes: int = 0  # Stagger offset for interval jobs
    chat_id: str = ""  # Override channel (empty = use global default)
    watchlist_id: str = ""  # Only for watchlist type

    def display_name(self) -> str:
        """Get human-readable name."""
        if self.type == "global":
            return "Global Crypto"
        elif self.type == "global_onchain":
            return "Global On-Chain"
        elif self.type == "macro":
            return "Global Macro"
        elif self.type == "watchlist":
            return f"Watchlist {self.watchlist_id or 'main'}"
        else:
            return f"On-Chain {self.chain.upper()}" if self.chain else "On-Chain"

    def schedule_display(self) -> str:
        """Get human-readable schedule string."""
        if self.interval_hours > 0:
            s = f"Every {self.interval_hours}h"
            if self.offset_minutes > 0:
                s += f" +{self.offset_minutes}m"
            return s
        return self.time or "--:--"


@dataclass
class SchedulerConfig:
    timezone: str = "UTC"
    jobs: list[JobConfig] = field(default_factory=list)


@dataclass
class Config:
    telegram: TelegramConfig = field(default_factory=TelegramConfig)
    provider: ProviderConfig = field(default_factory=ProviderConfig)
    report: ReportConfig = field(default_factory=ReportConfig)
    onchain: OnchainConfig = field(default_factory=OnchainConfig)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    alerts: AlertConfig = field(default_factory=AlertConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    database_path: str = "marketwatcher.db"
    log_level: str = "INFO"
    project_root: Path = field(default_factory=Path.cwd)


def load_yaml_config(yaml_path: Path) -> dict[str, Any]:
    """Load YAML configuration file."""
    if not yaml_path.exists():
        return {}

    with open(yaml_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_config(config_dir: Path | None = None, env_file: Path | None = None) -> Config:
    """Load full configuration from .env and YAML files.

    Args:
        config_dir: Directory containing settings.yaml (defaults to project root/config)
        env_file: Path to .env file (defaults to project root/.env)
    """
    # Determine project root (parent of src/marketwatcher)
    project_root = Path(__file__).parent.parent.parent

    if config_dir is None:
        config_dir = project_root / "config"
    if env_file is None:
        env_file = project_root / ".env"

    # Load .env secrets
    if env_file.exists():
        load_dotenv(env_file)

    # Load YAML non-secrets
    yaml_config = load_yaml_config(config_dir / "settings.yaml")
    schedules_yaml = load_yaml_config(config_dir / "schedules.yaml")

    # Build config object
    config = Config(project_root=project_root)

    # Apply secrets from environment (loaded via load_dotenv above)
    import os
    config.telegram.bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    config.telegram.chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    config.database_path = os.environ.get("DATABASE_PATH", "marketwatcher.db")
    config.log_level = os.environ.get("LOG_LEVEL", "INFO")

    # Apply YAML settings
    if yaml_config:
        if "report" in yaml_config:
            for key, value in yaml_config["report"].items():
                if hasattr(config.report, key):
                    setattr(config.report, key, value)

        if "provider" in yaml_config:
            for key, value in yaml_config["provider"].items():
                if hasattr(config.provider, key):
                    setattr(config.provider, key, value)

        if "onchain" in yaml_config:
            for key, value in yaml_config["onchain"].items():
                if hasattr(config.onchain, key):
                    setattr(config.onchain, key, value)

        if "alerts" in yaml_config:
            for key, value in yaml_config["alerts"].items():
                if hasattr(config.alerts, key):
                    setattr(config.alerts, key, value)

        if "logging" in yaml_config:
            for key, value in yaml_config["logging"].items():
                if hasattr(config.logging, key):
                    setattr(config.logging, key, value)

    # Apply scheduler YAML settings (separate file managed by TUI)
    if schedules_yaml:
        scheduler_cfg = schedules_yaml.get("scheduler", schedules_yaml)
        if "timezone" in scheduler_cfg:
            config.scheduler.timezone = scheduler_cfg["timezone"]

        # Check for new format (jobs list)
        if "jobs" in scheduler_cfg and isinstance(scheduler_cfg["jobs"], list):
            for job_data in scheduler_cfg["jobs"]:
                if isinstance(job_data, dict):
                    job = JobConfig(
                        id=job_data.get("id", ""),
                        type=job_data.get("type", "global"),
                        chain=job_data.get("chain", ""),
                        time=str(job_data.get("time", "09:00")),
                        enabled=bool(job_data.get("enabled", True)),
                        interval_hours=int(job_data.get("interval_hours", 0)),
                        offset_minutes=int(job_data.get("offset_minutes", 0)),
                        chat_id=str(job_data.get("chat_id", "")),
                        watchlist_id=str(job_data.get("watchlist_id", "")),
                    )
                    if job.id:
                        config.scheduler.jobs.append(job)
        else:
            # Legacy format (global_crypto, onchain_solana, onchain_base)
            legacy_jobs = [
                ("global_crypto", "global", ""),
                ("onchain_solana", "onchain", "solana"),
                ("onchain_base", "onchain", "base"),
            ]
            for job_key, job_type, chain in legacy_jobs:
                if job_key in scheduler_cfg and isinstance(scheduler_cfg[job_key], dict):
                    job_data = scheduler_cfg[job_key]
                    job = JobConfig(
                        id=job_key,
                        type=job_type,
                        chain=chain,
                        time=str(job_data.get("time", "09:00")),
                        enabled=bool(job_data.get("enabled", False)),
                    )
                    config.scheduler.jobs.append(job)

    return config


def get_config(force_reload: bool = False) -> Config:
    """Get the global config instance (lazy-loaded).

    Args:
        force_reload: If True, reload from files even if already loaded
    """
    global _config_instance
    if _config_instance is None or force_reload:
        _config_instance = load_config()
    return _config_instance


def reload_config() -> Config:
    """Force reload config from files."""
    global _config_instance
    _config_instance = None
    return get_config(force_reload=True)


_config_instance: Config | None = None
