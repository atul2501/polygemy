"""Environment configuration shared with the Rust engine (same variable names, same rules)."""

import os
from dataclasses import dataclass
from typing import Mapping

APP_ENVS = ("local", "stockholm")
TRADING_MODES = ("development", "backtest", "replay", "paper", "shadow", "live")


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Settings:
    app_env: str
    trading_mode: str
    database_url: str
    log_level: str = "INFO"
    log_format: str = "json"

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> "Settings":
        def required(key: str) -> str:
            value = env.get(key, "")
            if not value:
                raise ConfigError(f"{key} is required")
            return value

        def choice(key: str, allowed: tuple[str, ...]) -> str:
            value = required(key)
            if value not in allowed:
                raise ConfigError(f"{key}={value!r} is invalid; allowed: {', '.join(allowed)}")
            return value

        url = required("DATABASE_URL")
        if not url.startswith(("postgres://", "postgresql://")):
            raise ConfigError("DATABASE_URL must be a PostgreSQL URL")
        log_format = env.get("LOG_FORMAT") or "json"
        if log_format not in ("json", "pretty"):
            raise ConfigError(f"LOG_FORMAT={log_format!r} is invalid; allowed: json, pretty")
        return cls(
            app_env=choice("APP_ENV", APP_ENVS),
            trading_mode=choice("TRADING_MODE", TRADING_MODES),
            database_url=url,
            log_level=(env.get("LOG_LEVEL") or "info").upper(),
            log_format=log_format,
        )
