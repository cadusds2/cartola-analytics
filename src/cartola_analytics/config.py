"""Environment-backed configuration for Cartola Analytics."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DEFAULT_TIMEOUT = 10.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_FACTOR = 0.5
DEFAULT_CACHE_TTL = 600
DEFAULT_CACHE_DIR = Path("data/cache")
DEFAULT_RAW_DIR = Path("data/raw")
DEFAULT_USER_AGENT = (
    "cartola-analytics/0.1 (+https://github.com/cadu-santos/cartola-analytics)"
)
DEFAULT_ACCEPT = "application/json"
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_FILE: Path | None = None


def _get_env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    try:
        return float(value)
    except ValueError as exc:  # pragma: no cover - defensive
        raise ValueError(f"Invalid float for {name}: {value}") from exc


def _get_env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    try:
        return int(value)
    except ValueError as exc:  # pragma: no cover - defensive
        raise ValueError(f"Invalid int for {name}: {value}") from exc


def _get_env_path(name: str, default: Path | None) -> Path | None:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    lowered = value.strip().lower()
    if lowered in {"none", "null", "false"}:
        return None
    return Path(value)


def _get_env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value


@dataclass(frozen=True)
class CartolaSettings:
    timeout: float = DEFAULT_TIMEOUT
    max_retries: int = DEFAULT_MAX_RETRIES
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR
    cache_ttl: int = DEFAULT_CACHE_TTL
    cache_dir: Path | None = DEFAULT_CACHE_DIR
    raw_dir: Path = DEFAULT_RAW_DIR
    user_agent: str = DEFAULT_USER_AGENT
    accept: str = DEFAULT_ACCEPT
    log_level: str = DEFAULT_LOG_LEVEL
    log_file: Path | None = DEFAULT_LOG_FILE

    @classmethod
    def from_env(cls) -> CartolaSettings:
        return cls(
            timeout=_get_env_float("CARTOLA_TIMEOUT", DEFAULT_TIMEOUT),
            max_retries=_get_env_int("CARTOLA_MAX_RETRIES", DEFAULT_MAX_RETRIES),
            backoff_factor=_get_env_float(
                "CARTOLA_BACKOFF_FACTOR", DEFAULT_BACKOFF_FACTOR
            ),
            cache_ttl=_get_env_int("CARTOLA_CACHE_TTL", DEFAULT_CACHE_TTL),
            cache_dir=_get_env_path("CARTOLA_CACHE_DIR", DEFAULT_CACHE_DIR),
            raw_dir=(
                _get_env_path("CARTOLA_RAW_DIR", DEFAULT_RAW_DIR) or DEFAULT_RAW_DIR
            ),
            user_agent=_get_env_str("CARTOLA_USER_AGENT", DEFAULT_USER_AGENT),
            accept=_get_env_str("CARTOLA_ACCEPT", DEFAULT_ACCEPT),
            log_level=_get_env_str("CARTOLA_LOG_LEVEL", DEFAULT_LOG_LEVEL).upper(),
            log_file=_get_env_path("CARTOLA_LOG_FILE", DEFAULT_LOG_FILE),
        )


def load_settings() -> CartolaSettings:
    """Load settings from environment variables."""
    return CartolaSettings.from_env()
