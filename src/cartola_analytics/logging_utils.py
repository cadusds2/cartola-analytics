"""Logging helpers with JSON output for Cartola Analytics.""" 

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from .config import DEFAULT_LOG_LEVEL, CartolaSettings

_LOGGING_SKIP_FIELDS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
}


class JsonFormatter(logging.Formatter):
    """Formatter that emits log records as JSON strings."""

    def format(self, record: logging.LogRecord) -> str:
        base: dict[str, Any] = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in _LOGGING_SKIP_FIELDS or key.startswith("_"):
                continue
            if key == "extra":
                continue
            base[key] = value
        return json.dumps(base, ensure_ascii=True)


def configure_logging(
    level: str = DEFAULT_LOG_LEVEL,
    log_file: Path | None = None,
) -> None:
    """Configure root logging with JSON formatter."""
    resolved_level = getattr(logging, level.upper(), logging.INFO)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(JsonFormatter())

    handlers: list[logging.Handler] = [stream_handler]
    if log_file is not None:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(JsonFormatter())
        handlers.append(file_handler)

    logging.basicConfig(level=resolved_level, handlers=handlers, force=True)


def configure_logging_from_settings(settings: CartolaSettings) -> None:
    """Configure logging using level stored in settings."""
    configure_logging(settings.log_level, settings.log_file)
