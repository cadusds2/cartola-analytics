"""Utilities to persist raw Cartola endpoint payloads."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

from ..config import CartolaSettings, load_settings
from ..endpoints import Endpoint
from ..http_client import CartolaClient

logger = logging.getLogger(__name__)


def build_output_path(
    base_dir: Path,
    endpoint: Endpoint,
    *,
    rodada: int | None,
    timestamp: datetime,
) -> Path:
    """Build target path for a raw payload file."""
    parts = [endpoint.name]
    if rodada is not None:
        parts.append(f"rodada={rodada:03d}")
    directory = base_dir.joinpath(*parts)
    directory.mkdir(parents=True, exist_ok=True)
    filename = timestamp.strftime("%Y%m%dT%H%M%SZ")
    return directory / f"{filename}.json"


def collect_endpoint_payload(
    endpoint: Endpoint,
    *,
    rodada: int | None = None,
    client: CartolaClient | None = None,
    settings: CartolaSettings | None = None,
    base_dir: Path | None = None,
    timestamp: datetime | None = None,
    use_cache: bool = False,
) -> Path:
    """Fetch endpoint payload and persist it to disk."""
    active_settings = settings or load_settings()
    target_dir = base_dir or active_settings.raw_dir
    current_timestamp = (timestamp or datetime.now(UTC)).astimezone(UTC)
    path = build_output_path(
        target_dir,
        endpoint,
        rodada=rodada,
        timestamp=current_timestamp,
    )

    owns_client = client is None
    active_client = client or CartolaClient(settings=active_settings)

    try:
        payload = active_client.fetch(endpoint, rodada=rodada, use_cache=use_cache)
    finally:
        if owns_client:
            active_client.close()

    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    logger.info(
        "raw_payload_saved",
        extra={
            "event": "raw_payload_saved",
            "endpoint": endpoint.name,
            "rodada": rodada,
            "path": str(path),
        },
    )
    return path

