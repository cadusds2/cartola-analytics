"""Transformation pipeline for mercado_status endpoint."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ..schema import SchemaSpec, load_schema

_STATUS_MAP = {
    1: "ABERTO",
    2: "FECHADO",
    3: "EM_MANUTENCAO",
}


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _parse_timestamp_from_name(path: Path) -> datetime:
    try:
        return datetime.strptime(path.stem, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    except ValueError:
        return datetime.now(tz=UTC)


def _require(payload: dict[str, Any], key: str) -> Any:
    value = payload.get(key)
    if value is None:
        raise ValueError(f"Campo obrigatorio ausente: {key}")
    return value


def _normalise_record(raw: dict[str, Any], collected_at: datetime) -> dict[str, Any]:
    fechamento_raw = raw.get("fechamento")
    fechamento_ts = (
        fechamento_raw.get("timestamp")
        if isinstance(fechamento_raw, dict)
        else None
    )
    fechamento_dt = (
        datetime.fromtimestamp(fechamento_ts, tz=UTC)
        if isinstance(fechamento_ts, (int, float))
        else collected_at
    )
    status_raw = raw.get("status_mercado")
    if isinstance(status_raw, (int, float)):
        status_int = int(status_raw)
        status_text = _STATUS_MAP.get(status_int, str(status_int))
    else:
        status_text = str(status_raw)

    return {
        "temporada": int(_require(raw, "temporada")),
        "rodada_atual": int(_require(raw, "rodada_atual")),
        "rodada_final": int(_require(raw, "rodada_final")),
        "status_mercado": status_text,
        "mercado_pos_rodada": bool(raw.get("mercado_pos_rodada")),
        "bola_rolando": bool(raw.get("bola_rolando")),
        "timestamp_fechamento": fechamento_dt,
        "timestamp_coleta": collected_at,
    }


def transform_mercado_status(
    *,
    base_dir: Path | None = None,
    schema: SchemaSpec | None = None,
) -> dict[str, Any]:
    """Transform raw mercado_status payloads into stage and processed datasets."""
    project_root = base_dir or _project_root()
    spec = schema or load_schema("mercado_status", base_dir=project_root)

    raw_dir = project_root / "data" / "raw" / spec.raw_source.get(
        "endpoint",
        "mercado_status",
    )
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw directory not found: {raw_dir}")

    raw_files = sorted(raw_dir.glob("*.json"))
    if not raw_files:
        raise FileNotFoundError(f"No raw files found in {raw_dir}")

    records: list[dict[str, Any]] = []
    for path in raw_files:
        collected_at = _parse_timestamp_from_name(path)
        payload = json.loads(path.read_text(encoding="utf-8"))
        records.append(_normalise_record(payload, collected_at))

    frame = pd.DataFrame(records)
    if frame.empty:
        raise ValueError("No records produced for mercado_status")

    for column in ("timestamp_fechamento", "timestamp_coleta"):
        frame[column] = pd.to_datetime(frame[column], utc=True)

    stage_dir = project_root / "data" / "stage" / "mercado_status"
    stage_dir.mkdir(parents=True, exist_ok=True)
    run_timestamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    stage_path = stage_dir / f"{run_timestamp}.parquet"
    frame.to_parquet(stage_path, index=False)

    processed_path = project_root / spec.processed.get(
        "dataset",
        "data/processed/mercado/mercado_status.parquet",
    )
    processed_path.parent.mkdir(parents=True, exist_ok=True)
    processed = (
        frame.sort_values("timestamp_coleta")
        .drop_duplicates(subset=["temporada"], keep="last")
        .sort_values("temporada")
        .reset_index(drop=True)
    )
    processed.to_parquet(processed_path, index=False)

    return {
        "stage_path": stage_path,
        "processed_path": processed_path,
        "rows_stage": len(frame),
        "rows_processed": len(processed),
    }
