"""Transformation pipeline for rodadas endpoint."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ..schema import SchemaSpec, load_schema
from ..validation import validate_with_logging


_DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ",
]


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _parse_timestamp_from_name(path: Path) -> datetime:
    try:
        return datetime.strptime(path.stem, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    except ValueError:
        return datetime.now(tz=UTC)


def _ensure_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("rodadas", "lista", "items", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _coerce_datetime(value: Any) -> str | datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC)
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        raw = value.strip()
        for fmt in _DATE_FORMATS:
            try:
                return datetime.strptime(raw, fmt).replace(tzinfo=UTC)
            except ValueError:
                continue
        parsed = pd.to_datetime(raw, utc=True, errors="coerce")
        if isinstance(parsed, pd.Timestamp) and pd.notna(parsed):
            return parsed.to_pydatetime()
    return None


def _normalise_record(raw: dict[str, Any], collected_at: datetime) -> dict[str, Any]:
    rodada_id = raw.get("rodada_id")
    if rodada_id is None:
        raise ValueError("Campo obrigatorio ausente: rodada_id")

    nome = raw.get("nome_rodada") or raw.get("rodada") or raw.get("nome")
    if not nome:
        raise ValueError("Campo obrigatorio ausente: nome_rodada")

    inicio_value = _coerce_datetime(raw.get("inicio") or raw.get("abertura"))
    fim_value = _coerce_datetime(raw.get("fim") or raw.get("fechamento"))

    return {
        "rodada_id": int(rodada_id),
        "nome_rodada": str(nome),
        "inicio": inicio_value or collected_at,
        "fim": fim_value or collected_at,
        "timestamp_coleta": collected_at,
    }


def transform_rodadas(
    *,
    base_dir: Path | None = None,
    schema: SchemaSpec | None = None,
    raw_root: Path | None = None,
) -> dict[str, Any]:
    """Transform raw rodadas payloads into stage and processed datasets."""
    project_root = base_dir or _project_root()
    spec = schema or load_schema("rodadas", base_dir=project_root)

    raw_base = Path(raw_root) if raw_root is not None else project_root / "data" / "raw"
    raw_dir = raw_base / spec.raw_source.get("endpoint", "rodadas")
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw directory not found: {raw_dir}")

    raw_files = sorted(raw_dir.glob("*.json"))
    if not raw_files:
        raise FileNotFoundError(f"No raw files found in {raw_dir}")

    records: list[dict[str, Any]] = []
    for path in raw_files:
        collected_at = _parse_timestamp_from_name(path)
        payload = json.loads(path.read_text(encoding="utf-8"))
        for item in _ensure_list(payload):
            records.append(_normalise_record(item, collected_at))

    frame = pd.DataFrame(records)
    if frame.empty:
        raise ValueError("No records produced for rodadas")

    for column in ("inicio", "fim", "timestamp_coleta"):
        frame[column] = pd.to_datetime(frame[column], utc=True)

    stage_dir = project_root / "data" / "stage" / "rodadas"
    stage_dir.mkdir(parents=True, exist_ok=True)
    run_timestamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    stage_path = stage_dir / f"{run_timestamp}.parquet"
    frame.to_parquet(stage_path, index=False)

    processed_path = project_root / spec.processed.get(
        "dataset",
        "data/processed/rodadas/rodadas.parquet",
    )
    processed_path.parent.mkdir(parents=True, exist_ok=True)
    processed = (
        frame.sort_values("timestamp_coleta")
        .drop_duplicates(subset=["rodada_id"], keep="last")
        .sort_values("rodada_id")
        .reset_index(drop=True)
    )
    validate_with_logging(processed, spec)
    processed.to_parquet(processed_path, index=False)

    return {
        "stage_path": stage_path,
        "processed_path": processed_path,
        "rows_stage": len(frame),
        "rows_processed": len(processed),
    }
