"""Transformation pipeline for partidas endpoint."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ..schema import SchemaSpec, load_schema

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


def _maybe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        for fmt in _DATE_FORMATS:
            try:
                return datetime.strptime(stripped, fmt).replace(tzinfo=UTC)
            except ValueError:
                continue
        parsed = pd.to_datetime(stripped, utc=True, errors="coerce")
        if isinstance(parsed, pd.Timestamp) and pd.notna(parsed):
            return parsed.to_pydatetime()
    return None


def _format_sequence(sequence: Any) -> str | None:
    if not isinstance(sequence, list):
        return None
    tokens = [str(item).strip() for item in sequence if item]
    cleaned = [token for token in tokens if token]
    return "".join(cleaned) if cleaned else None


def _normalise_record(
    raw: dict[str, Any],
    rodada: int | None,
    collected_at: datetime,
) -> dict[str, Any]:
    transmissao = raw.get("transmissao") or {}
    return {
        "rodada": rodada,
        "partida_id": int(raw["partida_id"]),
        "campeonato_id": int(raw.get("campeonato_id", 0)),
        "partida_data": _coerce_datetime(raw.get("partida_data")) or collected_at,
        "timestamp_partida": _coerce_datetime(raw.get("timestamp")),
        "timestamp_coleta": collected_at,
        "clube_casa_id": int(raw["clube_casa_id"]),
        "clube_casa_posicao": _maybe_int(raw.get("clube_casa_posicao")),
        "clube_visitante_id": int(raw["clube_visitante_id"]),
        "clube_visitante_posicao": _maybe_int(raw.get("clube_visitante_posicao")),
        "placar_oficial_mandante": _maybe_int(raw.get("placar_oficial_mandante")),
        "placar_oficial_visitante": _maybe_int(raw.get("placar_oficial_visitante")),
        "aproveitamento_mandante": _format_sequence(
            raw.get("aproveitamento_mandante")
        ),
        "aproveitamento_visitante": _format_sequence(
            raw.get("aproveitamento_visitante")
        ),
        "valida": bool(raw.get("valida")),
        "local": (raw.get("local") or "").strip() or None,
        "transmissao_label": (transmissao.get("label") or "").strip() or None,
        "transmissao_url": (transmissao.get("url") or "").strip() or None,
        "status_transmissao_tr": (raw.get("status_transmissao_tr") or "").strip() or None,
        "status_cronometro_tr": (raw.get("status_cronometro_tr") or "").strip() or None,
        "periodo_tr": (raw.get("periodo_tr") or "").strip() or None,
        "inicio_cronometro_tr": (raw.get("inicio_cronometro_tr") or "").strip() or None,
    }


def transform_partidas(
    *,
    base_dir: Path | None = None,
    schema: SchemaSpec | None = None,
    raw_root: Path | None = None,
) -> dict[str, Any]:
    """Transform raw partidas payloads into stage and processed datasets."""
    project_root = base_dir or _project_root()
    spec = schema or load_schema("partidas", base_dir=project_root)

    raw_base = Path(raw_root) if raw_root is not None else project_root / "data" / "raw"
    raw_dir = raw_base / spec.raw_source.get("endpoint", "partidas")
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw directory not found: {raw_dir}")

    raw_files = sorted(raw_dir.glob("*.json"))
    if not raw_files:
        raise FileNotFoundError(f"No raw files found in {raw_dir}")

    records: list[dict[str, Any]] = []
    for path in raw_files:
        collected_at = _parse_timestamp_from_name(path)
        payload = json.loads(path.read_text(encoding="utf-8"))
        rodada_value = _maybe_int(payload.get("rodada"))
        if rodada_value is None:
            raise ValueError("Campo 'rodada' ausente no payload de partidas")
        partidas = payload.get("partidas")
        if isinstance(partidas, list):
            for entry in partidas:
                if isinstance(entry, dict) and "partida_id" in entry:
                    records.append(
                        _normalise_record(entry, rodada_value, collected_at)
                    )

    frame = pd.DataFrame(records)
    if frame.empty:
        raise ValueError("No records produced for partidas")

    for column in ("partida_data", "timestamp_partida", "timestamp_coleta"):
        frame[column] = pd.to_datetime(frame[column], utc=True)

    stage_dir = project_root / "data" / "stage" / "partidas"
    stage_dir.mkdir(parents=True, exist_ok=True)
    run_timestamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    stage_path = stage_dir / f"{run_timestamp}.parquet"
    frame.to_parquet(stage_path, index=False)

    processed_path = project_root / spec.processed.get(
        "dataset",
        "data/processed/partidas/partidas.parquet",
    )
    processed_path.parent.mkdir(parents=True, exist_ok=True)
    processed = (
        frame.sort_values("timestamp_coleta")
        .drop_duplicates(subset=["partida_id"], keep="last")
        .sort_values("partida_id")
        .reset_index(drop=True)
    )
    processed.to_parquet(processed_path, index=False)

    return {
        "stage_path": stage_path,
        "processed_path": processed_path,
        "rows_stage": len(frame),
        "rows_processed": len(processed),
    }