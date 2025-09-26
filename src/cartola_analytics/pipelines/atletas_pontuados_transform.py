"""Transformation pipeline for atletas_pontuados endpoint."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ..schema import SchemaSpec, load_schema

_SCOUT_FIELDS = {
    "CA": "scout_ca",
    "CV": "scout_cv",
    "FS": "scout_fs",
    "FC": "scout_fc",
    "DS": "scout_ds",
    "G": "scout_g",
    "A": "scout_a",
    "FD": "scout_fd",
    "FF": "scout_ff",
    "FT": "scout_ft",
    "PP": "scout_pp",
    "DP": "scout_dp",
    "SG": "scout_sg",
    "GC": "scout_gc",
    "I": "scout_i",
}


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _parse_timestamp_from_name(path: Path) -> datetime:
    try:
        return datetime.strptime(path.stem, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    except ValueError:
        return datetime.now(tz=UTC)


def _maybe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _maybe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalise_record(
    atleta_id: int,
    payload: dict[str, Any],
    *,
    rodada: int,
    collected_at: datetime,
) -> dict[str, Any]:
    scout = payload.get("scout") or {}
    record = {
        "rodada": rodada,
        "atleta_id": atleta_id,
        "clube_id": _maybe_int(payload.get("clube_id")),
        "posicao_id": _maybe_int(payload.get("posicao_id")),
        "pontuacao": _maybe_float(payload.get("pontuacao")) or 0.0,
        "entrou_em_campo": bool(payload.get("entrou_em_campo")),
        "apelido": _clean(payload.get("apelido")),
        "slug": _clean(payload.get("slug")),
        "timestamp_coleta": collected_at,
    }
    for scout_key, column in _SCOUT_FIELDS.items():
        record[column] = _maybe_int(scout.get(scout_key)) or 0
    return record


def transform_atletas_pontuados(
    *,
    base_dir: Path | None = None,
    schema: SchemaSpec | None = None,
    raw_root: Path | None = None,
) -> dict[str, Any]:
    """Transform raw atletas_pontuados payload into stage and processed datasets."""
    project_root = base_dir or _project_root()
    spec = schema or load_schema("atletas_pontuados", base_dir=project_root)

    raw_base = Path(raw_root) if raw_root is not None else project_root / "data" / "raw"
    raw_dir = raw_base / spec.raw_source.get("endpoint", "atletas_pontuados")
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw directory not found: {raw_dir}")

    raw_files = sorted(raw_dir.glob("*.json"))
    if not raw_files:
        raise FileNotFoundError(f"No raw files found in {raw_dir}")

    records: list[dict[str, Any]] = []
    for path in raw_files:
        collected_at = _parse_timestamp_from_name(path)
        payload = json.loads(path.read_text(encoding="utf-8"))
        rodada = _maybe_int(payload.get("rodada"))
        if rodada is None:
            continue
        atletas = payload.get("atletas")
        if isinstance(atletas, dict):
            for key, value in atletas.items():
                try:
                    atleta_id = int(key)
                except (TypeError, ValueError):
                    continue
                if isinstance(value, dict):
                    records.append(
                        _normalise_record(
                            atleta_id,
                            value,
                            rodada=rodada,
                            collected_at=collected_at,
                        )
                    )

    frame = pd.DataFrame(records)
    if frame.empty:
        raise ValueError("No records produced for atletas_pontuados")

    frame["timestamp_coleta"] = pd.to_datetime(frame["timestamp_coleta"], utc=True)

    stage_dir = project_root / "data" / "stage" / "atletas_pontuados"
    stage_dir.mkdir(parents=True, exist_ok=True)
    run_timestamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    stage_path = stage_dir / f"{run_timestamp}.parquet"
    frame.to_parquet(stage_path, index=False)

    processed_path = project_root / spec.processed.get(
        "dataset",
        "data/processed/atletas/atletas_pontuados.parquet",
    )
    processed_path.parent.mkdir(parents=True, exist_ok=True)
    processed = (
        frame.sort_values("timestamp_coleta")
        .drop_duplicates(subset=["atleta_id"], keep="last")
        .sort_values(["rodada", "atleta_id"])
        .reset_index(drop=True)
    )
    processed.to_parquet(processed_path, index=False)

    return {
        "stage_path": stage_path,
        "processed_path": processed_path,
        "rows_stage": len(frame),
        "rows_processed": len(processed),
    }