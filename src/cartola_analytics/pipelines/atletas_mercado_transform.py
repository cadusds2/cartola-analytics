"""Transformation pipeline for atletas_mercado endpoint."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ..schema import SchemaSpec, load_schema
from ..validation import validate_with_logging

_SCOUT_FIELDS = {
    "CA": "scout_ca",
    "CV": "scout_cv",
    "FS": "scout_fs",
    "GC": "scout_gc",
    "G": "scout_g",
    "A": "scout_a",
    "FD": "scout_fd",
    "FF": "scout_ff",
    "FT": "scout_ft",
    "SG": "scout_sg",
    "PP": "scout_pp",
    "DP": "scout_dp",
    "D": "scout_d",
    "DS": "scout_ds",
    "FC": "scout_fc",
    "I": "scout_i",
    "PI": "scout_pi",
}


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _parse_timestamp_from_name(path: Path) -> datetime:
    try:
        return datetime.strptime(path.stem, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    except ValueError:
        return datetime.now(tz=UTC)


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _convert_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _convert_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalise_record(
    raw: dict[str, Any],
    *,
    collected_at: datetime,
    club_map: dict[int, str],
    position_map: dict[int, str],
    status_map: dict[int, str],
) -> dict[str, Any]:
    scout = raw.get("scout") or {}
    record = {
        "atleta_id": int(raw["atleta_id"]),
        "rodada_id": _convert_int(raw.get("rodada_id")),
        "clube_id": int(raw["clube_id"]),
        "posicao_id": int(raw["posicao_id"]),
        "status_id": int(raw.get("status_id", 0)),
        "status_nome": status_map.get(_convert_int(raw.get("status_id")) or 0),
        "clube_nome": club_map.get(int(raw["clube_id"])),
        "posicao_nome": position_map.get(int(raw["posicao_id"])),
        "nome": _clean(raw.get("nome")) or str(raw["atleta_id"]),
        "apelido": _clean(raw.get("apelido")) or _clean(raw.get("nome")),
        "apelido_abreviado": _clean(raw.get("apelido_abreviado")),
        "slug": _clean(raw.get("slug")),
        "foto": _clean(raw.get("foto")),
        "preco_num": float(raw.get("preco_num", 0.0)),
        "media_num": float(raw.get("media_num", 0.0)),
        "variacao_num": float(raw.get("variacao_num", 0.0)),
        "pontos_num": _convert_float(raw.get("pontos_num")),
        "jogos_num": int(raw.get("jogos_num", 0)),
        "entrou_em_campo": bool(raw.get("entrou_em_campo")),
        "timestamp_coleta": collected_at,
    }
    for scout_key, field_name in _SCOUT_FIELDS.items():
        record[field_name] = _convert_int(scout.get(scout_key)) or 0
    return record


def transform_atletas_mercado(
    *,
    base_dir: Path | None = None,
    schema: SchemaSpec | None = None,
    raw_root: Path | None = None,
) -> dict[str, Any]:
    """Transform raw atletas_mercado payload into stage and processed datasets."""
    project_root = base_dir or _project_root()
    spec = schema or load_schema("atletas_mercado", base_dir=project_root)

    raw_base = Path(raw_root) if raw_root is not None else project_root / "data" / "raw"
    raw_dir = raw_base / spec.raw_source.get("endpoint", "atletas_mercado")
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw directory not found: {raw_dir}")

    raw_files = sorted(raw_dir.glob("*.json"))
    if not raw_files:
        raise FileNotFoundError(f"No raw files found in {raw_dir}")

    records: list[dict[str, Any]] = []
    for path in raw_files:
        collected_at = _parse_timestamp_from_name(path)
        payload = json.loads(path.read_text(encoding="utf-8"))
        club_map = {
            int(k): v.get("nome_fantasia") or v.get("nome")
            for k, v in (payload.get("clubes") or {}).items()
            if isinstance(v, dict)
        }
        position_map = {
            int(k): v.get("nome")
            for k, v in (payload.get("posicoes") or {}).items()
            if isinstance(v, dict)
        }
        status_map = {
            int(k): v.get("nome")
            for k, v in (payload.get("status") or {}).items()
            if isinstance(v, dict)
        }
        atletas = payload.get("atletas")
        if isinstance(atletas, list):
            for entry in atletas:
                if isinstance(entry, dict) and "atleta_id" in entry:
                    records.append(
                        _normalise_record(
                            entry,
                            collected_at=collected_at,
                            club_map=club_map,
                            position_map=position_map,
                            status_map=status_map,
                        )
                    )

    frame = pd.DataFrame(records)
    if frame.empty:
        raise ValueError("No records produced for atletas_mercado")

    frame["timestamp_coleta"] = pd.to_datetime(frame["timestamp_coleta"], utc=True)

    stage_dir = project_root / "data" / "stage" / "atletas_mercado"
    stage_dir.mkdir(parents=True, exist_ok=True)
    run_timestamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    stage_path = stage_dir / f"{run_timestamp}.parquet"
    frame.to_parquet(stage_path, index=False)

    processed_path = project_root / spec.processed.get(
        "dataset",
        "data/processed/atletas/atletas_mercado.parquet",
    )
    processed_path.parent.mkdir(parents=True, exist_ok=True)
    processed = (
        frame.sort_values("timestamp_coleta")
        .drop_duplicates(subset=["atleta_id"], keep="last")
        .sort_values("atleta_id")
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
