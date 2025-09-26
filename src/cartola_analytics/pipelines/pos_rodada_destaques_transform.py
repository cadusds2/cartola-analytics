"""Transformation pipeline for pos_rodada_destaques endpoint.""" 

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ..schema import SchemaSpec, load_schema


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


def _normalise_record(payload: dict[str, Any], collected_at: datetime) -> dict[str, Any]:
    mito = payload.get("mito_rodada") or {}
    rodada = _maybe_int(mito.get("rodada_time_id")) or 0

    return {
        "rodada": rodada,
        "time_id": _maybe_int(mito.get("time_id")) or 0,
        "nome_time": _clean(mito.get("nome")) or "",
        "nome_cartola": _clean(mito.get("nome_cartola")) or "",
        "slug": _clean(mito.get("slug")),
        "clube_id": _maybe_int(mito.get("clube_id")),
        "media_pontos": _maybe_float(payload.get("media_pontos")) or 0.0,
        "media_cartoletas": _maybe_float(payload.get("media_cartoletas")) or 0.0,
        "tipo_escudo": _maybe_int(mito.get("tipo_escudo")),
        "tipo_camisa": _maybe_int(mito.get("tipo_camisa")),
        "esquema_id": _maybe_int(mito.get("esquema_id")),
        "url_escudo_svg": _clean(mito.get("url_escudo_svg")),
        "url_camisa_svg": _clean(mito.get("url_camisa_svg")),
        "assinante": bool(mito.get("assinante")),
        "timestamp_coleta": collected_at,
    }


def transform_pos_rodada_destaques(
    *,
    base_dir: Path | None = None,
    schema: SchemaSpec | None = None,
    raw_root: Path | None = None,
) -> dict[str, Any]:
    """Transform raw pos_rodada_destaques payload into stage and processed datasets."""
    project_root = base_dir or _project_root()
    spec = schema or load_schema("pos_rodada_destaques", base_dir=project_root)

    raw_base = Path(raw_root) if raw_root is not None else project_root / "data" / "raw"
    raw_dir = raw_base / spec.raw_source.get("endpoint", "pos_rodada_destaques")
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw directory not found: {raw_dir}")

    raw_files = sorted(raw_dir.glob("*.json"))
    if not raw_files:
        raise FileNotFoundError(f"No raw files found in {raw_dir}")

    records: list[dict[str, Any]] = []
    for path in raw_files:
        collected_at = _parse_timestamp_from_name(path)
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            continue
        records.append(_normalise_record(payload, collected_at))

    frame = pd.DataFrame(records)
    if frame.empty:
        raise ValueError("No records produced for pos_rodada_destaques")

    frame["timestamp_coleta"] = pd.to_datetime(frame["timestamp_coleta"], utc=True)

    stage_dir = project_root / "data" / "stage" / "pos_rodada_destaques"
    stage_dir.mkdir(parents=True, exist_ok=True)
    run_timestamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    stage_path = stage_dir / f"{run_timestamp}.parquet"
    frame.to_parquet(stage_path, index=False)

    processed_path = project_root / spec.processed.get(
        "dataset",
        "data/processed/rodadas/pos_rodada_destaques.parquet",
    )
    processed_path.parent.mkdir(parents=True, exist_ok=True)
    processed = (
        frame.sort_values("timestamp_coleta")
        .drop_duplicates(subset=["rodada"], keep="last")
        .sort_values("rodada")
        .reset_index(drop=True)
    )
    processed.to_parquet(processed_path, index=False)

    return {
        "stage_path": stage_path,
        "processed_path": processed_path,
        "rows_stage": len(frame),
        "rows_processed": len(processed),
    }