"""Transformation pipeline for clubes endpoint."""

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


def _clean_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalise_record(clube_id: int, raw: dict[str, Any], collected_at: datetime) -> dict[str, Any]:
    escudos = raw.get("escudos") or {}
    return {
        "clube_id": clube_id,
        "nome": _clean_string(raw.get("nome")) or str(clube_id),
        "nome_fantasia": _clean_string(raw.get("nome_fantasia")),
        "apelido": _clean_string(raw.get("apelido")),
        "abreviacao": _clean_string(raw.get("abreviacao")),
        "slug": _clean_string(raw.get("slug")),
        "escudo_30x30": _clean_string(escudos.get("30x30")),
        "escudo_45x45": _clean_string(escudos.get("45x45")),
        "escudo_60x60": _clean_string(escudos.get("60x60")),
        "url_editoria": _clean_string(raw.get("url_editoria")),
        "timestamp_coleta": collected_at,
    }


def transform_clubes(
    *,
    base_dir: Path | None = None,
    schema: SchemaSpec | None = None,
    raw_root: Path | None = None,
) -> dict[str, Any]:
    """Transform raw clubes payload into stage and processed datasets."""
    project_root = base_dir or _project_root()
    spec = schema or load_schema("clubes", base_dir=project_root)

    raw_base = Path(raw_root) if raw_root is not None else project_root / "data" / "raw"
    raw_dir = raw_base / spec.raw_source.get("endpoint", "clubes")
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw directory not found: {raw_dir}")

    raw_files = sorted(raw_dir.glob("*.json"))
    if not raw_files:
        raise FileNotFoundError(f"No raw files found in {raw_dir}")

    records: list[dict[str, Any]] = []
    for path in raw_files:
        collected_at = _parse_timestamp_from_name(path)
        payload = json.loads(path.read_text(encoding="utf-8"))
        for key, value in payload.items():
            try:
                clube_id = int(key)
            except (TypeError, ValueError):
                continue
            if isinstance(value, dict):
                records.append(_normalise_record(clube_id, value, collected_at))

    frame = pd.DataFrame(records)
    if frame.empty:
        raise ValueError("No records produced for clubes")

    frame["timestamp_coleta"] = pd.to_datetime(frame["timestamp_coleta"], utc=True)

    stage_dir = project_root / "data" / "stage" / "clubes"
    stage_dir.mkdir(parents=True, exist_ok=True)
    run_timestamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    stage_path = stage_dir / f"{run_timestamp}.parquet"
    frame.to_parquet(stage_path, index=False)

    processed_path = project_root / spec.processed.get(
        "dataset",
        "data/processed/clubes/clubes.parquet",
    )
    processed_path.parent.mkdir(parents=True, exist_ok=True)
    processed = (
        frame.sort_values("timestamp_coleta")
        .drop_duplicates(subset=["clube_id"], keep="last")
        .sort_values("clube_id")
        .reset_index(drop=True)
    )
    processed.to_parquet(processed_path, index=False)

    return {
        "stage_path": stage_path,
        "processed_path": processed_path,
        "rows_stage": len(frame),
        "rows_processed": len(processed),
    }