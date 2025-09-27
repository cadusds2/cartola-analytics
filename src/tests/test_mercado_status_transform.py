import json
from pathlib import Path

import pandas as pd

from cartola_analytics.pipelines import transform_mercado_status
from cartola_analytics.schema import load_schema
from cartola_analytics.validation import validate_dataframe_against_schema


def _write_schema_copy(base_dir: Path) -> None:
    schema_root = Path(__file__).resolve().parents[2] / "docs" / "schemas"
    schema_src = schema_root / "mercado_status.yaml"
    target = base_dir / "docs" / "schemas"
    target.mkdir(parents=True, exist_ok=True)
    target.joinpath("mercado_status.yaml").write_text(
        schema_src.read_text(encoding="utf-8"),
        encoding="utf-8",
    )


def _write_raw_payload(base_dir: Path, name: str, payload: dict[str, object]) -> None:
    raw_dir = base_dir / "data" / "raw" / "mercado_status"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.joinpath(name).write_text(json.dumps(payload), encoding="utf-8")


def test_transform_mercado_status(tmp_path: Path) -> None:
    _write_schema_copy(tmp_path)

    base_payload = {
        "temporada": 2025,
        "rodada_atual": 25,
        "rodada_final": 38,
        "status_mercado": 1,
        "mercado_pos_rodada": True,
        "bola_rolando": False,
        "fechamento": {"timestamp": 1758999540},
    }
    _write_raw_payload(tmp_path, "20250925T020000Z.json", base_payload)

    updated_payload = base_payload | {
        "status_mercado": 2,
        "mercado_pos_rodada": False,
        "fechamento": {"timestamp": 1759090000},
    }
    _write_raw_payload(tmp_path, "20250925T030000Z.json", updated_payload)

    schema = load_schema("mercado_status", base_dir=tmp_path)
    result = transform_mercado_status(base_dir=tmp_path, schema=schema)

    stage_df = pd.read_parquet(result["stage_path"])
    processed_df = pd.read_parquet(result["processed_path"])

    validate_dataframe_against_schema(processed_df, schema)

    assert len(stage_df) == 2
    assert set(stage_df.columns) == {
        "temporada",
        "rodada_atual",
        "rodada_final",
        "status_mercado",
        "mercado_pos_rodada",
        "bola_rolando",
        "timestamp_fechamento",
        "timestamp_coleta",
    }

    assert processed_df.loc[0, "status_mercado"] in {"ABERTO", "FECHADO"}
    assert str(processed_df["timestamp_coleta"].dt.tz) in {"UTC", "tzutc()"}
    assert processed_df.iloc[-1]["status_mercado"] == "FECHADO"


def test_transform_mercado_status_custom_raw_root(tmp_path: Path) -> None:
    _write_schema_copy(tmp_path)
    schema = load_schema("mercado_status", base_dir=tmp_path)

    raw_root = tmp_path / "alt_raw"
    payload = {
        "temporada": 2025,
        "rodada_atual": 25,
        "rodada_final": 38,
        "status_mercado": 1,
        "mercado_pos_rodada": False,
        "bola_rolando": False,
        "fechamento": {"timestamp": 1758999540},
    }
    raw_dir = raw_root / "mercado_status"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.joinpath("20250925T020000Z.json").write_text(json.dumps(payload), encoding="utf-8")

    result = transform_mercado_status(base_dir=tmp_path, schema=schema, raw_root=raw_root)

    processed_df = pd.read_parquet(result["processed_path"])
    assert processed_df.loc[0, "status_mercado"] == "ABERTO"