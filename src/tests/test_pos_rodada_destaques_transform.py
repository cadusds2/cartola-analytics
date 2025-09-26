import json
from pathlib import Path

import pandas as pd

from cartola_analytics.pipelines import transform_pos_rodada_destaques
from cartola_analytics.schema import load_schema


def _write_schema_copy(base_dir: Path) -> None:
    schema_root = Path(__file__).resolve().parents[2] / "docs" / "schemas"
    schema_src = schema_root / "pos_rodada_destaques.yaml"
    target = base_dir / "docs" / "schemas"
    target.mkdir(parents=True, exist_ok=True)
    target.joinpath("pos_rodada_destaques.yaml").write_text(
        schema_src.read_text(encoding="utf-8"),
        encoding="utf-8",
    )


def _write_raw_payload(base_dir: Path, name: str, payload: dict[str, object]) -> None:
    raw_dir = base_dir / "data" / "raw" / "pos_rodada_destaques"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.joinpath(name).write_text(json.dumps(payload), encoding="utf-8")


def test_transform_pos_rodada_destaques(tmp_path: Path) -> None:
    _write_schema_copy(tmp_path)

    base_payload = {
        "media_pontos": 45.2,
        "media_cartoletas": 128.5,
        "mito_rodada": {
            "rodada_time_id": 25,
            "time_id": 123,
            "clube_id": 55,
            "nome": "Time Mito",
            "nome_cartola": "Cartola Mito",
            "slug": "time-mito",
            "tipo_escudo": 1,
            "tipo_camisa": 3,
            "esquema_id": 2,
            "url_escudo_svg": "https://example.com/escudo.svg",
            "url_camisa_svg": "https://example.com/camisa.svg",
            "assinante": True,
        },
    }
    _write_raw_payload(tmp_path, "20250925T020000Z.json", base_payload)

    updated_payload = {
        "media_pontos": 50.0,
        "media_cartoletas": 130.1,
        "mito_rodada": base_payload["mito_rodada"] | {
            "rodada_time_id": 26,
            "time_id": 456,
            "nome": "Time Atualizado",
        },
    }
    _write_raw_payload(tmp_path, "20250925T030000Z.json", updated_payload)

    schema = load_schema("pos_rodada_destaques", base_dir=tmp_path)
    result = transform_pos_rodada_destaques(base_dir=tmp_path, schema=schema)

    stage_df = pd.read_parquet(result["stage_path"])
    processed_df = pd.read_parquet(result["processed_path"])

    assert len(stage_df) == 2
    assert set(stage_df.columns) >= {
        "rodada",
        "time_id",
        "nome_time",
        "nome_cartola",
        "media_pontos",
        "media_cartoletas",
        "timestamp_coleta",
    }
    assert processed_df.loc[0, "rodada"] == 25
    assert processed_df.loc[0, "time_id"] == 123
    assert processed_df.loc[1, "rodada"] == 26