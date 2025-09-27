import json
import logging
from pathlib import Path

import pandas as pd
import pytest

from cartola_analytics.pipelines import transform_atletas_mercado
from cartola_analytics.schema import load_schema
from cartola_analytics.validation import validate_dataframe_against_schema


def _write_schema_copy(base_dir: Path) -> None:
    schema_root = Path(__file__).resolve().parents[2] / "docs" / "schemas"
    schema_src = schema_root / "atletas_mercado.yaml"
    target = base_dir / "docs" / "schemas"
    target.mkdir(parents=True, exist_ok=True)
    target.joinpath("atletas_mercado.yaml").write_text(
        schema_src.read_text(encoding="utf-8"),
        encoding="utf-8",
    )


def _write_raw_payload(base_dir: Path, name: str, payload: dict[str, object]) -> None:
    raw_dir = base_dir / "data" / "raw" / "atletas_mercado"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.joinpath(name).write_text(json.dumps(payload), encoding="utf-8")


def test_transform_atletas_mercado(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    _write_schema_copy(tmp_path)
    caplog.set_level(logging.INFO, logger="cartola_analytics.validation")

    base_payload = {
        "clubes": {
            "1": {"nome": "OUT", "nome_fantasia": "Outros"},
            "2": {"nome": "ABC", "nome_fantasia": "ABC FC"},
        },
        "posicoes": {
            "1": {"nome": "Goleiro"},
            "3": {"nome": "Zagueiro"},
        },
        "status": {
            "7": {"nome": "Provavel"},
        },
        "atletas": [
            {
                "atleta_id": 10,
                "rodada_id": 25,
                "clube_id": 1,
                "posicao_id": 1,
                "status_id": 7,
                "nome": "Joao Silva",
                "apelido": "Joao",
                "apelido_abreviado": "J. Silva",
                "slug": "joao-silva",
                "foto": "https://example.com/joao.png",
                "preco_num": 5.0,
                "media_num": 4.5,
                "variacao_num": 0.2,
                "pontos_num": 7.5,
                "jogos_num": 10,
                "entrou_em_campo": True,
                "scout": {"CA": 1, "DS": 5, "G": 2},
            },
            {
                "atleta_id": 20,
                "clube_id": 2,
                "posicao_id": 3,
                "status_id": 7,
                "nome": "Pedro Souza",
                "apelido": "Pedro",
                "apelido_abreviado": "P. Souza",
                "slug": "pedro-souza",
                "foto": "https://example.com/pedro.png",
                "preco_num": 12.0,
                "media_num": 6.1,
                "variacao_num": -0.3,
                "pontos_num": 3.5,
                "jogos_num": 18,
                "entrou_em_campo": False,
                "scout": {},
            },
        ],
    }
    _write_raw_payload(tmp_path, "20250925T020000Z.json", base_payload)

    updated_payload = base_payload | {
        "atletas": [
            base_payload["atletas"][0] | {"preco_num": 5.5, "scout": {"CA": 2}},
            base_payload["atletas"][1],
        ]
    }
    _write_raw_payload(tmp_path, "20250925T030000Z.json", updated_payload)

    schema = load_schema("atletas_mercado", base_dir=tmp_path)
    result = transform_atletas_mercado(base_dir=tmp_path, schema=schema)

    stage_df = pd.read_parquet(result["stage_path"])
    processed_df = pd.read_parquet(result["processed_path"])

    validate_dataframe_against_schema(processed_df, schema)

    assert len(stage_df) == 4
    assert set(stage_df.columns) >= {
        "atleta_id",
        "clube_id",
        "posicao_id",
        "status_id",
        "preco_num",
        "media_num",
        "variacao_num",
        "jogos_num",
        "scout_ca",
        "scout_ds",
        "timestamp_coleta",
    }
    assert processed_df.loc[processed_df["atleta_id"] == 10, "preco_num"].iloc[0] == 5.5
    assert processed_df.loc[processed_df["atleta_id"] == 20, "clube_nome"].iloc[0] == "ABC FC"
    assert processed_df.loc[processed_df["atleta_id"] == 10, "scout_ca"].iloc[0] == 2
    assert any(
        record.message == "cli_validation_atletas_mercado" for record in caplog.records
    )
