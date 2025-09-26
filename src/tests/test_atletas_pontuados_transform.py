import json
from pathlib import Path

import pandas as pd

from cartola_analytics.pipelines import transform_atletas_pontuados
from cartola_analytics.schema import load_schema


def _write_schema_copy(base_dir: Path) -> None:
    schema_root = Path(__file__).resolve().parents[2] / "docs" / "schemas"
    schema_src = schema_root / "atletas_pontuados.yaml"
    target = base_dir / "docs" / "schemas"
    target.mkdir(parents=True, exist_ok=True)
    target.joinpath("atletas_pontuados.yaml").write_text(
        schema_src.read_text(encoding="utf-8"),
        encoding="utf-8",
    )


def _write_raw_payload(base_dir: Path, name: str, payload: dict[str, object]) -> None:
    raw_dir = base_dir / "data" / "raw" / "atletas_pontuados"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.joinpath(name).write_text(json.dumps(payload), encoding="utf-8")


def test_transform_atletas_pontuados(tmp_path: Path) -> None:
    _write_schema_copy(tmp_path)

    base_payload = {
        "rodada": 25,
        "atletas": {
            "10": {
                "clube_id": 1,
                "posicao_id": 2,
                "pontuacao": 12.5,
                "entrou_em_campo": True,
                "apelido": "Joao",
                "slug": "joao",
                "scout": {"G": 1, "FS": 2, "CA": 1},
            },
            "20": {
                "clube_id": 2,
                "posicao_id": 5,
                "pontuacao": -1.0,
                "entrou_em_campo": False,
                "apelido": "Pedro",
                "slug": "pedro",
                "scout": {},
            },
        },
    }
    _write_raw_payload(tmp_path, "20250925T020000Z.json", base_payload)

    updated_payload = {
        "rodada": 25,
        "atletas": {
            "10": base_payload["atletas"]["10"] | {"pontuacao": 13.0, "scout": {"G": 2}},
            "30": {
                "clube_id": 3,
                "posicao_id": 1,
                "pontuacao": 5.0,
                "entrou_em_campo": True,
                "apelido": "Carlos",
                "slug": "carlos",
                "scout": {"SG": 1},
            },
        },
    }
    _write_raw_payload(tmp_path, "20250925T030000Z.json", updated_payload)

    schema = load_schema("atletas_pontuados", base_dir=tmp_path)
    result = transform_atletas_pontuados(base_dir=tmp_path, schema=schema)

    stage_df = pd.read_parquet(result["stage_path"])
    processed_df = pd.read_parquet(result["processed_path"])

    assert len(stage_df) == 4
    assert set(stage_df.columns) >= {
        "rodada",
        "atleta_id",
        "pontuacao",
        "entrou_em_campo",
        "scout_g",
        "scout_ca",
        "timestamp_coleta",
    }
    assert processed_df.loc[processed_df["atleta_id"] == 10, "pontuacao"].iloc[0] == 13.0
    assert processed_df.loc[processed_df["atleta_id"] == 30, "scout_sg"].iloc[0] == 1
