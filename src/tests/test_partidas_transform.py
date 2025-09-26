import json
from pathlib import Path

import pandas as pd

from cartola_analytics.pipelines import transform_partidas
from cartola_analytics.schema import load_schema


def _write_schema_copy(base_dir: Path) -> None:
    schema_root = Path(__file__).resolve().parents[2] / "docs" / "schemas"
    schema_src = schema_root / "partidas.yaml"
    target = base_dir / "docs" / "schemas"
    target.mkdir(parents=True, exist_ok=True)
    target.joinpath("partidas.yaml").write_text(
        schema_src.read_text(encoding="utf-8"),
        encoding="utf-8",
    )


def _write_raw_payload(base_dir: Path, name: str, payload: dict[str, object]) -> None:
    raw_dir = base_dir / "data" / "raw" / "partidas"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.joinpath(name).write_text(json.dumps(payload), encoding="utf-8")


def test_transform_partidas(tmp_path: Path) -> None:
    _write_schema_copy(tmp_path)

    base_payload = {
        "rodada": 25,
        "partidas": [
            {
                "partida_id": 111,
                "campeonato_id": 26,
                "partida_data": "2025-09-27 16:00:00",
                "timestamp": 1758999600,
                "clube_casa_id": 10,
                "clube_casa_posicao": 4,
                "clube_visitante_id": 20,
                "clube_visitante_posicao": 7,
                "placar_oficial_mandante": None,
                "placar_oficial_visitante": None,
                "aproveitamento_mandante": ["v", "e"],
                "aproveitamento_visitante": ["d", "d"],
                "valida": True,
                "local": "Estadio Teste",
                "transmissao": {"label": "TV", "url": "https://example.com/tv"},
                "status_transmissao_tr": "",
                "status_cronometro_tr": "",
                "periodo_tr": "",
                "inicio_cronometro_tr": "",
            },
            {
                "partida_id": 222,
                "campeonato_id": 26,
                "partida_data": "2025-09-28 18:00:00",
                "timestamp": 1759086000,
                "clube_casa_id": 30,
                "clube_casa_posicao": 9,
                "clube_visitante_id": 40,
                "clube_visitante_posicao": 12,
                "placar_oficial_mandante": 1,
                "placar_oficial_visitante": 0,
                "aproveitamento_mandante": ["v"],
                "aproveitamento_visitante": ["d"],
                "valida": True,
                "local": None,
                "transmissao": {},
                "status_transmissao_tr": "",
                "status_cronometro_tr": "",
                "periodo_tr": "",
                "inicio_cronometro_tr": "",
            },
        ],
    }
    _write_raw_payload(tmp_path, "20250925T020000Z.json", base_payload)

    updated_payload = {
        "rodada": 25,
        "partidas": [
            base_payload["partidas"][0] | {
                "placar_oficial_mandante": 2,
                "placar_oficial_visitante": 1,
            }
        ],
    }
    _write_raw_payload(tmp_path, "20250925T030000Z.json", updated_payload)

    schema = load_schema("partidas", base_dir=tmp_path)
    result = transform_partidas(base_dir=tmp_path, schema=schema)

    stage_df = pd.read_parquet(result["stage_path"])
    processed_df = pd.read_parquet(result["processed_path"])

    assert len(stage_df) == 3
    assert set(stage_df.columns) >= {
        "rodada",
        "partida_id",
        "campeonato_id",
        "partida_data",
        "timestamp_partida",
        "timestamp_coleta",
        "clube_casa_id",
        "clube_visitante_id",
        "valida",
        "aproveitamento_mandante",
        "aproveitamento_visitante",
    }
    assert stage_df["partida_data"].dt.tz is not None
    assert processed_df.loc[processed_df["partida_id"] == 222, "placar_oficial_mandante"].iloc[0] == 1
    assert processed_df.loc[processed_df["partida_id"] == 111, "placar_oficial_mandante"].iloc[0] == 2
    assert processed_df.loc[processed_df["partida_id"] == 111, "aproveitamento_mandante"].iloc[0] == "ve"