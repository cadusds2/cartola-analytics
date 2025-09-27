import json
from pathlib import Path

import pandas as pd

from cartola_analytics.pipelines import transform_rodadas
from cartola_analytics.schema import load_schema
from cartola_analytics.validation import validate_dataframe_against_schema


def _write_schema_copy(base_dir: Path) -> None:
    schema_root = Path(__file__).resolve().parents[2] / "docs" / "schemas"
    schema_src = schema_root / "rodadas.yaml"
    target = base_dir / "docs" / "schemas"
    target.mkdir(parents=True, exist_ok=True)
    target.joinpath("rodadas.yaml").write_text(
        schema_src.read_text(encoding="utf-8"),
        encoding="utf-8",
    )


def _write_raw_payload(base_dir: Path, name: str, payload: list[dict[str, object]]) -> None:
    raw_dir = base_dir / "data" / "raw" / "rodadas"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.joinpath(name).write_text(json.dumps(payload), encoding="utf-8")


def test_transform_rodadas(tmp_path: Path) -> None:
    _write_schema_copy(tmp_path)

    first_payload = [
        {
            "rodada_id": 1,
            "nome_rodada": "Rodada 1",
            "inicio": "2025-03-29 18:30:00",
            "fim": "2025-03-31 20:00:00",
        },
        {
            "rodada_id": 2,
            "nome_rodada": "Rodada 2",
            "inicio": "2025-04-05 18:30:00",
            "fim": "2025-04-06 20:30:00",
        },
    ]
    _write_raw_payload(tmp_path, "20250925T020000Z.json", first_payload)

    updated_payload = [
        {
            "rodada_id": 2,
            "nome_rodada": "Rodada 2",
            "inicio": "2025-04-05 19:00:00",
            "fim": "2025-04-06 21:00:00",
        }
    ]
    _write_raw_payload(tmp_path, "20250925T030000Z.json", updated_payload)

    schema = load_schema("rodadas", base_dir=tmp_path)
    result = transform_rodadas(base_dir=tmp_path, schema=schema)

    stage_df = pd.read_parquet(result["stage_path"])
    processed_df = pd.read_parquet(result["processed_path"])

    validate_dataframe_against_schema(processed_df, schema)

    assert len(stage_df) == 3
    assert set(stage_df.columns) == {
        "rodada_id",
        "nome_rodada",
        "inicio",
        "fim",
        "timestamp_coleta",
    }

    assert stage_df["inicio"].dt.tz is not None
    assert processed_df["rodada_id"].tolist() == [1, 2]

    latest_round = processed_df.loc[processed_df["rodada_id"] == 2].iloc[0]
    assert latest_round["inicio"] == pd.Timestamp("2025-04-05 19:00:00", tz="UTC")
    assert latest_round["fim"] == pd.Timestamp("2025-04-06 21:00:00", tz="UTC")
    assert processed_df.loc[0, "nome_rodada"] == "Rodada 1"
