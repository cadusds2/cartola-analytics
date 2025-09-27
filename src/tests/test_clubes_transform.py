import json
from pathlib import Path

import pandas as pd

from cartola_analytics.pipelines import transform_clubes
from cartola_analytics.schema import load_schema
from cartola_analytics.validation import validate_dataframe_against_schema


def _write_schema_copy(base_dir: Path) -> None:
    schema_root = Path(__file__).resolve().parents[2] / "docs" / "schemas"
    schema_src = schema_root / "clubes.yaml"
    target = base_dir / "docs" / "schemas"
    target.mkdir(parents=True, exist_ok=True)
    target.joinpath("clubes.yaml").write_text(
        schema_src.read_text(encoding="utf-8"),
        encoding="utf-8",
    )


def _write_raw_payload(base_dir: Path, name: str, payload: dict[str, object]) -> None:
    raw_dir = base_dir / "data" / "raw" / "clubes"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.joinpath(name).write_text(json.dumps(payload), encoding="utf-8")


def test_transform_clubes(tmp_path: Path) -> None:
    _write_schema_copy(tmp_path)

    base_payload = {
        "1": {
            "nome": "OUT",
            "apelido": "Outros",
            "abreviacao": "OUT",
            "slug": "",
            "nome_fantasia": "Outros",
            "escudos": {
                "30x30": "https://example.com/30.png",
                "45x45": "https://example.com/45.png",
                "60x60": "https://example.com/60.png",
            },
            "url_editoria": None,
        },
        "2": {
            "nome": "ABC",
            "apelido": "Alvinegro",
            "abreviacao": "ABC",
            "slug": "abc",
            "nome_fantasia": "ABC Futebol Clube",
            "escudos": {},
            "url_editoria": "https://example.com/abc",
        },
    }
    _write_raw_payload(tmp_path, "20250925T020000Z.json", base_payload)

    updated_payload = {
        "1": base_payload["1"],
        "2": base_payload["2"] | {"apelido": "ABC"},
        "3": {
            "nome": "DEF",
            "abreviacao": "DEF",
            "nome_fantasia": "DEF Clube",
            "escudos": {
                "30x30": "https://example.com/def30.png",
                "45x45": "https://example.com/def45.png",
                "60x60": "https://example.com/def60.png",
            },
            "url_editoria": "https://example.com/def",
        },
    }
    _write_raw_payload(tmp_path, "20250925T030000Z.json", updated_payload)

    schema = load_schema("clubes", base_dir=tmp_path)
    result = transform_clubes(base_dir=tmp_path, schema=schema)

    stage_df = pd.read_parquet(result["stage_path"])
    processed_df = pd.read_parquet(result["processed_path"])

    validate_dataframe_against_schema(processed_df, schema)

    assert len(stage_df) == 5
    assert set(stage_df.columns) == {
        "clube_id",
        "nome",
        "nome_fantasia",
        "apelido",
        "abreviacao",
        "slug",
        "escudo_30x30",
        "escudo_45x45",
        "escudo_60x60",
        "url_editoria",
        "timestamp_coleta",
    }
    assert processed_df.loc[processed_df["clube_id"] == 2, "apelido"].iloc[0] == "ABC"
    assert processed_df.loc[processed_df["clube_id"] == 3, "escudo_30x30"].iloc[0].endswith("def30.png")
    assert processed_df["timestamp_coleta"].dt.tz is not None