from pathlib import Path

from cartola_analytics.schema import FieldSpec, load_schema, schema_dir


def test_load_schema_returns_fields(tmp_path: Path) -> None:
    schema_path = tmp_path / "docs" / "schemas"
    schema_path.mkdir(parents=True)
    (schema_path / "custom.yaml").write_text(
        """
name: custom
version: 1
raw_source: {endpoint: foo}
stage: {}
processed: {}
fields:
  - name: field_a
    type: string
    required: true
relationships: []
metadata: {}
""",
        encoding="utf-8",
    )

    schema = load_schema("custom", base_dir=tmp_path)

    assert schema.name == "custom"
    assert schema.version == 1
    assert schema.fields == [FieldSpec(name="field_a", type="string", required=True)]


def test_schema_dir_points_to_docs() -> None:
    schemas_path = schema_dir()
    assert schemas_path.name == "schemas"
    assert schemas_path.exists()
