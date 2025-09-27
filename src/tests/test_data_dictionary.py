from pathlib import Path

from cartola_analytics.data_dictionary import render_data_dictionary, write_data_dictionary


_SAMPLE_SCHEMA = """
name: sample
version: 1
raw_source:
  endpoint: sample
stage:
  output_path: data/stage/sample/{run_timestamp}.parquet
processed:
  dataset: data/processed/sample/sample.parquet
  primary_key:
    - id
  description: Sample dataset.
fields:
  - name: id
    type: int
    required: true
    description: Sample identifier.
  - name: status
    type: string
    required: false
    description: Status flag.
    enum:
      - A
      - B
relationships:
  - dataset: other.table
    on: id
metadata:
  owner: dados
  lineage:
    - fetch
    - transform
  updated_at: 2025-01-01T00:00:00Z
"""


def _setup_schema(tmp_path: Path) -> None:
    schema_dir = tmp_path / "docs" / "schemas"
    schema_dir.mkdir(parents=True, exist_ok=True)
    schema_dir.joinpath("sample.yaml").write_text(_SAMPLE_SCHEMA.strip() + "\n", encoding="utf-8")


def test_render_data_dictionary(tmp_path: Path) -> None:
    _setup_schema(tmp_path)
    markdown = render_data_dictionary(base_dir=tmp_path)

    assert "# Data Dictionary" in markdown
    assert "## sample" in markdown
    assert "- Dataset: `data/processed/sample/sample.parquet`" in markdown
    assert "- Owner: `dados`" in markdown
    assert "- Lineage: `fetch -> transform`" in markdown
    assert "| id | int | yes | Sample identifier." in markdown
    assert "| status | string | no | Status flag." in markdown
    assert "- Relationships:" in markdown
    assert "dataset=other.table" in markdown
    assert "on=id" in markdown


def test_write_data_dictionary_creates_file(tmp_path: Path) -> None:
    _setup_schema(tmp_path)
    output = tmp_path / "docs" / "data-dictionary" / "README.md"

    result_path = write_data_dictionary(output, base_dir=tmp_path)
    assert result_path == output
    assert output.exists()
    content = output.read_text(encoding="utf-8")
    assert "## sample" in content
