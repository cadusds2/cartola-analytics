from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

_BASE_DIR = Path(__file__).resolve().parents[2]
_SCHEMA_DIR = _BASE_DIR / "docs" / "schemas"


@dataclass(frozen=True)
class FieldSpec:
    name: str
    type: str
    required: bool = False
    description: str | None = None
    enum: list[str] | None = None


@dataclass(frozen=True)
class SchemaSpec:
    name: str
    version: int
    raw_source: dict[str, Any]
    stage: dict[str, Any]
    processed: dict[str, Any]
    fields: list[FieldSpec]
    relationships: list[dict[str, Any]]
    metadata: dict[str, Any]


def load_schema(schema_name: str, *, base_dir: Path | None = None) -> SchemaSpec:
    """Load a schema YAML from docs/schemas/ and parse into a SchemaSpec."""
    schema_dir = base_dir / "docs" / "schemas" if base_dir else _SCHEMA_DIR
    path = schema_dir / f"{schema_name}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"Schema file not found: {path}")

    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    fields = [FieldSpec(**field) for field in raw.get("fields", [])]
    return SchemaSpec(
        name=raw["name"],
        version=int(raw["version"]),
        raw_source=raw.get("raw_source", {}),
        stage=raw.get("stage", {}),
        processed=raw.get("processed", {}),
        fields=fields,
        relationships=raw.get("relationships", []),
        metadata=raw.get("metadata", {}),
    )


def schema_dir() -> Path:
    """Return the base directory where schemas are stored."""
    return _SCHEMA_DIR
