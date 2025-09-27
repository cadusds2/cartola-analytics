"""Utilities to generate a data dictionary markdown from schema definitions."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterable

try:  # pragma: no cover - fallback when executed as a script
    from .schema import FieldSpec, SchemaSpec, load_schema, schema_dir
except ImportError:  # pragma: no cover
    import sys

    SRC_ROOT = Path(__file__).resolve().parents[2]
    if str(SRC_ROOT) not in sys.path:
        sys.path.append(str(SRC_ROOT))
    from cartola_analytics.schema import FieldSpec, SchemaSpec, load_schema, schema_dir


def _resolve_schema_dir(base_dir: Path | None = None) -> Path:
    if base_dir is not None:
        return (base_dir / "docs" / "schemas").resolve()
    return schema_dir()


def _iter_schema_names(schema_path: Path) -> list[str]:
    return sorted(file.stem for file in schema_path.glob("*.yaml"))


def _format_required(field: FieldSpec) -> str:
    return "yes" if field.required else "no"


def _format_enum(field: FieldSpec) -> str:
    if field.enum:
        return ", ".join(field.enum)
    return ""


def _format_primary_key(spec: SchemaSpec) -> str:
    keys = spec.processed.get("primary_key") or []
    if isinstance(keys, Iterable) and not isinstance(keys, (str, bytes)):
        values = list(keys)
    else:
        values = [str(keys)] if keys else []
    return ", ".join(map(str, values))


def _format_relationships(spec: SchemaSpec) -> list[str]:
    relationships = spec.relationships or []
    lines: list[str] = []
    for rel in relationships:
        parts: list[str] = []

        field = rel.get("field") or rel.get("source")
        if field:
            parts.append(f"field={field}")

        references = rel.get("references") if isinstance(rel, dict) else None
        target = rel.get("dataset") or rel.get("target")
        key = None
        rel_type = None
        if isinstance(references, dict):
            target = target or references.get("dataset")
            key = references.get("key") or references.get("field")
            rel_type = references.get("type")

        if target:
            target_text = f"dataset={target}"
            if key:
                target_text += f" ({key})"
            parts.append(target_text)
        elif key:
            parts.append(f"key={key}")

        on = rel.get("on")
        if on is None and True in rel:
            on = rel[True]
        if on:
            parts.append(f"on={on}")

        if rel_type:
            parts.append(f"type={rel_type}")

        description = rel.get("description")
        if description:
            parts.append(str(description))

        if parts:
            lines.append("- " + "; ".join(parts))
    return lines


def _format_owner(spec: SchemaSpec) -> str:
    owner = (spec.metadata or {}).get("owner") if spec.metadata else None
    if owner is None:
        return ""
    if isinstance(owner, (list, tuple, set)):
        return ", ".join(str(item) for item in owner)
    return str(owner)


def _format_lineage(spec: SchemaSpec) -> str:
    lineage = (spec.metadata or {}).get("lineage") if spec.metadata else None
    if not lineage:
        return ""
    if isinstance(lineage, (list, tuple, set)):
        return " -> ".join(str(step) for step in lineage)
    return str(lineage)


def _format_updated_at(spec: SchemaSpec) -> str:
    raw_value = spec.metadata.get("updated_at") if spec.metadata else None
    if not raw_value:
        return ""
    try:
        parsed = datetime.fromisoformat(str(raw_value).replace("Z", "+00:00"))
        return parsed.isoformat().replace("+00:00", "Z")
    except ValueError:
        return str(raw_value)


def render_schema_markdown(spec: SchemaSpec) -> str:
    lines = [f"## {spec.name}"]
    description = None
    if spec.processed:
        description = spec.processed.get("description")
    if not description and spec.metadata:
        description = spec.metadata.get("description")
    if description:
        lines.append(description)
    dataset_path = spec.processed.get("dataset") if spec.processed else None
    if dataset_path:
        lines.append(f"- Dataset: `{dataset_path}`")
    stage_path = spec.stage.get("output_path") if spec.stage else None
    if stage_path:
        lines.append(f"- Stage output: `{stage_path}`")
    primary_key = _format_primary_key(spec)
    if primary_key:
        lines.append(f"- Primary key: `{primary_key}`")
    owner = _format_owner(spec)
    if owner:
        lines.append(f"- Owner: `{owner}`")
    lineage = _format_lineage(spec)
    if lineage:
        lines.append(f"- Lineage: `{lineage}`")
    updated_at = _format_updated_at(spec)
    if updated_at:
        lines.append(f"- Updated at: {updated_at}")
    relationship_lines = _format_relationships(spec)
    if relationship_lines:
        lines.append("- Relationships:")
        lines.extend(f"  {line}" for line in relationship_lines)
    lines.append("")
    lines.append("| Field | Type | Required | Description | Enum |")
    lines.append("| --- | --- | --- | --- | --- |")
    for field in spec.fields:
        description = field.description or ""
        enum_value = _format_enum(field)
        lines.append(
            f"| {field.name} | {field.type} | {_format_required(field)} | {description} | {enum_value} |"
        )
    lines.append("")
    return "\n".join(lines)


def render_data_dictionary(*, base_dir: Path | None = None) -> str:
    schema_path = _resolve_schema_dir(base_dir)
    schema_names = _iter_schema_names(schema_path)
    header = [
        "# Data Dictionary",
        "",
        "Generated automatically from docs/schemas.",
        "",
    ]
    sections: list[str] = []
    for name in schema_names:
        spec = load_schema(name, base_dir=base_dir)
        sections.append(render_schema_markdown(spec))
    return "\n".join(header + sections)


def write_data_dictionary(output_path: Path, *, base_dir: Path | None = None) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    content = render_data_dictionary(base_dir=base_dir)
    output_path.write_text(content + "\n", encoding="utf-8")
    return output_path


if __name__ == "__main__":  # pragma: no cover - CLI helper
    import argparse

    parser = argparse.ArgumentParser(description="Generate data dictionary markdown.")
    parser.add_argument(
        "output",
        nargs="?",
        default="docs/data-dictionary/README.md",
        help="Path to write the markdown output.",
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        help="Optional project root to resolve schema files.",
    )
    args = parser.parse_args()
    write_data_dictionary(Path(args.output), base_dir=args.base_dir)
