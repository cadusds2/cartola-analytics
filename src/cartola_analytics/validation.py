"""Schema-based validation helpers for processed datasets."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, Iterable

import pandas as pd
from pandas.api import types as pdt

from .schema import FieldSpec, SchemaSpec


class SchemaValidationError(ValueError):
    """Raised when a dataset does not comply with its schema."""


@dataclass(frozen=True)
class ValidationIssue:
    column: str
    message: str


_TYPE_CHECKS: dict[str, Callable[[object], bool]] = {
    "float": pdt.is_float_dtype,
    "string": lambda dtype: pdt.is_string_dtype(dtype) or pdt.is_object_dtype(dtype),
    "timestamp": pdt.is_datetime64_any_dtype,
}


_LOGGER = logging.getLogger(__name__)


def _check_type(series: pd.Series, field: FieldSpec) -> ValidationIssue | None:
    expected = field.type.lower()
    if expected == "int":
        if pdt.is_integer_dtype(series.dtype):
            return None
        if pdt.is_float_dtype(series.dtype):
            non_na = series.dropna()
            if non_na.empty:
                return None
            if (non_na == non_na.astype(int)).all():
                return None
        return ValidationIssue(field.name, f"expected type {field.type}, found {series.dtype}")
    if expected == "bool":
        if pdt.is_bool_dtype(series.dtype):
            return None
        if pdt.is_integer_dtype(series.dtype):
            non_na = series.dropna()
            if non_na.empty:
                return None
            if set(non_na.unique()) <= {0, 1}:
                return None
        return ValidationIssue(field.name, f"expected type {field.type}, found {series.dtype}")
    checker = _TYPE_CHECKS.get(expected)
    if checker is None:
        return None
    if checker(series.dtype):
        return None
    return ValidationIssue(field.name, f"expected type {field.type}, found {series.dtype}")


def _check_required(series: pd.Series, field: FieldSpec) -> ValidationIssue | None:
    if not field.required:
        return None
    if series.isna().any():
        return ValidationIssue(field.name, "contains nulls but field is required")
    return None


def _check_enum(series: pd.Series, field: FieldSpec) -> ValidationIssue | None:
    if not field.enum:
        return None
    allowed = set(field.enum)
    values = series.dropna().unique()
    invalid = sorted(str(value) for value in values if value not in allowed)
    if invalid:
        return ValidationIssue(field.name, f"values outside enum: {', '.join(invalid)}")
    return None


def _primary_key_columns(spec: SchemaSpec) -> list[str]:
    primary = spec.processed.get("primary_key") if spec.processed else None
    if not primary:
        return []
    if isinstance(primary, (str, bytes)):
        return [str(primary)]
    if isinstance(primary, Iterable):
        return [str(col) for col in primary]
    return [str(primary)]


def validate_dataframe_against_schema(frame: pd.DataFrame, schema: SchemaSpec) -> None:
    """Validate that a pandas DataFrame complies with a SchemaSpec."""
    missing_columns = [field.name for field in schema.fields if field.name not in frame.columns]
    if missing_columns:
        raise SchemaValidationError(f"missing columns: {', '.join(missing_columns)}")

    issues: list[ValidationIssue] = []
    for field in schema.fields:
        series = frame[field.name]
        issue = _check_required(series, field)
        if issue:
            issues.append(issue)
            continue
        type_issue = _check_type(series, field)
        if type_issue:
            issues.append(type_issue)
        enum_issue = _check_enum(series, field)
        if enum_issue:
            issues.append(enum_issue)

    pk_columns = _primary_key_columns(schema)
    if pk_columns:
        duplicated = frame.duplicated(subset=pk_columns, keep=False)
        if duplicated.any():
            issues.append(
                ValidationIssue(
                    "primary_key",
                    f"duplicated values detected for primary key columns: {', '.join(pk_columns)}",
                )
            )

    if issues:
        summary = "; ".join(f"{issue.column}: {issue.message}" for issue in issues)
        raise SchemaValidationError(summary)


def validate_with_logging(
    frame: pd.DataFrame,
    schema: SchemaSpec,
    *,
    logger: logging.Logger | None = None,
) -> None:
    """Validate and emit structured log events for success/failure."""

    event_base = f"cli_validation_{schema.name}"
    dataset_path = (schema.processed or {}).get("dataset") if schema.processed else None
    log = logger or _LOGGER

    try:
        validate_dataframe_against_schema(frame, schema)
    except SchemaValidationError as err:
        extra = {
            "event": f"{event_base}_failed",
            "dataset": schema.name,
            "rows": len(frame),
            "error": str(err),
        }
        if dataset_path:
            extra["processed_path"] = dataset_path
        log.error(f"{event_base}_failed", extra=extra)
        raise

    extra = {
        "event": event_base,
        "dataset": schema.name,
        "rows": len(frame),
    }
    if dataset_path:
        extra["processed_path"] = dataset_path
    log.info(event_base, extra=extra)


__all__ = [
    "SchemaValidationError",
    "validate_dataframe_against_schema",
    "validate_with_logging",
]
