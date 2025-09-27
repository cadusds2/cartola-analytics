import logging

import pandas as pd
import pytest

from cartola_analytics.schema import FieldSpec, SchemaSpec
from cartola_analytics.validation import (
    SchemaValidationError,
    validate_dataframe_against_schema,
    validate_with_logging,
)


@pytest.fixture
def sample_schema() -> SchemaSpec:
    return SchemaSpec(
        name="sample",
        version=1,
        raw_source={},
        stage={},
        processed={"dataset": "data/processed/sample.parquet", "primary_key": ["id"]},
        fields=[
            FieldSpec(name="id", type="int", required=True, description="Identifier"),
            FieldSpec(name="value", type="string", required=False, description="Value", enum=["A", "B"]),
            FieldSpec(name="flag", type="bool", required=True, description="Flag"),
        ],
        relationships=[],
        metadata={},
    )


def test_validate_dataframe_against_schema_ok(sample_schema: SchemaSpec) -> None:
    frame = pd.DataFrame({
        "id": [1, 2],
        "value": ["A", "B"],
        "flag": [True, False],
    })

    validate_dataframe_against_schema(frame, sample_schema)


def test_validate_dataframe_against_schema_missing_required(sample_schema: SchemaSpec) -> None:
    frame = pd.DataFrame({
        "id": [1, 1],
        "value": ["A", "C"],
        "flag": [True, None],
    })

    with pytest.raises(SchemaValidationError) as exc:
        validate_dataframe_against_schema(frame, sample_schema)

    message = str(exc.value)
    assert "flag" in message
    assert "primary_key" in message
    assert "enum" in message


def test_validate_with_logging_success(sample_schema: SchemaSpec, caplog: pytest.LogCaptureFixture) -> None:
    frame = pd.DataFrame({
        "id": [1],
        "value": ["A"],
        "flag": [True],
    })

    caplog.set_level(logging.INFO, logger="cartola_analytics.validation")
    validate_with_logging(frame, sample_schema)

    assert any(record.message == "cli_validation_sample" for record in caplog.records)


def test_validate_with_logging_failure(sample_schema: SchemaSpec, caplog: pytest.LogCaptureFixture) -> None:
    frame = pd.DataFrame({
        "id": [1, 1],
        "value": ["A", "C"],
        "flag": [True, None],
    })

    caplog.set_level(logging.INFO, logger="cartola_analytics.validation")
    with pytest.raises(SchemaValidationError):
        validate_with_logging(frame, sample_schema)

    assert any(
        record.message == "cli_validation_sample_failed"
        for record in caplog.records
    )
