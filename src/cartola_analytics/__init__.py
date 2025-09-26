"""Cartola Analytics package entry point."""

from .config import CartolaSettings, load_settings
from .endpoints import Endpoint, iter_endpoints, list_endpoints
from .http_client import CartolaClient, default_headers
from .logging_utils import configure_logging, configure_logging_from_settings
from .pipelines import (
    build_output_path,
    collect_endpoint_payload,
    transform_clubes,
    transform_mercado_status,
    transform_partidas,
    transform_atletas_mercado,
    transform_atletas_pontuados,
    transform_rodadas,
)
from .schema import FieldSpec, SchemaSpec, load_schema, schema_dir

__all__ = [
    "Endpoint",
    "iter_endpoints",
    "list_endpoints",
    "CartolaClient",
    "default_headers",
    "CartolaSettings",
    "load_settings",
    "configure_logging",
    "configure_logging_from_settings",
    "collect_endpoint_payload",
    "build_output_path",
    "transform_clubes",
    "transform_mercado_status",
    "transform_partidas",
    "transform_atletas_mercado",
    "transform_atletas_pontuados",
    "transform_rodadas",
    "FieldSpec",
    "SchemaSpec",
    "load_schema",
    "schema_dir",
]

