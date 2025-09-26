"""Command line interface for Cartola Analytics data collection."""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any, Callable

import httpx

from . import (
    CartolaClient,
    Endpoint,
    collect_endpoint_payload,
    configure_logging_from_settings,
    list_endpoints,
    load_settings,
    transform_clubes,
    transform_mercado_status,
    transform_partidas,
    transform_rodadas,
)

_logger = logging.getLogger(__name__)


_AUTO_TRANSFORMERS: dict[str, Callable[..., dict[str, Any]]] = {
    "rodadas": transform_rodadas,
    "mercado_status": transform_mercado_status,
    "partidas": transform_partidas,
    "clubes": transform_clubes,
}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cartola-fetch",
        description="Coleta payloads brutos dos endpoints do Cartola FC.",
    )
    parser.add_argument(
        "endpoints",
        nargs="*",
        help="Nomes dos endpoints para coletar (use --list para ver as opcoes)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Coletar todos os endpoints conhecidos.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Apenas listar endpoints disponiveis e sair.",
    )
    parser.add_argument(
        "--rodada",
        type=int,
        help="Numero da rodada a ser utilizado em endpoints dependentes de rodada.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Diretorio base para salvar os arquivos (sobrescreve CARTOLA_RAW_DIR).",
    )
    parser.add_argument(
        "--use-cache",
        action="store_true",
        help="Permite usar cache local do cliente HTTP.",
    )
    return parser


def _select_endpoints(
    catalog: Sequence[Endpoint],
    all_flag: bool,
    names: Iterable[str],
) -> list[str]:
    if all_flag:
        return [endpoint.name for endpoint in catalog]
    selected = list(names)
    if not selected:
        raise SystemExit("Nenhum endpoint informado. Use --all ou especifique nomes.")
    known = {endpoint.name for endpoint in catalog}
    unknown = sorted(set(selected) - known)
    if unknown:
        raise SystemExit(f"Endpoints desconhecidos: {', '.join(unknown)}")
    return selected


def _resolve_endpoints(
    catalog: Sequence[Endpoint],
    names: Iterable[str],
) -> list[Endpoint]:
    mapping = {endpoint.name: endpoint for endpoint in catalog}
    return [mapping[name] for name in names]


def _validate_round(
    endpoints: Sequence[Endpoint], rodada: int | None
) -> None:
    missing_round = [
        ep.name
        for ep in endpoints
        if ep.requires_round and rodada is None
    ]
    if missing_round:
        joined = ", ".join(missing_round)
        raise SystemExit(
            f"Os endpoints {joined} exigem o parametro --rodada. "
            "Informe-o e tente novamente."
        )


def _discover_all_rounds(
    client: CartolaClient, catalog: Sequence[Endpoint]
) -> list[int]:
    rodada_endpoint = next((ep for ep in catalog if ep.name == "rodadas"), None)
    if rodada_endpoint is None:
        raise SystemExit(
            "Endpoint 'rodadas' nao encontrado para descobrir rodadas disponiveis."
        )

    payload = client.fetch(rodada_endpoint, use_cache=True)
    rounds: list[int] = []

    def _extract(value: Any) -> None:
        if isinstance(value, dict):
            candidate = (
                value.get("rodada_id")
                or value.get("rodada")
                or value.get("numero")
            )
            if candidate is not None:
                try:
                    rounds.append(int(candidate))
                except (TypeError, ValueError):  # pragma: no cover - defensive
                    pass
        elif isinstance(value, list):
            for item in value:
                _extract(item)

    _extract(payload)

    if not rounds:
        raise SystemExit(
            "Nao foi possivel extrair rodadas a partir do endpoint 'rodadas'."
        )

    unique = sorted(set(rounds))
    _logger.info(
        "rounds_discovered",
        extra={"event": "rounds_discovered", "count": len(unique)},
    )
    return unique



def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.list:
        for endpoint in list_endpoints():
            suffix = " (rodada)" if endpoint.requires_round else ""
            print(f"{endpoint.name}{suffix}")
        return 0

    settings = load_settings()
    configure_logging_from_settings(settings)
    catalog = list_endpoints()
    base_dir = args.output or settings.raw_dir
    base_dir.mkdir(parents=True, exist_ok=True)
    _logger.debug(
        "cli_start",
        extra={
            "event": "cli_start",
            "cli_args": vars(args),
        },
    )

    endpoint_names = _select_endpoints(catalog, args.all, args.endpoints)
    endpoints = _resolve_endpoints(catalog, endpoint_names)

    if not args.all:
        _validate_round(endpoints, args.rodada)

    rounds_to_use: list[int] = []
    failures: list[tuple[str, int | None, str]] = []
    successful_endpoints: set[str] = set()

    with CartolaClient(settings=settings) as client:
        if args.rodada is not None:
            rounds_to_use = [args.rodada]
        elif args.all:
            rounds_to_use = _discover_all_rounds(client, catalog)

        for endpoint in endpoints:
            if endpoint.requires_round:
                active_rounds = rounds_to_use or []
                if not active_rounds:
                    raise SystemExit(
                        "Nenhuma rodada identificada."
                        " Use --rodada ou verifique o endpoint de rodadas."
                    )
                for rodada in active_rounds:
                    try:
                        _logger.info(
                            "cli_collect",
                            extra={
                                "event": "cli_collect",
                                "endpoint": endpoint.name,
                                "rodada": rodada,
                                "output_dir": str(base_dir),
                            },
                        )
                        collect_endpoint_payload(
                            endpoint,
                            rodada=rodada,
                            client=client,
                            settings=settings,
                            base_dir=base_dir,
                            use_cache=args.use_cache,
                        )
                        successful_endpoints.add(endpoint.name)
                    except (httpx.HTTPError, ValueError) as err:
                        _logger.error(
                            "cli_collect_failed",
                            extra={
                                "event": "cli_collect_failed",
                                "endpoint": endpoint.name,
                                "rodada": rodada,
                                "error": str(err),
                            },
                        )
                        failures.append((endpoint.name, rodada, str(err)))
            else:
                try:
                    _logger.info(
                        "cli_collect",
                        extra={
                            "event": "cli_collect",
                            "endpoint": endpoint.name,
                            "rodada": args.rodada,
                            "output_dir": str(base_dir),
                        },
                    )
                    collect_endpoint_payload(
                        endpoint,
                        rodada=args.rodada,
                        client=client,
                        settings=settings,
                        base_dir=base_dir,
                        use_cache=args.use_cache,
                    )
                    successful_endpoints.add(endpoint.name)
                except (httpx.HTTPError, ValueError) as err:
                    _logger.error(
                        "cli_collect_failed",
                        extra={
                            "event": "cli_collect_failed",
                            "endpoint": endpoint.name,
                            "rodada": args.rodada,
                            "error": str(err),
                        },
                    )
                    failures.append((endpoint.name, args.rodada, str(err)))

    if successful_endpoints:
        try:
            raw_root = base_dir.resolve()
        except OSError:
            raw_root = base_dir

        for endpoint_name, transformer in _AUTO_TRANSFORMERS.items():
            if endpoint_name not in successful_endpoints:
                continue
            event_base = f'cli_transform_{endpoint_name}'
            try:
                result = transformer(raw_root=raw_root)
                _logger.info(
                    event_base,
                    extra={
                        'event': event_base,
                        'raw_root': str(raw_root),
                        'stage_path': str(result.get('stage_path', '')),
                        'processed_path': str(result.get('processed_path', '')),
                        'rows_stage': result.get('rows_stage'),
                        'rows_processed': result.get('rows_processed'),
                    },
                )
            except Exception as err:  # pragma: no cover - defensive
                failure_event = f'{event_base}_failed'
                _logger.error(
                    failure_event,
                    extra={
                        'event': failure_event,
                        'raw_root': str(raw_root),
                        'error': str(err),
                    },
                )
                failures.append((f'transform_{endpoint_name}', None, str(err)))

    if failures:
        for name, rodada_value, message in failures:
            rodada_label = rodada_value if rodada_value is not None else "-"
            print(
                f"[erro] endpoint={name} rodada={rodada_label}: {message}",
                file=sys.stderr,
            )
        return 1

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

