from pathlib import Path

import httpx
import pytest

import cartola_analytics.cli as cli
from cartola_analytics import CartolaSettings, Endpoint


@pytest.fixture(autouse=True)
def disable_logging(monkeypatch):
    monkeypatch.setattr(
        cli,
        "configure_logging_from_settings",
        lambda *_args, **_kwargs: None,
    )


@pytest.fixture
def fake_settings(tmp_path, monkeypatch):
    settings = CartolaSettings(
        cache_dir=tmp_path / "cache",
        raw_dir=tmp_path / "data" / "raw",
        log_level="INFO",
    )
    monkeypatch.setattr(cli, "load_settings", lambda: settings)
    return settings


@pytest.fixture
def fake_endpoints(monkeypatch):
    endpoints = [
        Endpoint(name="clubes", url="https://example.com/clubes"),
        Endpoint(name="mercado_status", url="https://example.com/mercado_status"),
        Endpoint(name="partidas", url="https://example.com/partidas"),
        Endpoint(name="rodadas", url="https://example.com/rodadas"),
        Endpoint(
            name="partidas_por_rodada",
            url="https://example.com/partidas/{rodada}",
            requires_round=True,
        ),
    ]
    monkeypatch.setattr(cli, "list_endpoints", lambda: endpoints)
    return endpoints


@pytest.fixture(autouse=True)
def fake_client(monkeypatch):
    class DummyClient:
        def __init__(self, *args, **kwargs):
            self.closed = False

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            self.close()

        def close(self):
            self.closed = True

        def fetch(self, endpoint, rodada=None, use_cache=False):  # pragma: no cover
            raise AssertionError("fetch should not be called in CLI tests")

    monkeypatch.setattr(cli, "CartolaClient", DummyClient)


@pytest.fixture(autouse=True)
def auto_transform_spy(monkeypatch):
    calls: dict[str, list[dict[str, object]]] = {}

    def make_transform(name: str):
        def _transform(*, raw_root: Path | None = None, **kwargs):
            calls.setdefault(name, []).append({"raw_root": raw_root, **kwargs})
            return {
                "stage_path": Path(f"{name}_stage.parquet"),
                "processed_path": Path(f"{name}_processed.parquet"),
                "rows_stage": 1,
                "rows_processed": 1,
            }

        return _transform

    monkeypatch.setattr(
        cli,
        "_AUTO_TRANSFORMERS",
        {
            "rodadas": make_transform("rodadas"),
            "mercado_status": make_transform("mercado_status"),
            "partidas": make_transform("partidas"),
            "clubes": make_transform("clubes"),
        },
    )
    return calls


def test_cli_list(capsys, fake_endpoints):
    exit_code = cli.main(["--list"])
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "clubes" in out
    assert "partidas_por_rodada" in out
    assert "rodadas" in out
    assert "mercado_status" in out
    assert "partidas" in out


def test_cli_collect_specific(monkeypatch, fake_settings, fake_endpoints, tmp_path):
    calls: list[tuple[str, Path]] = []

    def fake_collect(endpoint, **kwargs):
        calls.append((endpoint.name, Path(kwargs["base_dir"])))
        return Path(tmp_path / "result.json")

    monkeypatch.setattr(cli, "collect_endpoint_payload", fake_collect)

    out_dir = tmp_path / "custom"
    exit_code = cli.main(["clubes", "--output", str(out_dir)])

    assert exit_code == 0
    assert calls == [("clubes", out_dir)]


def test_cli_requires_round(fake_settings, fake_endpoints):
    with pytest.raises(SystemExit) as exc:
        cli.main(["partidas_por_rodada"])
    assert "exigem o parametro" in str(exc.value)


def test_cli_collect_all(monkeypatch, fake_settings, fake_endpoints):
    collected = []

    def fake_collect(endpoint, **kwargs):
        collected.append(endpoint.name)
        return fake_settings.raw_dir / endpoint.name / "payload.json"

    monkeypatch.setattr(cli, "collect_endpoint_payload", fake_collect)

    exit_code = cli.main(["--all", "--rodada", "1"])
    assert exit_code == 0
    assert collected == [endpoint.name for endpoint in fake_endpoints]


def test_cli_collect_all_discovers_rounds(monkeypatch, fake_settings, fake_endpoints):
    collected: list[tuple[str, int | None]] = []

    def fake_collect(endpoint, **kwargs):
        collected.append((endpoint.name, kwargs.get("rodada")))
        return fake_settings.raw_dir / endpoint.name / "payload.json"

    monkeypatch.setattr(cli, "collect_endpoint_payload", fake_collect)
    monkeypatch.setattr(cli, "_discover_all_rounds", lambda *_args, **_kwargs: [1, 2])

    exit_code = cli.main(["--all"])
    assert exit_code == 0

    assert ("clubes", None) in collected
    assert ("rodadas", None) in collected
    assert ("mercado_status", None) in collected
    assert ("partidas", None) in collected
    assert ("partidas_por_rodada", 1) in collected
    assert ("partidas_por_rodada", 2) in collected
    assert len(collected) == 6


def test_cli_collect_handles_failure(
    monkeypatch, fake_settings, fake_endpoints, capsys,
):
    def failing(endpoint, **kwargs):
        request = httpx.Request("GET", endpoint.url)
        response = httpx.Response(404, request=request)
        raise httpx.HTTPStatusError("404", request=request, response=response)

    monkeypatch.setattr(cli, "collect_endpoint_payload", failing)
    monkeypatch.setattr(cli, "_discover_all_rounds", lambda *_: [1])

    exit_code = cli.main(["--all"])
    assert exit_code == 1

    captured = capsys.readouterr()
    assert "endpoint=partidas_por_rodada" in captured.err
    assert "404" in captured.err


def test_cli_runs_transform_for_clubes(
    monkeypatch, fake_settings, auto_transform_spy,
):
    endpoints = [Endpoint(name="clubes", url="https://example.com/clubes")]
    monkeypatch.setattr(cli, "list_endpoints", lambda: endpoints)

    def fake_collect(endpoint, **kwargs):
        return fake_settings.raw_dir / "clubes" / "payload.json"

    monkeypatch.setattr(cli, "collect_endpoint_payload", fake_collect)

    exit_code = cli.main(["clubes"])
    assert exit_code == 0
    assert len(auto_transform_spy.get("clubes", [])) == 1


def test_cli_runs_transform_for_rodadas(
    monkeypatch, fake_settings, auto_transform_spy,
):
    endpoints = [Endpoint(name="rodadas", url="https://example.com/rodadas")]
    monkeypatch.setattr(cli, "list_endpoints", lambda: endpoints)

    def fake_collect(endpoint, **kwargs):
        return fake_settings.raw_dir / "rodadas" / "payload.json"

    monkeypatch.setattr(cli, "collect_endpoint_payload", fake_collect)

    exit_code = cli.main(["rodadas"])
    assert exit_code == 0
    assert len(auto_transform_spy.get("rodadas", [])) == 1
    call = auto_transform_spy["rodadas"][0]
    assert Path(call["raw_root"]).resolve() == fake_settings.raw_dir.resolve()


def test_cli_runs_transform_for_mercado(
    monkeypatch, fake_settings, auto_transform_spy,
):
    endpoints = [Endpoint(name="mercado_status", url="https://example.com/mercado_status")]
    monkeypatch.setattr(cli, "list_endpoints", lambda: endpoints)

    def fake_collect(endpoint, **kwargs):
        return fake_settings.raw_dir / "mercado_status" / "payload.json"

    monkeypatch.setattr(cli, "collect_endpoint_payload", fake_collect)

    exit_code = cli.main(["mercado_status"])
    assert exit_code == 0
    assert len(auto_transform_spy.get("mercado_status", [])) == 1


def test_cli_runs_transform_for_partidas(
    monkeypatch, fake_settings, auto_transform_spy,
):
    endpoints = [Endpoint(name="partidas", url="https://example.com/partidas")]
    monkeypatch.setattr(cli, "list_endpoints", lambda: endpoints)

    def fake_collect(endpoint, **kwargs):
        return fake_settings.raw_dir / "partidas" / "payload.json"

    monkeypatch.setattr(cli, "collect_endpoint_payload", fake_collect)

    exit_code = cli.main(["partidas"])
    assert exit_code == 0
    assert len(auto_transform_spy.get("partidas", [])) == 1


def test_cli_transform_failure_propagates(
    monkeypatch, fake_settings, capsys,
):
    endpoints = [Endpoint(name="rodadas", url="https://example.com/rodadas")]
    monkeypatch.setattr(cli, "list_endpoints", lambda: endpoints)

    def fake_collect(endpoint, **kwargs):
        return fake_settings.raw_dir / "rodadas" / "payload.json"

    monkeypatch.setattr(cli, "collect_endpoint_payload", fake_collect)

    def boom(**_kwargs):
        raise ValueError("boom")

    monkeypatch.setattr(cli, "_AUTO_TRANSFORMERS", {"rodadas": boom})

    exit_code = cli.main(["rodadas"])
    assert exit_code == 1

    err = capsys.readouterr().err
    assert "endpoint=transform_rodadas" in err
    assert "boom" in err


def test_cli_transform_uses_custom_output(
    monkeypatch, fake_settings, auto_transform_spy, tmp_path,
):
    endpoints = [Endpoint(name="rodadas", url="https://example.com/rodadas")]
    monkeypatch.setattr(cli, "list_endpoints", lambda: endpoints)

    def fake_collect(endpoint, **kwargs):
        return kwargs["base_dir"] / endpoint.name / "payload.json"

    monkeypatch.setattr(cli, "collect_endpoint_payload", fake_collect)

    out_dir = tmp_path / "custom_raw"
    exit_code = cli.main(["rodadas", "--output", str(out_dir)])
    assert exit_code == 0
    assert len(auto_transform_spy.get("rodadas", [])) == 1
    assert auto_transform_spy["rodadas"][0]["raw_root"].resolve() == out_dir.resolve()