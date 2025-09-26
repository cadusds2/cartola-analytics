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
        raw_dir=tmp_path / "raw",
        log_level="INFO",
    )
    monkeypatch.setattr(cli, "load_settings", lambda: settings)
    return settings


@pytest.fixture
def fake_endpoints(monkeypatch):
    endpoints = [
        Endpoint(name="clubes", url="https://example.com/clubes"),
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


def test_cli_list(capsys, fake_endpoints):
    exit_code = cli.main(["--list"])
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "clubes" in out
    assert "partidas_por_rodada" in out


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
        return fake_settings.raw_dir / f"{endpoint.name}.json"

    monkeypatch.setattr(cli, "collect_endpoint_payload", fake_collect)

    exit_code = cli.main(["--all", "--rodada", "1"])
    assert exit_code == 0
    assert collected == [endpoint.name for endpoint in fake_endpoints]



def test_cli_collect_all_discovers_rounds(monkeypatch, fake_settings, fake_endpoints):
    collected: list[tuple[str, int | None]] = []

    def fake_collect(endpoint, **kwargs):
        collected.append((endpoint.name, kwargs.get("rodada")))
        return fake_settings.raw_dir / f"{endpoint.name}.json"

    monkeypatch.setattr(cli, "collect_endpoint_payload", fake_collect)
    monkeypatch.setattr(cli, "_discover_all_rounds", lambda *_args, **_kwargs: [1, 2])

    exit_code = cli.main(["--all"])
    assert exit_code == 0

    assert ("clubes", None) in collected
    assert ("partidas_por_rodada", 1) in collected
    assert ("partidas_por_rodada", 2) in collected
    assert len(collected) == 3


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
