import json
from datetime import UTC, datetime

import httpx
import pytest
import respx

from cartola_analytics import (
    CartolaClient,
    CartolaSettings,
    Endpoint,
    build_output_path,
    collect_endpoint_payload,
)


@pytest.fixture(name="sample_endpoint")
def fixture_sample_endpoint() -> Endpoint:
    return Endpoint(name="clubes", url="https://example.com/clubes")


def test_build_output_path_handles_rodada(tmp_path, sample_endpoint):
    timestamp = datetime(2025, 1, 2, 3, 4, 5, tzinfo=UTC)
    path = build_output_path(tmp_path, sample_endpoint, rodada=7, timestamp=timestamp)
    expected_dir = tmp_path / "clubes" / "rodada=007"
    assert path.parent == expected_dir
    assert path.name.startswith("20250102T030405Z")
    assert path.suffix == ".json"


@respx.mock
def test_collect_endpoint_payload_writes_file(tmp_path, sample_endpoint):
    respx.get(sample_endpoint.url).mock(
        return_value=httpx.Response(200, json={"items": []})
    )
    settings = CartolaSettings(
        cache_dir=tmp_path / "cache",
        raw_dir=tmp_path / "raw",
        log_level="ERROR",
    )
    client = CartolaClient(settings=settings, cache_dir=settings.cache_dir, cache_ttl=0)

    timestamp = datetime(2025, 1, 1, tzinfo=UTC)
    output_path = collect_endpoint_payload(
        sample_endpoint,
        rodada=None,
        client=client,
        settings=settings,
        base_dir=settings.raw_dir,
        timestamp=timestamp,
        use_cache=False,
    )

    assert output_path.exists()
    assert json.loads(output_path.read_text()) == {"items": []}
    assert output_path.parent == settings.raw_dir / sample_endpoint.name
    assert client._client.is_closed is False


@respx.mock
def test_collect_endpoint_payload_uses_settings_when_client_missing(
    tmp_path,
    sample_endpoint,
    monkeypatch,
):
    respx.get(sample_endpoint.url).mock(
        return_value=httpx.Response(200, json={"value": 1})
    )
    monkeypatch.setenv("CARTOLA_RAW_DIR", str(tmp_path))
    monkeypatch.setenv("CARTOLA_CACHE_DIR", str(tmp_path / "cache"))

    output_path = collect_endpoint_payload(sample_endpoint, use_cache=False)

    assert output_path.exists()
    assert output_path.parent == tmp_path / sample_endpoint.name
    payload = json.loads(output_path.read_text())
    assert payload == {"value": 1}

    timestamp_str = output_path.stem
    datetime.strptime(timestamp_str, "%Y%m%dT%H%M%SZ")
