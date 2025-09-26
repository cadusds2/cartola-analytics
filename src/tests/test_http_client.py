import httpx
import pytest
import respx

from cartola_analytics import CartolaClient, CartolaSettings, Endpoint


@pytest.fixture(name="sample_endpoint")
def fixture_sample_endpoint() -> Endpoint:
    return Endpoint(name="sample", url="https://example.com/sample")


@respx.mock
def test_fetch_success(tmp_path, sample_endpoint):
    respx.get(sample_endpoint.url).mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    with CartolaClient(cache_dir=tmp_path, cache_ttl=0) as client:
        data = client.fetch(sample_endpoint, use_cache=False)
    assert data == {"ok": True}


@respx.mock
def test_fetch_retries_on_server_error(tmp_path, sample_endpoint):
    respx.get(sample_endpoint.url).mock(
        side_effect=[
            httpx.Response(500, json={"error": "server"}),
            httpx.Response(200, json={"ok": True}),
        ]
    )
    with CartolaClient(
        cache_dir=tmp_path,
        cache_ttl=0,
        max_retries=2,
        backoff_factor=0,
    ) as client:
        data = client.fetch(sample_endpoint, use_cache=False)
    assert data["ok"] is True


@respx.mock
def test_fetch_uses_cache(tmp_path, sample_endpoint):
    respx.get(sample_endpoint.url).mock(
        side_effect=[
            httpx.Response(200, json={"count": 1}),
            Exception("network should not be hit on cache"),
        ]
    )
    with CartolaClient(cache_dir=tmp_path, cache_ttl=3600) as client:
        first = client.fetch(sample_endpoint)
        second = client.fetch(sample_endpoint)
    assert first == {"count": 1}
    assert second == {"count": 1}


@respx.mock
def test_fetch_raises_after_exhausted_retries(tmp_path, sample_endpoint):
    respx.get(sample_endpoint.url).mock(return_value=httpx.Response(503))
    with CartolaClient(
        cache_dir=tmp_path,
        cache_ttl=0,
        max_retries=1,
        backoff_factor=0,
    ) as client:
        with pytest.raises(httpx.HTTPStatusError):
            client.fetch(sample_endpoint, use_cache=False)


def test_client_respects_settings_from_env(monkeypatch, tmp_path):
    monkeypatch.setenv("CARTOLA_TIMEOUT", "2")
    monkeypatch.setenv("CARTOLA_MAX_RETRIES", "5")
    monkeypatch.setenv("CARTOLA_BACKOFF_FACTOR", "1")
    monkeypatch.setenv("CARTOLA_CACHE_TTL", "0")
    monkeypatch.setenv("CARTOLA_CACHE_DIR", str(tmp_path))

    with CartolaClient.from_env() as client:
        assert client.timeout == 2.0
        assert client.max_retries == 5
        assert client.backoff_factor == 1.0
        assert client.cache_ttl == 0
        assert client.cache_dir == tmp_path

    manual_settings = CartolaSettings(timeout=1.0, max_retries=1, backoff_factor=0.1)
    with CartolaClient(settings=manual_settings) as client:
        assert client.timeout == 1.0
        assert client.max_retries == 1
        assert client.backoff_factor == 0.1

@respx.mock
def test_fetch_logs_events(caplog, tmp_path, sample_endpoint):
    respx.get(sample_endpoint.url).mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    with caplog.at_level("INFO"):
        with CartolaClient(cache_dir=tmp_path, cache_ttl=0) as client:
            client.fetch(sample_endpoint, use_cache=False)
    events = [record.event for record in caplog.records if hasattr(record, "event")]
    assert "fetch_start" in events
    assert "fetch_success" in events
    assert any(event == "http_success" for event in events)


@respx.mock
def test_fetch_handles_empty_body(tmp_path, sample_endpoint):
    respx.get(sample_endpoint.url).mock(return_value=httpx.Response(200, content=b""))
    with CartolaClient(cache_dir=tmp_path, cache_ttl=0) as client:
        data = client.fetch(sample_endpoint, use_cache=False)
    assert data == {}


@respx.mock
def test_fetch_invalid_json(tmp_path, sample_endpoint):
    respx.get(sample_endpoint.url).mock(
        return_value=httpx.Response(200, text="<html></html>")
    )
    with CartolaClient(cache_dir=tmp_path, cache_ttl=0) as client:
        with pytest.raises(ValueError):
            client.fetch(sample_endpoint, use_cache=False)
