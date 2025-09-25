import pytest

from cartola_analytics import Endpoint, iter_endpoints, list_endpoints


def test_list_endpoints_returns_copy():
    endpoints = list_endpoints()
    endpoints.append(Endpoint(name="fake", url="https://example.com"))
    assert len(list_endpoints()) == len(endpoints) - 1


def test_iter_endpoints_matches_catalog():
    assert list(iter_endpoints()) == list_endpoints()


def test_round_endpoint_requires_round():
    rodada_endpoint = next(ep for ep in list_endpoints() if ep.requires_round)
    with pytest.raises(ValueError):
        rodada_endpoint.resolve()
    assert rodada_endpoint.resolve(rodada=12).endswith("/12")
