import importlib


def test_cartola_package_importable():
    module = importlib.import_module("cartola_analytics")
    assert module is not None
