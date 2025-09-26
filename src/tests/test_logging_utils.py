import logging

from cartola_analytics import CartolaSettings
from cartola_analytics.logging_utils import (
    JsonFormatter,
    configure_logging,
    configure_logging_from_settings,
)


def test_configure_logging_sets_handlers(tmp_path, capfd):
    log_file = tmp_path / "logs" / "app.log"
    configure_logging("DEBUG", log_file=log_file)

    root = logging.getLogger()
    assert root.handlers, "Root logger deve possuir handlers configurados"
    assert isinstance(root.handlers[0].formatter, JsonFormatter)

    logger = logging.getLogger("test")
    logger.info("ola", extra={"event": "demo"})

    _stdout, stderr = capfd.readouterr()
    assert '"event": "demo"' in stderr
    assert '"event": "demo"' in log_file.read_text()


def test_configure_logging_from_settings(tmp_path, capfd):
    log_file = tmp_path / "log.json"
    settings = CartolaSettings(log_level="WARNING", log_file=log_file)
    configure_logging_from_settings(settings)

    logger = logging.getLogger("cartola.test")
    logger.warning("an", extra={"event": "warn"})
    _stdout, stderr = capfd.readouterr()
    assert '"event": "warn"' in stderr
    assert '"event": "warn"' in log_file.read_text()

    logger.info("ignored", extra={"event": "info"})
    _stdout, stderr = capfd.readouterr()
    assert '"event": "info"' not in stderr
    assert '"event": "info"' not in log_file.read_text()
