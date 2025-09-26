from cartola_analytics import config


def test_load_settings_defaults(monkeypatch):
    keys = [
        "CARTOLA_TIMEOUT",
        "CARTOLA_MAX_RETRIES",
        "CARTOLA_BACKOFF_FACTOR",
        "CARTOLA_CACHE_TTL",
        "CARTOLA_CACHE_DIR",
        "CARTOLA_USER_AGENT",
        "CARTOLA_ACCEPT",
        "CARTOLA_LOG_LEVEL",
        "CARTOLA_RAW_DIR",
        "CARTOLA_LOG_FILE",
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)

    settings = config.load_settings()

    assert settings.timeout == config.DEFAULT_TIMEOUT
    assert settings.max_retries == config.DEFAULT_MAX_RETRIES
    assert settings.backoff_factor == config.DEFAULT_BACKOFF_FACTOR
    assert settings.cache_ttl == config.DEFAULT_CACHE_TTL
    assert settings.cache_dir == config.DEFAULT_CACHE_DIR
    assert settings.raw_dir == config.DEFAULT_RAW_DIR
    assert settings.user_agent == config.DEFAULT_USER_AGENT
    assert settings.accept == config.DEFAULT_ACCEPT
    assert settings.log_level == config.DEFAULT_LOG_LEVEL
    assert settings.log_file == config.DEFAULT_LOG_FILE


def test_load_settings_from_env(monkeypatch, tmp_path):
    raw_dir = tmp_path / "raw"
    log_file = tmp_path / "logs" / "app.log"
    monkeypatch.setenv("CARTOLA_TIMEOUT", "5.5")
    monkeypatch.setenv("CARTOLA_MAX_RETRIES", "7")
    monkeypatch.setenv("CARTOLA_BACKOFF_FACTOR", "0.75")
    monkeypatch.setenv("CARTOLA_CACHE_TTL", "120")
    monkeypatch.setenv("CARTOLA_CACHE_DIR", str(tmp_path))
    monkeypatch.setenv("CARTOLA_RAW_DIR", str(raw_dir))
    monkeypatch.setenv("CARTOLA_USER_AGENT", "custom-agent")
    monkeypatch.setenv("CARTOLA_ACCEPT", "application/vnd.api+json")
    monkeypatch.setenv("CARTOLA_LOG_LEVEL", "debug")
    monkeypatch.setenv("CARTOLA_LOG_FILE", str(log_file))

    settings = config.load_settings()

    assert settings.timeout == 5.5
    assert settings.max_retries == 7
    assert settings.backoff_factor == 0.75
    assert settings.cache_ttl == 120
    assert settings.cache_dir == tmp_path
    assert settings.raw_dir == raw_dir
    assert settings.user_agent == "custom-agent"
    assert settings.accept == "application/vnd.api+json"
    assert settings.log_level == "DEBUG"
    assert settings.log_file == log_file


def test_load_settings_allows_disabling_cache(monkeypatch):
    monkeypatch.setenv("CARTOLA_CACHE_DIR", "none")

    settings = config.load_settings()

    assert settings.cache_dir is None
    assert config.CartolaSettings().cache_dir == config.DEFAULT_CACHE_DIR
