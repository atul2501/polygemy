import pytest

from polygemy.config import ConfigError, Settings

BASE = {"APP_ENV": "local", "TRADING_MODE": "paper", "DATABASE_URL": "postgres://u:p@h/db"}


def test_valid():
    s = Settings.from_env(BASE)
    assert (s.app_env, s.trading_mode, s.log_level) == ("local", "paper", "INFO")


@pytest.mark.parametrize(
    "override",
    [
        {"APP_ENV": "prod"},
        {"TRADING_MODE": "yolo"},
        {"DATABASE_URL": "sqlite:///x.db"},
        {"DATABASE_URL": ""},
        {"LOG_FORMAT": "xml"},
    ],
)
def test_rejects_invalid(override):
    with pytest.raises(ConfigError):
        Settings.from_env({**BASE, **override})
