"""The product rename must not disconnect existing deployments."""

from dataspine import auth, db
from dataspine.cli import load_dotenv
from dataspine.config import env


def test_preferred_name_and_legacy_fallback(monkeypatch):
    monkeypatch.delenv("WISHD_DATABASE_URL", raising=False)
    monkeypatch.setenv("DATASPINE_DATABASE_URL", "legacy")
    assert db.database_url() == "legacy"
    monkeypatch.setenv("WISHD_DATABASE_URL", "preferred")
    assert db.database_url() == "preferred"


def test_explicit_empty_preferred_value_disables_legacy_setting(monkeypatch):
    monkeypatch.setenv("DATASPINE_API_TOKENS", "legacy")
    monkeypatch.setenv("WISHD_API_TOKENS", "")
    assert auth.configured_tokens() == []


def test_non_product_credentials_are_not_rewritten(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "test-region")
    assert env["AWS_DEFAULT_REGION"] == "test-region"


def test_dotenv_loads_wishd_settings(tmp_path, monkeypatch):
    monkeypatch.setenv("WISHD_API_TOKENS", "temporary")
    monkeypatch.delenv("WISHD_API_TOKENS")
    path = tmp_path / ".env"
    path.write_text("WISHD_API_TOKENS=local:fixture-token\n")
    load_dotenv(path)
    assert auth.configured_tokens() == ["fixture-token"]


def test_exported_legacy_setting_beats_preferred_dotenv(tmp_path, monkeypatch):
    monkeypatch.delenv("WISHD_API_TOKENS", raising=False)
    monkeypatch.setenv("DATASPINE_API_TOKENS", "exported-token")
    path = tmp_path / ".env"
    path.write_text("WISHD_API_TOKENS=file-token\n")
    assert load_dotenv(path) == {}
    assert auth.configured_tokens() == ["exported-token"]


def test_dotenv_last_duplicate_wins(tmp_path, monkeypatch):
    monkeypatch.setenv("WISHD_API_TOKENS", "temporary")
    monkeypatch.delenv("WISHD_API_TOKENS")
    monkeypatch.delenv("DATASPINE_API_TOKENS", raising=False)
    path = tmp_path / ".env"
    path.write_text("WISHD_API_TOKENS=\nWISHD_API_TOKENS=generated-token\n")
    load_dotenv(path)
    assert auth.configured_tokens() == ["generated-token"]
