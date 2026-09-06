"""Exercise an installed wheel from outside the checkout, against fresh Postgres.

Run with the wheel and pgserver installed in a dedicated environment. CI does
this after building an sdist and building the wheel from that sdist.
"""

from __future__ import annotations

import importlib.metadata
import os
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path

import pgserver

# .gitignore negations can behave differently in build tools than in Git. Inspect
# the actual archives too, so a clean working tree cannot mask packaged local data.
private_parts = {".env", ".dev", ".venv", ".git", ".claude", ".codex", ".dataspine", ".wishd"}
for distribution in (Path(__file__).resolve().parents[1] / "dist").iterdir():
    if distribution.suffix == ".whl":
        with zipfile.ZipFile(distribution) as wheel:
            members = wheel.namelist()
    elif distribution.name.endswith(".tar.gz"):
        with tarfile.open(distribution) as sdist:
            members = sdist.getnames()
    else:
        continue
    for member in members:
        assert not private_parts.intersection(Path(member).parts), (distribution.name, member)
        assert "tests/fixtures/local/" not in member, (distribution.name, member)

# The checkout must not provide imports, migrations, templates, or assets.
with tempfile.TemporaryDirectory(prefix="wishd-package-") as directory:
    os.chdir(directory)
    from dataspine import __version__, db

    assert importlib.metadata.version("wishd") == __version__
    sql = db.migrations_dir()
    assert "site-packages" in str(sql), sql
    assert len(list(sql.glob("*.sql"))) == 22
    assert (sql.parent / "THIRD_PARTY_NOTICES.md").is_file()
    assert (sql.parent / "static" / "fonts" / "OFL.txt").is_file()
    assert (sql.parent / "templates" / "login.html").is_file()
    assert (sql.parent / "static" / "style.css").is_file()
    assert list((sql.parent / "static" / "fonts").glob("*.woff2"))

    server = pgserver.get_server(Path(directory) / "pgdata", cleanup_mode="stop")
    os.environ["WISHD_DATABASE_URL"] = server.get_uri()
    os.environ["WISHD_API_TOKENS"] = "smoke:fixture-token"
    os.environ["WISHD_INGEST_ASYNC"] = "false"
    os.environ["WISHD_UPKEEP"] = "off"
    cli = str(Path(sys.executable).parent / "wishd")
    subprocess.run([cli, "migrate"], check=True)
    subprocess.run([cli, "migrate"], check=True)
    subprocess.run([cli, "--help"], check=True, stdout=subprocess.DEVNULL)
    from fastapi.testclient import TestClient

    from dataspine.api import app

    with TestClient(app) as client:
        assert client.get("/ready").status_code == 200
        assert client.get("/api/v1/runs").status_code == 401
        response = client.post(
            "/login", data={"token": "fixture-token"}, follow_redirects=False
        )
        assert response.status_code == 303
        assert client.get("/").status_code == 200
        assert client.get("/static/style.css").status_code == 200
    db.reset_pool()
print("Installed wheel: migrations, authentication, UI and static assets passed.")
