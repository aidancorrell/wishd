"""A copied .env.example must become a working, private token file."""

import os
import stat
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "envfile.sh"


def test_ensure_replaces_blank_values_and_preserves_existing_secret(tmp_path):
    path = tmp_path / ".env"
    path.write_text("WISHD_API_TOKENS=\nUNCHANGED=yes\n")
    path.chmod(0o644)
    environment = {**os.environ, "ENV_FILE": str(path)}
    for token in ("local:generated-token", "local:should-not-replace"):
        subprocess.run(
            ["sh", str(SCRIPT), "ensure", "WISHD_API_TOKENS", token],
            env=environment, check=True,
        )
    assert path.read_text().count("WISHD_API_TOKENS=") == 1
    assert "WISHD_API_TOKENS=local:generated-token" in path.read_text()
    assert "UNCHANGED=yes" in path.read_text()
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
