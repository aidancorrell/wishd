"""Turn a real dbt Cloud capture into the committed stub fixtures.

The dbt Cloud artifacts are genuine captures — that is the whole reason they are
worth having, per the fixture rule in CLAUDE.md — but they arrive stamped with
the account they came from: dbt Cloud's account, project, job, environment and
run ids, the warehouse database name, and the operator's dbt user id. None are
credentials, and all of them identify a real account, so the raw capture stays
out of the repository and this produces the stub that goes in.

    tests/fixtures/local/          the real capture, gitignored
    tests/fixtures/dbt_cloud_*     the stub, committed

Run it after re-capturing:

    .venv/bin/python tests/fixtures/sanitize_dbt_cloud.py

**Nothing real is written down here.** The values to replace are discovered from
the capture rather than listed, which matters twice over. A hand-written list
silently misses whatever the next capture happens to contain — the first version
of this script did exactly that, and a second run id, from the run whose state
dbt Cloud deferred to, survived into the stub looking finished. And a list would
have to spell out the very ids this exists to keep out of the repository, so the
script would leak what the fixtures no longer do.

Substitutions are length-preserving. dbt Cloud ids appear both as JSON values and
embedded inside the query comment dbt stamps onto every statement and the
`/tmp/jobs/<run id>/target` paths it records; a shorter replacement would leave
the stub subtly unlike the thing it stands in for. Everything else — timings,
statuses, `adapter_response` query ids, the compiled SQL — is left exactly as
captured, because that is what the tests are actually about.
"""

from __future__ import annotations

import json
import pathlib
import re

HERE = pathlib.Path(__file__).parent
SOURCE = HERE / "local"

FILES = ("dbt_cloud_snowflake_run_results.json", "dbt_cloud_snowflake_manifest.json")

# dbt Cloud stamps its ids into the artifacts' env block under predictable keys,
# which is what makes discovery possible without naming any of them.
CLOUD_ID = re.compile(r'"DBT_CLOUD_(\w*?)_?ID"\s*:\s*"(\d+)"')

# A digit for each kind, so a stub stays readable to someone reading a failing
# assertion: an account is all 1s, a project all 2s, and so on.
KIND_DIGIT = {"ACCOUNT": "1", "PROJECT": "2", "JOB": "3", "ENVIRONMENT": "4", "RUN": "5"}
UNKNOWN_DIGIT = "9"

STUB_UUID = "00000000-0000-4000-8000-000000000000"


def _discover(blobs: list[str], manifest: dict) -> dict[str, str]:
    """Real value -> stub, derived entirely from the capture."""
    kinds: dict[str, str] = {}
    for blob in blobs:
        for kind, value in CLOUD_ID.findall(blob):
            kinds.setdefault(value, kind)

    mapping: dict[str, str] = {}
    seen: dict[str, int] = {}
    # Sorted so the same capture always yields the same stub. A mapping that
    # shifted between runs would show up as a spurious diff on a fixture nobody
    # meant to change.
    for value in sorted(kinds):
        kind = kinds[value]
        digit = KIND_DIGIT.get(kind, UNKNOWN_DIGIT)
        index = seen.get(kind, 0)
        seen[kind] = index + 1
        # Same length, and the trailing counter separates two ids of one kind
        # (two jobs, two runs) without either colliding with the other.
        stub = (digit * len(value))[: len(value) - 1] + str(index)
        mapping[value] = stub

    # The warehouse database every node was built into, and the dbt user id.
    for node in (manifest.get("nodes") or {}).values():
        database = node.get("database")
        if isinstance(database, str) and database:
            mapping.setdefault(database, "ANALYTICS_DB")
    user_id = (manifest.get("metadata") or {}).get("user_id")
    if isinstance(user_id, str) and user_id:
        mapping.setdefault(user_id, STUB_UUID)

    return mapping


def sanitize(blob: str, mapping: dict[str, str]) -> str:
    # Longest first: ids share a prefix, so replacing a shorter one first could
    # cut a longer one in half and leave a fragment of the real value behind.
    for real in sorted(mapping, key=len, reverse=True):
        blob = blob.replace(real, mapping[real])
    return blob


def main() -> int:
    if not SOURCE.is_dir():
        print(f"no capture to sanitize: {SOURCE} does not exist")
        return 1

    raw = {name: (SOURCE / name).read_text() for name in FILES}
    manifest = json.loads(raw["dbt_cloud_snowflake_manifest.json"])
    mapping = _discover(list(raw.values()), manifest)
    print(f"discovered {len(mapping)} value(s) to replace")

    for name, blob in raw.items():
        clean = sanitize(blob, mapping)

        # Fail loudly rather than committing a half-scrubbed fixture. A stub that
        # still carries one real value is worse than no stub: it looks done.
        leaked = sorted(real for real in mapping if real in clean)
        if leaked:
            raise SystemExit(f"{name}: {len(leaked)} value(s) survived sanitizing")
        # Parse before writing: a substitution that broke the JSON would only
        # surface later as an unrelated-looking test failure.
        json.loads(clean)

        (HERE / name).write_text(clean)
        print(f"wrote {name} ({len(clean):,} bytes)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
