# Contributing to wish:d

Bug reports, documentation corrections, and sanitized captures from real producers are welcome.
For vulnerabilities, follow [SECURITY.md](SECURITY.md) instead of opening a public issue.

## Local development

Install Git, Make, OpenSSL and [uv](https://docs.astral.sh/uv/getting-started/installation/).
On macOS/Linux (or WSL2):

```bash
make install
make lint
make test
```

Tests start an embedded Postgres and require no Docker or cloud credentials. The
`pgserver` test helper currently has wheels for Python 3.11/3.12; use those versions
for development, even though the runtime package allows newer Python versions. Use
`.venv/bin/pytest tests/test_auth.py -q` for a focused run. Tools live in `.venv/bin`.
Use `make dev-db`, `make migrate`, `make seed`, and
`.venv/bin/wishd serve --host 127.0.0.1` for local UI development.

Start a branch before editing. If another agent or developer owns uncommitted changes, use
`git worktree add -b my-change ../wishd-my-change HEAD` and install a separate environment there.
Avoid edits to someone else's working tree. Keep commits focused and describe observable behavior.

## Checks before a pull request

- Run `make lint` and `make test`. Add regression coverage for behavior and security fixes.
- For packaging changes, run `uv build`, install the resulting wheel and `pgserver` into an
  isolated environment, then run `scripts/check_package.py` with that environment's Python.
- For deployment changes, run the Docker CI smoke checks against disposable volumes.
- Audit dependencies with `uvx pip-audit -r constraints.txt --strict`.
- Scan files and history with [Gitleaks](https://github.com/gitleaks/gitleaks), using
  `--config .gitleaks.toml --redact`. Reports can contain secrets; keep them outside Git.

`pytest -m load` runs performance floors. `pytest -m scale` generates a million runs.
`pytest -m aws` requires an explicitly configured test bucket/account and may incur costs;
these three groups are excluded from the default suite. Do not run AWS tests against production.

CI tests Python 3.11–3.12, builds the distribution, checks Docker boot/auth/storage, audits
pinned runtime dependencies, and scans tracked files. The manually dispatched workflow also
scans all history as a publication gate. No workflow publishes packages or changes visibility.

## Code and data conventions

The internal Python package is `dataspine`; the public product/CLI is wish:d / `wishd`.
Keep `DATASPINE_*` compatibility when changing settings. Numbered SQL migrations are immutable
once applied: add a new file instead of editing old SQL. Test database behavior against real
Postgres. Preserve monotonic run state and out-of-order parent/child stitching.

Document what you tested and label integrations that still need live validation. Never imply a
simulator establishes compatibility with a real producer. Use the guidance in
[CLAUDE.md](CLAUDE.md) and the [architecture decisions](docs/architecture.md).

Raw event logs, dbt manifests, billing exports, screenshots and error traces can contain
credentials, account IDs, personal names and SQL. Keep raw captures in ignored local folders.
Sanitize before adding fixtures; preserve schema and behavior, replace identities with explicit
synthetic values, and record provenance in [tests/fixtures/README.md](tests/fixtures/README.md).
Do not suppress a secret-scanner finding just because it is in a test fixture.

When updating dependencies, regenerate the runtime/AWS constraints with
`uv pip compile pyproject.toml --extra aws --upgrade -o constraints.txt`, audit them, and run CI.

## Submitting work

Explain the problem, resulting behavior and validation in the PR. Include UI screenshots when
useful, with synthetic data. Discuss substantial architecture or compatibility changes in an
issue first. Contributions use this repository's Apache-2.0 license; bundled third-party assets
retain their own licenses. Be respectful, assume good intent, and keep reviews about the work.
