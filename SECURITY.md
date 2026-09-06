# Security policy

wish:d is a single-team metadata service. All API tokens grant full application access; there
is no tenant isolation, RBAC or per-user audit trail. Treat the database, artifacts, logs and
signed handoff links as sensitive. SQL and producer error messages may contain credentials.

## Reporting a vulnerability

Do not post tokens, raw captures or an exploit containing private data in a public issue.
Use the repository's **Security → Advisories → Report a vulnerability** form when enabled:
[private reporting](https://github.com/aidancorrell/wishd/security/advisories/new).
Before public launch the maintainer must enable that channel and verify it works.
If it is unavailable, open an issue asking for a private contact channel without including
vulnerability details. Do not send production secrets in any report.

Include the affected version, setup, impact and a minimal reproduction using synthetic data.
There is no guaranteed response SLA. The latest release candidate is the current maintenance
focus; no older version is promised security backports.

## Deployment boundaries

- Use `wishd serve` so the bind guard runs. Direct `uvicorn dataspine.api:app` bypasses that
  CLI check; the operator must explicitly configure authentication before starting it.
- Shared deployments need TLS, a trusted reverse proxy and rate limiting. Only trust forwarded
  headers from that proxy; HTTPS requests receive a `Secure`, `HttpOnly`, `SameSite=Lax` cookie.
- `/health`, `/ready` and `/metrics` are public operational endpoints. Restrict them at ingress
  if counts, readiness or resource usage are sensitive. Other API endpoints require a token,
  except the dbt Cloud webhook, which requires its own HMAC signature.
- Slack callbacks require a signature. Signed handoff URLs grant access to the referenced
  briefing and do not expire automatically; leave the feature off if that sharing model is
  unsuitable. Rotate the handoff secret to revoke existing links.
- Request sizes are bounded, including unsigned webhooks and multipart uploads. Set upstream
  connection/time/request limits too. API tokens are not a substitute for network isolation.
- Monitor/source configuration can trigger outbound requests or database queries. Only trusted
  operators should control it. Optional integrations need the least privileges they require.
- Back up Postgres and artifact storage together. Keep secrets out of images, logs and Git.

## If a credential reaches Git

Revoke or rotate it first, including any producer using it. Removing it in a later commit does
not remove it from history. Coordinate history replacement across clones and hosted refs, scan
all reachable history again, and verify the old credential no longer authenticates. Never
publish a known credential and rely on repository deletion to undo the exposure.

See [operations](docs/operations.md) and the maintainer's
[release checklist](docs/releasing.md) for the current release gates.
