# Releasing wish:d

The frontend redesign and release hardening are merged. The current package version is
`1.0.0rc1`, published from a fresh history in this repository; the private `dataspine`
repository is retained as the development archive. Publishing from a new repository is what
closes the retained-hosted-reference item below — no PR refs, caches or old blobs followed
the tree across.

The checklist below is the standard for a *final* release. `1.0.0rc1` is a release candidate
and does not meet all of it. The publication record at the end of this file states exactly
which gates were verified and which remain open; do not describe an unverified path as
validated.

## Validate the final commit

- [ ] Run CI on the combined tree: lint, Python 3.11/3.12 tests, dependency audit,
  tracked-file secret scan, package checks and Docker smoke checks.
- [ ] Follow the README from a clean checkout. Verify login, the seeded run tree and monitors,
  then verify the no-Docker instructions independently.
- [ ] Build from the sdist and install the wheel outside the checkout. Check migrations,
  authenticated pages, static assets and archive contents using the packaging CI procedure.
- [ ] Check Docker authentication and uploaded artifact persistence across gateway recreation.
- [ ] Review the UI with synthetic data: light/dark themes, mobile layout, keyboard focus,
  login/logout, a failed run, lineage and incident navigation.
- [ ] Confirm visible wish:d branding, copyable `wishd` commands and all bundled font notices.

Record results against the final commit in the release PR. Earlier branch test reports do
not substitute for verification of the combined build. Live integration gaps remain labelled
in the [roadmap](../ROADMAP.md) and [integration guide](integrations.md).

## Security and repository settings

- [ ] Resolve the retained GitHub PR/cache history review before publication. The previously
  exposed local token was rotated and writable history cleaned; retained hosted references
  still need review. Keep incident details and support correspondence outside public docs.
- [ ] Scan the final tracked tree and hosted history after all merges. The manual CI history
  scan is a publication gate; investigate findings instead of broadly allowlisting fixtures.
- [ ] Confirm no old credential authenticates and no producer still uses it.
- [ ] Enable private vulnerability reporting and verify the path in [SECURITY.md](../SECURITY.md).
- [ ] Review repository metadata, branch protection, CI permissions and issue/PR templates.

## Version and distribution

- [ ] Choose the final version and reconcile package metadata and changelog.
- [ ] Verify source and third-party licenses in the built artifacts.
- [ ] Confirm repository/package/image names and update URLs if the repository is renamed.
- [ ] Decide which distribution channels will actually be published. Document only available
  packages/images and test their installation commands once published.
- [ ] Prepare release notes and an announcement with accurate validation limitations.
- [ ] Obtain explicit owner approval before changing repository visibility, publishing a tag,
  package or container image, or posting the announcement.

See [CONTRIBUTING.md](../CONTRIBUTING.md) for check commands and
[operations](operations.md) for deployment and upgrade guidance.

## Publication record — 1.0.0rc1

What was actually verified before this repository was made public, on the tree that became
the initial commit. Anything not listed here was not checked.

Verified:

- `ruff check src tests` clean.
- Full suite green: 972 passed, 20 deselected (`load`, `scale` and `aws` markers), against a
  real Postgres booted by the session fixture.
- **The full-history release gate passed on this repository.** `gitleaks git --log-opts=--all`
  is `workflow_dispatch`-only and does not run on push, so it was dispatched deliberately
  against `main`: run 34068405193. The tracked-tree scan passed alongside it. A local scan of
  the archive's 48 commits for Slack bot tokens, dbt Cloud tokens, GitHub tokens, AWS access
  keys and private key blocks also found nothing. `.env` is untracked, and no live dbt Cloud,
  Snowflake or Slack workspace identifier appears in a tracked file.
- Dependency audit: `pip-audit -r constraints.txt --strict` clean. Note that its first attempt
  failed on a pypi.org read timeout, not a finding — a network failure here looks like a gate
  failure and is worth re-running before investigating.
- Retained hosted references: closed by publishing from a new repository rather than by
  changing the archive's visibility.
- Package build and install outside the checkout: `wishd-1.0.0rc1` sdist and wheel build, the
  wheel installs into a clean environment, `wishd --help` runs, and the 22 migrations, static
  assets and templates are present in the installed package.
- Docker, in CI: image builds, the gateway refuses an unauthenticated public bind, boots with
  auth, answers `/ready`, returns 401 on an unauthenticated API call, ingests a seeded
  pipeline, and keeps its artifact directory across a forced container recreation.
- Review is enforced: CODEOWNERS assigns the owner, and `main` is protected.

Still open, and therefore not to be described as validated:

- Following the README end to end from a clean checkout, including the no-Docker path.
- The UI review: light/dark themes, mobile layout, keyboard focus, login/logout.
- License review of the built artifacts.
- Distribution beyond this repository. No package registry or container image has been
  published, so no installation command for one belongs in the docs yet.

Live integration gaps remain labelled in the [roadmap](../ROADMAP.md) and
[integration guide](integrations.md).
