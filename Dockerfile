# Two stages: build wheels with a toolchain, run without one.
#
# The single-stage version compiled and installed as root, leaving the build
# step's toolchain and a root-owned runtime in an image whose whole job is to
# accept events from every machine in a data platform. Wheels are built in the
# first stage and installed in the second, which never resolves anything.
#
# The base is pinned by digest, not just by tag. `python:3.12-slim` is a moving
# target -- the same Dockerfile builds a different image next week -- which is
# the same reproducibility problem as an unpinned dependency, one layer down.
FROM python:3.14-slim@sha256:cad9a2c871761c413caa6fdd6441c783451e740a48aaeba60ae62a8b53525ef6 AS build

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build
COPY pyproject.toml README.md LICENSE THIRD_PARTY_NOTICES.md ./
COPY src ./src
COPY migrations ./migrations

# Wheels, not an install: the runtime stage installs these without needing pip
# to resolve anything, so the build is reproducible from the constraints file.
COPY constraints.txt ./
RUN pip wheel --no-cache-dir --wheel-dir /wheels -c constraints.txt ".[aws]"


FROM python:3.14-slim@sha256:cad9a2c871761c413caa6fdd6441c783451e740a48aaeba60ae62a8b53525ef6

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# A non-root user with no shell and no home to write to. The gateway needs to
# bind a port and talk to Postgres; it has never needed to be root, and an
# ingest endpoint is exactly the wrong place to find out that it was.
RUN useradd --system --create-home --shell /usr/sbin/nologin --uid 10001 dataspine

WORKDIR /app

# Bind-mounted rather than COPYed: a COPY of the wheel directory persists as a
# layer even when a later RUN deletes it, which made the "smaller" image bigger
# than the one it replaced.
RUN --mount=type=bind,from=build,source=/wheels,target=/wheels \
    pip install --no-cache-dir --no-index --find-links=/wheels "wishd[aws]"

# Persistent artifacts must be writable by the unprivileged service account.
RUN mkdir -p /var/lib/wishd/artifacts && chown -R dataspine:dataspine /var/lib/wishd
ENV WISHD_ARTIFACT_DIR=/var/lib/wishd/artifacts

USER dataspine

EXPOSE 8080

# In the image as well as in compose, so anything that runs this container --
# Kubernetes, Nomad, a bare `docker run` -- gets the liveness signal without
# having to know how to construct it.
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=5 \
  CMD python -c "import httpx,sys; sys.exit(0 if httpx.get('http://localhost:8080/health', timeout=2).status_code==200 else 1)"

CMD ["wishd", "serve", "--host", "0.0.0.0", "--port", "8080"]
