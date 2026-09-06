.PHONY: help install env up down logs migrate seed demo test coverage lint fmt dev-db dev-down clean validate-thrift capture-thrift validate-iceberg capture-iceberg bench

PY := .venv/bin/python
DATASPINE := .venv/bin/wishd
GATEWAY_URL ?= http://localhost:8080

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install:  ## Create the venv and install the package with dev extras
	uv venv --python 3.12 .venv
	uv pip install -e ".[dev]"

# ---------------------------------------------------------------- docker path

env:  ## Generate an API token into .env if there isn't one
	@if grep -q '^WISHD_API_TOKENS=' .env 2>/dev/null || [ -z "$$(sh scripts/envfile.sh get DATASPINE_API_TOKENS)" ]; then \
		sh scripts/envfile.sh ensure WISHD_API_TOKENS "local:$$(openssl rand -hex 24)"; fi
	@echo "API token is stored in .env (owner-readable only)."

up: env  ## Start Postgres + gateway
	@sh scripts/envfile.sh ensure WISHD_POSTGRES_PASSWORD "dataspine"
	docker compose up -d --build --wait --wait-timeout 120
	@pg_port=$${WISHD_PG_PORT:-$$(sh scripts/envfile.sh get WISHD_PG_PORT)}; \
	pg_port=$${pg_port:-$${DATASPINE_PG_PORT:-$$(sh scripts/envfile.sh get DATASPINE_PG_PORT)}}; \
	pg_password=$${WISHD_POSTGRES_PASSWORD:-$$(sh scripts/envfile.sh get WISHD_POSTGRES_PASSWORD)}; \
	sh scripts/envfile.sh set WISHD_DATABASE_URL \
		"postgresql://dataspine:$$pg_password@localhost:$${pg_port:-5432}/dataspine"
	@echo "Gateway is ready. .env points the CLI at the container's Postgres."

down:  ## Stop everything (keeps the volume)
	docker compose down

logs:  ## Tail the gateway
	docker compose logs -f gateway

demo: up  ## Start the stack and push a simulated EMR pipeline through it
		$(DATASPINE) seed --url $(GATEWAY_URL)
	@echo
	$(DATASPINE) runs --roots

# ------------------------------------------------------------- no-docker path
# Embedded Postgres for developing before Docker is installed, and for CI.

dev-db: env  ## Start a local Postgres without Docker and write .env
	$(DATASPINE) dev-db
	@sh scripts/envfile.sh set WISHD_DATABASE_URL "$$($(PY) -c \
		"import pgserver,pathlib; \
		 print(pgserver.get_server(pathlib.Path('.dev/pgdata').resolve(), cleanup_mode=None).get_uri())")"
	@echo ".env now points the CLI at the embedded Postgres."

dev-down:  ## Stop the embedded Postgres
	$(DATASPINE) dev-db --stop

# ------------------------------------------------------------------ workflow

migrate:  ## Apply migrations
	$(DATASPINE) migrate

seed:  ## Ingest a simulated Airflow -> dbt -> Spark/EMR pipeline
	$(DATASPINE) seed

# ------------------------------------------------------- validation stacks

validate-thrift:  ## Run dbt-spark over a real Thrift Server (settles D6)
	docker compose -f docker-compose.yml -f docker-compose.dev.yml \
		--profile thrift up -d --build
	@echo "waiting for the thrift server to accept connections..."
	@until docker compose -f docker-compose.yml -f docker-compose.dev.yml \
		exec -T thrift bash -c '</dev/tcp/localhost/10000' 2>/dev/null; \
		do sleep 5; printf .; done; echo " up"
	# dbt runs inside the Airflow worker, which is the real-world pattern --
	# not a separate service that fakes the handoff.
	docker compose -f docker-compose.yml -f docker-compose.dev.yml exec -T airflow \
		bash -lc 'cd /opt/airflow/dbt && \
		  OPENLINEAGE_URL=http://gateway:8080 \
		  OPENLINEAGE_NAMESPACE=dbt://analytics \
		  dbt-ol run --consume-structured-logs \
		    --profile analytics_spark --profiles-dir . --project-dir .'
	@echo
	@echo "now: make capture-thrift"

capture-thrift:  ## Print what dbt-spark-over-thrift actually emitted
	$(PY) scripts/capture_thrift.py

validate-iceberg:  ## Run dbt-spark materialising Iceberg, through a REST catalog
	docker compose -f docker-compose.yml -f docker-compose.dev.yml \
		--profile thrift up -d --build
	@echo "waiting for the iceberg REST catalog..."
	@until curl -sf http://localhost:8181/v1/config >/dev/null 2>&1; \
		do sleep 3; printf .; done; echo " up"
	@echo "waiting for the thrift server to accept connections..."
	@until docker compose -f docker-compose.yml -f docker-compose.dev.yml \
		exec -T thrift bash -c '</dev/tcp/localhost/10000' 2>/dev/null; \
		do sleep 5; printf .; done; echo " up"
	# Same invocation as validate-thrift, with one variable changed: the file
	# format. That is the point -- anything different in what OpenLineage
	# reports is attributable to Iceberg and nothing else.
	docker compose -f docker-compose.yml -f docker-compose.dev.yml exec -T airflow \
		bash -lc 'cd /opt/airflow/dbt && \
		  OPENLINEAGE_URL=http://gateway:8080 \
		  OPENLINEAGE_NAMESPACE=dbt://analytics \
		  DBT_FILE_FORMAT=iceberg \
		  dbt-ol run --consume-structured-logs \
		    --profile analytics_iceberg --profiles-dir . --project-dir .'
	@echo
	@echo "now: make capture-iceberg"

capture-iceberg:  ## Print what dbt + Iceberg actually emitted, and what the catalog holds
	$(PY) scripts/capture_iceberg.py

test:  ## Run the test suite (boots its own Postgres)
	.venv/bin/pytest -q

coverage:  ## Which code the suite never reaches
	.venv/bin/pytest -q --cov=src/dataspine --cov-report=term-missing:skip-covered

bench:  ## Read-side latency at 10^6 runs (slow; generates a million rows)
	.venv/bin/pytest tests/test_scale.py -s -m scale

lint:
	.venv/bin/ruff check src tests scripts/check_package.py scripts/prepare_public_mirror.py deploy/github-action/impact.py

fmt:
	.venv/bin/ruff format src tests
	.venv/bin/ruff check --fix src tests

clean:
	rm -rf .dev .env .pytest_cache .ruff_cache
