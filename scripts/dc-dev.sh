#!/usr/bin/env sh
# docker compose with the dev overlay. Saves repeating both -f flags.
exec docker compose -f docker-compose.yml -f docker-compose.dev.yml "$@"
