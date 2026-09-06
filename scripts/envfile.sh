#!/usr/bin/env sh
# Idempotently set a key in .env.
#
# Both the Docker path and the embedded-Postgres path write here, and the CLI
# reads it. Whichever backend you started last owns DATASPINE_DATABASE_URL,
# which is the semantics you want: `.env` points at whatever is running.
#
# Usage:  scripts/envfile.sh set KEY VALUE
#         scripts/envfile.sh ensure KEY VALUE   # only if KEY is absent
#         scripts/envfile.sh get KEY

set -eu
umask 077

ENV_FILE="${ENV_FILE:-.env}"
ACTION="$1"
KEY="$2"

touch "$ENV_FILE"
chmod 600 "$ENV_FILE"

case "$ACTION" in
  get)
    grep "^${KEY}=" "$ENV_FILE" 2>/dev/null | tail -1 | cut -d= -f2- | tr -d '"' || true
    ;;
  ensure)
    if [ -n "$(grep "^${KEY}=" "$ENV_FILE" 2>/dev/null | tail -1 | cut -d= -f2- | tr -d '"')" ]; then
      exit 0
    fi
    exec sh "$0" set "$KEY" "$3"
    ;;
  set)
    # Rewrite rather than append, so repeated `make up` does not accumulate
    # stale duplicate keys that later shadow each other unpredictably.
    tmp="${ENV_FILE}.tmp.$$"
    grep -v "^${KEY}=" "$ENV_FILE" > "$tmp" 2>/dev/null || true
    printf '%s=%s\n' "$KEY" "$3" >> "$tmp"
    mv "$tmp" "$ENV_FILE"
    ;;
  *)
    echo "usage: envfile.sh {get|set|ensure} KEY [VALUE]" >&2
    exit 2
    ;;
esac
