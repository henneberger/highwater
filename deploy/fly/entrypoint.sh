#!/usr/bin/env bash
set -euo pipefail

api_token=${HIGHWATER_API_TOKEN:-}
if (( ${#api_token} < 32 )); then
  echo "HIGHWATER_API_TOKEN must contain at least 32 bytes" >&2
  exit 1
fi

install -d -m 0700 /run/highwater /data/state /data/objects
printf '%s' "$api_token" > /run/highwater/api-token
unset HIGHWATER_API_TOKEN api_token

highwater-server \
  --listen 0.0.0.0:8080 \
  --execution-listen 127.0.0.1:7234 \
  --state-dir /data/state \
  --object-store-dir /data/objects \
  --node-id "${FLY_MACHINE_ID:-cloud}" \
  --api-token-file /run/highwater/api-token &
server_pid=$!

stop() {
  kill -TERM "$server_pid" "${worker_supervisor_pid:-}" "${source_supervisor_pid:-}" "${public_source_supervisor_pid:-}" 2>/dev/null || true
  wait "$server_pid" "${worker_supervisor_pid:-}" "${source_supervisor_pid:-}" "${public_source_supervisor_pid:-}" 2>/dev/null || true
}
trap stop EXIT INT TERM

supervise() {
  local label=$1
  shift
  local child_pid status
  trap 'kill -TERM "${child_pid:-}" 2>/dev/null || true; wait "${child_pid:-}" 2>/dev/null || true; exit 0' TERM INT
  while true; do
    "$@" &
    child_pid=$!
    if wait "$child_pid"; then
      status=0
    else
      status=$?
    fi
    echo "$label exited with status $status; restarting" >&2
    sleep 1
  done
}

python - <<'PY'
import socket
import time

deadline = time.monotonic() + 30
while time.monotonic() < deadline:
    try:
        with socket.create_connection(("127.0.0.1", 7234), timeout=1):
            break
    except OSError:
        time.sleep(0.1)
else:
    raise SystemExit("Highwater execution gateway did not start")
PY

supervise worker highwater-worker examples.catalog \
  --target http://127.0.0.1:7234 \
  --process-poll-width 4 &
worker_supervisor_pid=$!

supervise order-source env HIGHWATER_API_KEY="$(< /run/highwater/api-token)" \
  python -m examples.continuous_order_enrichment \
  --target http://127.0.0.1:8080 &
source_supervisor_pid=$!

supervise wikimedia-source env HIGHWATER_API_KEY="$(< /run/highwater/api-token)" \
  python -m examples.wikimedia_recent_changes \
  --target http://127.0.0.1:8080 &
public_source_supervisor_pid=$!

while kill -0 "$server_pid" 2>/dev/null \
  && kill -0 "$worker_supervisor_pid" 2>/dev/null \
  && kill -0 "$source_supervisor_pid" 2>/dev/null \
  && kill -0 "$public_source_supervisor_pid" 2>/dev/null; do
  sleep 1
done
exit 1
