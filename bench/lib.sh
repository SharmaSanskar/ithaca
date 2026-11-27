#!/usr/bin/env bash
# Shared helpers for the benchmark experiments. Sourced by run.sh.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BENCH="$ROOT/bench"
RESULTS="$BENCH/results"
LOGS="$ROOT/tmp/bench"
BIN="$ROOT/bin/ithaca"
K6_IMAGE="grafana/k6:2.4.0"
COMPOSE="docker compose -f $ROOT/deploy/compose/docker-compose.deps.yml"
mkdir -p "$RESULTS" "$LOGS"

# The durable benchmark Postgres (port 5433), one fresh database per run.
export BENCH_DB_ADMIN="postgres://ithaca:ithaca@localhost:5433/ithaca?sslmode=disable"
export ITHACA_DATABASE_URL="postgres://ithaca:ithaca@localhost:5433/bench?sslmode=disable"
export ITHACA_REDIS_URL="redis://localhost:6379/0"
export ITHACA_ENV=dev ITHACA_LOG_LEVEL=warn ITHACA_LOG_FORMAT=json

log() { printf '\033[2m%s\033[0m %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

q() { # q "SQL" → tab-separated rows from the bench database
  $COMPOSE exec -T postgres-bench psql -U ithaca -d bench -tA -F $'\t' -c "$1"
}

dbnow() { q "SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"')"; }

latency() { # latency FROM TO → exact percentiles table
  $COMPOSE exec -T postgres-bench psql -U ithaca -d bench -v from="$1" -v to="$2" -P footer=off < "$BENCH/latency.sql"
}

stop_all() {
  pkill -f "$BIN " 2>/dev/null || true
  "$ROOT/scripts/dev.sh" stop >/dev/null 2>&1 || true
  for _ in $(seq 1 50); do pgrep -f "$BIN " >/dev/null || return 0; sleep 0.1; done
  pkill -9 -f "$BIN " 2>/dev/null || true
}

fresh_db() {
  log "fresh database"
  $COMPOSE --profile bench up -d --wait postgres-bench >/dev/null 2>&1
  $COMPOSE exec -T postgres-bench psql -U ithaca -d ithaca -q -c "DROP DATABASE IF EXISTS bench WITH (FORCE)" -c "CREATE DATABASE bench"
  "$BIN" migrate up >/dev/null 2>&1
}

start() { # start SERVICE [ENV=VALUE ...] — runs one service in the background
  local svc="$1"; shift
  env "$@" "$BIN" "$svc" >>"$LOGS/$svc.log" 2>&1 &
  echo $! >"$LOGS/$svc.pid"
}

wait_http() { # wait_http URL
  for _ in $(seq 1 100); do curl -sf -o /dev/null "$1" && return 0; sleep 0.1; done
  echo "timed out waiting for $1" >&2; return 1
}

# setup_tenant → exports KEY. Starts api + receiver-sim (they stay up).
setup_tenant() {
  start receiver-sim ITHACA_RECEIVER_TRACK_DELIVERIES="${TRACK:-false}"
  start api
  wait_http localhost:9080/readyz
  wait_http localhost:9085/healthz
  KEY=$("$BIN" tenant create bench 2>/dev/null | sed -n 's/^api_key=//p')
  export KEY
}

api() { curl -sf -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' -X "$1" "localhost:8080/api/v1$2" ${3:+-d "$3"}; }

# endpoint SINK [SUB_JSON_EXTRA] → creates an endpoint on receiver-sim's sink
# plus a '*' subscription, and prints the endpoint id.
endpoint() {
  local sink="$1" extra="${2:-}" id
  id=$(api POST /endpoints/ "{\"url\":\"http://localhost:8090/sink/$sink\",\"description\":\"$sink\",\"timeout_ms\":5000}" | python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])')
  api POST /subscriptions/ "{\"endpoint_id\":\"$id\",\"event_types\":[\"*\"]${extra:+,$extra}}" >/dev/null
  echo "$id"
}

sink() { curl -sf -o /dev/null -X PUT "localhost:8090/sink/$1/behavior" -d "$2"; }

# Endpoints verify themselves through the dispatcher's verifier (the real
# handshake). Start a dispatcher briefly if none is running.
verify_endpoints() {
  local temp=0
  if ! pgrep -f "$BIN dispatcher" >/dev/null; then start dispatcher; temp=1; fi
  for _ in $(seq 1 100); do
    [ "$(q "SELECT count(*) FROM endpoints WHERE state <> 'enabled'")" = 0 ] && break
    sleep 0.1
  done
  [ "$temp" = 1 ] && stop dispatcher
  log "endpoints verified: $(q "SELECT string_agg(description || '=' || state, ' ') FROM endpoints")"
}

stop() { # stop SERVICE — graceful (SIGTERM) and wait
  local pf="$LOGS/$1.pid"
  [ -f "$pf" ] || return 0
  kill "$(cat "$pf")" 2>/dev/null || true
  for _ in $(seq 1 100); do kill -0 "$(cat "$pf")" 2>/dev/null || break; sleep 0.1; done
  rm -f "$pf"
}

k6() { # k6 NAME [ENV=VALUE ...] — runs bench/k6/ingest.js, writes results/NAME-k6.json
  local name="$1"; shift
  local envs=(-e "KEY=$KEY" -e "OUT=/out/$name-k6.json")
  for kv in "$@"; do envs+=(-e "$kv"); done
  docker run --rm -i --add-host=host.docker.internal:host-gateway \
    -v "$BENCH/k6:/scripts:ro" -v "$RESULTS:/out" "${envs[@]}" \
    "$K6_IMAGE" run --quiet --no-usage-report /scripts/ingest.js 2>&1 | tail -1
}

pending() { q "SELECT count(*) FROM deliveries"; }

now_s() { python3 -c 'import time;print(time.time())'; }

wait_drained() { # wait_drained TIMEOUT_S → prints seconds taken, or TIMEOUT
  # Deadline in WALL-CLOCK seconds: each check is a ~100ms docker exec, so
  # counting iterations would silently stretch the timeout several-fold.
  local start deadline
  start=$(now_s); deadline=$(python3 -c "print($start + $1)")
  while python3 -c "import sys,time;sys.exit(0 if time.time() < $deadline else 1)"; do
    if [ "$(q "SELECT (SELECT count(*) FROM deliveries) + (SELECT count(*) FROM delivery_streams WHERE state='leased')")" = 0 ]; then
      python3 -c "import time;print(round(time.time() - $start, 2))"
      return 0
    fi
    sleep 0.05
  done
  echo "TIMEOUT"
}

leftovers() { # explain every delivery still pending: why is it not done?
  q "SELECT e.description, d.state, d.attempt, round(EXTRACT(EPOCH FROM d.next_attempt_at - now())) AS due_in_s,
            s.state AS stream_state, round(EXTRACT(EPOCH FROM s.next_visible_at - now())) AS visible_in_s,
            e.circuit_state, left(coalesce(d.last_error,''), 40)
     FROM deliveries d JOIN delivery_streams s ON s.id = d.stream_id JOIN endpoints e ON e.id = d.endpoint_id
     ORDER BY d.next_attempt_at LIMIT 10" | sed 's/^/  leftover: /'
}

machine() {
  echo "machine: $(sysctl -n machdep.cpu.brand_string), $(sysctl -n hw.ncpu) cores, $(( $(sysctl -n hw.memsize) / 1073741824 )) GB"
  echo "docker:  $(docker info --format '{{.NCPU}} CPUs, {{.MemTotal}}' | awk '{printf "%s %s %.1f GB", $1, $2, $3/1073741824}')"
  echo "commit:  $(git -C "$ROOT" rev-parse --short HEAD)$(git -C "$ROOT" diff --quiet || echo '-dirty')  go: $(go version | awk '{print $3}')  k6: $K6_IMAGE"
  echo "postgres: 17, fsync=on, synchronous_commit=on (ack path uses synchronous_commit=off per transaction, by design)"
}
