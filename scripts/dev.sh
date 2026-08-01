#!/usr/bin/env bash
#
# Start and stop the whole local stack: six Go services plus the dashboard dev
# server, all in the background with PID files and per-service logs.
#
# The application deliberately runs on the host rather than in compose — a Go
# rebuild is two seconds that way, versus rebuilding an image. The cost is that
# six background processes need managing, which is what this script is for.
#
#   scripts/dev.sh start [service...]   default: all
#   scripts/dev.sh stop
#   scripts/dev.sh status
#   scripts/dev.sh logs [service]
#   scripts/dev.sh restart

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

RUN_DIR="tmp/run"
LOG_DIR="tmp/log"
BIN="bin/ithaca"

GO_SERVICES=(api ingest ws dispatcher worker receiver-sim)
ALL_SERVICES=("${GO_SERVICES[@]}" web)

# Ports, for the status readout. Must match internal/platform/config.
#
# Written as case statements rather than associative arrays because macOS still
# ships bash 3.2, where `declare -A` silently becomes an indexed array and every
# lookup explodes under `set -u`.
admin_port() {
  case "$1" in
    api) echo 9080 ;; ingest) echo 9081 ;; ws) echo 9082 ;;
    dispatcher) echo 9083 ;; worker) echo 9084 ;; receiver-sim) echo 9085 ;;
    *) echo "" ;;
  esac
}
http_port() {
  case "$1" in
    api) echo 8080 ;; ingest) echo 8081 ;; ws) echo 8082 ;;
    receiver-sim) echo 8090 ;; web) echo 5173 ;;
    *) echo "" ;;
  esac
}

c_reset=$'\033[0m'; c_dim=$'\033[2m'; c_green=$'\033[32m'; c_red=$'\033[31m'; c_cyan=$'\033[36m'

pidfile() { echo "$RUN_DIR/$1.pid"; }
logfile() { echo "$LOG_DIR/$1.log"; }

is_running() {
  local pf; pf="$(pidfile "$1")"
  [[ -f "$pf" ]] || return 1
  local pid; pid="$(cat "$pf" 2>/dev/null || true)"
  [[ -n "$pid" ]] || return 1
  kill -0 "$pid" 2>/dev/null
}

start_one() {
  local svc="$1"
  if is_running "$svc"; then
    echo "  ${c_dim}already running${c_reset}  $svc"
    return
  fi

  mkdir -p "$RUN_DIR" "$LOG_DIR"

  if [[ "$svc" == "web" ]]; then
    ( cd web && exec npm run dev ) > "$(logfile web)" 2>&1 &
  else
    ITHACA_ENV="${ITHACA_ENV:-dev}" "$BIN" "$svc" > "$(logfile "$svc")" 2>&1 &
  fi
  echo $! > "$(pidfile "$svc")"
  echo "  ${c_green}started${c_reset}         $svc"
}

stop_one() {
  local svc="$1" pf pid
  pf="$(pidfile "$svc")"
  [[ -f "$pf" ]] || return 0
  pid="$(cat "$pf" 2>/dev/null || true)"
  rm -f "$pf"
  [[ -n "$pid" ]] || return 0
  kill -0 "$pid" 2>/dev/null || return 0

  # SIGTERM, then wait for the graceful drain the services actually implement.
  # SIGKILL only if they refuse, which would itself be a bug worth seeing.
  kill -TERM "$pid" 2>/dev/null || true
  for _ in $(seq 1 40); do
    kill -0 "$pid" 2>/dev/null || { echo "  ${c_green}stopped${c_reset}         $svc"; return 0; }
    sleep 0.25
  done
  echo "  ${c_red}force-killed${c_reset}    $svc (did not drain in 10s)"
  kill -KILL "$pid" 2>/dev/null || true
  # npm spawns vite as a child; killing the wrapper can orphan it.
  pkill -P "$pid" 2>/dev/null || true
}

cmd_start() {
  local -a want=("$@")
  [[ ${#want[@]} -gt 0 ]] || want=("${ALL_SERVICES[@]}")

  if [[ ! -x "$BIN" ]]; then
    echo "building $BIN ..."
    make build >/dev/null
  fi
  if [[ " ${want[*]} " == *" web "* && ! -d web/node_modules ]]; then
    echo "installing dashboard dependencies ..."
    ( cd web && npm ci --no-audit --no-fund >/dev/null )
  fi

  for svc in "${want[@]}"; do start_one "$svc"; done
  echo
  sleep 2
  cmd_status
}

cmd_stop() {
  for svc in "${ALL_SERVICES[@]}"; do stop_one "$svc"; done
  # Anything started before this script existed, or orphaned by a hard kill.
  pkill -TERM -f "$BIN " 2>/dev/null || true
  rmdir "$RUN_DIR" 2>/dev/null || true
}

cmd_status() {
  printf "  %-14s %-8s %-7s %s\n" SERVICE STATE PID URL
  for svc in "${ALL_SERVICES[@]}"; do
    local state pid url="" hp ap
    if is_running "$svc"; then
      state="${c_green}up${c_reset}  "
      pid="$(cat "$(pidfile "$svc")")"
    else
      state="${c_dim}down${c_reset}"
      pid="-"
    fi
    hp="$(http_port "$svc")"
    ap="$(admin_port "$svc")"
    [[ -n "$hp" ]] && url="http://localhost:$hp"
    [[ -n "$ap" ]] && url="$url ${c_dim}admin :$ap${c_reset}"
    printf "  %-14s %b   %-7s %b\n" "$svc" "$state" "$pid" "$url"
  done
  echo
  echo "  ${c_cyan}dashboard${c_reset}  http://localhost:5173/design"
  echo "  ${c_cyan}grafana${c_reset}    http://localhost:3000"
  echo "  ${c_cyan}prometheus${c_reset} http://localhost:9090/targets"
  echo "  ${c_cyan}jaeger${c_reset}     http://localhost:16686"
}

cmd_logs() {
  local svc="${1:-}"
  if [[ -n "$svc" ]]; then
    tail -f "$(logfile "$svc")"
  else
    tail -f "$LOG_DIR"/*.log
  fi
}

case "${1:-status}" in
  start)   shift; cmd_start "$@" ;;
  stop)    cmd_stop ;;
  restart) cmd_stop; sleep 1; shift || true; cmd_start "$@" ;;
  status)  cmd_status ;;
  logs)    shift; cmd_logs "$@" ;;
  *)       echo "usage: $0 {start|stop|restart|status|logs} [service...]"; exit 1 ;;
esac
