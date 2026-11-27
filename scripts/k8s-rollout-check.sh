#!/usr/bin/env bash
#
# The Kubernetes gate: restart EVERY Ithaca deployment while events are being
# ingested, and prove nothing was rejected and nothing was lost.
#
# This is what the drain delay, readiness probes, maxUnavailable: 0 and the
# grace period exist for, exercised together. Any of them wrong shows up here:
# a missing drain delay as 502s from the ingress, a short grace period as
# deliveries redelivered or stuck, a bad readiness probe as a stalled rollout.
#
#   scripts/k8s-rollout-check.sh          (needs `make k8s-deploy` first)

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
NS=ithaca
RATE=${RATE:-100}
DURATION=${DURATION:-90}
INGRESS="http://ithaca.localhost:8088"
OUT=tmp/k8s
mkdir -p "$OUT"

log() { printf '\033[1m▸ %s\033[0m\n' "$*"; }
sql() { kubectl -n "$NS" exec statefulset/postgres -- psql -U ithaca -tA -c "$1"; }

# A fresh tenant with one subscription, so the counts below are exact and
# not mixed up with the demo's own traffic. The binary has no shell, so the
# CLI runs directly through `exec` with the pod's own config.
log "creating a tenant and an endpoint"
KEY=$(kubectl -n "$NS" exec deploy/api -c api -- /ithaca tenant create "rollout-$(date +%s)" 2>/dev/null | sed -n 's/^api_key=//p')
api() { curl -sf -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' -X "$1" "$INGRESS/api/v1$2" ${3:+-d "$3"}; }
EP=$(api POST /endpoints/ '{"url":"http://receiver-sim:8090/sink/rollout","description":"rollout"}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])')
api POST /subscriptions/ "{\"endpoint_id\":\"$EP\",\"event_types\":[\"*\"]}" >/dev/null
for _ in $(seq 1 60); do
  [ "$(sql "SELECT state FROM endpoints WHERE id = '$EP'")" = enabled ] && break; sleep 1
done
[ "$(sql "SELECT state FROM endpoints WHERE id = '$EP'")" = enabled ] || { echo "endpoint never verified" >&2; exit 1; }

log "ingesting $RATE events/s for ${DURATION}s through the ingress"
docker run --rm --add-host=host.docker.internal:host-gateway \
  -v "$PWD/bench/k6:/scripts:ro" -v "$PWD/$OUT:/out" \
  -e KEY="$KEY" -e TARGET=http://host.docker.internal:8088 -e HOST=ithaca.localhost \
  -e MODE=constant -e RATE="$RATE" -e DURATION_S="$DURATION" -e OUT=/out/rollout-k6.json \
  grafana/k6:2.4.0 run --quiet --no-usage-report /scripts/ingest.js >/dev/null 2>&1 &
k6pid=$!

sleep 15
log "restarting every deployment, one rollout after another, under load"
for d in ingest api ws dispatcher worker receiver-sim; do
  kubectl -n "$NS" rollout restart "deployment/$d" >/dev/null
  kubectl -n "$NS" rollout status "deployment/$d" --timeout=180s >/dev/null
  echo "  restarted $d"
done

wait "$k6pid"
log "load finished; waiting for the backlog to drain"
for _ in $(seq 1 120); do
  [ "$(sql "SELECT count(*) FROM deliveries WHERE endpoint_id = '$EP'")" = 0 ] && break; sleep 1
done

ingest_ok=0
python3 - "$OUT/rollout-k6.json" <<'PY' || ingest_ok=1
import json, sys
g = json.load(open(sys.argv[1]))["groups"]["main"]
print(f"ingest: {g['requests']:.0f} requests · {g['ok_ratio']*100:.2f}% accepted (202) · "
      f"p50 {g['p50_ms']:.1f} ms · p99 {g['p99_ms']:.1f} ms · dropped {g['dropped']:.0f}")
sys.exit(0 if g["ok_ratio"] == 1 and not g["dropped"] else 1)
PY

events=$(sql "SELECT count(*) FROM events e JOIN endpoints p ON p.tenant_id = e.tenant_id WHERE p.id = '$EP'")
ok=$(sql "SELECT count(*) FROM deliveries_history WHERE endpoint_id = '$EP' AND final_state = 'succeeded'")
dead=$(sql "SELECT count(*) FROM deliveries_history WHERE endpoint_id = '$EP' AND final_state = 'dead'")
pending=$(sql "SELECT count(*) FROM deliveries WHERE endpoint_id = '$EP'")
echo "deliveries: $events events → $ok succeeded, $dead dead, $pending pending"

if [ "$ingest_ok" = 0 ] && [ "$ok" = "$events" ] && [ "$pending" = 0 ] && [ "$dead" = 0 ]; then
  echo "ROLLOUT UNDER LOAD: ✓ every request accepted, every event delivered"
else
  echo "ROLLOUT UNDER LOAD: ✗"; exit 1
fi
