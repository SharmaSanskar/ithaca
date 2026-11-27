#!/usr/bin/env bash
#
# The autoscaling demo: a burst of 60k events (120k deliveries) through the
# ingress, and the dispatchers scaling out and back in on the queue itself.
# The rate is what a laptop k3d cluster ingests cleanly while it is also
# delivering (one Postgres serves both); it is ~3x what two dispatchers can
# deliver against 250 ms subscribers, so a real backlog forms. Prints a live timeline
# and a summary. Run it next to the dashboard and `kubectl get pods -w` to
# record it.
#
#   scripts/k8s-burst.sh               KEDA scales the dispatchers
#   BASELINE=1 scripts/k8s-burst.sh    pinned at 2 dispatchers, for comparison
#
# ASSERT_SCALE=N turns it into a gate (CI): exit non-zero unless the
# dispatchers reached N ready pods, every request was accepted, and the
# backlog drained.
#
# Env: RATE (events/s, default 400), DURATION (s, default 150), KEYS
# (ordering keys, default 5000), SLOW_SINK (default orders) and
# SINK_LATENCY_MS (default 1000).
#
# Why a slow ORDERED consumer: it is the case where more dispatchers help. An
# ordered key delivers one event at a time, so 160 orders/s against a 1 s
# subscriber needs ~160 streams in flight, and only more workers provide
# them. (At the demo's default 40 ms, two dispatchers simply keep up; and an
# unordered lane is capped by the subscriber's own max_concurrency, which
# scaling must not, and does not, try to exceed.)

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
NS=ithaca
RATE=${RATE:-400}
DURATION=${DURATION:-150}
KEYS=${KEYS:-5000}
SINK_LATENCY_MS=${SINK_LATENCY_MS:-1000}
SLOW_SINK=${SLOW_SINK:-orders}
SINKS="$SLOW_SINK"
OUT=tmp/k8s
mkdir -p "$OUT"
TSV="$OUT/burst${BASELINE:+-baseline}.tsv"

sql() { kubectl -n "$NS" exec statefulset/postgres -- psql -U ithaca -tA -F $'\t' -c "$1"; }
now() { python3 -c 'import time;print(time.time())'; }

# The demo tenant, so the burst is what the dashboard shows. Its original
# key was printed once at seed time; issue another one.
KEY=$(kubectl -n "$NS" exec deploy/api -c api -- /ithaca tenant key demo 2>/dev/null | sed -n 's/^api_key=//p')
[ -n "$KEY" ] || { echo "could not issue a demo key" >&2; exit 1; }

# Slow the subscribers down, and put them back afterwards. receiver-sim only
# admits the dispatcher and worker (NetworkPolicy), so go through the API
# server with a port-forward rather than open a policy for a script.
kubectl -n "$NS" port-forward svc/receiver-sim 18090:8090 >/dev/null 2>&1 &
pf=$!
for _ in $(seq 1 50); do curl -sf -o /dev/null localhost:18090/ && break; sleep 0.1; done
for sk in $SINKS; do # saved to files, not an associative array: macOS ships bash 3.2
  curl -sf "localhost:18090/sink/$sk/behavior" >"$OUT/sink-$sk.json"
  curl -sf -o /dev/null -X PUT "localhost:18090/sink/$sk/behavior" -d "{\"latency_ms\":$SINK_LATENCY_MS}"
done
restore() {
  for sk in $SINKS; do curl -sf -o /dev/null -X PUT "localhost:18090/sink/$sk/behavior" --data-binary "@$OUT/sink-$sk.json" || true; done
  kill "$pf" 2>/dev/null || true
  if [ -n "${BASELINE:-}" ]; then
    kubectl -n "$NS" annotate scaledobject dispatcher autoscaling.keda.sh/paused-replicas- >/dev/null || true
  fi
}
trap restore EXIT

if [ -n "${BASELINE:-}" ]; then
  kubectl -n "$NS" annotate scaledobject dispatcher autoscaling.keda.sh/paused-replicas=2 --overwrite >/dev/null
  echo "BASELINE: autoscaling paused, dispatchers pinned at 2"
fi
kubectl -n "$NS" rollout status deploy/dispatcher --timeout=120s >/dev/null
# Start from the floor, or "it scaled up" could be leftover scale from
# whatever ran before (in CI, the rollout test's load).
if [ -z "${BASELINE:-}" ]; then
  for _ in $(seq 1 150); do
    [ "$(kubectl -n "$NS" get deploy dispatcher -o jsonpath='{.spec.replicas}')" = 2 ] && break; sleep 2
  done
  [ "$(kubectl -n "$NS" get deploy dispatcher -o jsonpath='{.spec.replicas}')" = 2 ] \
    || { echo "dispatchers never settled at 2 before the burst" >&2; exit 1; }
fi
# receiver-sim keeps sink behavior in memory: if it restarts mid-burst, the
# slow consumer silently becomes fast, and the run measures something else.
rcv_restarts() { kubectl -n "$NS" get pods -l app.kubernetes.io/name=receiver-sim \
  -o jsonpath='{range .items[*]}{.status.containerStatuses[0].restartCount}{" "}{end}'; }
# Wait out any pod replacement first (a deploy just before the run leaves the
# old receiver terminating), or the comparison below sees a different pod.
until [ "$(kubectl -n "$NS" get pods -l app.kubernetes.io/name=receiver-sim --no-headers | wc -l | tr -d ' ')" = 1 ]; do sleep 2; done
rcv_before=$(rcv_restarts)

# The scaler's own query, so the timeline shows what KEDA sees.
SCALER="SELECT CEIL(count(*) / 32.0)::int FROM delivery_streams
        WHERE state = 'leased' OR (state = 'ready' AND next_visible_at <= now())"

echo "burst: $RATE events/s × ${DURATION}s, $KEYS ordering keys, demo tenant (fanout 2 per event), $SLOW_SINK at ${SINK_LATENCY_MS} ms"
docker run --rm --add-host=host.docker.internal:host-gateway \
  -v "$PWD/bench/k6:/scripts:ro" -v "$PWD/$OUT:/out" \
  -e KEY="$KEY" -e TARGET=http://host.docker.internal:8088 -e HOST=ithaca.localhost \
  -e MODE=constant -e RATE="$RATE" -e DURATION_S="$DURATION" -e KEYS="$KEYS" -e OUT=/out/burst-k6.json \
  grafana/k6:2.4.0 run --quiet --no-usage-report /scripts/ingest.js >/dev/null 2>&1 &
k6pid=$!
t0=$(now)

printf '%6s %9s %7s %8s %8s %12s\n' "t (s)" "pending" "scaler" "desired" "ready" "delivered/s" | tee "$OUT/burst.log"
: >"$TSV"
last_done=$(sql "SELECT count(*) FROM deliveries_history"); last_t=$t0
# ONE exec per sample, one query: every `kubectl exec` is a stream through the
# API server, and a sampler that hammers it is itself load on a laptop cluster.
SAMPLE="SELECT (SELECT count(*) FROM deliveries), ($SCALER), (SELECT count(*) FROM deliveries_history)"
drained_at=""; settled_at=""
while :; do
  t=$(now)
  read -r pending scaler done_n <<<"$(sql "$SAMPLE" | tr '\t' ' ')" || true
  if [ -z "${done_n:-}" ]; then sleep 1; continue; fi # a failed sample (exec hiccup): skip it
  desired=$(kubectl -n "$NS" get deploy dispatcher -o jsonpath='{.spec.replicas}')
  ready=$(kubectl -n "$NS" get deploy dispatcher -o jsonpath='{.status.readyReplicas}')
  rate=$(python3 -c "print(round(($done_n - $last_done) / max($t - $last_t, 0.001)))")
  last_done=$done_n; last_t=$t
  el=$(python3 -c "print(round($t - $t0, 1))")
  printf '%6s %9s %7s %8s %8s %12s\n' "$el" "$pending" "$scaler" "$desired" "${ready:-0}" "$rate" | tee -a "$OUT/burst.log"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$el" "$pending" "$scaler" "$desired" "${ready:-0}" "$rate" >>"$TSV"

  k6_running=1; kill -0 "$k6pid" 2>/dev/null || k6_running=0
  # "Drained": load finished and the backlog is down to the demo's ambient trickle.
  if [ -z "$drained_at" ] && [ "$k6_running" = 0 ] && [ "$pending" -lt 50 ]; then drained_at=$el; fi
  if [ -n "$drained_at" ] && { [ -n "${BASELINE:-}" ] || [ "${ready:-0}" -le 2 ] && [ "$desired" -le 2 ]; }; then
    settled_at=$el; break
  fi
  python3 -c "import sys; sys.exit(0 if $el < 900 else 1)" || { echo "gave up after 900 s"; break; }
  sleep 3
done
wait "$k6pid" || true
if [ "$(rcv_restarts)" != "$rcv_before" ]; then
  echo "WARNING: receiver-sim restarted during the run; its sink settings were reset, so this run is not representative"
fi

python3 - "$OUT/burst-k6.json" "$TSV" "$drained_at" "$settled_at" "${BASELINE:-}" "${ASSERT_SCALE:-}" <<'PY'
import json, sys
g = json.load(open(sys.argv[1]))["groups"]["main"]
rows = []
for l in open(sys.argv[2]):
    try:
        rows.append(list(map(float, l.split("\t"))))
    except ValueError:
        pass # a partial sample
drained, settled, baseline = sys.argv[3], sys.argv[4], sys.argv[5]
peak = max(r[4] for r in rows)
first_up = next((r[0] for r in rows if r[3] > 2), None)
at_peak = next((r[0] for r in rows if r[4] == peak), None)
peak_rate = max(r[5] for r in rows)
print()
print(f"ingest:   {g['requests']:.0f} events · {g['ok_ratio']*100:.2f}% accepted · p99 {g['p99_ms']:.0f} ms")
print(f"backlog:  peak {max(r[1] for r in rows):.0f} deliveries · peak drain {peak_rate:.0f} deliveries/s")
if not baseline:
    print(f"scaling:  first scale-up at t={first_up:.0f}s · {peak:.0f} pods ready at t={at_peak:.0f}s" if first_up is not None else "scaling:  never scaled up")
print(f"drained:  t={drained}s after the burst began" if drained else "drained:  never")
if not baseline and settled:
    print(f"settled:  back to 2 pods at t={settled}s")
want = sys.argv[6]
if want:
    ok = peak >= int(want) and g["ok_ratio"] == 1 and bool(drained)
    print(f"GATE: {'✓' if ok else '✗'} reached {peak:.0f} pods (need {want}), "
          f"{g['ok_ratio']*100:.2f}% accepted, {'drained' if drained else 'NOT drained'}")
    sys.exit(0 if ok else 1)
PY
