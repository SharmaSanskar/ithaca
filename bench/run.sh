#!/usr/bin/env bash
# Ithaca benchmarks. Every experiment starts from a fresh database on the
# durable benchmark Postgres, prints its full configuration with its numbers,
# and writes raw output to bench/results/.
#
#   bench/run.sh e1|e2|e3|e4|e5|e8|all
#
# Rules: warm-up is excluded from every latency window; percentiles are exact
# (computed in SQL from the attempt log, or by k6 from every request); no
# means are reported.
source "$(dirname "$0")/lib.sh"

header() {
  echo "════════════════════════════════════════════════════════════════════"
  echo "$1"
  echo "────────────────────────────────────────────────────────────────────"
  machine
  echo
}

# ── E1: ingest ceiling ──────────────────────────────────────────────────────
# Ingest ALONE (dispatcher off, the queue absorbs everything), so the number
# is the write path's capacity: auth, idempotency-free insert, fanout to one
# subscription, commit with full durability.
e1() {
  header "E1 · ingest ceiling (stepped open-model load, dispatcher off)"
  stop_all; fresh_db; setup_tenant
  endpoint a >/dev/null; verify_endpoints
  start ingest; wait_http localhost:9081/readyz
  echo "config: 1 subscription (fanout 1), payload ~120 B, k6 in Docker → host ingest, 20 s per step"
  k6 e1 MODE=steps STEPS=250,500,1000,2000,3000,4000,6000 STEP_S=20 >/dev/null
  python3 - "$RESULTS/e1-k6.json" <<'PY'
import json, sys
g = json.load(open(sys.argv[1]))["groups"]
print(f"{'target/s':>9} {'achieved/s':>11} {'ok%':>7} {'dropped':>8} {'p50 ms':>8} {'p95 ms':>8} {'p99 ms':>8}")
best = None
for k in sorted(g, key=int):
    v = g[k]; ach = v["requests"] / 20
    ok = v["ok_ratio"] * 100 if v["ok_ratio"] is not None else 0
    print(f"{k:>9} {ach:>11.0f} {ok:>7.2f} {v['dropped']:>8.0f} {v['p50_ms']:>8.2f} {v['p95_ms']:>8.2f} {v['p99_ms']:>8.2f}")
    if ach >= 0.95 * int(k) and ok == 100 and v["p99_ms"] < 50 and not v["dropped"]:
        best = int(k)
print(f"\nceiling (highest step with ≥95% of target achieved, 100% accepted, p99 < 50 ms, nothing dropped): {best}/s")
PY
  stop_all
}

# ── E2: drain rate vs worker count ──────────────────────────────────────────
# Preload a backlog with the dispatcher off, then time how long a dispatcher
# with W workers takes to drain it. Receiver latency is fixed at 20 ms so the
# work is I/O-bound, as webhook delivery is.
drain_sweep() { # drain_sweep LABEL SUB_EXTRA KEYS N
  local label="$1" extra="$2" keys="$3" n="$4" rate=2000
  stop_all; fresh_db; setup_tenant
  endpoint a "$extra" >/dev/null; sink a '{"latency_ms":20}'; verify_endpoints
  start ingest; wait_http localhost:9081/readyz
  echo "── $label  (backlog $n deliveries, receiver 20 ms, max_concurrency 8 per stream)"
  printf '%8s %10s %14s\n' workers "drain (s)" "deliveries/s"
  for w in 1 2 4 8 16 32 64; do
    k6 "e2-preload" MODE=constant RATE=$rate DURATION_S=$(( n / rate )) KEYS="$keys" >/dev/null
    local have; have=$(pending)
    start dispatcher ITHACA_DISPATCH_WORKERS=$w
    local t; t=$(wait_drained 600)
    stop dispatcher
    if [ "$t" = TIMEOUT ]; then printf '%8s %10s %14s\n' "$w" TIMEOUT -; continue; fi
    python3 -c "print(f'{$w:>8} {$t:>10.2f} {$have/$t:>14.0f}')"
  done
  stop_all
}
e2() {
  header "E2 · drain rate vs worker count"
  drain_sweep "unordered, lane_count=16" '"lane_count":16' 0 40000
  drain_sweep "unordered, lane_count=64" '"lane_count":64' 0 40000
  drain_sweep "ordered, 256 ordering keys" '"ordered":true' 256 4000
}

# ── E3: end-to-end latency, decomposed ──────────────────────────────────────
e3() {
  header "E3 · end-to-end latency decomposed (queue wait vs delivery)"
  for rate in 200 20; do
    stop_all; fresh_db; setup_tenant
    endpoint a >/dev/null; endpoint b >/dev/null
    sink a '{"latency_ms":20}'; sink b '{"latency_ms":20}'; verify_endpoints
    start ingest; start dispatcher ITHACA_DISPATCH_WORKERS=64; wait_http localhost:9081/readyz
    echo "── $rate events/s for 70 s (first 10 s excluded), fanout 2, receivers 20 ms, 64 workers, default polling (50 ms floor → 500 ms ceiling)"
    local t0; t0=$(dbnow)
    k6 "e3-$rate" MODE=constant RATE=$rate DURATION_S=70 >/dev/null
    local from; from=$(python3 -c "import datetime as d;print((d.datetime.fromisoformat('${t0%Z}')+d.timedelta(seconds=10)).isoformat()+'Z')")
    local to; to=$(dbnow)
    wait_drained 60 >/dev/null
    echo "ingest (k6, every request): $(python3 -c "import json;v=json.load(open('$RESULTS/e3-$rate-k6.json'))['groups']['main'];print(f\"p50 {v['p50_ms']:.2f} ms · p99 {v['p99_ms']:.2f} ms · {v['ok_ratio']*100:.2f}% accepted\")")"
    echo "delivery (exact, from the attempt log; ms):"
    latency "$from" "$to"
    echo
  done
  stop_all
}

# ── E4: a failing neighbor vs a healthy endpoint's latency ──────────────────
e4() {
  header "E4 · does a 30%-failing endpoint hurt a healthy one?"
  for variant in baseline neighbor; do
    stop_all; fresh_db; setup_tenant
    endpoint healthy >/dev/null; endpoint flaky >/dev/null
    sink healthy '{"latency_ms":20}'
    if [ "$variant" = neighbor ]; then sink flaky '{"latency_ms":20,"fail_rate":0.3,"fail_status":503}'; else sink flaky '{"latency_ms":20}'; fi
    verify_endpoints
    start ingest; start dispatcher ITHACA_DISPATCH_WORKERS=64 ITHACA_RETRY_BASE=1s; wait_http localhost:9081/readyz
    echo "── $variant: 200 events/s × 70 s (first 10 s excluded), fanout 2, receivers 20 ms, retry base 1 s$([ "$variant" = neighbor ] && echo ', flaky endpoint fails 30% with 503')"
    local t0; t0=$(dbnow)
    k6 "e4-$variant" MODE=constant RATE=200 DURATION_S=70 >/dev/null
    local from; from=$(python3 -c "import datetime as d;print((d.datetime.fromisoformat('${t0%Z}')+d.timedelta(seconds=10)).isoformat()+'Z')")
    local to; to=$(dbnow)
    wait_drained 120 >/dev/null
    latency "$from" "$to"
    echo "attempts per delivered event, flaky endpoint: $(q "SELECT round(count(*)::numeric / NULLIF(count(DISTINCT a.delivery_id),0), 3) FROM delivery_attempts a JOIN endpoints e ON e.id=a.endpoint_id WHERE e.description='flaky'")"
    echo
  done
  stop_all
}

# ── E5: total outage of one endpoint, then recovery ─────────────────────────
e5() {
  header "E5 · one endpoint down for 60 s, then recovered (default breaker: 10 failures, 30 s cooldown doubling)"
  stop_all; fresh_db; setup_tenant
  endpoint steady >/dev/null; endpoint outage >/dev/null
  sink steady '{"latency_ms":20}'; sink outage '{"latency_ms":20}'; verify_endpoints
  start ingest; start dispatcher ITHACA_DISPATCH_WORKERS=64; wait_http localhost:9081/readyz
  echo "config: 150 events/s for 150 s, fanout 2. t=30s 'outage' answers 503 to everything; t=90s it recovers."
  local depth="$LOGS/e5-depth.tsv"; : >"$depth"
  local t0; t0=$(now_s)
  # Samples carry REAL elapsed seconds (each sample is two docker execs).
  ( while :; do echo -e "$(python3 -c "import time;print(round(time.time()-$t0,1))")\t$(q "SELECT count(*) FROM deliveries d JOIN endpoints e ON e.id=d.endpoint_id WHERE e.description='outage'")\t$(q "SELECT circuit_state FROM endpoints WHERE description='outage'")" >>"$depth"; sleep 0.5; done ) &
  local poller=$!
  k6 e5 MODE=constant RATE=150 PHASES=30,60,60 >/dev/null &
  local loadgen=$!
  sleep 30; sink outage '{"latency_ms":20,"fail_rate":1,"fail_status":503}'; log "outage begins"
  sleep 60; sink outage '{"latency_ms":20}'; log "outage ends"
  local recovered; recovered=$(python3 -c 'import time;print(time.time())')
  wait "$loadgen"
  local drained; drained=$(wait_drained 300)
  local done_at; done_at=$(python3 -c 'import time;print(time.time())')
  kill "$poller" 2>/dev/null || true
  python3 - "$RESULTS/e5-k6.json" "$depth" "$recovered" "$done_at" "$t0" "$drained" <<'PY'
import json, sys
g = json.load(open(sys.argv[1]))["groups"]
print("ingest during each phase (k6, every request):")
for ph in ("before", "during", "after"):
    v = g[ph]
    print(f"  {ph:>7}: {v['ok_ratio']*100:.2f}% accepted · p50 {v['p50_ms']:.2f} ms · p99 {v['p99_ms']:.2f} ms · {v['requests']:.0f} requests")
rows = [(float(a), int(b), c.strip()) for a, b, c in (l.split("\t") for l in open(sys.argv[2]) if l.strip())]
peak = max(r[1] for r in rows)
states = []
for t, _, st in rows:
    if not states or states[-1][1] != st:
        states.append((t, st))
rec = float(sys.argv[3]) - float(sys.argv[5])
print(f"'outage' backlog peak: {peak} deliveries (parked, not lost)")
print("circuit timeline (s since start): " + " → ".join(f"{t:.0f}s {st}" for t, st in states))
closed = next((t for t, st in states if st == "closed" and t > rec), None)
bulk = next((t for t, d, _ in rows if t > rec and d <= max(1, peak // 100)), None)
print(f"endpoint recovered at t={rec:.0f}s · circuit closed at t={closed:.0f}s ({closed - rec:.0f} s: waiting for the next probe)" if closed else "circuit never closed")
print(f"bulk backlog (≥99%) drained by t={bulk:.0f}s, {bulk - rec:.0f} s after recovery" if bulk else "bulk never drained")
print(f"fully drained (every last delivery): {sys.argv[6]} s after load stopped")
PY
  [ "$(pending)" = 0 ] || leftovers
  echo "dead-lettered: $(q "SELECT count(*) FROM deliveries_history WHERE final_state='dead'")   delivered: $(q "SELECT count(*) FROM deliveries_history WHERE final_state='succeeded'") of $(( $(q 'SELECT count(*) FROM events') * 2 ))"
  stop_all
}

# ── E8: chaos — kill -9 dispatchers under load, then prove conservation ─────
e8() {
  header "E8 · chaos: kill -9 a dispatcher every 20 s under load, then check conservation and phantom acks"
  stop_all; fresh_db; TRACK=true setup_tenant
  endpoint healthy >/dev/null; endpoint flaky >/dev/null
  sink healthy '{"latency_ms":10}'; sink flaky '{"latency_ms":10,"fail_rate":0.2,"fail_status":503}'; verify_endpoints
  start ingest; wait_http localhost:9081/readyz
  local denv=(ITHACA_DISPATCH_WORKERS=32 ITHACA_RETRY_BASE=1s ITHACA_RETRY_MAX_ATTEMPTS=25)
  start dispatcher "${denv[@]}"; cp "$LOGS/dispatcher.pid" "$LOGS/d1.pid"
  start dispatcher "${denv[@]}" ITHACA_ADMIN_ADDR=:9093; cp "$LOGS/dispatcher.pid" "$LOGS/d2.pid"
  echo "config: 2 dispatchers × 32 workers, 100 events/s × 120 s, fanout 2 (healthy 10 ms; flaky 10 ms, 20% 503), retry base 1 s, 25 attempts, lease 30 s"
  k6 e8 MODE=constant RATE=100 DURATION_S=120 >/dev/null &
  local loadgen=$! kills=0
  for round in 1 2 3 4 5; do
    sleep 20
    local victim="d$(( (round % 2) + 1 ))"
    kill -9 "$(cat "$LOGS/$victim.pid")" 2>/dev/null && kills=$((kills + 1))
    log "kill -9 $victim"
    sleep 2
    if [ "$victim" = d1 ]; then start dispatcher "${denv[@]}"; else start dispatcher "${denv[@]}" ITHACA_ADMIN_ADDR=:9093; fi
    cp "$LOGS/dispatcher.pid" "$LOGS/$victim.pid"
  done
  wait "$loadgen"
  log "load done; waiting for quiescence (orphaned leases expire after 30 s)"
  local t; t=$(wait_drained 300)
  local events hist pend leased succ dead
  events=$(q "SELECT count(*) FROM events"); hist=$(q "SELECT count(*) FROM deliveries_history")
  pend=$(q "SELECT count(*) FROM deliveries"); leased=$(q "SELECT count(*) FROM delivery_streams WHERE state='leased'")
  succ=$(q "SELECT count(*) FROM deliveries_history WHERE final_state='succeeded'"); dead=$(q "SELECT count(*) FROM deliveries_history WHERE final_state='dead'")
  echo "kills: $kills · quiescent after load stopped: ${t}s"
  echo "events ingested: $events · expected deliveries (×2): $(( events * 2 ))"
  echo "terminal: $hist (succeeded $succ + dead $dead) · still pending: $pend · leases held: $leased"
  [ "$hist" = "$(( events * 2 ))" ] && [ "$pend" = 0 ] && echo "CONSERVATION: exact ✓ (ingested × fanout == delivered + dead-lettered, nothing pending)" || echo "CONSERVATION: VIOLATED ✗"
  for s in healthy flaky; do curl -sf "localhost:8090/sink/$s/delivered" >"$LOGS/rcv-$s.json"; done
  q "SELECT e.description, h.id FROM deliveries_history h JOIN endpoints e ON e.id=h.endpoint_id WHERE h.final_state='succeeded'" >"$LOGS/db-succeeded.tsv"
  python3 - "$LOGS" <<'PY'
import json, sys
d = sys.argv[1]
rcv = {s: json.load(open(f"{d}/rcv-{s}.json"))["ids"] for s in ("healthy", "flaky")}
db = {"healthy": set(), "flaky": set()}
for line in open(f"{d}/db-succeeded.tsv"):
    s, i = line.rstrip("\n").split("\t"); db[s].add(i)
phantom = sum(1 for s in db for i in db[s] if i not in rcv[s])
unrecorded = sum(1 for s in rcv for i in rcv[s] if i not in db[s])
total = sum(sum(v.values()) for v in rcv.values()); unique = sum(len(v) for v in rcv.values())
print(f"PHANTOM ACKS (marked succeeded, never received a 2xx): {phantom} {'✓' if phantom == 0 else '✗'}")
print(f"received 2xx but not recorded succeeded: {unrecorded}")
print(f"duplicate 2xx deliveries at the receiver: {total - unique} of {unique} ({(total - unique) / max(unique, 1) * 100:.2f}%) — the at-least-once cost of {sys.argv[1] and ''}the crashes")
PY
  stop_all
}

case "${1:-}" in
  e1|e2|e3|e4|e5|e8) "$1" 2>&1 | tee "$RESULTS/$1.txt" ;;
  all) for e in e1 e2 e3 e4 e5 e8; do "$e" 2>&1 | tee "$RESULTS/$e.txt"; done ;;
  *) echo "usage: bench/run.sh e1|e2|e3|e4|e5|e8|all"; exit 1 ;;
esac
