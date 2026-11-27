#!/usr/bin/env python3
"""Generate the Ithaca Grafana dashboard (dashboards as code).

    python3 deploy/compose/grafana/dashboards.py   # or: make dashboards

Writes grafana/provisioning/dashboards/json/ithaca.json, which Grafana loads
at startup. Edit THIS file, not the JSON: 1,500 lines of hand-edited panel
JSON do not stay correct.

Layout follows the pipeline, left to right and top to bottom:
ingest → queue → delivery → pool & reliability → live layer.
"""
import json
import pathlib

DS = {"type": "prometheus", "uid": "prometheus"}
panels = []
_id = 0
y = 0


def pid():
    global _id
    _id += 1
    return _id


def row(title):
    global y
    panels.append({"type": "row", "title": title, "id": pid(), "collapsed": False,
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}})
    y += 1


def ts(title, targets, x, w=8, h=8, unit="short", desc="", thresholds=None, stack=False):
    p = {
        "type": "timeseries", "title": title, "id": pid(), "datasource": DS, "description": desc,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        # range=True is required: without it Grafana runs an INSTANT query,
        # gets one point per series, and a single point draws no line.
        "targets": [{"refId": chr(65 + i), "expr": e, "legendFormat": l, "datasource": DS,
                     "range": True, "instant": False, "editorMode": "code"} for i, (e, l) in enumerate(targets)],
        "fieldConfig": {"defaults": {"unit": unit, "custom": {"lineWidth": 1, "fillOpacity": 10 if stack else 0,
                                                               "stacking": {"mode": "normal" if stack else "none"}}},
                        "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
    }
    if thresholds:
        p["fieldConfig"]["defaults"]["thresholds"] = {"mode": "absolute", "steps": [{"color": "green", "value": None}] +
                                                     [{"color": c, "value": v} for v, c in thresholds]}
        p["fieldConfig"]["defaults"]["custom"]["thresholdsStyle"] = {"mode": "line"}
    panels.append(p)


def stat(title, expr, x, unit="short", w=4, desc=""):
    panels.append({
        "type": "stat", "title": title, "id": pid(), "datasource": DS, "description": desc,
        "gridPos": {"h": 4, "w": w, "x": x, "y": y},
        "targets": [{"refId": "A", "expr": expr, "datasource": DS, "range": True, "instant": False, "editorMode": "code"}],
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "graphMode": "area", "colorMode": "none"},
    })


def q(quantile, metric, by="", extra=""):
    sel = "{" + extra + "}" if extra else ""
    return f"histogram_quantile({quantile}, sum by (le{by}) (rate({metric}_bucket{sel}[1m])))"


ING = 'listener="ingest",route="/v1/events"'

# ── headline ─────────────────────────────────────────────────────────────────
stat("Ingest", 'sum(rate(ithaca_ingest_events_total{result="accepted"}[1m]))', 0, "reqps")
stat("Delivered", 'sum(rate(ithaca_delivery_completed_total{final_state="succeeded"}[1m]))', 4, "reqps")
stat("Backlog", "max(ithaca_queue_pending_deliveries)", 8)
stat("Oldest pending", "max(ithaca_queue_oldest_pending_seconds)", 12, "s",
     desc="The single best health signal: throughput can look fine while this climbs.")
stat("Dead-lettered (1h)", 'sum(increase(ithaca_delivery_completed_total{final_state="dead"}[1h]))', 16)
stat("Pool busy", "sum(ithaca_pool_slots_in_flight) / sum(ithaca_pool_slots_capacity)", 20, "percentunit")
y += 4

row("Ingest")
ts("Accepted events / s", [('sum by (result) (rate(ithaca_ingest_events_total[1m]))', "{{result}}"),
                           ('sum(rate(ithaca_ingest_deliveries_enqueued_total[1m]))', "deliveries enqueued (fanout)")], 0, unit="reqps")
ts("Ingest latency", [(q(0.5, "ithaca_http_request_duration_seconds", extra=ING), "p50"),
                      (q(0.99, "ithaca_http_request_duration_seconds", extra=ING), "p99")], 8, unit="s",
   desc="POST /v1/events end to end at the server: auth, idempotency claim, event insert, fanout, commit.",
   thresholds=[(0.05, "orange")])
# One unit per panel, always: mixing a rate and a duration on one axis
# plots "deliveries/s" as "41.7 mins".
ts("Ingest transaction", [(q(0.5, "ithaca_ingest_tx_duration_seconds"), "p50"),
                          (q(0.99, "ithaca_ingest_tx_duration_seconds"), "p99")], 16, unit="s",
   desc="The single transaction: idempotency claim + event insert + fanout + durable commit.")
y += 8

row("Queue")
ts("Backlog", [("max(ithaca_queue_pending_deliveries)", "pending deliveries"),
               ("max(ithaca_queue_runnable_streams)", "runnable streams"),
               ("max(ithaca_queue_leased_streams)", "leased streams")], 0)
ts("Oldest pending (s)", [("max(ithaca_queue_oldest_pending_seconds)", "oldest due delivery")], 8, unit="s",
   thresholds=[(60, "orange"), (300, "red")])
ts("Queue wait", [(q(0.5, "ithaca_delivery_queue_wait_seconds"), "p50"), (q(0.99, "ithaca_delivery_queue_wait_seconds"), "p99")],
   16, unit="s", desc="Ingest → first attempt starts. The time an event spends queued.")
y += 8

row("Delivery")
ts("Attempts by outcome / s", [('sum by (outcome) (rate(ithaca_delivery_attempts_total[1m]))', "{{outcome}}")], 0, unit="reqps", stack=True)
ts("Attempt latency (subscriber)", [(q(0.5, "ithaca_delivery_attempt_duration_seconds"), "p50"),
                                    (q(0.99, "ithaca_delivery_attempt_duration_seconds"), "p99")], 8, unit="s")
ts("End to end (ingest → delivered)", [(q(0.5, "ithaca_delivery_end_to_end_seconds"), "p50"),
                                       (q(0.99, "ithaca_delivery_end_to_end_seconds"), "p99")], 16, unit="s")
y += 8

row("Worker pool & reliability")
ts("Worker slots", [("sum(ithaca_pool_slots_in_flight)", "in flight"), ("sum(ithaca_pool_slots_capacity)", "capacity")], 0, w=6,
   desc="Backpressure: the dispatcher leases only as many streams as it has free slots.")
ts("Circuit breaker", [('sum by (transition) (increase(ithaca_breaker_transitions_total[5m]))', "{{transition}}"),
                       ("sum(increase(ithaca_breaker_probes_started_total[5m]))", "probes")], 6, w=6)
ts("Leases", [("sum(increase(ithaca_queue_leases_reaped_total[5m]))", "reaped (a worker died)"),
              ("sum(increase(ithaca_queue_leases_lost_total[5m]))", "lost (fenced out)")], 12, w=6,
   desc="Nonzero reaps in steady state mean workers are dying or leases are too short.")
ts("Ack transaction", [(q(0.5, "ithaca_queue_ack_duration_seconds"), "p50"),
                       (q(0.99, "ithaca_queue_ack_duration_seconds"), "p99")], 18, w=6, unit="s",
   desc="The fenced ack: attempts insert + move to history + reschedule, in one transaction.")
y += 8

row("Live layer")
ts("WebSocket clients", [("sum(ithaca_ws_connections)", "connections")], 0)
ts("Frames", [("sum(rate(ithaca_ws_frames_offered_total[1m]))", "offered/s"),
              ("sum(rate(ithaca_ws_frames_superseded_total[1m]))", "superseded/s (slow clients)")], 8)
ts("Live bus", [("sum by (kind) (rate(ithaca_live_messages_received_total[1m]))", "{{kind}}"),
                ("sum(rate(ithaca_live_publish_dropped_total[1m]))", "dropped (bus slow/down)")], 16)
y += 8

dash = {
    "uid": "ithaca-overview", "title": "Ithaca · overview", "tags": ["ithaca"], "timezone": "browser",
    "schemaVersion": 39, "version": 1, "refresh": "5s", "time": {"from": "now-15m", "to": "now"},
    "panels": panels, "templating": {"list": []}, "annotations": {"list": []},
}
out = pathlib.Path(__file__).parent / "provisioning/dashboards/json/ithaca.json"
out.write_text(json.dumps(dash, indent=2) + "\n")
print(f"wrote {out} ({len(panels)} panels)")
