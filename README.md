<div align="center">

# Ithaca

**A webhook and event delivery platform.**
Ingest events, deliver them reliably, and watch the whole pipeline move in real time.

`Go 1.26` · `Postgres 17` · `Kubernetes` · `KEDA` · `Prometheus` · `OpenTelemetry` · `React 19`

</div>

---

Customers register HTTPS endpoints. Ithaca accepts their events, durably queues them, and delivers
them with exponential backoff, optional per-key ordering, idempotency, `Retry-After`-aware
throttling, circuit breaking, dead-lettering and replay — behind a dashboard where an event is
visible moving from ingest through the queue to the subscriber, or down into the dead-letter queue.

There is no CRUD filler in this project. Every component is a distributed-systems concern, because
the product cannot exist without one.

## Status

**Phase 4 — The live layer.** Ingest and the dispatcher stream what they do (post-commit, never
blocking) through Redis to a WebSocket hub, which sends every dashboard a 10Hz frame of exact
counts plus lossless entity deltas reconciled from Postgres. A self-running demo drives the real
system through five narrated scenarios: happy path, endpoint down, slow consumer, bad deploy, and
recovery with replay.

| Phase | | |
|---|---|---|
| 0 | Foundation: skeleton, containers, CI, design system | done |
| 1 | Data model + ingest | done |
| 2 | Queue, bounded worker pool, delivery | done |
| 3 | DLQ replay, circuit breaker, endpoint verification | done |
| 4 | Live layer: WebSocket hub + demo scenario engine | **done** |
| 5 | Dashboard | next |
| 6 | Benchmarks + Grafana dashboards | |
| 7 | Ship: public demo + write-up | |
| 8 | Kubernetes (k3d + Kustomize) | |
| 9 | KEDA autoscaling on queue depth | |

## Quick start

```bash
make up                  # containers: postgres, redis, prometheus, grafana, jaeger
make migrate             # apply the schema
make tenant NAME=acme    # create a tenant; prints its API key (shown once)
make dev                 # all six services + the dashboard, in the background
make ps                  # what is running, and where
```

Send an event:

```bash
curl -i localhost:8081/v1/events \
  -H "Authorization: Bearer $ITHACA_API_KEY" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: order-42-created" \
  -d '{"type":"order.created","ordering_key":"order_42","payload":{"total":99.5}}'
```

Endpoints and subscriptions are managed at `localhost:8080/api/v1/{endpoints,subscriptions}` with
the same key.

Or run the whole self-driving demo:

```bash
make up && make demo            # seed the demo tenant, start everything in demo mode
curl -X POST localhost:8080/api/v1/demo/scenarios/endpoint_down
# live feed: ws://localhost:8082/ws  →  {"op":"subscribe","ch":"flow"} and {"op":"subscribe","ch":"demo"}
```

| | |
|---|---|
| Dashboard | <http://localhost:5173/design> |
| API · ingest · ws · receiver-sim | :8080 · :8081 · :8082 · :8090 |
| Health / metrics (per service) | <http://localhost:9080/readyz> · <http://localhost:9080/metrics> |
| Grafana | <http://localhost:3000> |
| Prometheus targets | <http://localhost:9090/targets> |
| Jaeger | <http://localhost:16686> |

Shutting down:

```bash
make stop        # everything: services, dashboard, containers
make dev-stop    # just the services, leave the containers up
make down        # just the containers, keep the data
make nuke        # containers and volumes — wipes the database
```

Other useful ones:

```bash
make tail                  # follow every service log
make tail SVC=dispatcher   # follow one
make run  SVC=dispatcher   # run one in the foreground instead
make dev-restart           # rebuild and restart
```

`make help` lists everything. `make check` runs exactly what CI runs.

## Architecture

One Go module, one container image, a subcommand per service:

```
                       ┌──────────────┐
   POST /v1/events ───▶│    ingest    │──┐   202 in <10ms, zero outbound calls
                       └──────────────┘  │
                                         ▼
                              ┌─────────────────────┐
                              │      Postgres       │  system of record
                              │  streams · queue    │  AND scheduler
                              └──────────┬──────────┘
                                         │  lease a STREAM, not a message
                                         ▼
                       ┌──────────────────────────────┐
                       │         dispatcher           │──▶ subscriber endpoint
                       │  bounded pool · backpressure │◀── retry / breaker / DLQ
                       └──────────────┬───────────────┘
                                      │ publish
                                      ▼
                       ┌───────┐   ┌──────┐   ┌─────────────────┐
                       │ redis │──▶│  ws  │──▶│ dashboard (SPA) │
                       └───────┘   └──────┘   └─────────────────┘
```

| Service | Responsibility | Scales on |
|---|---|---|
| `ingest` | Accept and durably record events. Makes no outbound calls, so its availability never depends on a subscriber's health. | request rate |
| `dispatcher` | Lease streams, deliver, retry, break circuits, ack. | **queue depth (KEDA)** |
| `api` | Control plane, and serves the embedded dashboard. | CPU |
| `ws` | WebSocket hub for the live feed. Stateful, so it deploys separately. | connections |
| `worker` | Idempotency-key janitor, lease reaper, DLQ replay. | fixed |

### The four decisions that matter

**Lease streams, not messages.** Every delivery belongs to a lane keyed by `(subscription,
ordering_key)`. A lane is held by at most one worker at a time, so ordering falls out of the lease
rather than out of the query — `SKIP LOCKED` with `ORDER BY seq` *looks* ordered and is not, because
skipping a locked row is exactly what reorders it. Unordered subscriptions are the same code path
over a synthetic lane key, so there is one dequeue path and one ack path, not two.

**Backpressure means acquiring capacity before dequeuing.** The dispatcher takes worker slots first
and then leases exactly that many streams. Dequeuing first and looking for somewhere to put the work
converts a queue-depth problem into a memory problem, and takes out leases that expire unserviced.

**The queue table holds only pending work.** Terminal deliveries are moved out of the hot table in
the same statement that terminates them, so the queue stays proportional to backlog rather than to
lifetime volume, and a healthy system's queue fits entirely in memory.

**Rate limits and open circuits are scheduling decisions, not failures.** A throttled endpoint
re-dates its stream and frees the worker slot immediately — no HTTP request, no consumed retry
attempt, no wasted lease.

Full design documents: [`docs/design/delivery-engine.md`](docs/design/delivery-engine.md) and
[`docs/design/dashboard.md`](docs/design/dashboard.md).
Decision records: [`docs/DECISIONS/`](docs/DECISIONS/).

## Guarantees

Ithaca is **at-least-once**, and says so. Exactly-once delivery over HTTP would require a
distributed transaction with the subscriber, which does not exist — so instead every delivery
carries a stable `Ithaca-Id` and the docs enumerate precisely where duplicates arise.

- **No loss.** After quiescence, every (event × subscription) pair has reached a terminal state.
- **No phantom acks.** Nothing is marked delivered that the subscriber did not receive.
- **No reordering.** Within an ordering key, the sequence a subscriber observes is monotonic.
- **Ingest never blocks.** A total subscriber outage does not affect event acceptance.

These are enforced as tests, not prose.

## Repository layout

```
cmd/ithaca/          one entrypoint, a subcommand per service
internal/
  platform/          config, logging, health, shutdown, metrics, http
  ingest/            POST /v1/events: idempotency, fanout, per-stream seq
  api/               control plane: endpoints, subscriptions
  auth/              API keys: prefix lookup, SHA-256, constant-time compare
  store/             pgx pool, sqlc queries (store/queries) and generated code
  queue/             the driver boundary; queue/pg is the Postgres driver
  worker/            bounded pool, slots, heartbeat, drain, executor, reaper
  deliver/           HTTP client, HMAC signing, SSRF dialer, classification
  retry/             full-jitter backoff (a pure function)
  breaker/           circuit-breaker state machine (a pure function)
  dlq/               dead-letter replay
  verify/            endpoint verification handshake
  live/              live feed: publisher, Redis bus, WebSocket hub
  demo/              the self-running demo: seed, traffic, narrated scenarios
  receiver/          programmable subscriber for tests and the receiver-sim
  web/               the dashboard, compiled in with embed.FS
web/                 dashboard source (Vite + React 19 + Tailwind v4)
migrations/          goose, forward-only
deploy/compose/      local dependency stack
deploy/k8s/          Kustomize bases and overlays
bench/               k6 scripts and the benchmark report
docs/                architecture, design docs, ADRs, runbook
```

## Development

```bash
make check              # fmt, vet, lint, test — the same gates CI runs
make test               # unit tests with the race detector
make test-integration   # testcontainers against real Postgres (needs Docker)
make generate           # regenerate sqlc code after editing a query or migration
make build-full         # binary with the dashboard embedded
make docker             # container image
```

Services run on the host rather than in compose, so a Go rebuild is two seconds
instead of an image rebuild. `scripts/dev.sh` manages the resulting background
processes; `make stop` is always enough to get back to zero.

The dashboard is compiled into the Go binary, so production is one binary, one image, one
Deployment, one Ingress — with same-origin cookies and WebSockets and no CORS anywhere. In
development Vite proxies `/api` and `/ws` to the Go process so the two setups behave identically.

`go build ./...` works without ever running npm; the `embedweb` build tag is what pulls the
dashboard in.

## License

MIT
