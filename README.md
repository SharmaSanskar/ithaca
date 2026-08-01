<div align="center">

# Ithaca

**A webhook and event delivery platform.**
Ingest events, deliver them reliably, and watch the whole pipeline move in real time.

`Go 1.25` · `Postgres 17` · `Redis` · `Kubernetes` · `Terraform` · `React 19`

</div>

---

Customers register HTTPS endpoints. Ithaca accepts their events, durably queues them, and delivers
them with exponential backoff, optional per-key ordering, idempotency, per-endpoint rate limiting,
circuit breaking, dead-lettering and replay — behind a dashboard where an event is visible moving
from ingest through the queue to the subscriber, or down into the dead-letter queue.

There is no CRUD filler in this project. Every component is a distributed-systems concern, because
the product cannot exist without one.

## Status

**Phase 0 — Foundation.** The service skeleton, container build, dependency stack, CI and design
system are in place. The delivery engine arrives in Phase 2.

| Phase | | |
|---|---|---|
| 0 | Foundation: skeleton, containers, CI, design system | **done** |
| 1 | Data model + ingest | next |
| 2 | Queue, bounded worker pool, delivery | |
| 3 | Retries, DLQ, breaker, rate limits, endpoint verification | |
| 4 | Control plane + WebSocket hub | |
| 5 | Dashboard | |
| 6 | Demo console + benchmarks | |
| 7–8 | Kubernetes + KEDA autoscaling | |
| 9 | Terraform, SQS driver, AWS demo week | |
| 10 | Permanent deploy + write-up | |

## Quick start

```bash
make up      # containers: postgres, redis, prometheus, grafana, jaeger
make dev     # all six services + the dashboard, in the background
make ps      # what is running, and where
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
| `worker` | Fanout expansion, lease reaper, partition management, DLQ replay. | fixed |

### The four decisions that matter

**Lease streams, not messages.** Every delivery belongs to a lane keyed by `(subscription,
ordering_key)`. A lane is held by at most one worker at a time, so ordering falls out of the lease
rather than out of the query — `SKIP LOCKED` with `ORDER BY seq` *looks* ordered and is not, because
skipping a locked row is exactly what reorders it. Unordered subscriptions are the same code path
over a synthetic lane key, so there is one dequeue path and one ack path, not two.

**Backpressure means acquiring capacity before dequeuing.** The dispatcher takes worker slots first
and then leases exactly that many streams. Dequeuing first and looking for somewhere to put the work
converts a queue-depth problem into a memory problem, and takes out leases that expire unserviced.

**Nothing is deleted row by row.** Terminal deliveries are moved out of the hot table in the same
statement that terminates them, so the queue stays proportional to backlog rather than to lifetime
volume. History, events and attempts are range-partitioned by time; retention is `DROP TABLE`.

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
  queue/             the driver boundary: Postgres and SQS FIFO
  worker/            bounded pool, slots, heartbeat, drain
  deliver/           HTTP client, HMAC signing, SSRF dialer, classification
  web/               the dashboard, compiled in with embed.FS
web/                 dashboard source (Vite + React 19 + Tailwind v4)
migrations/          goose, forward-only
deploy/compose/      local dependency stack
deploy/k8s/          Kustomize bases and overlays
infra/terraform/     AWS: VPC, EKS, RDS, ElastiCache, SQS
bench/               k6 scripts and the benchmark report
docs/                architecture, design docs, ADRs, runbook
```

## Development

```bash
make check              # fmt, vet, lint, test — the same gates CI runs
make test               # unit tests with the race detector
make test-integration   # testcontainers against real Postgres
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
