# Ithaca on Kubernetes

A local cluster that behaves like a real one: k3d (k3s in Docker) with one server and two agents,
a container registry, Traefik as the ingress controller, and NetworkPolicies that are actually
enforced. Kustomize, no Helm for the app.

```bash
make k8s-up           # create the cluster (≈30 s)
make k8s-deploy       # build, push, deploy BY DIGEST, wait for every rollout
open http://ithaca.localhost:8088

make k8s-monitoring   # optional: Prometheus + Grafana with Ithaca's dashboard and alerts
open http://grafana.localhost:8088

make k8s-check        # prove the policies are enforced, then restart everything under load
make k8s-burst        # the autoscaling demo: 60k events, dispatchers scale out and back
make k8s-down         # delete the cluster
```

The same `k8s-deploy` and `k8s-check` scripts run in CI on a throwaway cluster per change
([`e2e-k3d.yml`](../../.github/workflows/e2e-k3d.yml)).

## Layout

```
base/                    the app, cluster-agnostic: 6 Deployments, Services, Ingress, PDBs, NetworkPolicies
components/local-deps/   Postgres (StatefulSet) + Redis, for local and CI clusters only
components/autoscaling/  KEDA ScaledObject for the dispatchers (removes their fixed replica count)
overlays/local/          the k3d cluster: demo mode, hostname, pinned receiver ClusterIP, secrets
monitoring/              kube-prometheus-stack values, ServiceMonitor, alerts, dashboard
k3d.yaml                 the cluster itself
```

A real environment is another overlay that drops `local-deps`, points `ITHACA_DATABASE_URL` at
a managed Postgres, and sources `ithaca-secrets` from a secret manager. `base` doesn't change.

## The decisions

**Deployed by digest, never by tag.** `scripts/k8s-deploy.sh` pushes the image, reads back the
registry's digest, and pins the overlay to `ithaca@sha256:…` in a throwaway kustomization (the
digest never lands in git). Then it checks that every running container has exactly that digest.
A tag is a pointer anyone can move: re-push `:v1.2.0` mid-rollout and two pods of "the same
version" run different code.

**Three probes, three questions.** Startup (`/startupz`): has boot finished? Until it passes,
liveness isn't checked. Liveness (`/healthz`): is the *process* wedged? It never touches a
dependency, because a liveness probe that pings Postgres turns a 30-second database blip into
every pod restarting at once. Readiness (`/readyz`): should traffic come here? It checks Postgres
and Redis, and a failing pod leaves the Service instead of being killed.

**Shutdown is a drain, timed to the kubelet.** On SIGTERM a service fails readiness, keeps
serving for 5 s while endpoints update across the cluster, then drains within
`ITHACA_SHUTDOWN_GRACE` (30 s). `terminationGracePeriodSeconds: 45` exceeds the sum, so the
kubelet never SIGKILLs mid-drain. There is no preStop sleep: the binary does its own drain delay,
and the distroless image has no `sleep` to exec. Rollouts surge before they remove a pod
(`maxSurge: 1, maxUnavailable: 0`).

**Schema before code, per pod.** Each database client has an init container running
`ithaca migrate up`. Migrations are forward-only and take a Postgres advisory lock, so replicas
starting together serialize, and every pod after the first sees a no-op. No separate Job to
order against the Deployments.

**Zero trust inside the namespace.** Default-deny for ingress *and* egress, then one policy per
real flow: the ingress controller to the public ports, the monitoring namespace to the admin
ports, database clients to Postgres and Redis, the dispatcher to the internet on 80/443 and to
the demo receiver. The dispatcher's internet rule excludes every private, link-local and CGNAT
range. That is the application's SSRF guard enforced a second time, at the network layer, so
even a bypassed dial-time check can't reach `169.254.169.254`. `scripts/k8s-netpol-check.sh`
launches probe pods and asserts 11 allow/deny outcomes, because a policy the CNI ignores looks
identical to a working one in `kubectl get`.

**Locked-down pods.** The namespace enforces the `restricted` Pod Security Standard. Every
container runs as non-root with a read-only root filesystem, drops all capabilities, uses seccomp
`RuntimeDefault`, and gets no service-account token (nothing in Ithaca talks to the Kubernetes
API). That includes Postgres and Redis, which run as their images' own unprivileged users.

**Requests and memory limits, no CPU limits.** CFS throttling adds latency spikes to a service
whose job is latency, and an idle core is better lent than wasted. Memory is limited because
exceeding it should kill the pod, not the node. Each pod's database pool is capped
(`ITHACA_DB_MAX_CONNS=8`), because without a CPU limit Go sizes it from the node's cores, and a
dozen pods × 28 connections would exhaust Postgres.

**Disruption budgets and spreading.** Multi-replica services have a PodDisruptionBudget
(`maxUnavailable: 1`) and spread across nodes, so a `kubectl drain` or a cluster upgrade never
takes out both API replicas at once.

**Config changes roll pods.** The ConfigMap and Secret are generated, so their names carry a
hash of their contents. Change a setting and every pod that reads it is replaced, exactly as if
the image had changed.

## Autoscaling

KEDA scales the dispatchers on **worker demand**: streams being delivered plus streams due and
waiting, divided by 32 workers per pod. It counts no parked, paused or backing-off work, and no
raw queue depth. Why that and not CPU or depth, and the version that failed first, are in
[ADR-0006](../../docs/DECISIONS/adr-0006-keda-on-parallelizable-work.md).

`make k8s-burst` sends 60k events (120k deliveries) through the ingress at 400/s for 150 s,
with the ordered `orders-service` subscriber slowed to 1 s. An ordered key delivers one event at
a time, so this is the workload where more dispatchers genuinely help: 160 orders/s needs ~160
deliveries in flight. Same burst, same cluster:

| | dispatchers pinned at 2 | KEDA, 2 → 10 |
|---|---:|---:|
| peak backlog | 89,965 deliveries | **5,510** |
| fully drained | t = 411 s (261 s after the burst ended) | **t = 152 s (2 s after)** |
| scale-out | — | first scale-up at 17 s, 10 pods ready at 37 s |
| under sustained load | — | settled at 7 pods: the measured demand (6–7), no oscillation |
| scale-in | — | back to 2 by 228 s, in steps, no flapping |
| ingest | 100% accepted, p99 12 ms | 100% of sent accepted, p99 884 ms |

The ingest tail is the cost of the win: ten dispatchers share the one Postgres with ingest. (k6
also dropped 222 of 60,001 events in the autoscaled run while latency spiked.)

To record it: `make k8s-burst` in one terminal, `kubectl -n ithaca get pods -l
app.kubernetes.io/name=dispatcher -w` in a second, and http://ithaca.localhost:8088 beside them.

### What the burst found

- **Scaling on backlog oscillates.** The first scaler counted due deliveries. Once the pods caught
  up, the queue was short, so it said "1 pod" and scaled 9 → 3 mid-burst while the load
  continued. The backlog regrew to 6.4k and it scaled out again. Backlog measures work waiting;
  worker demand measures work waiting or in flight.
- **The subscriber ran out of memory first.** receiver-sim holds every request open for its
  configured latency. At 128 Mi it was OOM-killed mid-burst, which reset its in-memory sink
  settings and silently turned the slow consumer fast. The system handled it as designed: the
  breaker opened, the streams parked, the scaler correctly ignored parked work, and all
  129,900 deliveries in the window succeeded. But it invalidated the run. The limit is now
  512 Mi, and the burst script warns if the receiver restarts.
- **A saturated pool lets a slow consumer starve its neighbors.** Pinned at 2 pods, the 1 s
  `orders` streams held nearly every worker, and the 40 ms subscribers queued behind them. E4
  showed a *failing* neighbor costs nothing, but that was with spare capacity. Autoscaling hides
  this by adding workers. The real fix is fair leasing (a cap on how many workers one endpoint's
  streams may hold), which is not built.
- **One VM has a ceiling, and it is Postgres.** Past ~10 dispatchers, the single in-cluster
  Postgres (shared with ingest) saturated, ingest timed out, and the API server stalled with it.
  The local overlay caps the scaler at 10, the base allows 20, and the server node is tainted
  so workloads can't starve the control plane.

## Verified

| check | result |
|---|---|
| fresh cluster → every rollout complete | `make k8s-up && make k8s-deploy`, also in CI |
| NetworkPolicies enforced | 11 of 11 outcomes as expected (`k8s-netpol-check.sh`) |
| restart all six deployments under 100 events/s through the ingress | 9,001 of 9,001 accepted, 9,001 of 9,001 delivered, 0 dead (`k8s-rollout-check.sh`) |
| the gate catches a bad rollout strategy | with ingest switched to `Recreate`: 91.3% accepted → ✗ |
| monitoring | 10 of 10 pods scraped through the policies, 10 alert rules loaded, the compose dashboard renders unchanged |
| autoscaling | 2 → 10 → 2 under a 60k-event burst, drained 2.7× faster than pinned (above); a smaller burst gates CI |

## Gotchas found on the way

- **Traefik ranks routes by the length of the rule string**, not by path specificity. `/ws` as
  an `Exact` path (`Path(/ws)`, 11 characters) lost to the catch-all `PathPrefix(/)` (15), and
  every WebSocket got the dashboard's HTML. As a `Prefix` (`PathPrefix(/ws)`, 16) it wins.
- **The demo engine ingests in-process**, so the "Ingest" panels read 0 while deliveries flowed.
  The worker now registers the same ingest metrics as the gateway.
- **The KEDA operator lives in another namespace**, so the database URL's short hostname
  `postgres` didn't resolve for it. The URL is fully qualified now.
- **Local Postgres commits asynchronously.** Its volume shares the Docker disk with k3s's own
  datastore, and fsync there stretched every per-stream sequence lock into a queue (19 backends
  waiting on row locks at 800 events/s). The local cluster demonstrates scheduling and scaling;
  durability is measured in `bench/` with fsync on.
- **A Job is the wrong tool for an idempotent seed.** Its pod template can't change on
  re-apply. The demo seed runs as a second init container on the worker instead.
