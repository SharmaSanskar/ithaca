# ADR-0001: One binary with subcommands, not five binaries

**Status:** accepted · **Date:** 2026-07-31 · **Phase:** 0

## Context

Ithaca is five processes with genuinely different scaling characteristics: an ingest gateway that
scales on request rate, a dispatcher that scales on queue depth, a control-plane API, a stateful
WebSocket hub, and a periodic worker. They are separate *deployments* for good reasons — an endpoint
that times out for 60 seconds must not be able to consume ingest capacity, and a control-plane
deploy must not drop every dashboard's WebSocket.

The question is whether that means five build targets and five images.

## Decision

One Go module, one `cmd/ithaca` entrypoint, one container image. The service is selected by
subcommand:

```
ithaca ingest | dispatcher | api | ws | worker | receiver-sim
```

Kubernetes Deployments differ only in `args` and their resource requests.

## Consequences

**Good.**

- The CI matrix halves. One build, one scan, one push, one set of cache layers.
- Version skew becomes impossible. The dispatcher and the ingest gateway share a database schema;
  with separate images it is entirely possible to deploy a dispatcher from a commit that predates
  the migration the gateway depends on. With one image and one tag, a rollback rolls back
  everything or nothing.
- Shared platform code — config, logging, health, shutdown — has one obvious home and no temptation
  toward a `common` module with its own release cycle.
- Local development is `go run ./cmd/ithaca <service>`, which means the whole system runs on a
  laptop from six terminals with no orchestration.

**Bad, and accepted.**

- The image contains code the running service does not use. Irrelevant here: the binary is ~30MB on
  a distroless base, and the alternative is five images that each contain most of the same code
  anyway.
- A change to the dispatcher rebuilds and redeploys an image the ingest gateway also uses. This
  looks like coupling but is not: the *rollout* is still independent per Deployment, and rolling
  every service onto a new image is the behaviour we want given they share a schema.
- Startup must dispatch on `os.Args[1]`, so a typo produces a runtime error rather than a
  nonexistent binary. Mitigated by validating the subcommand against a known list and printing
  usage.

## Alternatives considered

**Five `cmd/` directories.** The conventional Go layout. Rejected because it buys nothing here — the
services share a module, a schema and a release cadence — while costing five entries in every build,
scan, tag and deploy path.

**A monolith with everything in one process.** Rejected outright: the entire point of the
architecture is that a slow subscriber cannot starve event acceptance, and that delivery workers
scale on a different signal than HTTP traffic. One process makes both impossible.
