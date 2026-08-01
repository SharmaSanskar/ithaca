# ADR-0002: A Vite SPA compiled into the Go binary, not Next.js

**Status:** accepted · **Date:** 2026-07-31 · **Phase:** 0

## Context

The dashboard is the part of this project that makes invisible distributed-systems work visible: a
live pipeline canvas, a high-cardinality event table, and a demo console. It needs to be good.

The default reach would be Next.js — it is what I have used for every recent project, and it is the
more recognisable name. The question is whether it earns its place *here*.

## Decision

**Vite + React 19 + React Router 7, built to `internal/web/dist` and compiled into the `api` binary
with `embed.FS`** behind an `embedweb` build tag.

## Rationale

**Next.js earns its keep through the server.** RSC, streaming, ISR, `next/image`, middleware — all
of it is about rendering on a server. Ithaca has zero server-renderable content. Every screen is a
live view behind a login, changing ten times a second over a WebSocket. Running a Node process to
server-render a shell that WebSocket data immediately replaces is paying the full price for none of
the benefit.

**The Kubernetes argument is decisive.** Next-with-Node means a second image, a second Deployment, a
second Service, a second readiness probe, a second thing to roll back, and either CORS or a reverse
proxy for every API call. The embedded build is one binary, one image, one Deployment, one Ingress —
and because the SPA and the API share an origin, the session cookie and the WebSocket handshake work
with no CORS configuration and no token in JavaScript.

This is how Grafana, Prometheus and Traefik ship their UIs, for the same reasons.

**The migration cost is about forty lines.** `next/font` becomes a self-hosted `@fontsource`
import. `next-themes` becomes a thirty-line provider plus a blocking script in `index.html` — which
is *better*, because it eliminates the theme flash rather than merely managing it. `next/link`
becomes React Router's `Link`.

## Consequences

**Good.**

- One deployable artifact. `docker run ithaca api` serves the API and the dashboard.
- Same-origin cookies (`HttpOnly`, `SameSite=Lax`) with no token exposed to JavaScript, and a
  WebSocket handshake that is authenticated for free. This is a real security improvement over
  passing a token in a query string, where it lands in every proxy and access log.
- No CORS configuration anywhere, in any environment.
- Development is identical to production: Vite proxies `/api` and `/ws` to the Go process, so the
  browser is on one origin in both.

**Bad, and accepted.**

- The word "Next.js" does not appear in the project. Judged a poor trade against being able to
  explain *why* the SPA is embedded, which is a better signal than the framework name.
- No SSR, so the first paint is a shell. Irrelevant behind a login with no SEO surface.
- The build has an ordering constraint: npm before Go. Handled by the `embedweb` tag, so
  `go build ./...` still works on a checkout where npm has never run, and by a committed `.gitkeep`
  that keeps the `//go:embed` directive compiling on a clean tree.

## Alternatives considered

**Next.js as a separate Deployment.** The familiar option. Rejected on the Kubernetes surface area
above.

**Serve the SPA from a CDN / S3 + CloudFront.** Correct for a high-traffic marketing site, wrong
here: it reintroduces cross-origin auth, adds an invalidation step to every deploy, and adds a
service to the demo-week bill for a dashboard with a handful of users.
