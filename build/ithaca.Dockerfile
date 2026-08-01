# syntax=docker/dockerfile:1.10
#
# One image for every Ithaca service. The entrypoint takes a subcommand:
#
#   docker run ithaca:dev ingest
#   docker run ithaca:dev dispatcher
#
# Shipping one image instead of five halves the CI matrix and makes it
# impossible to run a dispatcher built from a different commit than the ingest
# gateway it shares a database schema with.

ARG GO_VERSION=1.25
ARG NODE_VERSION=22
ARG ALPINE_VERSION=3.21

# ── stage 1: dashboard ───────────────────────────────────────────────────────
# Built first and separately so a Go-only change does not invalidate the npm
# layers, and vice versa.
FROM --platform=$BUILDPLATFORM node:${NODE_VERSION}-alpine AS web

WORKDIR /web

# package*.json alone first: dependencies only re-install when they change.
COPY web/package.json web/package-lock.json* ./
RUN --mount=type=cache,target=/root/.npm \
    npm ci --no-audit --no-fund

COPY web/ ./
# Vite writes to ../internal/web/dist (see vite.config.ts), so create the
# parent the config expects.
RUN mkdir -p /internal/web && npm run build

# ── stage 2: compile ─────────────────────────────────────────────────────────
# BUILDPLATFORM keeps the compiler running natively even when cross-compiling
# for arm64/amd64 — QEMU-emulating the Go toolchain is roughly 10x slower.
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS build

WORKDIR /src

RUN apk add --no-cache git ca-certificates tzdata

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
COPY --from=web /internal/web/dist ./internal/web/dist

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

# CGO_ENABLED=0 gives a static binary, which is what makes the distroless
# `static` base (no libc at all) viable.
#
# -trimpath strips local filesystem paths from the binary: without it, panic
# stack traces published in an issue tell the world your home directory layout.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build \
      -tags embedweb \
      -trimpath \
      -ldflags="-s -w \
        -X github.com/SharmaSanskar/ithaca/internal/platform/version.Version=${VERSION} \
        -X github.com/SharmaSanskar/ithaca/internal/platform/version.Commit=${COMMIT} \
        -X github.com/SharmaSanskar/ithaca/internal/platform/version.Date=${BUILD_DATE}" \
      -o /out/ithaca ./cmd/ithaca

# ── stage 3: runtime ─────────────────────────────────────────────────────────
# distroless/static: no shell, no package manager, no libc. Nothing for an
# attacker who achieves RCE to pivot with, and nothing for a CVE scanner to
# flag that we cannot patch.
FROM gcr.io/distroless/static-debian12:nonroot AS runtime

COPY --from=build /out/ithaca /ithaca

# `nonroot` is uid 65532. Declared here as well as in the base so the
# Kubernetes securityContext and the image agree.
USER 65532:65532

# Documentation only; the actual ports come from configuration.
#   8080 api · 8081 ingest · 8082 ws · 8090 receiver-sim
#   9080-9085 admin (probes, metrics, pprof)
EXPOSE 8080 8081 8082 8090 9080 9081 9082 9083 9084 9085

ENTRYPOINT ["/ithaca"]
CMD ["api"]
