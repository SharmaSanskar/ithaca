#!/usr/bin/env bash
#
# Build, push, and deploy Ithaca to the local k3d cluster BY DIGEST, then wait
# for every rollout. Used by `make k8s-deploy` and by the e2e-k3d CI job, so
# the laptop and CI deploy the same way.
#
#   scripts/k8s-deploy.sh            build the image first
#   SKIP_BUILD=1 scripts/k8s-deploy.sh   reuse the local ithaca:dev image
#
# Why a digest: a tag is a pointer anyone can move. `ithaca:latest` (or even
# `ithaca:v1.2.0`) re-pushed mid-rollout means two pods of "the same version"
# running different code. A digest is the content itself.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

PUSH_REPO="localhost:5001/ithaca"                     # the registry, from the host
PULL_REPO="ithaca-registry.localhost:5001/ithaca"     # the same registry, from the nodes
TAG="$(git rev-parse --short HEAD)$(git diff --quiet || echo -dirty)"
OUT="tmp/k8s"

log() { printf '\033[1m▸ %s\033[0m\n' "$*"; }

if [ "${SKIP_BUILD:-0}" != 1 ]; then
  log "building image"
  make docker >/dev/null
fi

log "pushing $PUSH_REPO:$TAG"
docker tag ithaca:dev "$PUSH_REPO:$TAG"
docker push --quiet "$PUSH_REPO:$TAG" >/dev/null
DIGEST="$(docker inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$PUSH_REPO:$TAG" \
          | grep "^$PUSH_REPO@" | head -1 | cut -d@ -f2)"
[ -n "$DIGEST" ] || { echo "could not read the pushed digest" >&2; exit 1; }
log "pinned to $DIGEST"

# A throwaway kustomization on top of the committed overlay, so the digest
# never lands in git and the overlay stays environment-neutral.
mkdir -p "$OUT"
cat >"$OUT/kustomization.yaml" <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../deploy/k8s/overlays/local
images:
  - name: $PULL_REPO
    digest: $DIGEST
EOF

log "applying"
kustomize build "$OUT" | kubectl apply --server-side --force-conflicts -f - >/dev/null

log "waiting for rollouts"
kubectl -n ithaca rollout status statefulset/postgres --timeout=180s
for d in redis receiver-sim api ingest ws dispatcher worker; do
  kubectl -n ithaca rollout status "deployment/$d" --timeout=180s
done

# Every pod must be running the digest we just pushed, not some cached tag.
wrong=$(kubectl -n ithaca get pods -l app.kubernetes.io/part-of=ithaca \
          -o jsonpath='{range .items[*]}{range .spec.containers[*]}{.image}{"\n"}{end}{end}' \
        | grep "ithaca" | grep -vc "@$DIGEST" || true)
[ "$wrong" = 0 ] || { echo "$wrong container(s) not running $DIGEST" >&2; exit 1; }

log "deployed $TAG ($DIGEST) — http://ithaca.localhost:8088"
