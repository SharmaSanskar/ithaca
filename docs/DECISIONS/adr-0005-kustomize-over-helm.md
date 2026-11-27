# ADR-0005: Kustomize for Ithaca's manifests; Helm only for third-party charts

**Status:** accepted · **Date:** 2026-11-26 · **Phase:** 8

## Context

Ithaca needs one set of manifests with per-environment differences: where Postgres is, whether
the demo is on, the hostname, the image digest. Helm and Kustomize both solve that, in
different ways. Helm renders text templates from values. Kustomize patches plain YAML in layers.

## Decision

The application is plain YAML in `deploy/k8s/base`, with environment differences as Kustomize
overlays and optional pieces as components (`local-deps`, `autoscaling`). Third-party software
we don't own (kube-prometheus-stack, KEDA) is installed from its upstream Helm chart, with the
version pinned in the Makefile.

## Consequences

**Good.**

- **What is reviewed is what is applied.** Every manifest in base is valid Kubernetes YAML that
  can be read top to bottom. There is no `{{- if .Values.x }}` logic to evaluate in your head, and
  `kustomize build` output is diffable in a PR.
- **Optional features are additive.** The autoscaling component adds a ScaledObject, a policy
  and a config value, and *removes* `replicas` from the dispatcher (the HPA owns it). Turning
  autoscaling off means removing one line from an overlay, not threading a flag through every
  template.
- **Generated config rolls pods.** `configMapGenerator` and `secretGenerator` hash their contents
  into the name, so a config change replaces every pod that reads it. Helm needs a checksum
  annotation convention to get the same effect.
- **The digest never lands in git.** The deploy script stacks a throwaway kustomization (just an
  `images:` digest) on the committed overlay.

**Bad, and accepted.**

- No packaging or release versioning for the manifests themselves. Ithaca isn't distributed for
  others to install, so there's nobody to package for. If that changed, a chart would wrap the
  same base.
- Kustomize only rewrites references in the kinds it knows. The KEDA TriggerAuthentication's
  Secret reference needed a small `configurations:` entry (`components/autoscaling/nameref.yaml`),
  or it would point at the unhashed name and silently fail.
- Sharing the Grafana dashboard file with the compose stack needs
  `--load-restrictor LoadRestrictionsNone`, because the file lives outside the kustomization's
  directory. That's preferable to keeping two copies of a 27-panel dashboard in sync.
