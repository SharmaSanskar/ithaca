# ADR-0004: k3d for the local and CI cluster, not kind or minikube

**Status:** accepted · **Date:** 2026-11-26 · **Phase:** 8

## Context

The manifests need a real cluster to be tested against: probes, rollouts, PodDisruptionBudgets,
NetworkPolicies and, in Phase 9, an autoscaler. The same cluster must start on a laptop and on
a GitHub Actions runner in well under a minute, or nobody runs the checks.

The candidates were kind (Kubernetes in Docker, upstream kubeadm), minikube (a VM or container
driver), and k3d (k3s in Docker).

## Decision

k3d, from a checked-in config (`deploy/k8s/k3d.yaml`): one server, two agents, a standalone
registry, and the Service CIDR pinned.

## Consequences

**Good.**

- **NetworkPolicies are enforced out of the box.** k3s embeds a policy controller, so default-deny
  actually denies, and `scripts/k8s-netpol-check.sh` proves it on every CI run. Elsewhere,
  enforcement depends on which CNI the cluster runs, and historically kind's default one did not
  enforce at all. A policy the CNI ignores applies cleanly and does nothing, which is worse than
  having none, because everything looks protected.
- **Ingress and a load balancer are built in.** Traefik plus k3s's service load balancer map
  host port 8088 to the ingress with one config line, with no ingress-nginx install to wait on.
- **A registry in the config.** Images are pushed and pulled by digest exactly as in a real
  cluster, instead of `kind load docker-image` side-loading, which skips the registry path
  entirely.
- **Fast.** The three-node cluster is ready in about 30 seconds, so CI creates a fresh one per
  run.

**Bad, and accepted.**

- k3s is not upstream kubeadm: one binary, SQLite instead of etcd by default, and Traefik
  rather than whatever a managed cluster uses. Nothing in `deploy/k8s/base` depends on any of
  that. One k3s-specific detail did leak into a decision: Traefik ranks routes by the length of
  the rule string, which is why `/ws` is a Prefix path. The fix is portable.
- The control-plane components (scheduler, controller manager) aren't separately scrapeable, so
  those kube-prometheus-stack targets are disabled locally.
