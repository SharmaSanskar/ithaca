#!/usr/bin/env bash
#
# Prove the NetworkPolicies are ENFORCED, not merely applied: launch probe pods
# with chosen labels and check which connections succeed. A policy that the
# CNI silently ignores looks identical to a working one in `kubectl get`.
#
#   scripts/k8s-netpol-check.sh      exits non-zero on any unexpected result

set -euo pipefail
NS=ithaca
IMG=curlimages/curl:8.17.0
fail=0

# A pod that satisfies the namespace's `restricted` Pod Security profile.
probe_pod() { # probe_pod NAME LABELS
  kubectl -n "$NS" run "$1" --image="$IMG" --restart=Never --labels="$2" \
    --overrides='{"spec":{"securityContext":{"runAsNonRoot":true,"runAsUser":100,"seccompProfile":{"type":"RuntimeDefault"}},
      "containers":[{"name":"'"$1"'","image":"'"$IMG"'","command":["sleep","300"],
      "securityContext":{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]}}}]}}' >/dev/null
  kubectl -n "$NS" wait --for=condition=Ready "pod/$1" --timeout=60s >/dev/null
}

expect() { # expect POD allow|deny URL WHY
  local got=deny
  if kubectl -n "$NS" exec "$1" -- curl -s -o /dev/null --connect-timeout 3 --max-time 4 "$3" 2>/dev/null; then got=allow; fi
  if [ "$got" = "$2" ]; then printf '  ✓ %-6s %-10s %-38s %s\n' "$2" "$1" "$3" "$4"
  else printf '  ✗ %-6s %-10s %-38s %s (got %s)\n' "$2" "$1" "$3" "$4" "$got"; fail=1; fi
}

cleanup() { kubectl -n "$NS" delete pod outsider fake-disp --ignore-not-found --now >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

probe_pod outsider "app.kubernetes.io/name=outsider"
probe_pod fake-disp "app.kubernetes.io/name=fake-disp,ithaca.dev/egress=webhooks"

echo "an unlabeled pod in the namespace (a compromised sidecar, say):"
expect outsider deny  http://api:8080/                 "public port: only the ingress controller"
expect outsider deny  http://dispatcher:9083/metrics   "admin port: only the monitoring namespace"
expect outsider deny  telnet://postgres:5432           "database: only db clients"
expect outsider deny  http://receiver-sim:8090/        "receiver: only dispatcher and worker"
expect outsider deny  https://example.com/             "internet: no egress by default"

echo "a pod with the dispatcher's egress label:"
expect fake-disp allow https://example.com/            "webhooks go to the public internet"
expect fake-disp deny  http://receiver-sim:8090/        "both ends check: receiver admits only the real dispatcher/worker"
expect fake-disp deny  http://169.254.169.254/         "cloud metadata: SSRF blocked at the network"
expect fake-disp deny  http://10.0.0.1/                "private ranges: same"
expect fake-disp deny  telnet://postgres:5432          "egress label alone is not a db client"

echo "the real dispatcher, through the same policies:"
delivered() { kubectl -n "$NS" exec statefulset/postgres -- psql -U ithaca -tA -c \
  "SELECT count(*) FROM deliveries_history WHERE final_state = 'succeeded'"; }
before=$(delivered); sleep 5; after=$(delivered)
if [ "$after" -gt "$before" ]; then
  printf '  ✓ allow  dispatcher receiver-sim:8090                       %s deliveries succeeded in 5 s\n' $((after - before))
else
  printf '  ✗ allow  dispatcher receiver-sim:8090                       no deliveries in 5 s\n'; fail=1
fi

exit $fail
