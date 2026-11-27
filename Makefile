# Ithaca — one entrypoint for every task.
#
# Everything CI runs is a make target, and every make target runs locally.
# When those two drift you get "works on my machine" in the other direction:
# a green laptop and a red pipeline nobody can reproduce.

SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help
MAKEFLAGS += --no-print-directory

# ── metadata ─────────────────────────────────────────────────────────────────
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT     ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
PKG        := github.com/SharmaSanskar/ithaca
LDFLAGS    := -X $(PKG)/internal/platform/version.Version=$(VERSION) \
              -X $(PKG)/internal/platform/version.Commit=$(COMMIT) \
              -X $(PKG)/internal/platform/version.Date=$(BUILD_DATE)

COMPOSE := docker compose -f deploy/compose/docker-compose.deps.yml
BIN     := bin/ithaca

# sqlc runs via `go run` at a pinned version, so there is nothing to install
# and every machine (and CI) generates byte-identical code.
SQLC := go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1

# Local connection strings. Overridable, but these match compose exactly so
# `make up && make dev` works with no further setup.
export ITHACA_DATABASE_URL ?= postgres://ithaca:ithaca@localhost:5432/ithaca?sslmode=disable
export ITHACA_REDIS_URL    ?= redis://localhost:6379/0
export ITHACA_ENV          ?= dev

# ── help ─────────────────────────────────────────────────────────────────────
.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nithaca $(VERSION)\n\nusage: make <target>\n\n"} \
		/^[a-zA-Z0-9_.-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 } \
		/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)
	@echo

##@ Environment

.PHONY: up
up: ## Start Postgres, Redis, Prometheus, Grafana, Jaeger
	$(COMPOSE) up -d --wait
	@echo
	@echo "  postgres    localhost:5432    (ithaca/ithaca)"
	@echo "  redis       localhost:6379"
	@echo "  prometheus  http://localhost:9090"
	@echo "  grafana     http://localhost:3000  (anonymous admin)"
	@echo "  jaeger      http://localhost:16686"
	@echo

.PHONY: up-aws
up-aws: ## Start the stack plus LocalStack (needed only for the SQS driver)
	$(COMPOSE) --profile aws up -d --wait
	@echo "  localstack  localhost:4566"

.PHONY: down
down: ## Stop the stack, keep the data
	$(COMPOSE) --profile aws down

.PHONY: nuke
nuke: ## Stop the stack and delete all volumes
	$(COMPOSE) --profile aws down -v --remove-orphans

.PHONY: logs
logs: ## Tail dependency logs
	$(COMPOSE) logs -f --tail=100

.PHONY: prom-reload
prom-reload: ## Reload prometheus.yml without restarting Prometheus
	curl -sS -X POST http://localhost:9090/-/reload && echo "prometheus reloaded"

.PHONY: psql
psql: ## Open a psql shell
	$(COMPOSE) exec postgres psql -U ithaca -d ithaca

.PHONY: migrate
migrate: ## Apply database migrations
	go run ./cmd/ithaca migrate up

.PHONY: migrate-status
migrate-status: ## List migrations and whether each is applied
	go run ./cmd/ithaca migrate status

.PHONY: tenant
tenant: ## Create a tenant and print its API key (make tenant NAME=acme)
	go run ./cmd/ithaca tenant create $(or $(NAME),dev)

.PHONY: demo
demo: build ## Run the full demo locally: seed, then every service in demo mode
	go run ./cmd/ithaca migrate up
	ITHACA_DEMO_MODE=true go run ./cmd/ithaca demo seed
	ITHACA_DEMO_MODE=true ITHACA_RETRY_BASE=500ms ITHACA_RETRY_MAX_ATTEMPTS=6 \
	ITHACA_BREAKER_COOLDOWN=3s ITHACA_BREAKER_MAX_COOLDOWN=10s ./scripts/dev.sh start

.PHONY: bench-up
bench-up: ## Start the durable benchmark Postgres (port 5433)
	$(COMPOSE) --profile bench up -d --wait postgres-bench

.PHONY: bench
bench: build bench-up ## Run one benchmark: make bench E=e3 (e1 e2 e3 e4 e5 e8 all)
	./bench/run.sh $(or $(E),all)

.PHONY: dashboards
dashboards: ## Regenerate the Grafana dashboard JSON and the k8s PrometheusRule from their sources
	python3 deploy/compose/grafana/dashboards.py
	python3 deploy/k8s/monitoring/gen_rules.py

.PHONY: redis-cli
redis-cli: ## Open a redis shell
	$(COMPOSE) exec redis redis-cli

##@ Development

.PHONY: dev
dev: build ## Start all six services + the dashboard, in the background
	@./scripts/dev.sh start

.PHONY: dev-stop
dev-stop: ## Stop the services (leaves containers running)
	@./scripts/dev.sh stop

.PHONY: dev-restart
dev-restart: build ## Rebuild and restart the services
	@./scripts/dev.sh restart

.PHONY: ps
ps: ## Show what is running
	@./scripts/dev.sh status

.PHONY: tail
tail: ## Tail every service log (make tail SVC=dispatcher for one)
	@./scripts/dev.sh logs $(SVC)

.PHONY: stop
stop: ## Stop EVERYTHING: services, dashboard and containers
	@./scripts/dev.sh stop
	@$(COMPOSE) --profile aws down
	@echo "everything stopped"

.PHONY: run
run: ## Run one service in the foreground (make run SVC=dispatcher)
	go run ./cmd/ithaca $(or $(SVC),api)

.PHONY: web-install
web-install: ## Install dashboard dependencies
	cd web && npm ci

.PHONY: web-dev
web-dev: ## Run the dashboard dev server in the foreground
	cd web && npm run dev

.PHONY: web-build
web-build: ## Build the dashboard into internal/web/dist
	cd web && npm run build

.PHONY: web-lint
web-lint: ## Typecheck and lint the dashboard
	cd web && npm run check

##@ Build

.PHONY: build
build: ## Build the binary (no dashboard)
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o $(BIN) ./cmd/ithaca
	@echo "built $(BIN) ($(VERSION))"

.PHONY: build-full
build-full: web-build ## Build the binary with the dashboard embedded
	CGO_ENABLED=0 go build -trimpath -tags embedweb -ldflags "$(LDFLAGS)" -o $(BIN) ./cmd/ithaca
	@echo "built $(BIN) ($(VERSION), dashboard embedded)"

.PHONY: docker
docker: ## Build the container image
	VERSION=$(VERSION) COMMIT=$(COMMIT) BUILD_DATE=$(BUILD_DATE) \
		docker buildx bake -f build/docker-bake.hcl ithaca

# The production stack (deploy/prod) on this machine, at https://localhost.
# Same compose file and Caddyfile as the server; only the env file differs.
PROD_COMPOSE := docker compose -f deploy/prod/docker-compose.yml --env-file tmp/prod.env

.PHONY: prod-local
prod-local: docker ## Run the production stack locally at https://localhost
	@mkdir -p tmp
	@[ -f tmp/prod.env ] || printf 'ITHACA_DOMAIN=localhost\nPOSTGRES_PASSWORD=%s\nITHACA_IMAGE=ithaca\nITHACA_VERSION=dev\n' "$$(openssl rand -hex 16)" > tmp/prod.env
	$(PROD_COMPOSE) up -d
	@printf '\n  https://localhost  (accept the local certificate once)\n\n'

.PHONY: prod-local-down
prod-local-down: ## Stop the local production stack (keeps its data)
	$(PROD_COMPOSE) down

# ── Kubernetes (k3d) ─────────────────────────────────────────────────────────
KPS_VERSION  := 94.1.0 # kube-prometheus-stack chart
KEDA_VERSION := 2.21.1 # KEDA chart

.PHONY: k8s-up
k8s-up: ## Create the local k3d cluster (1 server, 2 agents, a registry) with KEDA
	k3d cluster create --config deploy/k8s/k3d.yaml
	$(MAKE) k8s-keda

.PHONY: k8s-keda
k8s-keda: ## Install the KEDA operator (the dispatchers' autoscaler)
	helm repo add kedacore https://kedacore.github.io/charts >/dev/null 2>&1 || true
	helm upgrade --install keda kedacore/keda --version $(KEDA_VERSION) \
		--namespace keda --create-namespace --wait --timeout 5m

.PHONY: k8s-deploy
k8s-deploy: ## Build, push and deploy by digest; waits for every rollout
	scripts/k8s-deploy.sh
	@printf '\n  http://ithaca.localhost:8088\n\n'

.PHONY: k8s-monitoring
k8s-monitoring: ## Install Prometheus + Grafana in the cluster, with Ithaca's dashboard and alerts
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null 2>&1 || true
	helm upgrade --install kps prometheus-community/kube-prometheus-stack --version $(KPS_VERSION) \
		--namespace monitoring --create-namespace -f deploy/k8s/monitoring/values.yaml --wait --timeout 10m
	kustomize build --load-restrictor LoadRestrictionsNone deploy/k8s/monitoring | kubectl apply -f -
	@printf '\n  Grafana     http://grafana.localhost:8088  (Dashboards → Ithaca · overview)\n  Prometheus  http://prometheus.localhost:8088\n\n'

.PHONY: k8s-check
k8s-check: ## Prove NetworkPolicies are enforced, then restart everything under load
	scripts/k8s-netpol-check.sh
	scripts/k8s-rollout-check.sh

.PHONY: k8s-burst
k8s-burst: ## The autoscaling demo: a 60k-event burst, dispatchers scale out and back (BASELINE=1 to pin at 2)
	@# caffeinate (macOS) keeps the laptop awake for the run; elsewhere run it directly.
	@if command -v caffeinate >/dev/null; then caffeinate -i scripts/k8s-burst.sh; else scripts/k8s-burst.sh; fi

.PHONY: k8s-down
k8s-down: ## Delete the local cluster and its registry
	k3d cluster delete --config deploy/k8s/k3d.yaml

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf bin/ internal/web/dist/* coverage.out coverage.html
	@printf '# Placeholder so `go build -tags embedweb` compiles on a clean checkout.\n' > internal/web/dist/.gitkeep

##@ Quality

.PHONY: fmt
fmt: ## Format Go code
	gofmt -w -s .
	@command -v goimports >/dev/null 2>&1 && goimports -w -local $(PKG) . || true

.PHONY: lint
lint: ## Run golangci-lint
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint not found: brew install golangci-lint"; exit 1; }
	golangci-lint run ./...

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: test
test: ## Run unit tests with the race detector
	go test -race -shuffle=on -count=1 ./...

.PHONY: test-integration
test-integration: ## Run integration tests (needs Docker; testcontainers)
	go test -race -count=1 -tags=integration -timeout=10m ./...

.PHONY: generate
generate: ## Regenerate sqlc code from migrations + queries
	$(SQLC) generate

.PHONY: generate-check
generate-check: ## Fail if generated code is stale
	$(SQLC) diff

.PHONY: cover
cover: ## Run tests and open the coverage report
	go test -race -covermode=atomic -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "coverage.html written"

.PHONY: tidy
tidy: ## Tidy and verify modules
	go mod tidy
	go mod verify

.PHONY: check
check: fmt vet lint test ## Everything CI runs, in one command

##@ Housekeeping

.PHONY: tools
tools: ## Install the developer toolchain (macOS/Homebrew)
	brew install golangci-lint sqlc k6 k3d helm kubectl
	go install github.com/pressly/goose/v3/cmd/goose@latest
	go install golang.org/x/tools/cmd/goimports@latest

.PHONY: version
version: ## Print build metadata
	@echo "version:    $(VERSION)"
	@echo "commit:     $(COMMIT)"
	@echo "build date: $(BUILD_DATE)"
