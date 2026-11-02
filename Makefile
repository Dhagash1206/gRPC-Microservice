SHELL := /bin/bash
VENV := .venv
PYTHON := $(abspath $(VENV)/bin/python)

.PHONY: help setup certs proto lint test test-python test-go up down clean logs demo e2e token dlq-replay

help: ## List targets
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-12s %s\n", $$1, $$2}'

setup: ## One-time: venv + Python deps, Go protoc plugins, dev certs, generated code
	python3 -m venv $(VENV)
	$(PYTHON) -m pip install -q -r services-python/requirements-dev.txt
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
	$(MAKE) certs proto

certs: ## Regenerate the dev CA, mTLS certs and JWT signing keys
	./tools/gen_certs.sh

certs/ca.crt:
	./tools/gen_certs.sh

proto: ## Regenerate Python, Go and Envoy code from api/
	./tools/generate_protos.sh all

lint: ## golangci-lint, ruff and buf (same checks as CI)
	cd services-go && go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.1.6 run ./...
	$(VENV)/bin/ruff check services-python tools
	$(VENV)/bin/ruff format --check services-python tools
	go run github.com/bufbuild/buf/cmd/buf@v1.50.0 lint

test: test-python test-go ## All unit/integration tests (Docker needed for Postgres)

test-python:
	cd services-python && $(PYTHON) -m pytest

test-go:
	cd services-go && go vet ./... && go test ./...

up: certs/ca.crt ## Build and start the whole stack, wait until healthy
	docker compose up --build -d --wait

down: ## Stop the stack (keeps data)
	docker compose down

clean: ## Stop the stack and delete all data volumes
	docker compose down -v

logs: ## Follow all service logs
	docker compose logs -f

demo: ## Walk through the REST API (stack must be up)
	./tools/demo.sh

e2e: ## End-to-end tests through Envoy (stack must be up)
	cd services-python && $(PYTHON) -m pytest -m e2e tests/e2e -v

token: ## Print a dev JWT: make token SUB=customer-42
	@$(PYTHON) tools/dev_auth.py token --sub $(or $(SUB),customer-42)

dlq-replay: ## Move dead-lettered notifications back for reprocessing
	docker compose exec notification /app replay-dlq
