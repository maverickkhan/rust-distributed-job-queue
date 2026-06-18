# Developer entrypoints. `make help` lists targets.
.DEFAULT_GOAL := help
SHELL := /bin/bash

DATABASE_URL ?= postgres://postgres:postgres@localhost:5432/djq
export DATABASE_URL

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS=":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: fmt
fmt: ## Format the workspace
	cargo fmt --all

.PHONY: fmt-check
fmt-check: ## Check formatting
	cargo fmt --all -- --check

.PHONY: lint
lint: ## Clippy with warnings denied
	cargo clippy --workspace --all-targets --all-features -- -D warnings

.PHONY: build
build: ## Debug build
	cargo build --workspace

.PHONY: release
release: ## Release build of the binaries
	cargo build --release -p djq-api -p djq-worker

.PHONY: test
test: ## Run all tests (needs a database; see `make db`)
	cargo test --workspace --all-features

.PHONY: audit
audit: ## Security audit of dependencies (requires cargo-audit)
	cargo audit

.PHONY: deny
deny: ## License/advisory checks (requires cargo-deny)
	cargo deny check

.PHONY: db
db: ## Start a throwaway Postgres for local testing on :5432
	docker run -d --name djq-pg -e POSTGRES_PASSWORD=postgres \
		-e POSTGRES_DB=djq -p 5432:5432 postgres:16

.PHONY: db-stop
db-stop: ## Stop and remove the throwaway Postgres
	docker rm -f djq-pg

.PHONY: up
up: ## Bring up the full stack with docker compose
	docker compose up --build

.PHONY: down
down: ## Tear down the docker compose stack and volumes
	docker compose down -v

.PHONY: run-api
run-api: ## Run the API locally
	cargo run -p djq-api

.PHONY: run-worker
run-worker: ## Run a worker locally
	cargo run -p djq-worker

.PHONY: load-test
load-test: ## Submit a burst of jobs against a running API (see scripts/load_test.sh)
	./scripts/load_test.sh

.PHONY: ci
ci: fmt-check lint test ## Everything CI runs (minus Docker)
