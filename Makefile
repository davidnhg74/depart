.PHONY: help up down logs restart build clean migrate migrate-rev test test-unit test-integration smoke lint format typecheck oracle-up oracle-down

# Default goal
.DEFAULT_GOAL := help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

up: ## Start postgres + api + web
	docker compose up -d --build

down: ## Stop and remove containers (preserves volumes)
	docker compose down

logs: ## Tail logs from all services
	docker compose logs -f --tail=100

restart: ## Restart api + web (rebuild)
	docker compose up -d --build api web

build: ## Build api + web images without starting
	docker compose build api web

clean: ## Stop and remove everything including volumes (DESTRUCTIVE)
	docker compose down -v

migrate: ## Apply all pending Alembic migrations
	cd apps/api && alembic upgrade head

grammar: ## Generate the ANTLR PL/SQL parser into src/source/oracle/_generated/
	cd apps/api && python3 scripts/generate_grammar.py

grammar-force: ## Force-regenerate the ANTLR parser even if up-to-date
	cd apps/api && python3 scripts/generate_grammar.py --force

eval-app-impact: ## Run the app-impact AI prompt eval (requires ANTHROPIC_API_KEY)
	cd apps/api && python3 -m src.ai.eval app_impact

eval-runbook: ## Run the runbook AI prompt eval (requires ANTHROPIC_API_KEY)
	cd apps/api && python3 -m src.ai.eval runbook

eval-dry: ## Smoke-test the eval harness with a mocked LLM (no API calls)
	cd apps/api && python3 -m src.ai.eval app_impact --dry || true
	cd apps/api && python3 -m src.ai.eval runbook --dry || true

migrate-rev: ## Generate a new Alembic revision: make migrate-rev MSG="add table x"
	cd apps/api && alembic revision --autogenerate -m "$(MSG)"

test: test-unit ## Run all tests (alias for test-unit; integration is opt-in)

test-unit: ## Run unit tests against an in-process SQLAlchemy session
	cd apps/api && pytest tests/ -v -m "not integration and not oracle"

test-integration: ## Run integration tests (requires postgres up)
	cd apps/api && pytest tests/ -v -m "integration"

test-oracle: ## Run Oracle-touching tests (requires `make oracle-up` to have completed start_period)
	cd apps/api && pytest tests/ -v -m "oracle"

smoke: ## End-to-end smoke: build, boot, hit /health, tear down
	docker compose up -d --build postgres api
	@echo "Waiting for api healthcheck..."
	@for i in $$(seq 1 30); do \
		if curl -fsS http://localhost:8000/health > /dev/null 2>&1; then \
			echo "OK: /health responded"; \
			docker compose down; \
			exit 0; \
		fi; \
		sleep 2; \
	done; \
	echo "FAIL: /health did not respond in 60s"; \
	docker compose logs api; \
	docker compose down; \
	exit 1

lint: ## Lint with ruff
	cd apps/api && ruff check src/ tests/

format: ## Format with black
	cd apps/api && black src/ tests/

typecheck: ## Static type-check with mypy
	cd apps/api && mypy src/

oracle-up: ## Start the Oracle Free test container (multi-GB, slow first boot)
	docker compose --profile oracle up -d oracle
	@echo "Oracle starting. Watch readiness with: docker logs -f depart_oracle"

oracle-down: ## Stop the Oracle test container
	docker compose --profile oracle down oracle
