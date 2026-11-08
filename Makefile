# Makefile — B2B Price & Market Monitor (ODCS v3.2.0 | SQLite WAL | Transactional Outbox)
SHELL := /bin/bash
UV := uv
PYTHON := $(UV) run python
PYTEST := $(UV) run pytest
DATACONTRACT := $(UV) run datacontract
DOCKER_IMAGE := b2b-price-monitor:latest

.PHONY: help setup test simulate ingest dispatch status clean-outbox vacuum run contract-lint docker-build docker-run docker-test all

help:
	@echo "================================================================================"
	@echo " B2B Price & Market Monitor — Deterministic CLI & Docker Operations"
	@echo "================================================================================"
	@echo "  make setup          -> Sync Python 3.12 virtual environment (.venv via uv)"
	@echo "  make test           -> Run the full 65-test verification suite (pytest -v)"
	@echo "  make simulate       -> Simulate market drift & ingest batch into SQLite WAL"
	@echo "  make ingest         -> Ingest external JSON file (e.g. make ingest FILE=examples/sample_scrape_batch.json)"
	@echo "  make dispatch       -> Dispatch pending outbox notifications (Discord/Slack/Console)"
	@echo "  make status         -> Display executive terminal status dashboard"
	@echo "  make clean-outbox   -> Purge DISPATCHED outbox records (>30 days retention)"
	@echo "  make vacuum         -> Truncate SQLite WAL journal and VACUUM free pages"
	@echo "  make run            -> End-to-end flow: simulate -> dispatch -> status"
	@echo "  make contract-lint  -> Validate ODCS v3.2.0 data contract syntax"
	@echo "  make docker-build   -> Build multi-stage Docker container image"
	@echo "  make docker-run     -> Run end-to-end pipeline inside isolated Docker container"
	@echo "  make docker-test    -> Execute all 65 unit/integration tests + ODCS lint inside Docker"
	@echo "  make all            -> Run full local verification loop (test + contract-lint + run)"

setup:
	$(UV) sync

test:
	$(PYTEST) -v

simulate:
	$(PYTHON) -m src.main simulate

ingest:
ifndef FILE
	$(PYTHON) -m src.main ingest --file examples/sample_scrape_batch.json
else
	$(PYTHON) -m src.main ingest --file $(FILE)
endif

dispatch:
	$(PYTHON) -m src.main dispatch

status:
	$(PYTHON) -m src.main status

clean-outbox:
	$(PYTHON) -m src.main clean-outbox

vacuum:
	$(PYTHON) -m src.main vacuum

run:
	$(PYTHON) -m src.main run

contract-lint:
	@for f in ./contracts/*.odcs.yaml; do \
		echo "Linting $$f..."; \
		$(DATACONTRACT) lint "$$f" || exit 1; \
	done

docker-build:
	docker build -t $(DOCKER_IMAGE) .

docker-run:
	docker run --rm -e B2B_WEBHOOK_URL="$${B2B_WEBHOOK_URL:-}" $(DOCKER_IMAGE) run --items 5

docker-test:
	docker run --rm --entrypoint pytest $(DOCKER_IMAGE) -v
	docker run --rm --entrypoint datacontract $(DOCKER_IMAGE) lint ./contracts/b2b_price_monitor_v1.odcs.yaml

all: test contract-lint
	$(PYTHON) -m src.main run --dry-run
	@echo "✅ All 65 tests, ODCS v3.2.0 Contract, and End-to-End CDC Pipeline verified!"
