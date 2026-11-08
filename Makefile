# Makefile — DE Agentic Boilerplate (Bruin + dbt Core + DuckDB + DataContract CLI)
SHELL := /bin/bash
UV := /home/Hugo/.local/bin/uv
BRUIN := /home/Hugo/.local/bin/bruin
DBT := $(UV) run --offline dbt
DATACONTRACT := $(UV) run --offline datacontract
DBT_FLAGS := --project-dir ./dbt_project --profiles-dir ./dbt_project
PYTHON := $(UV) run python

.PHONY: help setup ingest compile build build-modified init-state contract-lint contract-test validate all simulate dispatch status clean-outbox vacuum run

help:
	@echo "======================================================================"
	@echo " DE Agentic Boilerplate — Local Data Engineering Commands"
	@echo "======================================================================"
	@echo "  make setup          -> Sync Python virtual environment (.venv via uv)"
	@echo "  make simulate       -> Simulate market drift & ingest batch into SQLite"
	@echo "  make ingest         -> Run Bruin ingestion or ingest JSON (make ingest FILE=sample.json)"
	@echo "  make dispatch       -> Dispatch pending outbox notifications"
	@echo "  make status         -> Display executive terminal status dashboard"
	@echo "  make clean-outbox   -> Purge DISPATCHED outbox records (>30 days retention)"
	@echo "  make vacuum         -> Truncate SQLite WAL journal and VACUUM free pages"
	@echo "  make run            -> Complete flow: simulate -> dispatch -> status"
	@echo "  make build          -> Build & test Silver/Gold models in dbt Core"
	@echo "  make init-state     -> Save baseline manifest.json to ./target/prod_manifest"
	@echo "  make compile        -> Compile only modified models (state:modified+)"
	@echo "  make build-modified -> Selective incremental build (--select state:modified+ --defer)"
	@echo "  make contract-lint  -> Validate ODCS v3.2.0 contract syntax"
	@echo "  make contract-test  -> Execute ODCS v3.2.0 contract quality checks against DuckDB"
	@echo "  make all            -> Run full Medallion pipeline + ODCS contract verification"

setup:
	$(UV) sync

simulate:
	$(PYTHON) -m src.main simulate

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

ingest:
ifdef FILE
	$(PYTHON) -m src.main ingest --file $(FILE)
else
	$(BRUIN) validate ./pipelines/
	$(BRUIN) run ./pipelines/
endif

build:
	$(DBT) build $(DBT_FLAGS)

init-state:
	mkdir -p ./target/prod_manifest
	cp ./dbt_project/target/manifest.json ./target/prod_manifest/manifest.json
	@echo "Baseline manifest updated at ./target/prod_manifest/manifest.json"

compile:
	$(DBT) compile $(DBT_FLAGS) --select state:modified+ --state $(CURDIR)/target/prod_manifest

build-modified:
	$(DBT) build $(DBT_FLAGS) --select state:modified+ --defer --state $(CURDIR)/target/prod_manifest

contract-lint:
	@for f in ./contracts/*.odcs.yaml; do \
		echo "Linting $$f..."; \
		$(DATACONTRACT) lint "$$f" || exit 1; \
	done

contract-test:
	@for f in ./contracts/*.odcs.yaml; do \
		echo "Testing $$f..."; \
		$(DATACONTRACT) test "$$f" || exit 1; \
	done

all: ingest build init-state contract-lint contract-test
	@echo "✅ Medallion Pipeline (Bronze -> Silver -> Gold) and ODCS v3.2.0 Contract 100% verified!"
