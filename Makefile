.PHONY: help up down build shell logs test bronze reprocess silver gold pipeline queries lineage optimize report ui generate generate-large clean
.DEFAULT_GOAL := help

COMPOSE := docker compose
GIT_SHA := $(shell git describe --always --dirty 2>/dev/null)
PIPELINE := $(COMPOSE) exec -e GIT_SHA=$(GIT_SHA) pipeline

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

up: ## Build and start the container in the background
	$(COMPOSE) up -d --build

down: ## Stop and remove the container
	$(COMPOSE) down

build: ## Rebuild the image without starting the container
	$(COMPOSE) build

shell: ## Open an interactive shell inside the container
	$(PIPELINE) bash

logs: ## Tail container logs
	$(COMPOSE) logs -f

test: ## Run the test suite inside the container
	$(PIPELINE) pytest tests/ -v

bronze: ## Ingest sample data into the Bronze layer (idempotent), then run table maintenance
	$(PIPELINE) python -m etl_jobs.bronze.ingest_to_bronze
	$(PIPELINE) python -m etl_jobs.maintenance.optimize_tables

reprocess: ## Re-ingest specific files into Bronze, replacing their rows, then run table maintenance (FILES="a.parquet b.parquet" REASON="...")
	@test -n "$(FILES)" || (echo 'Usage: make reprocess FILES="transactions_batch_1.parquet" REASON="why"' && exit 1)
	$(PIPELINE) python -m etl_jobs.bronze.ingest_to_bronze --reprocess $(FILES) $(if $(REASON),--reason "$(REASON)")
	$(PIPELINE) python -m etl_jobs.maintenance.optimize_tables

silver: ## Build the Silver layer from Bronze (full rebuild, or REFERENCE_DATE=YYYY-MM-DD for one date), then run table maintenance
	$(PIPELINE) python -m etl_jobs.silver.build_silver $(if $(REFERENCE_DATE),--reference-date $(REFERENCE_DATE))
	$(PIPELINE) python -m etl_jobs.maintenance.optimize_tables

gold: ## Build the Gold layer from Silver (full rebuild, or REFERENCE_DATE=YYYY-MM-DD for one date), then run table maintenance
	$(PIPELINE) python -m etl_jobs.gold.build_gold $(if $(REFERENCE_DATE),--reference-date $(REFERENCE_DATE))
	$(PIPELINE) python -m etl_jobs.maintenance.optimize_tables

pipeline: ## Run Bronze, Silver and Gold in order (REFERENCE_DATE=YYYY-MM-DD scopes Silver and Gold), then table maintenance
	$(PIPELINE) python -m etl_jobs.bronze.ingest_to_bronze
	$(PIPELINE) python -m etl_jobs.silver.build_silver $(if $(REFERENCE_DATE),--reference-date $(REFERENCE_DATE))
	$(PIPELINE) python -m etl_jobs.gold.build_gold $(if $(REFERENCE_DATE),--reference-date $(REFERENCE_DATE))
	$(PIPELINE) python -m etl_jobs.maintenance.optimize_tables

queries: ## Run the example analytical queries against Gold and write data/reports/gold_queries.md (CONSUMER=operations|cfo|compliance)
	$(PIPELINE) python -m etl_jobs.gold.run_queries --output data/reports/gold_queries.md $(if $(CONSUMER),--consumer $(CONSUMER))

lineage: ## Regenerate the table lineage graph (docs/pipeline_docs/lineage.md) from the SQL and the contracts
	$(PIPELINE) python -m etl_jobs.observability.lineage

optimize: ## Compact/Z-order Delta tables, vacuum expired files and clean up the log
	$(PIPELINE) python -m etl_jobs.maintenance.optimize_tables

report: ## Generate the pipeline health report (data/observability/reports/latest.md)
	$(PIPELINE) python -m etl_jobs.observability.health_report

ui: ## Open the DuckDB UI over the Delta tables (runs on the host, not in the container)
	uv run python run_duckdb_ui.py

LANDING := /app/data/landing

generate: ## Replace data/landing (pipeline source) with a small synthetic dataset (10k rows)
	$(PIPELINE) rm -rf $(LANDING)
	$(PIPELINE) python scripts/generate_sample_data.py \
		--rows 10000 --days 30 --merchants 100 --seed 42 --out $(LANDING)

generate-large: ## Replace data/landing (pipeline source) with a large synthetic dataset (1M rows)
	$(PIPELINE) rm -rf $(LANDING)
	$(PIPELINE) python scripts/generate_sample_data.py \
		--rows 1000000 --days 90 --merchants 500 --seed 42 --out $(LANDING)

clean: ## Remove the generated source data (data/landing) and stop the container
	-$(PIPELINE) rm -rf $(LANDING)
	$(COMPOSE) down
