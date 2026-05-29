# ══════════════════════════════════════════════════════════════
# Makefile — Pipeline ETL Medallion
# ══════════════════════════════════════════════════════════════

.PHONY: help up down logs test lint format pipeline clean

YEAR  ?= 2023
MONTH ?= 1

help:  ## Mostra esta ajuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Docker ────────────────────────────────────────────────────
up:  ## Sobe todo o ambiente Docker (Airflow + Postgres + Jupyter)
	docker compose up -d
	@echo ""
	@echo "✅ Ambiente iniciado:"
	@echo "   Airflow:  http://localhost:8080  (admin/admin)"
	@echo "   Jupyter:  http://localhost:8888"

down:  ## Para e remove os containers
	docker compose down -v

restart: down up  ## Reinicia o ambiente

logs:  ## Exibe logs do scheduler Airflow
	docker compose logs -f airflow-scheduler

logs-web:  ## Exibe logs do webserver Airflow
	docker compose logs -f airflow-webserver

# ── Pipeline ──────────────────────────────────────────────────
pipeline:  ## Roda o pipeline completo (YEAR=2023 MONTH=1)
	MEDALLION_BASE_PATH=./data USE_SAMPLE_DATA=true \
	  python run_pipeline.py --year $(YEAR) --month $(MONTH)

pipeline-all:  ## Roda todos os 12 meses do ano (YEAR=2023)
	MEDALLION_BASE_PATH=./data USE_SAMPLE_DATA=true \
	  python run_pipeline.py --year $(YEAR) --all-months

bronze:  ## Roda apenas a camada Bronze
	MEDALLION_BASE_PATH=./data USE_SAMPLE_DATA=true \
	  python run_pipeline.py --year $(YEAR) --month $(MONTH) --layers bronze

silver:  ## Roda apenas a camada Silver
	MEDALLION_BASE_PATH=./data USE_SAMPLE_DATA=true \
	  python run_pipeline.py --year $(YEAR) --month $(MONTH) --layers silver

gold:  ## Roda apenas a camada Gold
	MEDALLION_BASE_PATH=./data USE_SAMPLE_DATA=true \
	  python run_pipeline.py --year $(YEAR) --month $(MONTH) --layers gold

# ── Testes ────────────────────────────────────────────────────
test:  ## Roda todos os testes
	pytest

test-cov:  ## Testes com relatório de cobertura HTML
	pytest --cov=src --cov-report=html:docs/coverage
	@echo "📊 Relatório em: docs/coverage/index.html"

test-fast:  ## Testes sem Spark (rápidos)
	pytest tests/test_bronze.py -k "not Spark" -v

# ── Qualidade de código ────────────────────────────────────────
lint:  ## Verifica qualidade com ruff
	ruff check src/ tests/

format:  ## Formata o código com black
	black src/ tests/ dags/

type-check:  ## Verifica tipos com mypy
	mypy src/ --ignore-missing-imports

# ── Setup ─────────────────────────────────────────────────────
install:  ## Instala dependências Python
	pip install -r requirements.txt

install-dev:  ## Instala dependências de desenvolvimento
	pip install -r requirements.txt black ruff mypy

# ── Limpeza ───────────────────────────────────────────────────
clean:  ## Remove dados gerados e caches
	rm -rf data/bronze data/silver data/gold data/logs data/test
	rm -rf __pycache__ src/**/__pycache__ tests/__pycache__
	rm -rf .pytest_cache .coverage htmlcov docs/coverage
	find . -name "*.pyc" -delete

clean-spark:  ## Remove metadados do Spark
	rm -rf derby.log metastore_db spark-warehouse

clean-all: clean clean-spark  ## Limpeza completa
