"""
config.py — Configurações centrais do Pipeline Medallion
Dataset: Benefícios de Transferência de Renda (dados.gov.br / MDS)
"""

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional


# ─────────────────────────────────────────
# Caminhos base
# ─────────────────────────────────────────
BASE_PATH = Path(os.getenv("MEDALLION_BASE_PATH", "/opt/airflow/data"))

BRONZE_PATH = BASE_PATH / "bronze"
SILVER_PATH = BASE_PATH / "silver"
GOLD_PATH   = BASE_PATH / "gold"

# Garante que os diretórios existem
for _p in (BRONZE_PATH, SILVER_PATH, GOLD_PATH):
    _p.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────
# Fonte de dados — dados.gov.br
# API CKAN do Portal Brasileiro de Dados Abertos
# Dataset: Transferências de Renda — Bolsa Família
# ─────────────────────────────────────────
DATASET_ID  = "bolsa-familia-pagamentos"
API_BASE    = "https://api.dados.gov.br/ed/v1"
CKAN_BASE   = "https://dados.gov.br/api/3/action"

# URL direta CSV mensal (MDS / Ministério do Desenvolvimento Social)
# Formato: ano=AAAA, mes=MM
MDS_BASE_URL = (
    "https://www.transparencia.gov.br/download-de-dados/bolsa-familia-pagamentos"
)

# Fallback: arquivo de exemplo gerado localmente se API offline
USE_SAMPLE_DATA = os.getenv("USE_SAMPLE_DATA", "true").lower() == "true"


# ─────────────────────────────────────────
# PostgreSQL — Gold Layer
# ─────────────────────────────────────────
@dataclass
class PostgresConfig:
    host:     str = os.getenv("POSTGRES_HOST", "postgres")
    port:     int = int(os.getenv("POSTGRES_PORT", "5432"))
    database: str = os.getenv("POSTGRES_DB", "airflow")
    user:     str = os.getenv("POSTGRES_USER", "airflow")
    password: str = os.getenv("POSTGRES_PASSWORD", "airflow")
    schema:   str = "gold"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


PG_CONFIG = PostgresConfig()


# ─────────────────────────────────────────
# Spark / Delta Lake
# ─────────────────────────────────────────
SPARK_APP_NAME = "MedallionETL"
DELTA_VERSION  = "3.1.0"
SCALA_VERSION  = "2.12"

SPARK_CONF = {
    "spark.sql.extensions":
        "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog":
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.databricks.delta.retentionDurationCheck.enabled": "false",
    "spark.sql.shuffle.partitions": "4",      # baixo para dev local
    "spark.driver.memory": "2g",
    "spark.executor.memory": "2g",
}


# ─────────────────────────────────────────
# Pipeline — parâmetros de execução
# ─────────────────────────────────────────
DEFAULT_YEAR  = int(os.getenv("PIPELINE_YEAR",  "2023"))
DEFAULT_MONTH = int(os.getenv("PIPELINE_MONTH", "1"))

# Colunas esperadas no CSV raw (Transparência Gov)
RAW_COLUMNS = [
    "MES_COMPETENCIA",
    "UF",
    "CODIGO_MUNICIPIO_SIAFI",
    "NOME_MUNICIPIO",
    "CPF_FAVORECIDO",
    "NIS_FAVORECIDO",
    "NOME_FAVORECIDO",
    "VALOR_PARCELA",
]

# Schema Silver — tipos canônicos
SILVER_SCHEMA = {
    "ano":              "integer",
    "mes":              "integer",
    "uf":               "string",
    "codigo_municipio": "string",
    "nome_municipio":   "string",
    "nis_favorecido":   "string",
    "nome_favorecido":  "string",
    "valor_parcela":    "double",
    "dt_ingestao":      "timestamp",
}
