"""
bronze_ingestion.py — Camada Bronze
Responsabilidade: ingestão raw dos dados do Portal da Transparência (dados.gov.br)
Dataset: Pagamentos do Bolsa Família (MDS / Ministério do Desenvolvimento Social)

Nesta camada:
  ✔ Download / leitura do CSV original
  ✔ Gravação em Delta Lake particionado por ano/mês
  ✔ Nenhuma transformação — dado bruto preservado
  ✔ Metadados de auditoria (_source, _ingested_at, _file_name)
"""

from __future__ import annotations

import hashlib
import io
import os
import urllib.request
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from src.utils.config import (
    BRONZE_PATH,
    MDS_BASE_URL,
    RAW_COLUMNS,
    USE_SAMPLE_DATA,
)
from src.utils.logger import get_logger
from src.utils.sample_data import generate_csv

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Schema raw — todos StringType (Bronze não infere tipos)
# ─────────────────────────────────────────────────────────────────────────────
RAW_SCHEMA = StructType([
    StructField(col, StringType(), nullable=True) for col in RAW_COLUMNS
])


def _download_csv(year: int, month: int) -> tuple[str, str]:
    """
    Tenta baixar o CSV do Portal da Transparência.
    Fallback para dados de amostra se USE_SAMPLE_DATA=true ou API offline.

    Returns:
        (csv_content: str, source_label: str)
    """
    url = f"{MDS_BASE_URL}/{year}{month:02d}"

    if USE_SAMPLE_DATA:
        logger.info("USE_SAMPLE_DATA=true → gerando dados de amostra")
        seed = int(f"{year}{month:02d}")
        content = generate_csv(year, month, n_rows=50_000, seed=seed)
        return content, "sample_data"

    logger.info("Baixando dados: %s", url)
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 MedallionETL/1.0 "
                    "(https://github.com/seu-usuario/medallion-etl)"
                )
            },
        )
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw_bytes = resp.read()
        # Portal da Transparência usa Latin-1
        content = raw_bytes.decode("latin-1", errors="replace")
        logger.info("Download OK — %d bytes", len(raw_bytes))
        return content, url
    except Exception as exc:
        logger.warning("Falha no download (%s) → usando dados de amostra", exc)
        seed = int(f"{year}{month:02d}")
        content = generate_csv(year, month, n_rows=50_000, seed=seed)
        return content, "sample_data_fallback"


def _csv_to_dataframe(spark: SparkSession, csv_content: str) -> DataFrame:
    """
    Converte o CSV (string) em DataFrame Spark usando o schema raw.
    """
    lines  = csv_content.splitlines()
    header = lines[0] if lines else ""

    # Valida colunas mínimas
    expected = set(RAW_COLUMNS)
    actual   = set(c.strip().upper() for c in header.split(";"))
    missing  = expected - actual
    if missing:
        raise ValueError(f"CSV sem colunas esperadas: {missing}")

    # SparkContext RDD → DataFrame (funciona sem HDFS)
    rdd = spark.sparkContext.parallelize(lines)
    df  = (
        spark.read
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .option("quote", '"')
        .option("escape", '"')
        .csv(rdd)
    )
    return df


def ingest_bronze(
    spark: SparkSession,
    year: int,
    month: int,
    overwrite: bool = False,
) -> DataFrame:
    """
    Pipeline Bronze: baixa CSV e grava em Delta Lake particionado.

    Args:
        spark:     SparkSession ativa.
        year:      Ano de competência (ex: 2023).
        month:     Mês de competência (1–12).
        overwrite: Se True, sobrescreve partição existente.

    Returns:
        DataFrame bronze com metadados de auditoria.
    """
    layer_path = str(BRONZE_PATH / "bolsa_familia")
    partition  = f"ano={year}/mes={month:02d}"
    full_path  = f"{layer_path}/{partition}"

    # ── Idempotência: pula se já existir ──────────────────────────────────
    if not overwrite and Path(full_path).exists():
        logger.info("Bronze já existe para %d/%02d — pulando ingestão", year, month)
        return spark.read.format("delta").load(layer_path).filter(
            (F.col("ano") == year) & (F.col("mes") == month)
        )

    # ── Download ──────────────────────────────────────────────────────────
    csv_content, source = _download_csv(year, month)

    # ── Transforma CSV → DataFrame ────────────────────────────────────────
    df_raw = _csv_to_dataframe(spark, csv_content)

    # ── Metadados de auditoria (Bronze) ───────────────────────────────────
    file_hash = hashlib.md5(csv_content.encode()).hexdigest()  # noqa: S324
    df_bronze = (
        df_raw
        .withColumn("_source",      F.lit(source))
        .withColumn("_ingested_at", F.lit(datetime.utcnow().isoformat()))
        .withColumn("_file_hash",   F.lit(file_hash))
        .withColumn("ano",          F.lit(year))
        .withColumn("mes",          F.lit(month))
    )

    row_count = df_bronze.count()
    logger.info(
        "Bronze — %d/%02d | linhas: %d | fonte: %s",
        year, month, row_count, source,
    )

    # ── Grava em Delta Lake ───────────────────────────────────────────────
    write_mode = "overwrite" if overwrite else "append"
    (
        df_bronze.write
        .format("delta")
        .mode(write_mode)
        .partitionBy("ano", "mes")
        .option("overwriteSchema", "true")
        .save(layer_path)
    )

    logger.info("Bronze gravado em: %s", layer_path)
    return df_bronze


if __name__ == "__main__":
    from src.utils.spark_session import get_spark, stop_spark
    from src.utils.config import DEFAULT_YEAR, DEFAULT_MONTH

    spark = get_spark("BronzeIngestion")
    try:
        df = ingest_bronze(spark, DEFAULT_YEAR, DEFAULT_MONTH, overwrite=True)
        df.printSchema()
        df.show(5, truncate=True)
    finally:
        stop_spark(spark)
