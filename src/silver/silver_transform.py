"""
silver_transform.py — Camada Silver
Responsabilidade: limpeza, tipagem canônica, deduplicação e padronização

Transformações aplicadas:
  ✔ Rename das colunas para snake_case
  ✔ Tipagem: valor_parcela string → double (trata vírgula decimal BR)
  ✔ Extração de ano/mês a partir de MES_COMPETENCIA
  ✔ Normalização de strings (upper, strip)
  ✔ Deduplicação por NIS + competência
  ✔ Remoção de registros com campos críticos nulos
  ✔ Coluna de auditoria: dt_ingestao, _silver_version
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

from src.utils.config import BRONZE_PATH, SILVER_PATH
from src.utils.logger import get_logger

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Mapeamento Bronze → Silver (rename de colunas)
# ─────────────────────────────────────────────────────────────────────────────
COLUMN_MAP = {
    "MES_COMPETENCIA":        "competencia",
    "UF":                     "uf",
    "CODIGO_MUNICIPIO_SIAFI": "codigo_municipio",
    "NOME_MUNICIPIO":         "nome_municipio",
    "CPF_FAVORECIDO":         "cpf_favorecido",      # mascarado pelo gov
    "NIS_FAVORECIDO":         "nis_favorecido",
    "NOME_FAVORECIDO":        "nome_favorecido",
    "VALOR_PARCELA":          "valor_parcela_raw",
}


def _rename_columns(df: DataFrame) -> DataFrame:
    """Renomeia colunas para snake_case conforme mapeamento."""
    for old, new in COLUMN_MAP.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df


def _parse_valor(df: DataFrame) -> DataFrame:
    """
    Converte valor_parcela_raw (string BR com vírgula) para Double.
    Ex: '218,00' → 218.0
    """
    return (
        df
        .withColumn(
            "valor_parcela",
            F.regexp_replace(F.col("valor_parcela_raw"), r"\.", "")   # remove milhar
             .pipe(lambda c: F.regexp_replace(c, ",", "."))            # vírgula → ponto
             .cast(DoubleType())
        )
        .drop("valor_parcela_raw")
    )


def _extract_periodo(df: DataFrame) -> DataFrame:
    """
    Extrai ano e mês a partir da coluna competencia (YYYYMM).
    Ex: '202301' → ano=2023, mes=1
    """
    return (
        df
        .withColumn("ano", F.substring("competencia", 1, 4).cast(IntegerType()))
        .withColumn("mes", F.substring("competencia", 5, 2).cast(IntegerType()))
    )


def _normalize_strings(df: DataFrame) -> DataFrame:
    """Upper + trim nas colunas de texto."""
    str_cols = ["uf", "nome_municipio", "nome_favorecido"]
    for col in str_cols:
        if col in df.columns:
            df = df.withColumn(col, F.upper(F.trim(F.col(col))))
    return df


def _filter_nulls(df: DataFrame) -> tuple[DataFrame, int]:
    """Remove linhas com campos críticos nulos."""
    critical = ["nis_favorecido", "uf", "valor_parcela", "ano", "mes"]
    before   = df.count()
    df_clean = df.dropna(subset=critical)
    dropped  = before - df_clean.count()
    if dropped:
        logger.warning("Silver — %d linhas removidas por campos críticos nulos", dropped)
    return df_clean, dropped


def _filter_invalid_values(df: DataFrame) -> DataFrame:
    """Remove valores absurdos (valor ≤ 0, UF inválida)."""
    ufs_validas = {
        "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS",
        "MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC",
        "SP","SE","TO",
    }
    ufs_lit = list(ufs_validas)
    return (
        df
        .filter(F.col("valor_parcela") > 0)
        .filter(F.col("uf").isin(ufs_lit))
    )


def _deduplicate(df: DataFrame) -> tuple[DataFrame, int]:
    """
    Deduplicação por chave natural: nis_favorecido + competência.
    Mantém o registro com maior valor_parcela em caso de duplicata.
    """
    window = (
        __import__("pyspark.sql.window", fromlist=["Window"])
        .Window.partitionBy("nis_favorecido", "ano", "mes")
        .orderBy(F.col("valor_parcela").desc())
    )
    before   = df.count()
    df_dedup = (
        df
        .withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )
    duplicates = before - df_dedup.count()
    if duplicates:
        logger.info("Silver — %d duplicatas removidas", duplicates)
    return df_dedup, duplicates


def transform_silver(
    spark: SparkSession,
    year: int,
    month: int,
    overwrite: bool = False,
) -> DataFrame:
    """
    Pipeline Silver: lê da camada Bronze e grava tabela Silver limpa.

    Args:
        spark:     SparkSession ativa.
        year:      Ano de competência.
        month:     Mês de competência.
        overwrite: Se True, sobrescreve partição existente.

    Returns:
        DataFrame Silver limpo e tipado.
    """
    bronze_path = str(BRONZE_PATH / "bolsa_familia")
    silver_path = str(SILVER_PATH / "bolsa_familia")
    partition   = f"ano={year}/mes={month:02d}"
    full_path   = f"{silver_path}/{partition}"

    # ── Idempotência ──────────────────────────────────────────────────────
    if not overwrite and Path(full_path).exists():
        logger.info("Silver já existe para %d/%02d — pulando", year, month)
        return spark.read.format("delta").load(silver_path).filter(
            (F.col("ano") == year) & (F.col("mes") == month)
        )

    # ── Lê Bronze ─────────────────────────────────────────────────────────
    logger.info("Silver — lendo Bronze %d/%02d", year, month)
    df = (
        spark.read
        .format("delta")
        .load(bronze_path)
        .filter((F.col("ano") == year) & (F.col("mes") == month))
        # Remove metadados Bronze desnecessários na Silver
        .drop("_source", "_ingested_at", "_file_hash")
    )

    raw_count = df.count()
    logger.info("Silver — %d registros bronze lidos", raw_count)

    # ── Transformações ────────────────────────────────────────────────────
    df = _rename_columns(df)
    df = _parse_valor(df)
    df = _extract_periodo(df)
    df = _normalize_strings(df)
    df = _filter_invalid_values(df)
    df, nulls_dropped    = _filter_nulls(df)
    df, dupes_removed    = _deduplicate(df)

    # ── Auditoria Silver ──────────────────────────────────────────────────
    df = (
        df
        .withColumn("dt_ingestao",      F.lit(datetime.utcnow().isoformat()).cast("timestamp"))
        .withColumn("_silver_version",  F.lit("1.0"))
        .drop("competencia", "cpf_favorecido")   # não necessários nas próximas camadas
    )

    silver_count = df.count()
    logger.info(
        "Silver — %d/%02d | raw: %d | silver: %d | nulos: %d | dupes: %d",
        year, month, raw_count, silver_count, nulls_dropped, dupes_removed,
    )

    # ── Grava Delta Lake ─────────────────────────────────────────────────
    write_mode = "overwrite" if overwrite else "append"
    (
        df.write
        .format("delta")
        .mode(write_mode)
        .partitionBy("ano", "mes")
        .option("overwriteSchema", "true")
        .save(silver_path)
    )

    logger.info("Silver gravado em: %s", silver_path)
    return df


if __name__ == "__main__":
    from src.utils.spark_session import get_spark, stop_spark
    from src.utils.config import DEFAULT_YEAR, DEFAULT_MONTH

    spark = get_spark("SilverTransform")
    try:
        df = transform_silver(spark, DEFAULT_YEAR, DEFAULT_MONTH, overwrite=True)
        df.printSchema()
        df.show(5, truncate=False)
        print(f"\nTotal Silver: {df.count():,} registros")
    finally:
        stop_spark(spark)
