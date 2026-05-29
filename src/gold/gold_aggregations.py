"""
gold_aggregations.py — Camada Gold
Responsabilidade: agregações analíticas prontas para dashboard/BI

Tabelas geradas:
  1. transferencias_por_uf_mes   — total de benefícios e valor por UF/mês
  2. top_municipios_beneficios   — ranking dos municípios com mais benefícios
  3. serie_temporal_brasil       — série temporal mensal nacional + variação %

Saída dupla:
  ✔ Delta Lake (camada Gold local)
  ✔ PostgreSQL schema "gold" (para queries SQL / dashboards)
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F

from src.utils.config import SILVER_PATH, GOLD_PATH, PG_CONFIG
from src.utils.logger import get_logger

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _write_delta(df: DataFrame, table: str, overwrite: bool) -> None:
    path = str(GOLD_PATH / table)
    mode = "overwrite" if overwrite else "append"
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .save(path)
    )
    logger.info("Gold Delta gravado: %s (%d linhas)", path, df.count())


def _write_postgres(df: DataFrame, table: str) -> None:
    """Escreve no PostgreSQL via JDBC (modo overwrite)."""
    try:
        (
            df.write
            .format("jdbc")
            .option("url", PG_CONFIG.jdbc_url)
            .option("dbtable", f"{PG_CONFIG.schema}.{table}")
            .option("user", PG_CONFIG.user)
            .option("password", PG_CONFIG.password)
            .option("driver", "org.postgresql.Driver")
            .mode("overwrite")
            .save()
        )
        logger.info("Gold PostgreSQL gravado: gold.%s", table)
    except Exception as exc:
        logger.warning("Falha ao gravar no PostgreSQL (%s) — continuando sem PG", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Agregação 1 — Por UF e Mês
# ─────────────────────────────────────────────────────────────────────────────

def _agg_por_uf_mes(df_silver: DataFrame) -> DataFrame:
    """Total de benefícios e valor por UF/mês."""
    return (
        df_silver
        .groupBy("ano", "mes", "uf")
        .agg(
            F.count("nis_favorecido").alias("total_beneficios"),
            F.round(F.sum("valor_parcela"), 2).alias("total_valor_brl"),
            F.round(F.avg("valor_parcela"), 2).alias("media_valor_brl"),
        )
        .orderBy("ano", "mes", F.col("total_beneficios").desc())
    )


# ─────────────────────────────────────────────────────────────────────────────
# Agregação 2 — Ranking de Municípios
# ─────────────────────────────────────────────────────────────────────────────

def _agg_top_municipios(df_silver: DataFrame) -> DataFrame:
    """Top municípios por total de benefícios com ranking nacional."""
    w = Window.orderBy(F.col("total_beneficios").desc())

    return (
        df_silver
        .groupBy("ano", "nome_municipio", "uf", "codigo_municipio")
        .agg(
            F.count("nis_favorecido").alias("total_beneficios"),
            F.round(F.sum("valor_parcela"), 2).alias("total_valor_brl"),
        )
        .withColumn("ranking_nacional", F.row_number().over(w))
        .orderBy("ranking_nacional")
    )


# ─────────────────────────────────────────────────────────────────────────────
# Agregação 3 — Série Temporal Brasil
# ─────────────────────────────────────────────────────────────────────────────

def _agg_serie_temporal(df_silver: DataFrame) -> DataFrame:
    """Série temporal mensal nacional com variação percentual mês a mês."""
    w_lag = Window.orderBy("ano_mes")

    df_mensal = (
        df_silver
        .withColumn("ano_mes", F.concat_ws("-", "ano", F.lpad(F.col("mes"), 2, "0")))
        .groupBy("ano_mes")
        .agg(
            F.count("nis_favorecido").alias("total_beneficios"),
            F.round(F.sum("valor_parcela"), 2).alias("total_valor_brl"),
        )
        .orderBy("ano_mes")
    )

    df_com_lag = df_mensal.withColumn(
        "total_beneficios_anterior",
        F.lag("total_beneficios").over(w_lag),
    )

    return (
        df_com_lag
        .withColumn(
            "variacao_pct",
            F.when(
                F.col("total_beneficios_anterior").isNotNull()
                & (F.col("total_beneficios_anterior") != 0),
                F.round(
                    (F.col("total_beneficios") - F.col("total_beneficios_anterior"))
                    / F.col("total_beneficios_anterior")
                    * 100,
                    4,
                )
            ).otherwise(F.lit(None).cast("double"))
        )
        .drop("total_beneficios_anterior")
    )


# ─────────────────────────────────────────────────────────────────────────────
# Ponto de entrada principal
# ─────────────────────────────────────────────────────────────────────────────

def build_gold(
    spark: SparkSession,
    year: int,
    month: int | None = None,
    overwrite: bool = True,
) -> dict[str, DataFrame]:
    """
    Constrói todas as tabelas Gold a partir da camada Silver.

    Args:
        spark:    SparkSession ativa.
        year:     Ano base (obrigatório).
        month:    Mês específico ou None para todo o ano.
        overwrite: Sobrescreve tabelas Gold existentes.

    Returns:
        Dict com {nome_tabela: DataFrame}.
    """
    silver_path = str(SILVER_PATH / "bolsa_familia")

    logger.info("Gold — carregando Silver (ano=%d, mes=%s)", year, month)
    df_silver = spark.read.format("delta").load(silver_path)

    # Filtra período
    df_silver = df_silver.filter(F.col("ano") == year)
    if month is not None:
        df_silver = df_silver.filter(F.col("mes") == month)

    silver_count = df_silver.count()
    logger.info("Gold — %d registros Silver carregados", silver_count)

    if silver_count == 0:
        logger.error("Nenhum dado Silver encontrado para o período informado!")
        return {}

    # Cache para reutilizar nas 3 agregações
    df_silver.cache()

    results: dict[str, DataFrame] = {}

    # ── Tabela 1 ──────────────────────────────────────────────────────────
    logger.info("Gold — computando transferencias_por_uf_mes")
    df_uf_mes = _agg_por_uf_mes(df_silver)
    _write_delta(df_uf_mes, "transferencias_por_uf_mes", overwrite)
    _write_postgres(df_uf_mes, "transferencias_por_uf_mes")
    results["transferencias_por_uf_mes"] = df_uf_mes

    # ── Tabela 2 ──────────────────────────────────────────────────────────
    logger.info("Gold — computando top_municipios_beneficios")
    df_municipios = _agg_top_municipios(df_silver)
    _write_delta(df_municipios, "top_municipios_beneficios", overwrite)
    _write_postgres(df_municipios, "top_municipios_beneficios")
    results["top_municipios_beneficios"] = df_municipios

    # ── Tabela 3 ──────────────────────────────────────────────────────────
    logger.info("Gold — computando serie_temporal_brasil")
    df_serie = _agg_serie_temporal(df_silver)
    _write_delta(df_serie, "serie_temporal_brasil", overwrite)
    _write_postgres(df_serie, "serie_temporal_brasil")
    results["serie_temporal_brasil"] = df_serie

    df_silver.unpersist()
    logger.info("Gold concluído — %d tabelas geradas", len(results))
    return results


if __name__ == "__main__":
    from src.utils.spark_session import get_spark, stop_spark
    from src.utils.config import DEFAULT_YEAR, DEFAULT_MONTH

    spark = get_spark("GoldAggregations")
    try:
        tables = build_gold(spark, DEFAULT_YEAR, DEFAULT_MONTH, overwrite=True)
        for name, df in tables.items():
            print(f"\n{'─'*60}")
            print(f"📊 {name}")
            df.show(10, truncate=False)
    finally:
        stop_spark(spark)
