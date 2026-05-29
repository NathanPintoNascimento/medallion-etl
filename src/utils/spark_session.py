"""
spark_session.py — Factory para SparkSession com Delta Lake
"""

from __future__ import annotations
import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def get_spark(app_name: str = "MedallionETL") -> SparkSession:
    """
    Cria (ou recupera) uma SparkSession configurada para Delta Lake.
    Baixa automaticamente os JARs via Maven na primeira execução.
    """
    from src.utils.config import SPARK_CONF, DELTA_VERSION, SCALA_VERSION

    delta_pkg = (
        f"io.delta:delta-spark_{SCALA_VERSION}:{DELTA_VERSION},"
        f"io.delta:delta-storage:{DELTA_VERSION}"
    )

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", delta_pkg)
    )

    for key, value in SPARK_CONF.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("SparkSession criada — versão %s", spark.version)
    return spark


def stop_spark(spark: SparkSession) -> None:
    """Para a SparkSession de forma segura."""
    try:
        spark.stop()
        logger.info("SparkSession encerrada.")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Erro ao parar Spark: %s", exc)
