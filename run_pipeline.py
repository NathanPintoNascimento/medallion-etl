#!/usr/bin/env python3
"""
run_pipeline.py — Executor standalone do Pipeline Medallion
Alternativa ao Airflow para rodar o pipeline diretamente.

Uso:
    python run_pipeline.py --year 2023 --month 1
    python run_pipeline.py --year 2023 --all-months
    python run_pipeline.py --year 2023 --month 1 --layers bronze silver
    python run_pipeline.py --year 2023 --month 1 --no-overwrite
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from typing import Literal

from src.utils.logger import get_logger
from src.utils.spark_session import get_spark, stop_spark

logger = get_logger("run_pipeline", log_dir="data/logs")

Layer = Literal["bronze", "silver", "gold"]


# ─────────────────────────────────────────────────────────────────────────────
# Runner por camada
# ─────────────────────────────────────────────────────────────────────────────

def run_bronze(spark, year: int, month: int, overwrite: bool) -> int:
    from src.bronze.bronze_ingestion import ingest_bronze
    df = ingest_bronze(spark, year, month, overwrite=overwrite)
    return df.count()


def run_silver(spark, year: int, month: int, overwrite: bool) -> int:
    from src.silver.silver_transform import transform_silver
    df = transform_silver(spark, year, month, overwrite=overwrite)
    return df.count()


def run_gold(spark, year: int, month: int, overwrite: bool) -> dict:
    from src.gold.gold_aggregations import build_gold
    tables = build_gold(spark, year, month, overwrite=overwrite)
    return {k: v.count() for k, v in tables.items()}


# ─────────────────────────────────────────────────────────────────────────────
# Orquestrador principal
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline(
    year: int,
    month: int,
    layers: list[Layer],
    overwrite: bool = True,
) -> dict:
    """Executa as camadas solicitadas para um ano/mês."""
    results: dict = {"year": year, "month": month, "layers": {}}

    spark = get_spark("MedallionPipeline")

    try:
        for layer in layers:
            t0 = time.time()
            logger.info("═══ Iniciando %s (%d/%02d) ═══", layer.upper(), year, month)

            try:
                if layer == "bronze":
                    n = run_bronze(spark, year, month, overwrite)
                    results["layers"]["bronze"] = {"rows": n, "status": "ok"}
                    logger.info("Bronze OK — %d linhas (%.1fs)", n, time.time() - t0)

                elif layer == "silver":
                    n = run_silver(spark, year, month, overwrite)
                    results["layers"]["silver"] = {"rows": n, "status": "ok"}
                    logger.info("Silver OK — %d linhas (%.1fs)", n, time.time() - t0)

                elif layer == "gold":
                    tables = run_gold(spark, year, month, overwrite)
                    results["layers"]["gold"] = {"tables": tables, "status": "ok"}
                    logger.info("Gold OK — tabelas: %s (%.1fs)", tables, time.time() - t0)

            except Exception as exc:
                logger.error("%s FALHOU: %s", layer.upper(), exc, exc_info=True)
                results["layers"][layer] = {"status": "error", "error": str(exc)}
                raise  # propaga para interromper pipeline

    finally:
        stop_spark(spark)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline ETL Medallion — Bolsa Família",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python run_pipeline.py --year 2023 --month 1
  python run_pipeline.py --year 2023 --all-months
  python run_pipeline.py --year 2023 --month 3 --layers bronze silver
  python run_pipeline.py --year 2023 --month 1 --no-overwrite
        """,
    )
    parser.add_argument("--year",  type=int, default=2023,  help="Ano de competência")
    parser.add_argument("--month", type=int, default=1,     help="Mês de competência (1-12)")
    parser.add_argument("--all-months", action="store_true", help="Processa todos os 12 meses do ano")
    parser.add_argument(
        "--layers",
        nargs="+",
        choices=["bronze", "silver", "gold"],
        default=["bronze", "silver", "gold"],
        help="Camadas a executar (padrão: todas)",
    )
    parser.add_argument("--no-overwrite", action="store_true", help="Não sobrescreve dados existentes")
    return parser.parse_args()


def main() -> None:
    args      = parse_args()
    overwrite = not args.no_overwrite
    months    = list(range(1, 13)) if args.all_months else [args.month]

    print(f"""
╔══════════════════════════════════════════════════════╗
║        Pipeline ETL Medallion — Bolsa Família        ║
║        Fonte: dados.gov.br / Portal Transparência    ║
╠══════════════════════════════════════════════════════╣
║  Ano:      {args.year:<42}║
║  Meses:    {str(months):<42}║
║  Camadas:  {str(args.layers):<42}║
║  Overwrite:{str(overwrite):<42}║
╚══════════════════════════════════════════════════════╝
""")

    total_start = time.time()
    all_ok      = True

    for month in months:
        try:
            result = run_pipeline(args.year, month, args.layers, overwrite)
            print(f"  ✅ {args.year}/{month:02d} — {result['layers']}")
        except Exception as exc:
            print(f"  ❌ {args.year}/{month:02d} — ERRO: {exc}")
            all_ok = False

    elapsed = time.time() - total_start
    status  = "✅ CONCLUÍDO" if all_ok else "⚠️  COM ERROS"
    print(f"\n{status} em {elapsed:.1f}s")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
