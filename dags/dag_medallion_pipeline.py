"""
dag_medallion_pipeline.py — DAG Airflow do Pipeline Medallion
Orquestra: Bronze → Silver → Gold para os dados do Bolsa Família (dados.gov.br)

Schedule: diário às 06:00 BRT (09:00 UTC)
Catchup:  False (não reprocessa datas passadas automaticamente)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago


# ─────────────────────────────────────────────────────────────────────────────
# Defaults
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# Parâmetros do pipeline — altere via Airflow Variables ou conf no trigger
PIPELINE_YEAR  = int("{{ var.value.get('pipeline_year',  '2023') }}"
                     .replace("{{ var.value.get('pipeline_year',  '2023') }}", "2023"))
PIPELINE_MONTH = int("{{ var.value.get('pipeline_month', '1') }}"
                     .replace("{{ var.value.get('pipeline_month', '1') }}", "1"))


# ─────────────────────────────────────────────────────────────────────────────
# Funções Python chamadas pelas tasks
# ─────────────────────────────────────────────────────────────────────────────

def task_bronze(**context) -> None:
    """Task Bronze: ingestão raw dos dados."""
    from src.utils.spark_session import get_spark, stop_spark
    from src.bronze.bronze_ingestion import ingest_bronze

    conf  = context["dag_run"].conf or {}
    year  = int(conf.get("year",  PIPELINE_YEAR))
    month = int(conf.get("month", PIPELINE_MONTH))

    spark = get_spark("Bronze-Airflow")
    try:
        df = ingest_bronze(spark, year, month, overwrite=True)
        count = df.count()
        context["ti"].xcom_push(key="bronze_count", value=count)
        print(f"[Bronze] ✅ {count:,} registros ingeridos ({year}/{month:02d})")
    finally:
        stop_spark(spark)


def task_silver(**context) -> None:
    """Task Silver: limpeza, tipagem e deduplicação."""
    from src.utils.spark_session import get_spark, stop_spark
    from src.silver.silver_transform import transform_silver

    conf  = context["dag_run"].conf or {}
    year  = int(conf.get("year",  PIPELINE_YEAR))
    month = int(conf.get("month", PIPELINE_MONTH))

    spark = get_spark("Silver-Airflow")
    try:
        df    = transform_silver(spark, year, month, overwrite=True)
        count = df.count()
        context["ti"].xcom_push(key="silver_count", value=count)
        print(f"[Silver] ✅ {count:,} registros limpos ({year}/{month:02d})")
    finally:
        stop_spark(spark)


def task_gold(**context) -> None:
    """Task Gold: agregações analíticas."""
    from src.utils.spark_session import get_spark, stop_spark
    from src.gold.gold_aggregations import build_gold

    conf  = context["dag_run"].conf or {}
    year  = int(conf.get("year",  PIPELINE_YEAR))
    month = int(conf.get("month", PIPELINE_MONTH))

    spark = get_spark("Gold-Airflow")
    try:
        tables = build_gold(spark, year, month, overwrite=True)
        sizes  = {k: v.count() for k, v in tables.items()}
        context["ti"].xcom_push(key="gold_tables", value=sizes)
        print(f"[Gold] ✅ Tabelas geradas: {sizes}")
    finally:
        stop_spark(spark)


def task_data_quality(**context) -> None:
    """
    Task de Data Quality — validações básicas pós-pipeline.
    Falha a task (raise) se alguma checagem crítica falhar.
    """
    ti             = context["ti"]
    bronze_count   = ti.xcom_pull(task_ids="bronze_ingestion", key="bronze_count") or 0
    silver_count   = ti.xcom_pull(task_ids="silver_transform", key="silver_count") or 0
    gold_tables    = ti.xcom_pull(task_ids="gold_aggregations", key="gold_tables") or {}

    checks = []

    # 1. Bronze deve ter registros
    checks.append(("bronze > 0", bronze_count > 0))

    # 2. Silver não pode perder mais de 30% dos registros
    if bronze_count > 0:
        retention = silver_count / bronze_count
        checks.append((f"silver retention >= 70% ({retention:.1%})", retention >= 0.70))

    # 3. Todas as tabelas Gold devem existir e ter dados
    expected_tables = {
        "transferencias_por_uf_mes",
        "top_municipios_beneficios",
        "serie_temporal_brasil",
    }
    for t in expected_tables:
        n = gold_tables.get(t, 0)
        checks.append((f"gold.{t} > 0 ({n} rows)", n > 0))

    print("\n── Data Quality Report ──────────────────────────────")
    failed = []
    for name, passed in checks:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {status}  {name}")
        if not passed:
            failed.append(name)

    if failed:
        raise ValueError(f"Data Quality falhou em: {failed}")

    print("─────────────────────────────────────────────────────")
    print(f"  Todos os {len(checks)} checks passaram ✅")


# ─────────────────────────────────────────────────────────────────────────────
# DAG Definition
# ─────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="medallion_bolsa_familia",
    description="Pipeline ETL Medallion — Bolsa Família (dados.gov.br)",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 9 1 * *",   # Todo dia 1º do mês às 09:00 UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["medallion", "bronze", "silver", "gold", "dados-gov-br", "bolsa-familia"],
    doc_md="""
## Pipeline Medallion — Bolsa Família

Orquestra o pipeline ETL completo com arquitetura Medallion:

| Camada | Responsabilidade |
|--------|-----------------|
| 🟫 Bronze | Ingestão raw do CSV do Portal da Transparência |
| 🥈 Silver | Limpeza, tipagem, deduplicação |
| 🥇 Gold   | Agregações analíticas (UF, município, série temporal) |

### Trigger Manual com Parâmetros
```json
{ "year": 2023, "month": 6 }
```
""",
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    bronze = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=task_bronze,
        doc_md="**Bronze** — Download e ingestão raw em Delta Lake",
    )

    silver = PythonOperator(
        task_id="silver_transform",
        python_callable=task_silver,
        doc_md="**Silver** — Limpeza, tipagem e deduplicação",
    )

    gold = PythonOperator(
        task_id="gold_aggregations",
        python_callable=task_gold,
        doc_md="**Gold** — Agregações analíticas para BI/Dashboard",
    )

    dq = PythonOperator(
        task_id="data_quality_check",
        python_callable=task_data_quality,
        doc_md="**DQ** — Validações de qualidade pós-pipeline",
    )

    # ── Dependências: Bronze → Silver → Gold → DQ ────────────────────────
    start >> bronze >> silver >> gold >> dq >> end
