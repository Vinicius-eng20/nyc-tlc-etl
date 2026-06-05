# =============================================================================
# NYC TLC ETL — DAG Principal
# Orquestra: extract -> clean -> validate -> load silver -> load gold
#
# Trigger manual:
#   {"year": 2024, "month": 1}
#
# Schedule automático: todo dia 15 do mês seguinte ao de referência
# (o TLC publica os dados com ~2 meses de atraso)
# =============================================================================

from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# =============================================================================
# Argumentos padrão da DAG
# =============================================================================

DEFAULT_ARGS = {
    "owner":                     "nyc-tlc-etl",
    "depends_on_past":           False,
    "retries":                   2,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure":          False,
    "email_on_retry":            False,
}

# =============================================================================
# Helpers de parametrização
# =============================================================================

def _get_year_month(context: dict) -> tuple[int, int]:
    """
    Resolve year/month a partir de:
    1. dag_run.conf  (trigger manual com {"year": 2024, "month": 1})
    2. data_interval_start (execução agendada — usa o mês de referência)
    """
    conf = context["dag_run"].conf or {}

    if "year" in conf and "month" in conf:
        year  = int(conf["year"])
        month = int(conf["month"])
        logger.info("Parâmetros via conf: year=%s, month=%s", year, month)
        return year, month

    ds: datetime = context["data_interval_start"]
    year  = ds.year
    month = ds.month
    logger.info("Parâmetros via schedule: year=%s, month=%s", year, month)
    return year, month


# =============================================================================
# Funções das Tasks
# =============================================================================

def task_download(**context) -> str:
    """
    Task 1 — Extract
    Baixa o arquivo Parquet mensal do NYC TLC.
    Retorna o caminho local do arquivo via XCom.
    """
    import sys
    sys.path.insert(0, "/opt/airflow")

    from etl.extract.download_tlc import download_parquet

    year, month = _get_year_month(context)
    local_path  = download_parquet(year, month)

    context["ti"].xcom_push(key="raw_file_path", value=local_path)
    context["ti"].xcom_push(key="year",          value=year)
    context["ti"].xcom_push(key="month",         value=month)

    return local_path


def task_validate_raw_file(**context) -> None:
    """
    Task 2 — Validate Raw File
    Verifica se o arquivo baixado existe e tem tamanho mínimo esperado.
    Falha a task (raise) se o arquivo for inválido.
    """
    import sys
    sys.path.insert(0, "/opt/airflow")

    from etl.extract.download_tlc import validate_file

    year  = context["ti"].xcom_pull(key="year",  task_ids="download_parquet")
    month = context["ti"].xcom_pull(key="month", task_ids="download_parquet")

    result = validate_file(year, month)

    if not result["is_valid"]:
        raise ValueError(
            f"Arquivo inválido para {year}/{month:02d}: "
            f"path={result['path']}, size={result['size_mb']} MB"
        )

    logger.info(
        "Arquivo validado com sucesso: %.2f MB — %s",
        result["size_mb"], result["path"]
    )


def task_spark_clean(**context) -> str:
    """
    Task 3 — Spark Clean
    Executa a limpeza do Parquet bruto com PySpark.
    Persiste o DataFrame limpo em staging e empurra o path via XCom.
    """
    import sys
    sys.path.insert(0, "/opt/airflow")

    from etl.transform.spark_clean import get_spark_session, run_clean, save_staging

    raw_file = context["ti"].xcom_pull(key="raw_file_path", task_ids="download_parquet")
    year     = context["ti"].xcom_pull(key="year",          task_ids="download_parquet")
    month    = context["ti"].xcom_pull(key="month",         task_ids="download_parquet")

    spark = get_spark_session(app_name=f"nyc-tlc-clean-{year}-{month:02d}")

    try:
        df_clean     = run_clean(spark, raw_file, year, month)
        staging_path = save_staging(df_clean, year, month)
    finally:
        spark.stop()

    context["ti"].xcom_push(key="staging_path", value=staging_path)
    return staging_path


def task_spark_validate(**context) -> dict:
    """
    Task 4 — Spark Validate
    Aplica regras de validação, separa válidos/rejeitados.
    Empurra estatísticas de validação via XCom.
    """
    import sys
    sys.path.insert(0, "/opt/airflow")

    from etl.transform.spark_clean    import get_spark_session
    from etl.transform.spark_validate import run_validate

    staging_path = context["ti"].xcom_pull(key="staging_path", task_ids="spark_clean")
    year         = context["ti"].xcom_pull(key="year",          task_ids="download_parquet")
    month        = context["ti"].xcom_pull(key="month",         task_ids="download_parquet")

    spark = get_spark_session(app_name=f"nyc-tlc-validate-{year}-{month:02d}")

    try:
        df_staging             = spark.read.parquet(staging_path)
        df_valid, df_rejected, stats = run_validate(df_staging, year, month)

        valid_path    = staging_path.replace("staging", "staging_valid")
        rejected_path = staging_path.replace("staging", "staging_rejected")

        df_valid.coalesce(4).write.mode("overwrite").parquet(valid_path)
        df_rejected.coalesce(2).write.mode("overwrite").parquet(rejected_path)

    finally:
        spark.stop()

    context["ti"].xcom_push(key="valid_path",    value=valid_path)
    context["ti"].xcom_push(key="rejected_path", value=rejected_path)
    context["ti"].xcom_push(key="stats",         value=stats)

    logger.info("Estatísticas de validação: %s", stats)
    return stats


def task_load_silver(**context) -> dict:
    """
    Task 5 — Load Silver
    Carrega registros válidos e rejeitados no PostgreSQL (camada Silver).
    """
    import sys
    sys.path.insert(0, "/opt/airflow")

    from etl.transform.spark_clean      import get_spark_session
    from etl.transform.spark_validate   import rename_rejected_columns
    from etl.load.postgres_loader       import load_silver_clean, load_silver_rejected

    valid_path    = context["ti"].xcom_pull(key="valid_path",    task_ids="spark_validate")
    rejected_path = context["ti"].xcom_pull(key="rejected_path", task_ids="spark_validate")
    year          = context["ti"].xcom_pull(key="year",          task_ids="download_parquet")
    month         = context["ti"].xcom_pull(key="month",         task_ids="download_parquet")

    spark = get_spark_session(app_name=f"nyc-tlc-load-silver-{year}-{month:02d}")

    try:
        df_valid    = spark.read.parquet(valid_path)
        df_rejected = spark.read.parquet(rejected_path)
        df_rejected = rename_rejected_columns(df_rejected)

        n_valid    = load_silver_clean(spark, df_valid, year, month)
        n_rejected = load_silver_rejected(spark, df_rejected, year, month)

    finally:
        spark.stop()

    result = {"loaded_valid": n_valid, "loaded_rejected": n_rejected}
    logger.info("Silver carregado: %s", result)
    return result


def task_load_gold(**context) -> dict:
    """
    Task 6 — Load Gold
    Constrói as agregações e carrega as tabelas Gold no PostgreSQL.
    """
    import sys
    sys.path.insert(0, "/opt/airflow")

    from etl.transform.spark_clean import get_spark_session
    from etl.load.postgres_loader  import load_gold

    valid_path = context["ti"].xcom_pull(key="valid_path", task_ids="spark_validate")
    year       = context["ti"].xcom_pull(key="year",       task_ids="download_parquet")
    month      = context["ti"].xcom_pull(key="month",      task_ids="download_parquet")

    spark = get_spark_session(app_name=f"nyc-tlc-load-gold-{year}-{month:02d}")

    try:
        df_valid = spark.read.parquet(valid_path)
        counts   = load_gold(spark, df_valid, year, month)
    finally:
        spark.stop()

    logger.info("Gold carregado: %s", counts)
    return counts


def task_notify(**context) -> None:
    """
    Task 7 — Notify
    Consolida e loga o resumo completo da execução do pipeline.
    """
    year  = context["ti"].xcom_pull(key="year",  task_ids="download_parquet")
    month = context["ti"].xcom_pull(key="month", task_ids="download_parquet")

    stats        = context["ti"].xcom_pull(key="stats", task_ids="spark_validate") or {}
    silver_loads = context["ti"].xcom_pull(task_ids="load_silver") or {}
    gold_loads   = context["ti"].xcom_pull(task_ids="load_gold")   or {}

    logger.info("=" * 60)
    logger.info("PIPELINE CONCLUÍDO — %s/%02d", year, month)
    logger.info("=" * 60)
    logger.info("Registros totais lidos     : %s", stats.get("total",          "N/A"))
    logger.info("Registros válidos          : %s", stats.get("total_valid",    "N/A"))
    logger.info("Registros rejeitados       : %s", stats.get("total_rejected", "N/A"))
    logger.info("Percentual rejeitado       : %s%%", stats.get("pct_rejected", "N/A"))
    logger.info("Silver clean carregado     : %s linhas", silver_loads.get("loaded_valid",    "N/A"))
    logger.info("Silver rejected carregado  : %s linhas", silver_loads.get("loaded_rejected", "N/A"))
    logger.info("Gold tables carregadas     :")
    for table, count in (gold_loads or {}).items():
        logger.info("  %-35s %s linhas", table, count)
    logger.info("=" * 60)


# =============================================================================
# Definição da DAG
# =============================================================================

with DAG(
    dag_id="tlc_yellow_taxi_pipeline",
    description="Pipeline ETL mensal — NYC TLC Yellow Taxi (Bronze -> Silver -> Gold)",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 15 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["nyc-tlc", "etl", "pyspark", "yellow-taxi"],
    doc_md="""
## NYC TLC Yellow Taxi Pipeline

Pipeline ETL mensal que processa os dados públicos de corridas de táxi
amarelo da cidade de Nova York.

### Fluxo
download -> validate_raw -> spark_clean -> spark_validate -> [load_silver | load_gold] -> notify

### Trigger manual
```json
{"year": 2024, "month": 1}
```

### Reprocessamento via CLI
```bash
airflow dags trigger tlc_yellow_taxi_pipeline --conf '{"year": 2024, "month": 1}'
```
    """,
) as dag:

    download = PythonOperator(
        task_id="download_parquet",
        python_callable=task_download,
    )

    validate_raw = PythonOperator(
        task_id="validate_raw_file",
        python_callable=task_validate_raw_file,
    )

    spark_clean = PythonOperator(
        task_id="spark_clean",
        python_callable=task_spark_clean,
        execution_timeout=timedelta(minutes=60),
    )

    spark_validate = PythonOperator(
        task_id="spark_validate",
        python_callable=task_spark_validate,
        execution_timeout=timedelta(minutes=30),
    )

    load_silver = PythonOperator(
        task_id="load_silver",
        python_callable=task_load_silver,
        execution_timeout=timedelta(minutes=30),
    )

    load_gold = PythonOperator(
        task_id="load_gold",
        python_callable=task_load_gold,
        execution_timeout=timedelta(minutes=30),
    )

    notify = PythonOperator(
        task_id="notify",
        python_callable=task_notify,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Dependências
    download >> validate_raw >> spark_clean >> spark_validate
    spark_validate >> [load_silver, load_gold]
    [load_silver, load_gold] >> notify
