# =============================================================================
# NYC TLC ETL — Load
# Responsabilidade: carregar Silver e Gold no PostgreSQL DW
# =============================================================================

import os
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Configuração JDBC
# -----------------------------------------------------------------------------

def get_jdbc_url() -> str:
    host     = os.getenv("POSTGRES_DW_HOST",     "postgres-dw")
    port     = os.getenv("POSTGRES_DW_PORT",     "5432")
    database = os.getenv("POSTGRES_DW_DB",       "nyc_tlc_dw")
    return f"jdbc:postgresql://{host}:{port}/{database}"


def get_jdbc_properties() -> dict:
    return {
        "user":                 os.getenv("POSTGRES_DW_USER",     "dw_user"),
        "password":             os.getenv("POSTGRES_DW_PASSWORD", "dw_password"),
        "driver":               "org.postgresql.Driver",
        "stringtype":           "unspecified",
        "batchsize":            "10000",
        "rewriteBatchedInserts": "true",
    }


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _write_jdbc(
    df: DataFrame,
    table: str,
    mode: str = "append"
) -> None:
    """
    Escreve um DataFrame no PostgreSQL via JDBC.

    Args:
        df:    DataFrame a ser escrito
        table: Nome da tabela no formato 'schema.tabela'
        mode:  'append' ou 'overwrite'
    """
    logger.info("Escrevendo na tabela %s (mode=%s) — %d linhas", table, mode, df.count())
    (
        df.write
        .jdbc(
            url=get_jdbc_url(),
            table=table,
            mode=mode,
            properties=get_jdbc_properties(),
        )
    )
    logger.info("Escrita concluída: %s", table)


def _delete_partition(spark: SparkSession, schema_table: str, year: int, month: int) -> None:
    """
    Remove registros da partição mês/ano antes de recarregar,
    garantindo idempotência (reprocessamento seguro).
    """
    import psycopg2

    host     = os.getenv("POSTGRES_DW_HOST",     "postgres-dw")
    port     = int(os.getenv("POSTGRES_DW_PORT", "5432"))
    database = os.getenv("POSTGRES_DW_DB",       "nyc_tlc_dw")
    user     = os.getenv("POSTGRES_DW_USER",     "dw_user")
    password = os.getenv("POSTGRES_DW_PASSWORD", "dw_password")

    conn = psycopg2.connect(
        host=host, port=port, dbname=database,
        user=user, password=password
    )
    try:
        with conn.cursor() as cur:
            sql = (
                f"DELETE FROM {schema_table} "
                f"WHERE reference_year = %s AND reference_month = %s"
            )
            cur.execute(sql, (year, month))
            deleted = cur.rowcount
            conn.commit()
            logger.info(
                "Partição removida de %s: %d linhas (%s/%s)",
                schema_table, deleted, year, month
            )
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Silver
# -----------------------------------------------------------------------------

def load_silver_clean(
    spark: SparkSession,
    df_valid: DataFrame,
    year: int,
    month: int
) -> int:
    """
    Carrega os registros válidos na tabela silver.yellow_taxi_clean.
    Remove a partição do mês antes de inserir (idempotência).

    Returns:
        Número de linhas carregadas.
    """
    _delete_partition(spark, "silver.yellow_taxi_clean", year, month)

    # Seleciona apenas as colunas do schema Silver (sem raw_id ainda)
    df_silver = df_valid.select(
        F.col("source_file"),
        F.col("reference_year"),
        F.col("reference_month"),
        F.col("vendor_id"),
        F.col("pickup_datetime"),
        F.col("dropoff_datetime"),
        F.col("passenger_count"),
        F.col("trip_distance"),
        F.col("rate_code_id"),
        F.col("store_and_fwd_flag"),
        F.col("pu_location_id"),
        F.col("do_location_id"),
        F.col("payment_type"),
        F.col("fare_amount"),
        F.col("extra"),
        F.col("mta_tax"),
        F.col("tip_amount"),
        F.col("tolls_amount"),
        F.col("improvement_surcharge"),
        F.col("total_amount"),
        F.col("congestion_surcharge"),
        F.col("airport_fee"),
        F.col("trip_duration_minutes"),
        F.col("trip_speed_mph"),
    )

    count = df_silver.count()
    _write_jdbc(df_silver, "silver.yellow_taxi_clean", mode="append")
    return count


def load_silver_rejected(
    spark: SparkSession,
    df_rejected: DataFrame,
    year: int,
    month: int
) -> int:
    """
    Carrega os registros rejeitados na tabela silver.rejected_records.

    Returns:
        Número de linhas rejeitadas carregadas.
    """
    _delete_partition(spark, "silver.rejected_records", year, month)
    count = df_rejected.count()
    if count == 0:
        logger.info("Nenhum registro rejeitado para carregar.")
        return 0
    _write_jdbc(df_rejected, "silver.rejected_records", mode="append")
    return count


# -----------------------------------------------------------------------------
# Gold
# -----------------------------------------------------------------------------

def build_trips_by_day(df_valid: DataFrame, year: int, month: int) -> DataFrame:
    """Agrega volume e receita por dia de corrida."""
    return df_valid.groupBy(
        F.to_date("pickup_datetime").alias("trip_date")
    ).agg(
        F.count("*")                        .alias("total_trips"),
        F.sum("passenger_count")            .alias("total_passengers"),
        F.round(F.sum("trip_distance"), 2)  .alias("total_distance_miles"),
        F.round(F.sum("fare_amount"), 2)    .alias("total_fare_amount"),
        F.round(F.sum("tip_amount"), 2)     .alias("total_tip_amount"),
        F.round(F.sum("total_amount"), 2)   .alias("total_revenue"),
        F.round(F.avg("fare_amount"), 2)    .alias("avg_fare_amount"),
        F.round(F.avg("trip_distance"), 2)  .alias("avg_trip_distance"),
        F.round(
            F.avg("trip_duration_minutes"), 2
        )                                   .alias("avg_trip_duration_min"),
    ).withColumn(
        "reference_year",  F.lit(year).cast("short")
    ).withColumn(
        "reference_month", F.lit(month).cast("short")
    )


def build_trips_by_zone(df_valid: DataFrame, year: int, month: int) -> DataFrame:
    """Agrega volume e receita por zona de embarque."""
    return df_valid.groupBy("pu_location_id").agg(
        F.count("*")                        .alias("total_trips"),
        F.round(F.sum("total_amount"), 2)   .alias("total_revenue"),
        F.round(F.avg("fare_amount"), 2)    .alias("avg_fare_amount"),
        F.round(F.avg("trip_distance"), 2)  .alias("avg_trip_distance"),
        F.round(F.avg("tip_amount"), 2)     .alias("avg_tip_amount"),
    ).withColumn(
        "reference_year",  F.lit(year).cast("short")
    ).withColumn(
        "reference_month", F.lit(month).cast("short")
    )


def build_payment_summary(df_valid: DataFrame, year: int, month: int) -> DataFrame:
    """Agrega distribuição por tipo de pagamento com descrição."""
    payment_labels = {
        1: "Credit card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip",
    }

    total_trips = df_valid.count()

    df = df_valid.groupBy("payment_type").agg(
        F.count("*")                        .alias("total_trips"),
        F.round(F.sum("total_amount"), 2)   .alias("total_revenue"),
        F.round(F.sum("tip_amount"), 2)     .alias("total_tip_amount"),
        F.round(F.avg("tip_amount"), 2)     .alias("avg_tip_amount"),
    ).withColumn(
        "pct_of_total_trips",
        F.round(F.col("total_trips") / F.lit(total_trips) * 100, 2)
    )

    # Mapeia código para descrição
    mapping_expr = F.create_map(
        *[x for pair in
          [(F.lit(k), F.lit(v)) for k, v in payment_labels.items()]
          for x in pair]
    )
    df = df.withColumn(
        "payment_type_desc",
        mapping_expr[F.col("payment_type")]
    ).withColumn(
        "reference_year",  F.lit(year).cast("short")
    ).withColumn(
        "reference_month", F.lit(month).cast("short")
    )

    return df


def build_hourly_demand(df_valid: DataFrame, year: int, month: int) -> DataFrame:
    """Agrega demanda de corridas por hora do dia."""
    import calendar
    days_in_month = calendar.monthrange(year, month)[1]

    return df_valid.withColumn(
        "hour_of_day", F.hour("pickup_datetime")
    ).groupBy("hour_of_day").agg(
        F.count("*")                        .alias("total_trips"),
        F.round(
            F.count("*") / F.lit(days_in_month), 2
        )                                   .alias("avg_trips_per_day"),
        F.round(F.sum("total_amount"), 2)   .alias("total_revenue"),
        F.round(F.avg("fare_amount"), 2)    .alias("avg_fare_amount"),
    ).withColumn(
        "reference_year",  F.lit(year).cast("short")
    ).withColumn(
        "reference_month", F.lit(month).cast("short")
    )


def build_zone_avg_fare(df_valid: DataFrame, year: int, month: int) -> DataFrame:
    """Ticket médio e métricas por zona de embarque."""
    return df_valid.groupBy("pu_location_id").agg(
        F.count("*")                                .alias("total_trips"),
        F.round(F.avg("fare_amount"), 2)            .alias("avg_fare_amount"),
        F.round(F.avg("total_amount"), 2)           .alias("avg_total_amount"),
        F.round(F.avg("tip_amount"), 2)             .alias("avg_tip_amount"),
        F.round(F.avg("trip_distance"), 2)          .alias("avg_trip_distance"),
        F.round(F.avg("trip_duration_minutes"), 2)  .alias("avg_duration_minutes"),
    ).withColumn(
        "reference_year",  F.lit(year).cast("short")
    ).withColumn(
        "reference_month", F.lit(month).cast("short")
    )


def load_gold(
    spark: SparkSession,
    df_valid: DataFrame,
    year: int,
    month: int
) -> dict:
    """
    Constrói e carrega todas as tabelas Gold no PostgreSQL.
    Remove a partição do mês antes de inserir (idempotência).

    Returns:
        Dicionário com contagem de linhas por tabela Gold.
    """
    gold_tables = {
        "gold.trips_by_day":     build_trips_by_day(df_valid, year, month),
        "gold.trips_by_zone":    build_trips_by_zone(df_valid, year, month),
        "gold.payment_summary":  build_payment_summary(df_valid, year, month),
        "gold.hourly_demand":    build_hourly_demand(df_valid, year, month),
        "gold.zone_avg_fare":    build_zone_avg_fare(df_valid, year, month),
    }

    counts = {}
    for table, df_gold in gold_tables.items():
        _delete_partition(spark, table, year, month)
        count = df_gold.count()
        _write_jdbc(df_gold, table, mode="append")
        counts[table] = count
        logger.info("Gold carregado: %s — %d linhas", table, count)

    return counts