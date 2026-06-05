# =============================================================================
# NYC TLC ETL — Transform: Limpeza
# Responsabilidade: aplicar tipagem, tratar nulos e derivar campos
# =============================================================================

import os
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DoubleType, StringType, TimestampType
)

logger = logging.getLogger(__name__)

STAGING_DATA_PATH = os.getenv("STAGING_DATA_PATH", "/opt/airflow/data/staging")

# Schema explícito para garantir tipagem correta na leitura do Parquet bruto
RAW_SCHEMA = StructType([
    StructField("VendorID",                 IntegerType(),   True),
    StructField("tpep_pickup_datetime",     TimestampType(), True),
    StructField("tpep_dropoff_datetime",    TimestampType(), True),
    StructField("passenger_count",          DoubleType(),    True),
    StructField("trip_distance",            DoubleType(),    True),
    StructField("RatecodeID",               DoubleType(),    True),
    StructField("store_and_fwd_flag",       StringType(),    True),
    StructField("PULocationID",             IntegerType(),   True),
    StructField("DOLocationID",             IntegerType(),   True),
    StructField("payment_type",             DoubleType(),    True),
    StructField("fare_amount",              DoubleType(),    True),
    StructField("extra",                    DoubleType(),    True),
    StructField("mta_tax",                  DoubleType(),    True),
    StructField("tip_amount",               DoubleType(),    True),
    StructField("tolls_amount",             DoubleType(),    True),
    StructField("improvement_surcharge",    DoubleType(),    True),
    StructField("total_amount",             DoubleType(),    True),
    StructField("congestion_surcharge",     DoubleType(),    True),
    StructField("Airport_fee",              DoubleType(),    True),
])


def get_spark_session(app_name: str = "nyc-tlc-clean") -> SparkSession:
    """Cria ou recupera uma SparkSession conectada ao cluster."""
    spark_master = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "2g")

    return (
        SparkSession.builder
        .appName(app_name)
        .master(spark_master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.inferSchema", "false")
        # Necessário para escrita no PostgreSQL via JDBC
        .config(
            "spark.jars",
            "/opt/bitnami/spark/jars/postgresql-42.7.4.jar"
        )
        .getOrCreate()
    )


def read_raw_parquet(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Lê o arquivo Parquet bruto com schema explícito.

    Args:
        spark:     SparkSession ativa
        file_path: Caminho local do arquivo Parquet bruto

    Returns:
        DataFrame com os dados brutos
    """
    logger.info("Lendo Parquet bruto: %s", file_path)
    df = (
        spark.read
        .schema(RAW_SCHEMA)
        .parquet(file_path)
    )
    logger.info("Linhas lidas: %d", df.count())
    return df


def normalize_columns(df: DataFrame) -> DataFrame:
    """
    Renomeia colunas para snake_case padronizado e remove
    variações de capitalização presentes nos arquivos do TLC.
    """
    return df.select(
        F.col("VendorID").alias("vendor_id"),
        F.col("tpep_pickup_datetime").alias("pickup_datetime"),
        F.col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        F.col("passenger_count"),
        F.col("trip_distance"),
        F.col("RatecodeID").alias("rate_code_id"),
        F.col("store_and_fwd_flag"),
        F.col("PULocationID").alias("pu_location_id"),
        F.col("DOLocationID").alias("do_location_id"),
        F.col("payment_type"),
        F.col("fare_amount"),
        F.col("extra"),
        F.col("mta_tax"),
        F.col("tip_amount"),
        F.col("tolls_amount"),
        F.col("improvement_surcharge"),
        F.col("total_amount"),
        F.col("congestion_surcharge"),
        F.col("Airport_fee").alias("airport_fee"),
    )


def cast_types(df: DataFrame) -> DataFrame:
    """
    Aplica casting para tipos definitivos após normalização de colunas.
    Garante que campos numéricos inteiros não fiquem como double/float.
    """
    return df.withColumn(
        "vendor_id",        F.col("vendor_id").cast("short")
    ).withColumn(
        "passenger_count",  F.col("passenger_count").cast("short")
    ).withColumn(
        "rate_code_id",     F.col("rate_code_id").cast("short")
    ).withColumn(
        "pu_location_id",   F.col("pu_location_id").cast("short")
    ).withColumn(
        "do_location_id",   F.col("do_location_id").cast("short")
    ).withColumn(
        "payment_type",     F.col("payment_type").cast("short")
    ).withColumn(
        "fare_amount",      F.round(F.col("fare_amount"), 2)
    ).withColumn(
        "tip_amount",       F.round(F.col("tip_amount"), 2)
    ).withColumn(
        "total_amount",     F.round(F.col("total_amount"), 2)
    ).withColumn(
        "trip_distance",    F.round(F.col("trip_distance"), 2)
    )


def treat_nulls(df: DataFrame) -> DataFrame:
    """
    Trata campos nulos com valores padrão seguros.
    Campos críticos (pickup, dropoff, locations, fare) são deixados nulos
    para serem capturados pela validação e enviados para rejected_records.
    """
    return df.fillna({
        "vendor_id":                0,
        "store_and_fwd_flag":       "N",
        "rate_code_id":             1,
        "payment_type":             5,      # 5 = Unknown
        "extra":                    0.0,
        "mta_tax":                  0.0,
        "tip_amount":               0.0,
        "tolls_amount":             0.0,
        "improvement_surcharge":    0.0,
        "congestion_surcharge":     0.0,
        "airport_fee":              0.0,
    })


def derive_fields(df: DataFrame) -> DataFrame:
    """
    Calcula campos derivados úteis para análise:
    - trip_duration_minutes: duração da corrida em minutos
    - trip_speed_mph: velocidade média em mph
    """
    df = df.withColumn(
        "trip_duration_minutes",
        F.round(
            (
                F.unix_timestamp("dropoff_datetime") -
                F.unix_timestamp("pickup_datetime")
            ) / 60.0,
            2
        )
    )

    # Velocidade apenas quando duração e distância são válidas (evita divisão por zero)
    df = df.withColumn(
        "trip_speed_mph",
        F.when(
            (F.col("trip_duration_minutes") > 0) & (F.col("trip_distance") > 0),
            F.round(
                F.col("trip_distance") / (F.col("trip_duration_minutes") / 60.0),
                2
            )
        ).otherwise(F.lit(None))
    )

    return df


def add_metadata(df: DataFrame, year: int, month: int, source_file: str) -> DataFrame:
    """Adiciona colunas de rastreabilidade ao DataFrame."""
    return df.withColumn(
        "reference_year",  F.lit(year).cast("short")
    ).withColumn(
        "reference_month", F.lit(month).cast("short")
    ).withColumn(
        "source_file",     F.lit(source_file)
    )


def save_staging(df: DataFrame, year: int, month: int) -> str:
    """
    Salva o DataFrame limpo (ainda não validado) no diretório staging
    particionado por ano e mês.

    Returns:
        Caminho do diretório de saída.
    """
    from pathlib import Path

    month_str = str(month).zfill(2)
    output_path = (
        Path(STAGING_DATA_PATH)
        / f"year={year}"
        / f"month={month_str}"
    )
    output_path.mkdir(parents=True, exist_ok=True)

    out_str = str(output_path)
    logger.info("Salvando staging em: %s", out_str)

    (
        df.coalesce(4)         # Evita arquivos minúsculos — 4 parts por mês
        .write
        .mode("overwrite")
        .parquet(out_str)
    )

    logger.info("Staging salvo com sucesso.")
    return out_str


def run_clean(spark: SparkSession, file_path: str, year: int, month: int) -> DataFrame:
    """
    Orquestra todas as etapas de limpeza em sequência.

    Args:
        spark:     SparkSession ativa
        file_path: Caminho do Parquet bruto
        year:      Ano de referência
        month:     Mês de referência

    Returns:
        DataFrame limpo com campos derivados e metadados.
    """
    import os

    source_file = os.path.basename(file_path)

    logger.info("=== Iniciando limpeza: %s/%s ===", year, month)

    df = read_raw_parquet(spark, file_path)
    df = normalize_columns(df)
    df = cast_types(df)
    df = treat_nulls(df)
    df = derive_fields(df)
    df = add_metadata(df, year, month, source_file)

    logger.info("=== Limpeza concluída: %d linhas ===", df.count())
    return df