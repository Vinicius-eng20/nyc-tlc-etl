# =============================================================================
# NYC TLC ETL — Transform: Validação
# Responsabilidade: separar registros válidos dos rejeitados com motivo
# =============================================================================

import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

# Range válido de LocationIDs segundo o dicionário do TLC
LOCATION_ID_MIN = 1
LOCATION_ID_MAX = 263

# Range válido de RatecodeID segundo o dicionário do TLC
RATE_CODE_MIN = 1
RATE_CODE_MAX = 6

# Limites de negócio para distância e passageiros
MAX_PASSENGERS   = 8
MAX_DISTANCE_MPH = 500.0


def build_rejection_flags(df: DataFrame, year: int, month: int) -> DataFrame:
    """
    Adiciona uma coluna 'rejection_reason' ao DataFrame.
    Registros válidos terão rejection_reason = None.
    Registros inválidos terão a primeira razão de rejeição encontrada.

    A ordem das verificações segue da mais crítica para a menos crítica.
    """

    # Data mínima e máxima esperada para o mês de referência
    date_min = f"{year}-{str(month).zfill(2)}-01"
    # Último dia do mês: usa o primeiro dia do mês seguinte menos 1 segundo
    next_month = month + 1 if month < 12 else 1
    next_year  = year if month < 12 else year + 1
    date_max   = f"{next_year}-{str(next_month).zfill(2)}-01"

    df = df.withColumn(
        "rejection_reason",
        F.when(
            F.col("pickup_datetime").isNull(),
            F.lit("pickup_datetime is null")
        ).when(
            F.col("dropoff_datetime").isNull(),
            F.lit("dropoff_datetime is null")
        ).when(
            # Pickup fora do mês de referência do arquivo
            (F.col("pickup_datetime") < F.lit(date_min).cast("timestamp")) |
            (F.col("pickup_datetime") >= F.lit(date_max).cast("timestamp")),
            F.lit("pickup_datetime out of reference period")
        ).when(
            # Dropoff igual ou anterior ao pickup
            F.col("dropoff_datetime") <= F.col("pickup_datetime"),
            F.lit("dropoff_datetime <= pickup_datetime")
        ).when(
            F.col("passenger_count").isNull() |
            (F.col("passenger_count") <= 0) |
            (F.col("passenger_count") > MAX_PASSENGERS),
            F.lit("invalid passenger_count")
        ).when(
            F.col("trip_distance").isNull() |
            (F.col("trip_distance") <= 0) |
            (F.col("trip_distance") > MAX_DISTANCE_MPH),
            F.lit("invalid trip_distance")
        ).when(
            F.col("pu_location_id").isNull() |
            (F.col("pu_location_id") < LOCATION_ID_MIN) |
            (F.col("pu_location_id") > LOCATION_ID_MAX),
            F.lit("invalid pu_location_id")
        ).when(
            F.col("do_location_id").isNull() |
            (F.col("do_location_id") < LOCATION_ID_MIN) |
            (F.col("do_location_id") > LOCATION_ID_MAX),
            F.lit("invalid do_location_id")
        ).when(
            F.col("fare_amount").isNull() |
            (F.col("fare_amount") < 0),
            F.lit("invalid fare_amount")
        ).when(
            F.col("tip_amount") < 0,
            F.lit("negative tip_amount")
        ).when(
            F.col("rate_code_id").isNotNull() &
            (
                (F.col("rate_code_id") < RATE_CODE_MIN) |
                (F.col("rate_code_id") > RATE_CODE_MAX)
            ),
            F.lit("invalid rate_code_id")
        ).otherwise(F.lit(None).cast("string"))
    )

    return df


def split_valid_rejected(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Separa o DataFrame em dois: registros válidos e rejeitados.

    Returns:
        Tupla (df_valid, df_rejected)
    """
    df_valid    = df.filter(F.col("rejection_reason").isNull()) \
                    .drop("rejection_reason")

    df_rejected = df.filter(F.col("rejection_reason").isNotNull())

    return df_valid, df_rejected


def log_validation_stats(
    df_valid: DataFrame,
    df_rejected: DataFrame,
    year: int,
    month: int
) -> dict:
    """
    Loga e retorna estatísticas da validação para rastreabilidade na DAG.
    """
    total_valid    = df_valid.count()
    total_rejected = df_rejected.count()
    total          = total_valid + total_rejected
    pct_rejected   = (total_rejected / total * 100) if total > 0 else 0

    logger.info("=== Resultado da Validação %s/%s ===", year, month)
    logger.info("Total de registros : %d", total)
    logger.info("Válidos            : %d (%.2f%%)", total_valid, 100 - pct_rejected)
    logger.info("Rejeitados         : %d (%.2f%%)", total_rejected, pct_rejected)

    if total_rejected > 0:
        logger.info("--- Distribuição de motivos de rejeição ---")
        df_rejected.groupBy("rejection_reason") \
                   .count() \
                   .orderBy(F.col("count").desc()) \
                   .show(truncate=False)

    return {
        "total":          total,
        "total_valid":    total_valid,
        "total_rejected": total_rejected,
        "pct_rejected":   round(pct_rejected, 2),
    }


def rename_rejected_columns(df_rejected: DataFrame) -> DataFrame:
    """
    Renomeia as colunas do df_rejected para o schema da tabela
    silver.rejected_records (prefixo raw_).
    """
    return df_rejected.select(
        F.col("source_file"),
        F.col("reference_year"),
        F.col("reference_month"),
        F.col("rejection_reason"),
        F.col("vendor_id")          .alias("raw_vendor_id"),
        F.col("pickup_datetime")    .alias("raw_pickup_datetime"),
        F.col("dropoff_datetime")   .alias("raw_dropoff_datetime"),
        F.col("passenger_count")    .alias("raw_passenger_count"),
        F.col("trip_distance")      .alias("raw_trip_distance"),
        F.col("rate_code_id")       .alias("raw_rate_code_id"),
        F.col("pu_location_id")     .alias("raw_pu_location_id"),
        F.col("do_location_id")     .alias("raw_do_location_id"),
        F.col("payment_type")       .alias("raw_payment_type"),
        F.col("fare_amount")        .alias("raw_fare_amount"),
        F.col("tip_amount")         .alias("raw_tip_amount"),
        F.col("total_amount")       .alias("raw_total_amount"),
    )


def run_validate(
    df: DataFrame,
    year: int,
    month: int
) -> tuple[DataFrame, DataFrame, dict]:
    """
    Orquestra a validação completa.

    Args:
        df:    DataFrame limpo vindo do spark_clean
        year:  Ano de referência
        month: Mês de referência

    Returns:
        Tupla (df_valid, df_rejected_formatted, stats)
    """
    logger.info("=== Iniciando validação: %s/%s ===", year, month)

    df = build_rejection_flags(df, year, month)
    df_valid, df_rejected = split_valid_rejected(df)
    stats = log_validation_stats(df_valid, df_rejected, year, month)
    df_rejected = rename_rejected_columns(df_rejected)

    return df_valid, df_rejected, stats