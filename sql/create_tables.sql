-- =============================================================================
-- NYC TLC ETL — DDL do Data Warehouse
-- =============================================================================
-- Schemas:
--   raw    : Bronze — dados brutos carregados diretamente do Parquet
--   silver : Silver — dados limpos, validados e tipados
--   gold   : Gold   — agregações prontas para o dashboard Mendix
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Schemas
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;


-- =============================================================================
-- BRONZE — raw.yellow_taxi
-- =============================================================================
CREATE TABLE IF NOT EXISTS raw.yellow_taxi (
    id                      BIGSERIAL PRIMARY KEY,
    -- Identificação da carga
    loaded_at               TIMESTAMP NOT NULL DEFAULT NOW(),
    source_file             VARCHAR(255),
    reference_year          SMALLINT,
    reference_month         SMALLINT,
    -- Campos originais do dataset TLC
    vendor_id               INTEGER,
    tpep_pickup_datetime    TIMESTAMP,
    tpep_dropoff_datetime   TIMESTAMP,
    passenger_count         DOUBLE PRECISION,
    trip_distance           DOUBLE PRECISION,
    rate_code_id            DOUBLE PRECISION,
    store_and_fwd_flag      VARCHAR(3),
    pu_location_id          INTEGER,
    do_location_id          INTEGER,
    payment_type            DOUBLE PRECISION,
    fare_amount             DOUBLE PRECISION,
    extra                   DOUBLE PRECISION,
    mta_tax                 DOUBLE PRECISION,
    tip_amount              DOUBLE PRECISION,
    tolls_amount            DOUBLE PRECISION,
    improvement_surcharge   DOUBLE PRECISION,
    total_amount            DOUBLE PRECISION,
    congestion_surcharge    DOUBLE PRECISION,
    airport_fee             DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_raw_yellow_taxi_ref
    ON raw.yellow_taxi (reference_year, reference_month);

CREATE INDEX IF NOT EXISTS idx_raw_yellow_taxi_loaded_at
    ON raw.yellow_taxi (loaded_at);


-- =============================================================================
-- SILVER — silver.yellow_taxi_clean
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver.yellow_taxi_clean (
    id                      BIGSERIAL PRIMARY KEY,
    -- Rastreabilidade
    raw_id                  BIGINT,
    processed_at            TIMESTAMP NOT NULL DEFAULT NOW(),
    source_file             VARCHAR(255),
    reference_year          SMALLINT NOT NULL,
    reference_month         SMALLINT NOT NULL,
    -- Campos limpos e tipados corretamente
    vendor_id               SMALLINT,
    pickup_datetime         TIMESTAMP NOT NULL,
    dropoff_datetime        TIMESTAMP NOT NULL,
    passenger_count         SMALLINT NOT NULL,
    trip_distance           NUMERIC(10, 2) NOT NULL,
    rate_code_id            SMALLINT,
    store_and_fwd_flag      CHAR(1),
    pu_location_id          SMALLINT NOT NULL,
    do_location_id          SMALLINT NOT NULL,
    payment_type            SMALLINT,
    fare_amount             NUMERIC(10, 2) NOT NULL,
    extra                   NUMERIC(10, 2),
    mta_tax                 NUMERIC(10, 2),
    tip_amount              NUMERIC(10, 2),
    tolls_amount            NUMERIC(10, 2),
    improvement_surcharge   NUMERIC(10, 2),
    total_amount            NUMERIC(10, 2),
    congestion_surcharge    NUMERIC(10, 2),
    airport_fee             NUMERIC(10, 2),
    -- Campos derivados
    trip_duration_minutes   NUMERIC(10, 2),
    trip_speed_mph          NUMERIC(10, 2)
);

CREATE INDEX IF NOT EXISTS idx_silver_clean_pickup
    ON silver.yellow_taxi_clean (pickup_datetime);

CREATE INDEX IF NOT EXISTS idx_silver_clean_ref
    ON silver.yellow_taxi_clean (reference_year, reference_month);

CREATE INDEX IF NOT EXISTS idx_silver_clean_pu_location
    ON silver.yellow_taxi_clean (pu_location_id);

CREATE INDEX IF NOT EXISTS idx_silver_clean_do_location
    ON silver.yellow_taxi_clean (do_location_id);


-- =============================================================================
-- SILVER — silver.rejected_records
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver.rejected_records (
    id                      BIGSERIAL PRIMARY KEY,
    rejected_at             TIMESTAMP NOT NULL DEFAULT NOW(),
    source_file             VARCHAR(255),
    reference_year          SMALLINT,
    reference_month         SMALLINT,
    rejection_reason        VARCHAR(255) NOT NULL,
    -- Dados originais preservados para auditoria
    raw_vendor_id           INTEGER,
    raw_pickup_datetime     TIMESTAMP,
    raw_dropoff_datetime    TIMESTAMP,
    raw_passenger_count     DOUBLE PRECISION,
    raw_trip_distance       DOUBLE PRECISION,
    raw_rate_code_id        DOUBLE PRECISION,
    raw_pu_location_id      INTEGER,
    raw_do_location_id      INTEGER,
    raw_payment_type        DOUBLE PRECISION,
    raw_fare_amount         DOUBLE PRECISION,
    raw_tip_amount          DOUBLE PRECISION,
    raw_total_amount        DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_rejected_ref
    ON silver.rejected_records (reference_year, reference_month);

CREATE INDEX IF NOT EXISTS idx_rejected_reason
    ON silver.rejected_records (rejection_reason);


-- =============================================================================
-- GOLD — gold.trips_by_day
-- Volume de corridas e receita agregados por dia
-- =============================================================================
CREATE TABLE IF NOT EXISTS gold.trips_by_day (
    id                      BIGSERIAL PRIMARY KEY,
    refreshed_at            TIMESTAMP NOT NULL DEFAULT NOW(),
    reference_year          SMALLINT NOT NULL,
    reference_month         SMALLINT NOT NULL,
    trip_date               DATE NOT NULL,
    total_trips             INTEGER NOT NULL,
    total_passengers        INTEGER,
    total_distance_miles    NUMERIC(14, 2),
    total_fare_amount       NUMERIC(14, 2),
    total_tip_amount        NUMERIC(14, 2),
    total_revenue           NUMERIC(14, 2),
    avg_fare_amount         NUMERIC(10, 2),
    avg_trip_distance       NUMERIC(10, 2),
    avg_trip_duration_min   NUMERIC(10, 2),
    UNIQUE (reference_year, reference_month, trip_date)
);


-- =============================================================================
-- GOLD — gold.trips_by_zone
-- Volume de corridas e receita por zona de embarque
-- =============================================================================
CREATE TABLE IF NOT EXISTS gold.trips_by_zone (
    id                      BIGSERIAL PRIMARY KEY,
    refreshed_at            TIMESTAMP NOT NULL DEFAULT NOW(),
    reference_year          SMALLINT NOT NULL,
    reference_month         SMALLINT NOT NULL,
    pu_location_id          SMALLINT NOT NULL,
    total_trips             INTEGER NOT NULL,
    total_revenue           NUMERIC(14, 2),
    avg_fare_amount         NUMERIC(10, 2),
    avg_trip_distance       NUMERIC(10, 2),
    avg_tip_amount          NUMERIC(10, 2),
    UNIQUE (reference_year, reference_month, pu_location_id)
);


-- =============================================================================
-- GOLD — gold.payment_summary
-- Distribuição de corridas e receita por tipo de pagamento
-- =============================================================================
CREATE TABLE IF NOT EXISTS gold.payment_summary (
    id                      BIGSERIAL PRIMARY KEY,
    refreshed_at            TIMESTAMP NOT NULL DEFAULT NOW(),
    reference_year          SMALLINT NOT NULL,
    reference_month         SMALLINT NOT NULL,
    -- 1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided
    payment_type            SMALLINT NOT NULL,
    payment_type_desc       VARCHAR(50),
    total_trips             INTEGER NOT NULL,
    total_revenue           NUMERIC(14, 2),
    total_tip_amount        NUMERIC(14, 2),
    avg_tip_amount          NUMERIC(10, 2),
    pct_of_total_trips      NUMERIC(5, 2),
    UNIQUE (reference_year, reference_month, payment_type)
);


-- =============================================================================
-- GOLD — gold.hourly_demand
-- Demanda de corridas por hora do dia
-- =============================================================================
CREATE TABLE IF NOT EXISTS gold.hourly_demand (
    id                      BIGSERIAL PRIMARY KEY,
    refreshed_at            TIMESTAMP NOT NULL DEFAULT NOW(),
    reference_year          SMALLINT NOT NULL,
    reference_month         SMALLINT NOT NULL,
    hour_of_day             SMALLINT NOT NULL CHECK (hour_of_day BETWEEN 0 AND 23),
    total_trips             INTEGER NOT NULL,
    avg_trips_per_day       NUMERIC(10, 2),
    total_revenue           NUMERIC(14, 2),
    avg_fare_amount         NUMERIC(10, 2),
    UNIQUE (reference_year, reference_month, hour_of_day)
);


-- =============================================================================
-- GOLD — gold.zone_avg_fare
-- Ticket médio por zona de embarque (top zonas)
-- =============================================================================
CREATE TABLE IF NOT EXISTS gold.zone_avg_fare (
    id                      BIGSERIAL PRIMARY KEY,
    refreshed_at            TIMESTAMP NOT NULL DEFAULT NOW(),
    reference_year          SMALLINT NOT NULL,
    reference_month         SMALLINT NOT NULL,
    pu_location_id          SMALLINT NOT NULL,
    total_trips             INTEGER NOT NULL,
    avg_fare_amount         NUMERIC(10, 2),
    avg_total_amount        NUMERIC(10, 2),
    avg_tip_amount          NUMERIC(10, 2),
    avg_trip_distance       NUMERIC(10, 2),
    avg_duration_minutes    NUMERIC(10, 2),
    UNIQUE (reference_year, reference_month, pu_location_id)
);