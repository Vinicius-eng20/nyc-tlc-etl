# =============================================================================
# Spark 3.5 — Imagem customizada baseada no Bitnami
# Adiciona: psycopg2, pandas, pyarrow e dependencias do ETL
# =============================================================================

FROM bitnami/spark:3.5

USER root

# Instala pip e dependencias Python necessarias para os jobs Spark
RUN install_packages python3-pip && \
    pip3 install --no-cache-dir \
        pyspark==3.5.5 \
        psycopg2-binary==2.9.10 \
        pandas==2.2.3 \
        pyarrow==18.1.0 \
        requests==2.32.3 \
        SQLAlchemy==1.4.54

# Driver JDBC do PostgreSQL para escrita no DW direto pelo Spark
ARG POSTGRES_JDBC_VERSION=42.7.4
RUN curl -fsSL \
    "https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
    -o /opt/bitnami/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar

# Configura variaveis de ambiente Python para consistencia driver/worker
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

USER 1001
