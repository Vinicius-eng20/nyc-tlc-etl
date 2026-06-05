# =============================================================================
# Airflow 2.10 + Python 3.11
# Providers: celery, postgres, apache-spark
# =============================================================================

FROM apache/airflow:2.10.5-python3.11

USER root

# Dependencias de sistema necessarias para psycopg2 e PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    default-jdk \
    procps \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# JAVA_HOME necessario para o PySpark submeter jobs ao cluster
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Providers do Airflow necessarios para o pipeline
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres==5.12.0 \
    apache-airflow-providers-apache-spark==4.11.2 \
    apache-airflow-providers-celery==3.8.4

# Dependencias do ETL disponiveis no container do Airflow
# (para operadores que rodam localmente antes de submeter ao Spark)
RUN pip install --no-cache-dir \
    pyspark==3.5.5 \
    psycopg2-binary==2.9.10 \
    pandas==2.2.3 \
    pyarrow==18.1.0 \
    requests==2.32.3 \
    SQLAlchemy==1.4.54
