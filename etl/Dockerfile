# =============================================================================
# ETL — Imagem standalone para execucao local dos scripts
# Usada pelo Airflow via PythonOperator quando necessario rodar
# fora do cluster Spark (ex: download, validacao de arquivo)
# =============================================================================

FROM python:3.11-slim

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    default-jdk \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH="/app:${PYTHONPATH}"

CMD ["python", "-c", "print('ETL container ready')"]
