# 🚕 NYC TLC ETL Pipeline

Pipeline de dados completo utilizando **Python**, **Apache Airflow**, **PySpark** e **PostgreSQL**, totalmente containerizado com **Docker**. Os dados tratados são disponibilizados para consumo em um dashboard no **Mendix**.

---

## 📋 Índice

- [Visão Geral](#visão-geral)
- [Arquitetura](#arquitetura)
- [Fonte de Dados](#fonte-de-dados)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Pré-requisitos](#pré-requisitos)
- [Instalação e Execução](#instalação-e-execução)
- [Parametrização da DAG](#parametrização-da-dag)
- [Camadas de Dados](#camadas-de-dados)
- [Tabelas do Data Warehouse](#tabelas-do-data-warehouse)
- [Regras de Limpeza e Validação](#regras-de-limpeza-e-validação)
- [Integração com Mendix](#integração-com-mendix)
- [Serviços e Portas](#serviços-e-portas)

---

## 🔭 Visão Geral

Este projeto implementa um pipeline ETL (Extract, Transform, Load) sobre os dados públicos de corridas de táxi amarelo da cidade de Nova York, disponibilizados pelo **NYC Taxi & Limousine Commission (TLC)**.

O objetivo é ingerir dados brutos mensais, aplicar limpeza e transformações com PySpark, armazenar os resultados em um Data Warehouse PostgreSQL e disponibilizar agregações prontas para consumo em um dashboard Mendix.

### Stack Tecnológica

| Camada | Tecnologia |
|--------|-----------|
| Orquestração | Apache Airflow 2.8 |
| Processamento | PySpark 3.5 |
| Data Warehouse | PostgreSQL 15 |
| Containerização | Docker + Docker Compose |
| Dashboard | Mendix |
| Linguagem | Python 3.11 |

---

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Compose                            │
│                                                                 │
│  ┌──────────┐   ┌─────────────┐   ┌──────────┐   ┌──────────┐  │
│  │          │   │             │   │          │   │          │  │
│  │ Airflow  │──▶│   PySpark   │──▶│  Pandas  │──▶│ Postgres │  │
│  │          │   │ (transform) │   │  (load)  │   │   (DW)   │  │
│  └──────────┘   └─────────────┘   └──────────┘   └──────────┘  │
│       │                │                               │        │
│  Scheduler        Spark Master                   Mendix acessa  │
│  Webserver        Spark Worker                   tabelas Gold   │
│  Worker (Celery)                                               │
│  Redis (broker)                                               │
└─────────────────────────────────────────────────────────────────┘
```

### Modelo Medallion (Bronze → Silver → Gold)

```
RAW (Bronze)             STAGING (Silver)           MART (Gold)
─────────────            ────────────────           ───────────
Parquet bruto    ──▶     Dados limpos,      ──▶     Agregações
direto da fonte          tipados e                  prontas para
sem alteração            validados                  o dashboard
```

---

## 📦 Fonte de Dados

**NYC TLC Trip Record Data — Yellow Taxi**

- **URL:** https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- **Formato:** Parquet (particionado por mês/ano)
- **Disponibilidade:** Dados desde 2009, atualizados mensalmente
- **Licença:** Domínio público — sem necessidade de chave de API ou cadastro

### Principais problemas nos dados brutos

| Campo | Problema |
|-------|----------|
| `tpep_pickup_datetime` | Datas fora do range do mês/ano do arquivo |
| `tpep_dropoff_datetime` | Dropoff anterior ao pickup |
| `passenger_count` | Nulos, zeros, valores absurdos (ex: 255) |
| `trip_distance` | Zeros, negativos, valores impossíveis |
| `PULocationID / DOLocationID` | IDs fora do range válido (1–263) |
| `fare_amount` | Valores negativos, zeros em corridas reais |
| `tip_amount` | Valores negativos |
| `total_amount` | Inconsistente com a soma dos componentes |
| `RatecodeID` | Valores fora do range (1–6) |
| `payment_type` | Nulos e códigos inválidos |

---

## 📁 Estrutura do Projeto

```
nyc-tlc-etl/
├── docker-compose.yml             # Orquestração de todos os serviços
├── .env.example                   # Template de variáveis de ambiente
├── .gitignore
│
├── airflow/
│   ├── Dockerfile                 # Imagem customizada do Airflow
│   └── dags/
│       └── tlc_pipeline.py        # DAG principal parametrizada
│
├── etl/
│   ├── Dockerfile                 # Imagem do ambiente ETL + PySpark
│   ├── requirements.txt
│   ├── extract/
│   │   └── download_tlc.py        # Download do Parquet por mês/ano
│   ├── transform/
│   │   ├── spark_clean.py         # Limpeza e tipagem com PySpark
│   │   └── spark_validate.py      # Validações e separação de rejeitados
│   └── load/
│       └── postgres_loader.py     # Carga Silver e Gold no PostgreSQL
│
├── spark/
│   └── Dockerfile                 # Imagem customizada do Spark
│
├── sql/
│   └── create_tables.sql          # DDL completo do Data Warehouse
│
└── data/
    ├── raw/                       # Parquets brutos baixados
    ├── staging/                   # Parquets limpos (Silver)
    └── rejected/                  # Registros inválidos com motivo
```

---

## ✅ Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/) >= 24.x
- [Docker Compose](https://docs.docker.com/compose/install/) >= 2.x
- Mínimo de **8 GB de RAM** disponível para o Docker
- Mínimo de **10 GB de espaço em disco**

---

## 🚀 Instalação e Execução

### 1. Clone o repositório

```bash
git clone https://github.com/Vinicius-eng20/nyc-tlc-etl.git
cd nyc-tlc-etl
```

### 2. Configure as variáveis de ambiente

```bash
cp .env.example .env
```

Edite o arquivo `.env` com suas configurações. Gere uma Fernet Key para o Airflow:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 3. Suba os containers

```bash
docker compose up -d
```

Na primeira execução, aguarde alguns minutos para que todos os serviços inicializem corretamente.

### 4. Verifique os serviços

```bash
docker compose ps
```

Todos os serviços devem estar com status `healthy` ou `running`.

### 5. Acesse o Airflow

Abra o navegador em **http://localhost:8080**

- Usuário padrão: `airflow`
- Senha padrão: `airflow`

### 6. Execute o pipeline

Via interface do Airflow, acesse a DAG `tlc_yellow_taxi_pipeline` e faça um trigger manual passando a configuração do mês desejado (veja [Parametrização da DAG](#parametrização-da-dag)).

---

## ⚙️ Parametrização da DAG

A DAG pode ser disparada manualmente com um JSON de configuração, ou rodará automaticamente todo mês via schedule.

### Trigger manual via UI do Airflow

Na tela da DAG, clique em **"Trigger DAG w/ config"** e passe:

```json
{
  "year": 2024,
  "month": 1
}
```

### Trigger manual via CLI

```bash
docker exec -it airflow-scheduler airflow dags trigger \
  tlc_yellow_taxi_pipeline \
  --conf '{"year": 2024, "month": 1}'
```

### Reprocessar um intervalo de meses

```bash
docker exec -it airflow-scheduler airflow dags backfill \
  tlc_yellow_taxi_pipeline \
  --start-date 2024-01-01 \
  --end-date 2024-06-01
```

---

## 🗂️ Camadas de Dados

### 🥉 Bronze (Raw)
Dados brutos baixados diretamente do site do TLC no formato Parquet original, sem nenhuma alteração. Armazenados em `data/raw/`.

### 🥈 Silver (Staging)
Dados após limpeza e validação com PySpark:
- Tipos de dados corrigidos
- Registros inválidos removidos e enviados para `data/rejected/` com coluna `rejection_reason`
- Campos nulos tratados conforme regras de negócio
- Datas normalizadas e validadas

### 🥇 Gold (Mart)
Agregações prontas para consumo no dashboard Mendix:
- Volume e receita por dia
- Top zonas de embarque e desembarque
- Distribuição por forma de pagamento
- Demanda por hora do dia
- Ticket médio por zona

---

## 🗄️ Tabelas do Data Warehouse

| Tabela | Camada | Descrição |
|--------|--------|-----------|
| `raw.yellow_taxi` | Bronze | Dados brutos carregados |
| `silver.yellow_taxi_clean` | Silver | Dados limpos e validados |
| `silver.rejected_records` | Silver | Registros descartados com motivo |
| `gold.trips_by_day` | Gold | Volume e receita por dia |
| `gold.trips_by_zone` | Gold | Top zonas de embarque/desembarque |
| `gold.payment_summary` | Gold | Distribuição por forma de pagamento |
| `gold.hourly_demand` | Gold | Demanda por hora do dia |
| `gold.zone_avg_fare` | Gold | Ticket médio por zona de embarque |

---

## 🧹 Regras de Limpeza e Validação

Os registros que violam qualquer uma das regras abaixo são direcionados para `silver.rejected_records` com o campo `rejection_reason` preenchido.

| Regra | Condição de Rejeição |
|-------|---------------------|
| Data de pickup válida | Fora do mês/ano do arquivo |
| Dropoff após pickup | `dropoff <= pickup` |
| Passageiros válidos | `passenger_count <= 0` ou `> 8` ou nulo |
| Distância válida | `trip_distance <= 0` ou `> 500` |
| Location IDs válidos | `PULocationID` ou `DOLocationID` fora de 1–263 |
| Tarifa válida | `fare_amount < 0` |
| Gorjeta válida | `tip_amount < 0` |
| RatecodeID válido | Fora do range 1–6 |

---

## 📊 Integração com Mendix

O Mendix consome os dados diretamente das tabelas **Gold** do PostgreSQL via conexão JDBC.

### Configuração da conexão no Mendix

| Parâmetro | Valor |
|-----------|-------|
| Host | `<IP do servidor>` |
| Porta | `5433` |
| Database | `nyc_tlc_dw` |
| Schema | `gold` |
| Usuário | Definido no `.env` |

> As tabelas Gold são atualizadas automaticamente a cada execução mensal da DAG, sem necessidade de intervenção manual no Mendix.

---

## 🌐 Serviços e Portas

| Serviço | Porta | Descrição |
|---------|-------|-----------|
| Airflow Webserver | `8080` | Interface web do Airflow |
| Spark Master UI | `8081` | Interface web do Spark Master |
| Spark Worker UI | `8082` | Interface web do Spark Worker |
| PostgreSQL Airflow | `5432` | Banco de metadados do Airflow |
| PostgreSQL DW | `5433` | Data Warehouse (Mendix) |
| Redis | `6379` | Broker Celery do Airflow |

---

## 📄 Licença

Este projeto é de uso livre para fins educacionais e de portfólio.

Os dados utilizados são públicos e disponibilizados pelo [NYC Taxi & Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
