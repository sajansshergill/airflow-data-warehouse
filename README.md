# Airflow Crypto Data Warehouse Pipeline

An end-to-end data engineering project that builds a scheduled ETL pipeline using **Apache Airflow** to ingest cryptocurrency market data from a public API and load it into a relational **PostgreSQL data warehouse.**

This project demonstrated production-style data pipeline design: incremental ingestion, idempotent upserts, data quality checks, and containerized orchestration.

---

## 🚀 Project Overview

This pipeline runs daily and:

1. Extatcs hournly crypto privce data from the CoinGecko API
2. Transforms and normalizes timestamps
3. Loads data into a Potgres warehouse
4. Performs automated data quality checks

Assets ingested:
- Bitcoin (BTC)
- Ethereum (ETH)

---

## 🏗 Architecture

CoinGecko API
↓
Airflow DAG (ETL)
↓
PostgreSQL Data Warehouse
↓
Analytics-ready tables

Airflow orchestration extraction, transformation, loading, and validation using a task-based DAG.

---

## 📦 Tech Stack

- Apache Airflow 2.9
- PostgreSQL 16
- Docker + Docker Compose
- Pythin (requests, psycopg2)
- CoinGecko public API

---

## 🗂 Data Warehouse Schema

### Dimension Table 

`dim_crypto_asset`
| column | type |
|--------|------|
asset_id | text (PK) |
symbol | text |
name | text |

### Fact Table

`fact_crypto_price_hourly`
| column | type |
|--------|------|
asset_id | text (FK) |
ts | timestamptz |
price_usd | numeric |
load_ts | timestamptz |

Primary key: `(asset_id, ts)`

This ensure **idempotent loads** and prevents duplicates.

---

##  🔄 DAG Workflow

Airflow DAG: `crypto_to_postgres_warehouse`

Tasks:

1. `create_tables`
   - Creates warehouse schema id not exists
  
2. `upsert_dim_assets`
   - Loads asset metadata

3. `extract_transform`
   - Pulls yesterday's hourly data
   - Normalzies timestamps
  
4. `load_fact`
   - Performs upsert into fact table
  
5. `data_quality_checks`
   - Validates row counts
   - Checks null constraints
  
---

## 🧪 Features

- Incremental daily ingestion
- Idempotent upserts
- Retry + failure handling
- Data quality validation
- Dockerized Airflow + Postgres
- Production-style schema design

---

## ⚙️ Setup Instructions

### 1. Clone the repo

```bash
git clone <repo-url>
cd airflow-crypto-warehouse

2. Start services
docker compose up -d
docker compose run --rm airflow-init
docker compose up -d

3. Open Airflow UI
http://localhost:8080

Login
username: admin
password: admin

Enagble and trigger:
crypto_to_postgres_warehosue

---

## 🔍 Verify Data
Connect to postgres:
docker exec -it warehouse_postgres psql -U warehouse -d warehouse

Query:
SELECT * FROM dim_crypto_asset;

SELECT asset_id, ts, price_usd
FROM fact_crypto_price_hourly
ORDER BY ts DESC
LIMIT 20;

## 📈 Possible Extensions
