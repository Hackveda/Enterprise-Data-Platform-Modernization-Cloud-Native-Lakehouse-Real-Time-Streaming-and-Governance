# Enterprise Data Platform Modernization

Runnable proof-of-value for a **cloud-native lakehouse + warehouse + real-time streaming + governance** architecture.

## What this implements

| Requirement | Implementation in this PoV |
|---|---|
| Operational OLTP source | PostgreSQL `customers` + `orders` |
| Batch ingestion | Python extraction to Parquet |
| Real-time event ingestion | Redpanda (Kafka API) |
| Raw lake | MinIO object storage, partitioned Parquet |
| Curated analytical layer | DuckDB local warehouse + Parquet |
| Transformations | dbt models: staging → mart |
| Orchestration | Airflow DAG included |
| Data quality | dbt tests + pytest reconciliation |
| Metadata/catalog | machine-readable catalog JSON |
| Lineage | explicit source → staging → mart lineage JSON |
| PII policy | email classification + API masking |
| Data access | FastAPI `/v1/orders`, `/v1/kpis` |
| API security | Bearer token |
| Observability | Prometheus `/metrics` |
| Infrastructure as Code | Terraform target-state skeleton |

## Architecture

```text
PostgreSQL OLTP ──batch──> Parquet raw ──dbt──> DuckDB curated mart ──> FastAPI
       │
       └── simulated CDC/events ──> Redpanda/Kafka ──> MinIO raw lake

Governance: catalog + lineage + PII classification/masking + tests + reconciliation
Orchestration: Airflow DAG
Observability: Prometheus metrics
```

## 1. Start infrastructure

```bash
docker compose up -d postgres redpanda minio
```

## 2. Create Python environment

```bash
python -m venv .venv
source .venv/bin/activate   # Windows Git Bash: source .venv/Scripts/activate
pip install -r requirements.txt
cp .env.example .env
```

## 3. Run batch path and build warehouse

```bash
export DATABASE_URL=postgresql://platform:platform@localhost:5432/commerce
python batch/extract_postgres.py
python scripts/build_warehouse.py
pytest -q
```

Or use dbt:

```bash
dbt --project-dir warehouse/dbt --profiles-dir warehouse/dbt run
dbt --project-dir warehouse/dbt --profiles-dir warehouse/dbt test
```

## 4. Run API

```bash
uvicorn app.main:app --reload
```

Open API docs: `http://localhost:8000/docs`

```bash
curl -H "Authorization: Bearer talent-grid-demo-token" http://localhost:8000/v1/kpis
curl -H "Authorization: Bearer talent-grid-demo-token" http://localhost:8000/v1/orders
```

The API masks customer emails before returning data.

## 5. Demonstrate real-time streaming

Terminal A:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export MINIO_ENDPOINT=localhost:9000
python streaming/consumer_to_lake.py
```

Terminal B:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
python producer/order_events.py
```

Raw streaming records are written to paths like:

```text
s3://raw/orders/dt=YYYY-MM-DD/hour=HH/part-....parquet
```

MinIO console: `http://localhost:9001` (`minioadmin` / `minioadmin`).

## 6. Data quality / governance validation

```bash
pytest -q tests/test_data_quality.py
cat metadata/catalog.json
cat metadata/lineage.json
```

Quality checks cover row-count reconciliation, null keys and duplicate order IDs. dbt additionally validates uniqueness, not-null and customer referential integrity.

## Suggested production substitutions

- Redpanda → Confluent Cloud / Amazon MSK / Google Pub/Sub
- MinIO → S3 / GCS / ADLS
- DuckDB → Snowflake / BigQuery / Databricks SQL
- local Parquet → Delta Lake / Apache Iceberg
- JSON catalog → Collibra / DataHub / OpenMetadata / Dataplex
- bearer demo token → enterprise IAM/OAuth2/mTLS
- local Airflow → Cloud Composer / MWAA / Astronomer

## Interview demo sequence

1. Explain the business problem: silos, slow batch, inconsistent models and governance risk.
2. Show PostgreSQL as the legacy operational source.
3. Run batch extraction and show raw Parquet.
4. Run dbt / warehouse build and explain standardized staging + mart models.
5. Run tests and demonstrate reconciliation.
6. Start API and show masked PII.
7. Start Kafka producer + consumer and show event data landing in MinIO within seconds.
8. Open `catalog.json` and `lineage.json` to explain governance.
9. Open `/metrics` to explain operational SLO measurement.
10. Map local components to production managed cloud services.

## Target SLOs represented by the design

- Streaming critical-event latency: < 5 seconds
- Platform availability target: > 99.9%
- Pipeline success rate: > 99.5%
- Catalog/lineage coverage target: > 90%
- Daily source-to-target reconciliation for migrated domains

These are reference targets and should be calibrated against business SLAs.
