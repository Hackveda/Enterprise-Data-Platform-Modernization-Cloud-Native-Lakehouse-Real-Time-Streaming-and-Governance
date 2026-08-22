# Enterprise Data Platform Modernization

Runnable proof-of-value for a **cloud-native lakehouse + warehouse + real-time streaming + governance** architecture, now with a **live browser simulator**.

## What this implements

| Requirement | Implementation in this PoV |
|---|---|
| Operational OLTP source | PostgreSQL `customers` + `orders` |
| Batch ingestion | Python extraction to Parquet |
| Real-time event ingestion | Redpanda (Kafka API) |
| Raw lake | MinIO object storage, partitioned Parquet |
| Curated analytical layer | DuckDB local warehouse + Parquet |
| Transformations | dbt models: staging → mart |
| Orchestration | Airflow DAG included; install Airflow separately |
| Data quality | dbt tests + pytest reconciliation |
| Metadata/catalog | machine-readable catalog JSON |
| Lineage | explicit source → staging → mart lineage JSON |
| PII policy | email classification + API masking |
| Data access | FastAPI `/v1/orders`, `/v1/kpis` |
| Live simulator | FastAPI + WebSocket + Kafka + MinIO + DuckDB |
| API security | Bearer token |
| Observability | Prometheus `/metrics` |
| Infrastructure as Code | Terraform target-state skeleton |

## Live architecture

```text
Browser Live Simulator
        │ Start / Stop
        ▼
FastAPI synthetic producer
        │
        ▼
Redpanda / Kafka ── orders.events.v1
        │
        ├──> WebSocket live dashboard
        ├──> DuckDB live_events
        └──> MinIO raw/orders/dt=YYYY-MM-DD/hour=HH/*.parquet

Batch path:
PostgreSQL OLTP ──> Parquet raw ──> dbt ──> DuckDB curated mart ──> FastAPI
```

## 1. Start infrastructure

```bash
docker compose up -d postgres redpanda minio
```

Verify:

```bash
docker compose ps
```

The Redpanda configuration exposes two listeners:

- `localhost:9092` for Python processes running directly on the EC2 host
- `redpanda:29092` for Docker containers such as the API service

## 2. Python 3.12 environment

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

Airflow is intentionally not included in the core environment because its dependency constraints can conflict with dbt. Install it in a separate virtual environment when needed.

## 3. Run batch path and warehouse

```bash
export DATABASE_URL=postgresql://platform:platform@localhost:5432/commerce
python batch/extract_postgres.py
python scripts/build_warehouse.py
pytest -q
```

Run dbt:

```bash
dbt run --project-dir warehouse/dbt --profiles-dir warehouse/dbt
dbt test --project-dir warehouse/dbt --profiles-dir warehouse/dbt
```

## 4. Run the live simulator on EC2

Activate the environment and configure host-side service addresses:

```bash
source .venv/bin/activate
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export MINIO_ENDPOINT=localhost:9000
export MINIO_ACCESS_KEY=minioadmin
export MINIO_SECRET_KEY=minioadmin
export WAREHOUSE_PATH=./data/warehouse.duckdb
```

Start FastAPI:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Open:

```text
http://EC2_PUBLIC_IP:8000/
```

The dashboard provides:

- Start / Stop simulation controls
- Adjustable event rate
- live event count and throughput
- average event latency
- live GMV
- Kafka health
- MinIO object count
- animated Source → Kafka → Quality/Persist → Raw Lake pipeline
- live event stream
- status distribution
- selected event payload
- latest MinIO object URI

Swagger remains available at:

```text
http://EC2_PUBLIC_IP:8000/docs
```

## 5. Verify the raw MinIO bucket

MinIO console:

```text
http://EC2_PUBLIC_IP:9001
```

Default demo credentials:

```text
minioadmin / minioadmin
```

After the simulator starts, objects are written automatically under:

```text
raw/orders/dt=YYYY-MM-DD/hour=HH/part-....parquet
```

The API can also confirm lake writes:

```bash
curl http://localhost:8000/v1/live/lake
```

## 6. Manual streaming demo

The original command-line producer and lake consumer remain available.

Terminal A:

```bash
source .venv/bin/activate
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export MINIO_ENDPOINT=localhost:9000
python streaming/consumer_to_lake.py
```

Terminal B:

```bash
source .venv/bin/activate
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
python producer/order_events.py
```

## 7. API examples

```bash
curl http://localhost:8000/health
curl -H "Authorization: Bearer talent-grid-demo-token" http://localhost:8000/v1/kpis
curl -H "Authorization: Bearer talent-grid-demo-token" http://localhost:8000/v1/orders
curl http://localhost:8000/v1/live/events
curl http://localhost:8000/v1/live/lake
```

## 8. AWS Security Group

For a demo, allow these inbound ports only from your own public IP where possible:

| Port | Use |
|---:|---|
| 22 | SSH |
| 8000 | Live simulator / FastAPI |
| 9001 | MinIO Console |

Do not expose PostgreSQL 5432, Kafka 9092, or MinIO API 9000 publicly.

## Governance and observability

```bash
pytest -q tests/test_data_quality.py
cat metadata/catalog.json
cat metadata/lineage.json
```

Prometheus metrics:

```text
http://EC2_PUBLIC_IP:8000/metrics
```

## Suggested production substitutions

- Redpanda → Confluent Cloud / Amazon MSK / Google Pub/Sub
- MinIO → S3 / GCS / ADLS
- DuckDB → Snowflake / BigQuery / Databricks SQL
- local Parquet → Delta Lake / Apache Iceberg
- JSON catalog → Collibra / DataHub / OpenMetadata / Dataplex
- bearer demo token → enterprise IAM/OAuth2/mTLS
- local Airflow → Cloud Composer / MWAA / Astronomer

## Interview demo sequence

1. Open the live simulator.
2. Click **Start simulation**.
3. Show events moving through Source → Kafka → persistence → MinIO.
4. Open MinIO and refresh the `raw` bucket to show new partitioned Parquet objects.
5. Open `/v1/live/lake` to show object count and latest paths.
6. Query `live_events` in DuckDB to demonstrate durable analytical persistence.
7. Run dbt models and tests to explain the curated path and governance.
8. Open `/metrics` to discuss operational SLOs.
9. Map the local PoV components to managed cloud services.

## Target SLOs represented by the design

- Streaming critical-event latency: < 5 seconds
- Platform availability target: > 99.9%
- Pipeline success rate: > 99.5%
- Catalog/lineage coverage target: > 90%
- Daily source-to-target reconciliation for migrated domains

These are reference targets and should be calibrated against business SLAs.
