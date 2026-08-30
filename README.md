# Enterprise Data Platform Modernization

Runnable proof-of-value for a **cloud-native lakehouse + warehouse + real-time streaming + governance + master data management** architecture, with a **live browser simulator**.

## What this implements

| Requirement | Implementation in this PoV |
|---|---|
| Operational OLTP source | PostgreSQL `customers` + `orders` |
| Master Data Management | Golden customer ID, deterministic/fuzzy matching, source cross-reference, change audit |
| Batch ingestion | Python extraction to Parquet |
| Real-time event ingestion | Redpanda (Kafka API) |
| Raw lake | MinIO object storage, partitioned Parquet |
| Curated analytical layer | DuckDB local warehouse + Parquet |
| Transformations | dbt models: staging → mart |
| Orchestration | Airflow DAG included; install Airflow separately |
| Data quality | dbt tests + pytest reconciliation + MDM tests |
| Metadata/catalog | machine-readable catalog JSON |
| Lineage | explicit source → staging → mart lineage JSON |
| PII policy | email classification + API masking |
| Data access | FastAPI `/v1/orders`, `/v1/kpis` |
| Live simulator | FastAPI + WebSocket + Kafka + MinIO + DuckDB |
| API security | Bearer token |
| Observability | Prometheus `/metrics` |
| Infrastructure as Code | Terraform target-state skeleton |

## Master Data Management flow

The customer domain now has an explicit MDM layer before curated analytics. The operational source keeps its local `customer_id`, while MDM resolves that source record to one enterprise `master_customer_id`.

```text
PostgreSQL customers
        │
        ├──> Bronze/raw customers.parquet
        │
        ▼
Customer MDM
        │
        ├── Exact email match
        ├── Exact phone match
        ├── Fuzzy name + country match
        ├── Golden customer record
        ├── Source → Master cross-reference
        └── Attribute change audit
        │
        ├──> data/mdm/customers_golden.parquet
        └──> data/mdm/customer_xref.parquet
                    │
                    ▼
               dbt Silver
                    │
                    ▼
              Gold / Marts
                    │
                    ▼
             Data Warehouse
```

### MDM outcomes

For each incoming customer, `mdm/service.py` returns one of three outcomes:

- `created` — no identity match exists, so a new `MC-...` golden ID is generated.
- `duplicate` — the identity already exists and the existing master ID is reused.
- `updated` — the identity already exists but one or more mastered attributes changed; the golden record is updated and the old/new values are written to the audit table.

The implementation keeps three durable MDM structures in `data/mdm.sqlite`:

- `mdm_customer_golden` — one trusted current customer record per enterprise identity.
- `mdm_source_xref` — maps source-system customer IDs to the golden master ID.
- `mdm_change_audit` — stores mastered attribute changes with source and timestamp.

The dbt mart continues exposing a `customer_id` column for API compatibility, but that field is now the enterprise `master_customer_id`, not the source-system ID.

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

Batch + MDM path:
PostgreSQL OLTP
   ├──> raw Parquet
   └──> Customer MDM → Golden customer + source cross-reference
                              │
                              ▼
                         dbt staging
                              │
                              ▼
                       DuckDB curated mart
                              │
                              ▼
                           FastAPI
```

## Project Screenshots

### Live real-time simulator

The browser simulator shows live event throughput, latency, GMV, Kafka delivery, MinIO lake-object growth, platform health, status mix, and the full Source → Kafka → Quality/Persist → Raw Lake flow.

![Live Enterprise Data Platform Simulator](live-simulator-dashboard.jpg)

### MinIO raw lake

Real-time events are persisted as partitioned Parquet objects under `raw/orders/dt=YYYY-MM-DD/hour=HH/`.

![MinIO Raw Lake Parquet Objects](minio-raw-lake.jpg)

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

## 3. Run batch + MDM + warehouse path

```bash
export DATABASE_URL=postgresql://platform:platform@localhost:5432/commerce
python batch/extract_postgres.py
python scripts/build_warehouse.py
pytest -q
```

`batch/extract_postgres.py` now performs both raw extraction and customer identity resolution. After the run, inspect:

```bash
ls -lh data/raw_batch
ls -lh data/mdm
```

Expected MDM artifacts:

```text
data/mdm.sqlite
data/mdm/customers_golden.parquet
data/mdm/customer_xref.parquet
```

Run the MDM tests directly:

```bash
pytest -q tests/test_mdm.py
```

Run dbt:

```bash
dbt run --project-dir warehouse/dbt --profiles-dir warehouse/dbt
dbt test --project-dir warehouse/dbt --profiles-dir warehouse/dbt
```

The dbt lineage for customers is now:

```text
raw customers
    ↓
MDM golden customer + cross-reference
    ↓
stg_customers + stg_customer_xref
    ↓
mart_orders
```

## 4. Standalone MDM example

The MDM engine can also be invoked directly with JSON over stdin.

```bash
echo '{
  "source_system": "cars24_app",
  "source_customer_id": "C-1001",
  "full_name": "Asha Rao",
  "email": "asha@example.com",
  "phone": "+91 9876543210",
  "country": "IN",
  "address": "Hyderabad"
}' | python -m mdm.service
```

A first identity returns `created`. Sending the same person from another source with the same normalized email or phone returns the existing `master_customer_id` with `duplicate=true`. Sending changed mastered attributes for the same source identity returns `updated` and records the changed fields.

## 5. Run the live simulator on EC2

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
- Live event count and throughput
- Average event latency
- Live GMV
- Kafka health
- MinIO object count
- Animated Source → Kafka → Quality/Persist → Raw Lake pipeline
- Live event stream
- Status distribution
- Selected event payload
- Latest MinIO object URI

Swagger remains available at:

```text
http://EC2_PUBLIC_IP:8000/docs
```

## 6. Verify the raw MinIO bucket

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

## 7. Manual streaming demo

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

## 8. API examples

```bash
curl http://localhost:8000/health
curl -H "Authorization: Bearer talent-grid-demo-token" http://localhost:8000/v1/kpis
curl -H "Authorization: Bearer talent-grid-demo-token" http://localhost:8000/v1/orders
curl http://localhost:8000/v1/live/events
curl http://localhost:8000/v1/live/lake
```

## 9. AWS Security Group

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
pytest -q tests/test_mdm.py
cat metadata/catalog.json
cat metadata/lineage.json
```

Prometheus metrics:

```text
http://EC2_PUBLIC_IP:8000/metrics
```

## Suggested production substitutions

- MDM SQLite reference service → Informatica MDM / Reltio / Semarchy / cloud-native mastered domain service
- Redpanda → Confluent Cloud / Amazon MSK / Google Pub/Sub
- MinIO → S3 / GCS / ADLS
- DuckDB → Snowflake / BigQuery / Databricks SQL
- local Parquet → Delta Lake / Apache Iceberg
- JSON catalog → Collibra / DataHub / OpenMetadata / Dataplex
- bearer demo token → enterprise IAM/OAuth2/mTLS
- local Airflow → Cloud Composer / MWAA / Astronomer

## Target SLOs represented by the design

- Streaming critical-event latency: < 5 seconds
- MDM identity resolution target: < 1 second per synchronous customer request at PoV scale
- End-to-end customer master to analytical availability target: < 3 minutes
- Platform availability target: > 99.9%
- Pipeline success rate: > 99.5%
- Catalog/lineage coverage target: > 90%
- Daily source-to-target reconciliation for migrated domains

These are reference targets and should be calibrated against business SLAs.
