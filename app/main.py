import asyncio
import io
import json
import os
import random
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import snowflake.connector
from fastapi import FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from kafka import KafkaConsumer, KafkaProducer
from minio import Minio
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.responses import Response

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
WAREHOUSE = os.getenv("WAREHOUSE_PATH", "./data/warehouse.duckdb")
API_TOKEN = os.getenv("API_TOKEN", "talent-grid-demo-token")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
RAW_BUCKET = os.getenv("MINIO_BUCKET", "raw")
TOPIC = "orders.events.v1"

SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DATABASE", "ENTERPRISE_PLATFORM")
SNOWFLAKE_WH = os.getenv("SNOWFLAKE_WAREHOUSE", "PLATFORM_WH")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "PLATFORM_ROLE")

app = FastAPI(title="Enterprise Data Platform API", version="3.0.0")
REQUESTS = Counter("platform_api_requests_total", "API requests", ["path", "status"])
LATENCY = Histogram("platform_api_latency_seconds", "API latency", ["path"])
EVENTS = Counter("platform_stream_events_total", "Stream events consumed")

clients: set[WebSocket] = set()
main_loop: asyncio.AbstractEventLoop | None = None
simulator_thread: threading.Thread | None = None
simulator_stop = threading.Event()
consumer_thread: threading.Thread | None = None
consumer_stop = threading.Event()
latest_events: list[dict] = []
state_lock = threading.Lock()


def authorize(authorization: str | None):
    if authorization != f"Bearer {API_TOKEN}":
        raise HTTPException(status_code=401, detail="Invalid or missing bearer token")


def mask_email(email: str | None) -> str:
    if not email:
        return "—"
    local, _, domain = str(email).partition("@")
    if not domain:
        return "***"
    return f"{local[:1]}***@{domain}"


def warehouse_connection(read_only: bool = False):
    Path(WAREHOUSE).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(WAREHOUSE, read_only=read_only)


def snowflake_connection():
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise RuntimeError(f"Missing Snowflake environment variables: {', '.join(missing)}")
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=SNOWFLAKE_WH,
        database=SNOWFLAKE_DB,
        role=SNOWFLAKE_ROLE,
    )


def snowflake_rows(sql: str, params: tuple | None = None) -> list[dict]:
    conn = snowflake_connection()
    try:
        cur = conn.cursor(snowflake.connector.DictCursor)
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        return [{str(k).lower(): v for k, v in row.items()} for row in rows]
    finally:
        conn.close()


def ensure_live_table():
    con = warehouse_connection()
    con.execute("""
        CREATE TABLE IF NOT EXISTS live_events (
            event_id VARCHAR,
            event_type VARCHAR,
            event_ts TIMESTAMP,
            order_id BIGINT,
            customer_id BIGINT,
            amount DOUBLE,
            status VARCHAR,
            channel VARCHAR,
            region VARCHAR,
            schema_version INTEGER,
            ingested_at TIMESTAMP
        )
    """)
    con.close()


def persist_live_event(event: dict):
    ensure_live_table()
    con = warehouse_connection()
    con.execute(
        """INSERT INTO live_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            event.get("event_id"), event.get("event_type"), event.get("event_ts"),
            event.get("order_id"), event.get("customer_id"), event.get("amount"),
            event.get("status"), event.get("channel"), event.get("region"),
            event.get("schema_version", 1), datetime.now(timezone.utc),
        ],
    )
    con.close()


def minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def write_batch_to_minio(rows: list[dict]):
    if not rows:
        return None
    client = minio_client()
    if not client.bucket_exists(RAW_BUCKET):
        client.make_bucket(RAW_BUCKET)
    now = datetime.now(timezone.utc)
    table = pa.Table.from_pylist(rows)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    payload = buf.getvalue()
    key = f"orders/dt={now:%Y-%m-%d}/hour={now:%H}/part-{int(now.timestamp()*1000)}.parquet"
    client.put_object(
        RAW_BUCKET,
        key,
        io.BytesIO(payload),
        length=len(payload),
        content_type="application/octet-stream",
    )
    return f"s3://{RAW_BUCKET}/{key}"


async def broadcast(payload: dict):
    stale = []
    for ws in list(clients):
        try:
            await ws.send_json(payload)
        except Exception:
            stale.append(ws)
    for ws in stale:
        clients.discard(ws)


def publish_to_ui(payload: dict):
    global main_loop
    if main_loop and main_loop.is_running():
        asyncio.run_coroutine_threadsafe(broadcast(payload), main_loop)


def consumer_worker():
    batch: list[dict] = []
    last_flush = time.time()
    while not consumer_stop.is_set():
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                auto_offset_reset="latest",
                group_id="live-dashboard-v2",
                enable_auto_commit=True,
                consumer_timeout_ms=1000,
            )
            while not consumer_stop.is_set():
                received = False
                for msg in consumer:
                    received = True
                    event = msg.value
                    event["latency_ms"] = max(1, int((datetime.now(timezone.utc) - datetime.fromisoformat(event["event_ts"])).total_seconds() * 1000))
                    persist_live_event(event)
                    EVENTS.inc()
                    with state_lock:
                        latest_events.insert(0, event)
                        del latest_events[100:]
                    batch.append(event)
                    lake_uri = None
                    if len(batch) >= 10 or time.time() - last_flush >= 3:
                        lake_uri = write_batch_to_minio(batch)
                        batch.clear()
                        last_flush = time.time()
                    publish_to_ui({"type": "event", "event": event, "lake_uri": lake_uri})
                if not received and batch and time.time() - last_flush >= 3:
                    lake_uri = write_batch_to_minio(batch)
                    batch.clear()
                    last_flush = time.time()
                    publish_to_ui({"type": "lake_flush", "lake_uri": lake_uri})
            consumer.close()
        except Exception as exc:
            publish_to_ui({"type": "health", "kafka": "retrying", "detail": str(exc)})
            time.sleep(2)


def simulator_worker(rate: float):
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=5,
        )
        order_id = int(time.time()) % 100000
        statuses = ["CREATED", "PAID", "SHIPPED", "CANCELLED"]
        channels = ["Web", "Mobile", "Partner API"]
        regions = ["India", "APAC", "Europe", "North America"]
        while not simulator_stop.is_set():
            order_id += 1
            event = {
                "event_id": f"evt-{int(time.time()*1000)}-{order_id}",
                "event_type": "order.updated",
                "event_ts": datetime.now(timezone.utc).isoformat(),
                "order_id": order_id,
                "customer_id": random.randint(1, 500),
                "amount": round(random.uniform(100, 15000), 2),
                "status": random.choice(statuses),
                "channel": random.choice(channels),
                "region": random.choice(regions),
                "schema_version": 1,
            }
            producer.send(TOPIC, event).get(timeout=10)
            simulator_stop.wait(max(0.05, 1.0 / max(rate, 0.1)))
    except Exception as exc:
        publish_to_ui({"type": "health", "kafka": "error", "detail": str(exc)})
    finally:
        if producer:
            producer.flush(timeout=5)
            producer.close()


@app.on_event("startup")
async def startup_event():
    global main_loop, consumer_thread
    main_loop = asyncio.get_running_loop()
    ensure_live_table()
    consumer_stop.clear()
    consumer_thread = threading.Thread(target=consumer_worker, daemon=True)
    consumer_thread.start()


@app.on_event("shutdown")
async def shutdown_event():
    consumer_stop.set()
    simulator_stop.set()


@app.get("/", include_in_schema=False)
def dashboard():
    index_file = STATIC_DIR / "index.html"
    if not index_file.exists():
        raise HTTPException(status_code=503, detail="Dashboard static file is missing")
    return FileResponse(index_file)


@app.get("/marts", include_in_schema=False)
def marts_dashboard():
    page = STATIC_DIR / "marts.html"
    if not page.exists():
        raise HTTPException(status_code=503, detail="Marts dashboard static file is missing")
    return FileResponse(page)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "warehouse": WAREHOUSE,
        "kafka": KAFKA_BOOTSTRAP,
        "minio": MINIO_ENDPOINT,
        "bucket": RAW_BUCKET,
        "snowflake_database": SNOWFLAKE_DB,
        "snowflake_warehouse": SNOWFLAKE_WH,
    }


@app.get("/v1/snowflake/health")
def snowflake_health():
    try:
        rows = snowflake_rows(
            "SELECT CURRENT_DATABASE() database_name, CURRENT_WAREHOUSE() warehouse_name, CURRENT_ROLE() role_name, CURRENT_TIMESTAMP() checked_at"
        )
        return {"status": "connected", **rows[0]}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Snowflake unavailable: {exc}")


@app.get("/v1/marts/department/{department}")
def department_mart(department: str, limit: int = 30):
    department = department.lower().strip()
    allowed = {"executive", "sales", "finance", "customer-success", "operations"}
    if department not in allowed:
        raise HTTPException(status_code=404, detail="Unknown department")

    limit = min(max(limit, 5), 100)

    try:
        summary = snowflake_rows(f"""
            SELECT
                COUNT(*) AS order_count,
                COALESCE(ROUND(SUM(amount), 2), 0) AS gmv,
                COUNT(DISTINCT customer_id) AS customers,
                COALESCE(ROUND(AVG(amount), 2), 0) AS avg_order_value,
                COUNT_IF(status = 'PAID') AS paid_orders,
                COUNT_IF(status = 'SHIPPED') AS shipped_orders,
                COUNT_IF(status = 'CANCELLED') AS cancelled_orders,
                COUNT_IF(source_type = 'EVENT') AS event_backed_orders,
                MAX(updated_at) AS freshest_record
            FROM {SNOWFLAKE_DB}.MARTS.MART_ORDERS
        """)[0]

        if department == "sales":
            breakdown = snowflake_rows(f"""
                SELECT COALESCE(country, 'Unknown') label,
                       COUNT(*) orders,
                       COALESCE(ROUND(SUM(amount), 2), 0) value
                FROM {SNOWFLAKE_DB}.MARTS.MART_ORDERS
                GROUP BY 1 ORDER BY value DESC LIMIT 8
            """)
        elif department == "finance":
            breakdown = snowflake_rows(f"""
                SELECT status label,
                       COUNT(*) orders,
                       COALESCE(ROUND(SUM(amount), 2), 0) value
                FROM {SNOWFLAKE_DB}.MARTS.MART_ORDERS
                GROUP BY 1 ORDER BY value DESC
            """)
        elif department == "customer-success":
            breakdown = snowflake_rows(f"""
                SELECT COALESCE(country, 'Unknown') label,
                       COUNT(DISTINCT customer_id) orders,
                       COUNT_IF(status = 'CANCELLED') value
                FROM {SNOWFLAKE_DB}.MARTS.MART_ORDERS
                GROUP BY 1 ORDER BY value DESC, orders DESC LIMIT 8
            """)
        elif department == "operations":
            breakdown = snowflake_rows(f"""
                SELECT COALESCE(region, 'Unassigned') label,
                       COUNT(*) orders,
                       COUNT_IF(status = 'SHIPPED') value
                FROM {SNOWFLAKE_DB}.MARTS.MART_ORDERS
                GROUP BY 1 ORDER BY orders DESC LIMIT 8
            """)
        else:
            breakdown = snowflake_rows(f"""
                SELECT status label,
                       COUNT(*) orders,
                       COALESCE(ROUND(SUM(amount), 2), 0) value
                FROM {SNOWFLAKE_DB}.MARTS.MART_ORDERS
                GROUP BY 1 ORDER BY orders DESC
            """)

        rows = snowflake_rows(f"""
            SELECT order_id, customer_id, full_name, email, country,
                   amount, status, updated_at, event_id, channel, region, source_type
            FROM {SNOWFLAKE_DB}.MARTS.MART_ORDERS
            ORDER BY updated_at DESC
            LIMIT {limit}
        """)
        for row in rows:
            row["email"] = mask_email(row.get("email"))

        return {
            "department": department,
            "summary": summary,
            "breakdown": breakdown,
            "rows": rows,
            "source": f"{SNOWFLAKE_DB}.MARTS.MART_ORDERS",
        }
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Snowflake marts unavailable: {exc}")


@app.get("/v1/orders")
def orders(authorization: str | None = Header(default=None), limit: int = 50):
    authorize(authorization)
    started = time.perf_counter()
    path = "/v1/orders"
    try:
        con = warehouse_connection(read_only=True)
        rows = con.execute("""
            SELECT order_id, customer_id, full_name, email, country, amount, status, updated_at
            FROM mart_orders ORDER BY updated_at DESC LIMIT ?
        """, [min(limit, 500)]).fetchdf().to_dict("records")
        con.close()
        for row in rows:
            row["email"] = mask_email(str(row["email"]))
        REQUESTS.labels(path, "200").inc()
        return {"count": len(rows), "data": rows}
    except Exception as exc:
        REQUESTS.labels(path, "500").inc()
        raise HTTPException(status_code=503, detail=f"Warehouse unavailable: {exc}")
    finally:
        LATENCY.labels(path).observe(time.perf_counter() - started)


@app.get("/v1/kpis")
def kpis(authorization: str | None = Header(default=None)):
    authorize(authorization)
    con = warehouse_connection(read_only=True)
    row = con.execute("""
        SELECT count(*) order_count,
               round(sum(amount),2) gross_order_value,
               count(distinct customer_id) customers,
               max(updated_at) freshest_record
        FROM mart_orders
    """).fetchone()
    con.close()
    return {"order_count": row[0], "gross_order_value": row[1], "customers": row[2], "freshest_record": row[3]}


@app.get("/v1/live/events")
def live_events(limit: int = 50):
    with state_lock:
        data = list(latest_events[: min(max(limit, 1), 100)])
    return {"count": len(data), "data": data}


@app.get("/v1/live/lake")
def lake_status():
    client = minio_client()
    if not client.bucket_exists(RAW_BUCKET):
        return {"bucket": RAW_BUCKET, "objects": 0, "latest": []}
    objects = list(client.list_objects(RAW_BUCKET, prefix="orders/", recursive=True))
    objects.sort(key=lambda obj: obj.last_modified or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return {
        "bucket": RAW_BUCKET,
        "objects": len(objects),
        "latest": [obj.object_name for obj in objects[:10]],
    }


@app.post("/v1/simulation/start")
def start_simulation(rate: float = 2.0):
    global simulator_thread
    if simulator_thread and simulator_thread.is_alive():
        return {"status": "already_running", "rate": rate}
    rate = min(max(rate, 0.1), 20.0)
    simulator_stop.clear()
    simulator_thread = threading.Thread(target=simulator_worker, args=(rate,), daemon=True)
    simulator_thread.start()
    return {"status": "started", "rate": rate}


@app.post("/v1/simulation/stop")
def stop_simulation():
    simulator_stop.set()
    return {"status": "stopping"}


@app.websocket("/ws/live")
async def websocket_live(websocket: WebSocket):
    await websocket.accept()
    clients.add(websocket)
    try:
        await websocket.send_json({"type": "connected", "message": "Live stream connected"})
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        clients.discard(websocket)
    except Exception:
        clients.discard(websocket)


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
