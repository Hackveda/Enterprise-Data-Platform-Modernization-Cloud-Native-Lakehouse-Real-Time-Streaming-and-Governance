import os
import time
import duckdb
from fastapi import FastAPI, Header, HTTPException
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

WAREHOUSE = os.getenv("WAREHOUSE_PATH", "./data/warehouse.duckdb")
API_TOKEN = os.getenv("API_TOKEN", "talent-grid-demo-token")

app = FastAPI(title="Enterprise Data Platform API", version="1.0.0")
REQUESTS = Counter("platform_api_requests_total", "API requests", ["path", "status"])
LATENCY = Histogram("platform_api_latency_seconds", "API latency", ["path"])


def authorize(authorization: str | None):
    if authorization != f"Bearer {API_TOKEN}":
        raise HTTPException(status_code=401, detail="Invalid or missing bearer token")


def mask_email(email: str) -> str:
    local, _, domain = email.partition("@")
    if not domain:
        return "***"
    return f"{local[:1]}***@{domain}"

@app.get("/health")
def health():
    return {"status": "ok", "warehouse": WAREHOUSE}

@app.get("/v1/orders")
def orders(authorization: str | None = Header(default=None), limit: int = 50):
    authorize(authorization)
    started = time.perf_counter()
    path = "/v1/orders"
    try:
        con = duckdb.connect(WAREHOUSE, read_only=True)
        rows = con.execute("""
            SELECT order_id, customer_id, full_name, email, country, amount, status, updated_at
            FROM mart_orders ORDER BY updated_at DESC LIMIT ?
        """, [min(limit, 500)]).fetchdf().to_dict("records")
        for r in rows:
            r["email"] = mask_email(str(r["email"]))
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
    con = duckdb.connect(WAREHOUSE, read_only=True)
    row = con.execute("""
        SELECT count(*) order_count,
               round(sum(amount),2) gross_order_value,
               count(distinct customer_id) customers,
               max(updated_at) freshest_record
        FROM mart_orders
    """).fetchone()
    return {"order_count": row[0], "gross_order_value": row[1], "customers": row[2], "freshest_record": row[3]}

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
