import io, json, os, time
from datetime import datetime, timezone
import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer
from minio import Minio

bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
client = Minio(endpoint,
    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    secure=False)
bucket = "raw"
if not client.bucket_exists(bucket):
    client.make_bucket(bucket)

consumer = KafkaConsumer(
    "orders.events.v1", bootstrap_servers=bootstrap,
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    auto_offset_reset="earliest", group_id="lakehouse-raw-v1", enable_auto_commit=True)

batch=[]
last_flush=time.time()

def flush(rows):
    if not rows: return
    now=datetime.now(timezone.utc)
    table=pa.Table.from_pylist(rows)
    buf=io.BytesIO(); pq.write_table(table, buf, compression="snappy"); buf.seek(0)
    key=f"orders/dt={now:%Y-%m-%d}/hour={now:%H}/part-{int(now.timestamp()*1000)}.parquet"
    client.put_object(bucket, key, buf, length=len(buf.getvalue()), content_type="application/octet-stream")
    print(f"wrote s3://{bucket}/{key} rows={len(rows)}")

for msg in consumer:
    batch.append(msg.value)
    if len(batch) >= 20 or time.time()-last_flush >= 5:
        flush(batch); batch=[]; last_flush=time.time()
