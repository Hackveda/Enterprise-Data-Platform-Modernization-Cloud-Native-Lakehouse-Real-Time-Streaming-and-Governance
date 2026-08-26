import json
import os
import time

import snowflake.connector
from kafka import KafkaConsumer

TOPIC = "orders.events.v1"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DB = os.getenv("SNOWFLAKE_DATABASE", "ENTERPRISE_PLATFORM")
WH = os.getenv("SNOWFLAKE_WAREHOUSE", "PLATFORM_WH")
ROLE = os.getenv("SNOWFLAKE_ROLE", "PLATFORM_ROLE")
IDLE_SECONDS = int(os.getenv("SNOWFLAKE_EVENT_IDLE_SECONDS", "5"))
BATCH_SIZE = int(os.getenv("SNOWFLAKE_EVENT_BATCH_SIZE", "50"))

required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
missing = [name for name in required if not os.getenv(name)]
if missing:
    raise SystemExit(f"Missing environment variables: {', '.join(missing)}")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="snowflake-raw-events-v2",
    enable_auto_commit=False,
    consumer_timeout_ms=1000,
)

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    warehouse=WH,
    database=DB,
    schema="RAW",
    role=ROLE,
)

insert_sql = f"""
INSERT INTO {DB}.RAW.RAW_ORDER_EVENTS (
    event_id, event_type, event_ts, order_id, customer_id, amount, status,
    channel, region, schema_version, raw_payload
)
SELECT
    %s,
    %s,
    TO_TIMESTAMP_TZ(%s),
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    PARSE_JSON(%s)
WHERE NOT EXISTS (
    SELECT 1
    FROM {DB}.RAW.RAW_ORDER_EVENTS
    WHERE event_id = %s
)
"""


def flush(rows: list[dict]) -> int:
    if not rows:
        return 0

    cur = conn.cursor()
    inserted = 0
    try:
        for event in rows:
            event_id = str(event["event_id"])
            params = (
                event_id,
                event.get("event_type", "order.updated"),
                event["event_ts"],
                int(event["order_id"]),
                int(event["customer_id"]),
                float(event["amount"]),
                str(event["status"]).upper(),
                event.get("channel"),
                event.get("region"),
                int(event.get("schema_version", 1)),
                json.dumps(event),
                event_id,
            )
            cur.execute(insert_sql, params)
            inserted += max(cur.rowcount or 0, 0)
        conn.commit()
        return inserted
    finally:
        cur.close()


try:
    batch: list[dict] = []
    last_message_at = time.time()
    total_seen = 0
    total_inserted = 0

    print(f"Consuming {TOPIC} from {KAFKA_BOOTSTRAP} into {DB}.RAW.RAW_ORDER_EVENTS")

    while True:
        received = False
        for msg in consumer:
            received = True
            total_seen += 1
            last_message_at = time.time()
            batch.append(msg.value)

            if len(batch) >= BATCH_SIZE:
                total_inserted += flush(batch)
                consumer.commit()
                print(f"events_seen={total_seen} inserted={total_inserted}")
                batch.clear()

        if batch:
            total_inserted += flush(batch)
            consumer.commit()
            print(f"events_seen={total_seen} inserted={total_inserted}")
            batch.clear()

        if not received and time.time() - last_message_at >= IDLE_SECONDS:
            break

    print(
        "Snowflake event ingestion complete: "
        f"seen={total_seen} inserted={total_inserted}"
    )
finally:
    consumer.close()
    conn.close()
