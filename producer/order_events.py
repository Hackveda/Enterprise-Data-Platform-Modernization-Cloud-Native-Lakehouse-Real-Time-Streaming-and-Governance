import json, os, random, time
from datetime import datetime, timezone
from kafka import KafkaProducer

bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
producer = KafkaProducer(
    bootstrap_servers=bootstrap,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
)

statuses = ["CREATED", "PAID", "SHIPPED", "CANCELLED"]
for i in range(1, 101):
    event = {
        "event_id": f"evt-{int(time.time()*1000)}-{i}",
        "event_type": "order.updated",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "order_id": i,
        "customer_id": random.randint(1, 3),
        "amount": round(random.uniform(100, 5000), 2),
        "status": random.choice(statuses),
        "schema_version": 1,
    }
    producer.send("orders.events.v1", event)
    print(event)
    time.sleep(0.25)
producer.flush()
