import os

import snowflake.connector

required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
missing = [name for name in required if not os.getenv(name)]
if missing:
    raise SystemExit(f"Missing environment variables: {', '.join(missing)}")

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "PLATFORM_WH"),
    database=os.getenv("SNOWFLAKE_DATABASE", "ENTERPRISE_PLATFORM"),
    role=os.getenv("SNOWFLAKE_ROLE", "PLATFORM_ROLE"),
)

checks = [
    ("RAW.RAW_CUSTOMERS", "select count(*) from RAW.RAW_CUSTOMERS"),
    ("RAW.RAW_ORDERS", "select count(*) from RAW.RAW_ORDERS"),
    ("RAW.RAW_ORDER_EVENTS", "select count(*) from RAW.RAW_ORDER_EVENTS"),
    ("STAGING.STG_CUSTOMERS", "select count(*) from STAGING.STG_CUSTOMERS"),
    ("STAGING.STG_ORDERS", "select count(*) from STAGING.STG_ORDERS"),
    ("STAGING.STG_ORDER_EVENTS", "select count(*) from STAGING.STG_ORDER_EVENTS"),
    ("MARTS.MART_ORDER_EVENTS", "select count(*) from MARTS.MART_ORDER_EVENTS"),
    ("MARTS.MART_ORDERS", "select count(*) from MARTS.MART_ORDERS"),
]

try:
    cur = conn.cursor()
    print("\nSnowflake layer verification")
    print("-" * 72)
    for name, sql in checks:
        cur.execute(sql)
        count = cur.fetchone()[0]
        print(f"{name:<36} rows={count}")

    cur.execute(
        """
        select count(*) as order_count,
               round(sum(amount),2) as gmv,
               count(distinct customer_id) as customers,
               max(updated_at) as freshest_record,
               count_if(source_type = 'EVENT') as event_backed_orders
        from MARTS.MART_ORDERS
        """
    )
    row = cur.fetchone()

    cur.execute(
        """
        select event_id, order_id, customer_id, amount, status, event_ts
        from MARTS.MART_ORDER_EVENTS
        order by event_ts desc
        limit 5
        """
    )
    latest_events = cur.fetchall()

    print("-" * 72)
    print(
        "MART KPI: "
        f"orders={row[0]} gmv={row[1]} customers={row[2]} "
        f"freshest={row[3]} event_backed_orders={row[4]}"
    )
    print("\nLatest transformed events:")
    for event in latest_events:
        print(event)
finally:
    conn.close()
