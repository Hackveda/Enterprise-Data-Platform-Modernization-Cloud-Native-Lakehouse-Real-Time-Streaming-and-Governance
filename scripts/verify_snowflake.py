import os
import snowflake.connector

required = ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD']
missing = [k for k in required if not os.getenv(k)]
if missing:
    raise SystemExit(f"Missing environment variables: {', '.join(missing)}")

conn = snowflake.connector.connect(
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'PLATFORM_WH'),
    database=os.getenv('SNOWFLAKE_DATABASE', 'ENTERPRISE_PLATFORM'),
    role=os.getenv('SNOWFLAKE_ROLE', 'PLATFORM_ROLE'),
)

checks = [
    ('RAW.RAW_CUSTOMERS', 'select count(*) from RAW.RAW_CUSTOMERS'),
    ('RAW.RAW_ORDERS', 'select count(*) from RAW.RAW_ORDERS'),
    ('STAGING.STG_CUSTOMERS', 'select count(*) from STAGING.STG_CUSTOMERS'),
    ('STAGING.STG_ORDERS', 'select count(*) from STAGING.STG_ORDERS'),
    ('MARTS.MART_ORDERS', 'select count(*) from MARTS.MART_ORDERS'),
]

try:
    cur = conn.cursor()
    print('\nSnowflake layer verification')
    print('-' * 54)
    for name, sql in checks:
        cur.execute(sql)
        count = cur.fetchone()[0]
        print(f'{name:<32} rows={count}')

    cur.execute('''
        select count(*) as order_count,
               round(sum(amount),2) as gmv,
               count(distinct customer_id) as customers,
               max(updated_at) as freshest_record
        from MARTS.MART_ORDERS
    ''')
    row = cur.fetchone()
    print('-' * 54)
    print(f'MART KPI: orders={row[0]} gmv={row[1]} customers={row[2]} freshest={row[3]}')
finally:
    conn.close()
