import os
from pathlib import Path
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

DB = os.getenv('SNOWFLAKE_DATABASE', 'ENTERPRISE_PLATFORM')
WH = os.getenv('SNOWFLAKE_WAREHOUSE', 'PLATFORM_WH')
ROLE = os.getenv('SNOWFLAKE_ROLE', 'PLATFORM_ROLE')

required = ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD']
missing = [k for k in required if not os.getenv(k)]
if missing:
    raise SystemExit(f"Missing environment variables: {', '.join(missing)}")

raw_dir = Path('data/raw_batch')
customers_file = raw_dir / 'customers.parquet'
orders_file = raw_dir / 'orders.parquet'
if not customers_file.exists() or not orders_file.exists():
    raise SystemExit('Raw Parquet files are missing. Run: python batch/extract_postgres.py')

conn = snowflake.connector.connect(
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    warehouse=WH,
    database=DB,
    schema='RAW',
    role=ROLE,
)

try:
    cur = conn.cursor()
    cur.execute(f'USE WAREHOUSE {WH}')
    cur.execute(f'USE DATABASE {DB}')
    cur.execute('USE SCHEMA RAW')

    customers = pd.read_parquet(customers_file)
    orders = pd.read_parquet(orders_file)
    customers.columns = [c.upper() for c in customers.columns]
    orders.columns = [c.upper() for c in orders.columns]

    customers['SOURCE_FILE'] = str(customers_file)
    orders['SOURCE_FILE'] = str(orders_file)

    cur.execute('TRUNCATE TABLE RAW_CUSTOMERS')
    cur.execute('TRUNCATE TABLE RAW_ORDERS')

    ok1, chunks1, rows1, _ = write_pandas(conn, customers, 'RAW_CUSTOMERS', database=DB, schema='RAW', quote_identifiers=False)
    ok2, chunks2, rows2, _ = write_pandas(conn, orders, 'RAW_ORDERS', database=DB, schema='RAW', quote_identifiers=False)
    if not ok1 or not ok2:
        raise RuntimeError('Snowflake write_pandas reported a failed load')

    print(f'Snowflake RAW load complete: customers={rows1} ({chunks1} chunks), orders={rows2} ({chunks2} chunks)')
finally:
    conn.close()
