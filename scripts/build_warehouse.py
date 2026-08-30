from pathlib import Path

import duckdb

Path("data").mkdir(exist_ok=True)
con = duckdb.connect("data/warehouse.duckdb")

con.execute("""
CREATE OR REPLACE TABLE stg_customers AS
SELECT * FROM read_parquet('data/mdm/customers_golden.parquet')
""")

con.execute("""
CREATE OR REPLACE TABLE stg_customer_xref AS
SELECT * FROM read_parquet('data/mdm/customer_xref.parquet')
""")

con.execute("""
CREATE OR REPLACE TABLE stg_orders AS
SELECT * FROM read_parquet('data/raw_batch/orders.parquet')
""")

con.execute("""
CREATE OR REPLACE TABLE mart_orders AS
SELECT
    o.order_id,
    x.master_customer_id AS customer_id,
    c.full_name,
    c.email,
    c.country,
    CAST(o.amount AS DOUBLE) AS amount,
    UPPER(o.status) AS status,
    o.updated_at
FROM stg_orders o
JOIN stg_customer_xref x
    ON o.customer_id = x.source_customer_id
JOIN stg_customers c
    ON x.master_customer_id = c.master_customer_id
""")

print(
    con.execute(
        "select count(*) AS row_count, sum(amount) AS gmv from mart_orders"
    ).fetchall()
)
con.close()
