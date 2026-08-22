from pathlib import Path
import duckdb

Path("data").mkdir(exist_ok=True)
con=duckdb.connect("data/warehouse.duckdb")
con.execute("CREATE OR REPLACE TABLE stg_customers AS SELECT * FROM read_parquet('data/raw_batch/customers.parquet')")
con.execute("CREATE OR REPLACE TABLE stg_orders AS SELECT * FROM read_parquet('data/raw_batch/orders.parquet')")
con.execute("""
CREATE OR REPLACE TABLE mart_orders AS
SELECT o.order_id, o.customer_id, c.full_name, c.email, c.country,
       CAST(o.amount AS DOUBLE) amount, upper(o.status) status, o.updated_at
FROM stg_orders o JOIN stg_customers c USING(customer_id)
""")
print(con.execute("select count(*) rows, sum(amount) gmv from mart_orders").fetchall())
