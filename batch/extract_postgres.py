import os
from pathlib import Path
import pandas as pd
import psycopg

url=os.getenv("DATABASE_URL", "postgresql://platform:platform@localhost:5432/commerce")
out=Path("data/raw_batch"); out.mkdir(parents=True, exist_ok=True)
with psycopg.connect(url) as conn:
    customers=pd.read_sql("select * from customers", conn)
    orders=pd.read_sql("select * from orders", conn)
customers.to_parquet(out/"customers.parquet", index=False)
orders.to_parquet(out/"orders.parquet", index=False)
print(f"customers={len(customers)} orders={len(orders)} written to {out}")
