import os
from pathlib import Path

import pandas as pd
import psycopg

from mdm.service import CustomerMDM

url = os.getenv("DATABASE_URL", "postgresql://platform:platform@localhost:5432/commerce")
out = Path("data/raw_batch")
out.mkdir(parents=True, exist_ok=True)
mdm_out = Path("data/mdm")
mdm_out.mkdir(parents=True, exist_ok=True)

with psycopg.connect(url) as conn:
    customers = pd.read_sql("select * from customers", conn)
    orders = pd.read_sql("select * from orders", conn)

# Bronze/raw copies preserve the operational source exactly as received.
customers.to_parquet(out / "customers.parquet", index=False)
orders.to_parquet(out / "orders.parquet", index=False)

# MDM resolves every source customer to one enterprise-wide golden identity.
mdm = CustomerMDM()
master_rows = []
xref_rows = []
for row in customers.to_dict("records"):
    source_customer_id = str(row["customer_id"])
    result = mdm.upsert({
        "source_system": "postgres_oltp",
        "source_customer_id": source_customer_id,
        "full_name": row.get("full_name"),
        "email": row.get("email"),
        "phone": row.get("phone"),
        "country": row.get("country"),
        "address": row.get("address"),
    })
    golden = result.golden_record.copy()
    golden["source_customer_id"] = source_customer_id
    golden["match_type"] = result.match_type
    golden["match_score"] = result.match_score
    golden["mdm_action"] = result.action
    master_rows.append(golden)
    xref_rows.append({
        "source_customer_id": int(row["customer_id"]),
        "master_customer_id": result.master_customer_id,
    })

master_df = pd.DataFrame(master_rows).drop_duplicates(subset=["master_customer_id"])
xref_df = pd.DataFrame(xref_rows)

# Silver/mastered customer output consumed by dbt.
master_df.to_parquet(mdm_out / "customers_golden.parquet", index=False)
xref_df.to_parquet(mdm_out / "customer_xref.parquet", index=False)

print(
    f"customers={len(customers)} orders={len(orders)} "
    f"golden_customers={len(master_df)} written to {out} and {mdm_out}"
)
