import os
from pathlib import Path

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

DB = os.getenv("SNOWFLAKE_DATABASE", "ENTERPRISE_PLATFORM")
WH = os.getenv("SNOWFLAKE_WAREHOUSE", "PLATFORM_WH")
ROLE = os.getenv("SNOWFLAKE_ROLE", "PLATFORM_ROLE")

required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
missing = [name for name in required if not os.getenv(name)]
if missing:
    raise SystemExit(f"Missing environment variables: {', '.join(missing)}")

raw_dir = Path("data/raw_batch")
customers_file = raw_dir / "customers.parquet"
orders_file = raw_dir / "orders.parquet"

if not customers_file.exists() or not orders_file.exists():
    raise SystemExit(
        "Raw Parquet files are missing. Run: python batch/extract_postgres.py"
    )


def normalize_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Return exactly the columns expected by Snowflake RAW_CUSTOMERS."""
    df = df.copy()
    df.columns = [str(column).upper() for column in df.columns]

    required_columns = ["CUSTOMER_ID", "FULL_NAME", "EMAIL", "COUNTRY"]
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise RuntimeError(
            f"customers.parquet is missing required columns: {', '.join(missing_columns)}"
        )

    df["SOURCE_FILE"] = str(customers_file)

    # Do not send source-only columns such as CREATED_AT to Snowflake unless the
    # RAW table explicitly contains them. This prevents write_pandas from
    # generating INSERT statements with invalid identifiers.
    return df[["CUSTOMER_ID", "FULL_NAME", "EMAIL", "COUNTRY", "SOURCE_FILE"]]


def normalize_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Return exactly the columns expected by Snowflake RAW_ORDERS."""
    df = df.copy()
    df.columns = [str(column).upper() for column in df.columns]

    required_columns = ["ORDER_ID", "CUSTOMER_ID", "AMOUNT", "STATUS"]
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise RuntimeError(
            f"orders.parquet is missing required columns: {', '.join(missing_columns)}"
        )

    # The demo source may expose CREATED_AT while the analytical contract uses
    # UPDATED_AT. Prefer UPDATED_AT when present; otherwise use CREATED_AT.
    if "UPDATED_AT" not in df.columns:
        if "CREATED_AT" in df.columns:
            df["UPDATED_AT"] = df["CREATED_AT"]
        else:
            raise RuntimeError(
                "orders.parquet must contain UPDATED_AT or CREATED_AT"
            )

    df["SOURCE_FILE"] = str(orders_file)

    # Keep the dataframe aligned exactly with RAW_ORDERS. Snowflake supplies
    # LOADED_AT from the table default.
    return df[
        [
            "ORDER_ID",
            "CUSTOMER_ID",
            "AMOUNT",
            "STATUS",
            "UPDATED_AT",
            "SOURCE_FILE",
        ]
    ]


conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    warehouse=WH,
    database=DB,
    schema="RAW",
    role=ROLE,
)

try:
    cur = conn.cursor()
    cur.execute(f"USE WAREHOUSE {WH}")
    cur.execute(f"USE DATABASE {DB}")
    cur.execute("USE SCHEMA RAW")

    customers = normalize_customers(pd.read_parquet(customers_file))
    orders = normalize_orders(pd.read_parquet(orders_file))

    cur.execute("TRUNCATE TABLE RAW_CUSTOMERS")
    cur.execute("TRUNCATE TABLE RAW_ORDERS")

    ok1, chunks1, rows1, _ = write_pandas(
        conn,
        customers,
        "RAW_CUSTOMERS",
        database=DB,
        schema="RAW",
        quote_identifiers=False,
    )
    ok2, chunks2, rows2, _ = write_pandas(
        conn,
        orders,
        "RAW_ORDERS",
        database=DB,
        schema="RAW",
        quote_identifiers=False,
    )

    if not ok1 or not ok2:
        raise RuntimeError("Snowflake write_pandas reported a failed load")

    print(
        "Snowflake RAW load complete: "
        f"customers={rows1} ({chunks1} chunks), "
        f"orders={rows2} ({chunks2} chunks)"
    )
finally:
    conn.close()
