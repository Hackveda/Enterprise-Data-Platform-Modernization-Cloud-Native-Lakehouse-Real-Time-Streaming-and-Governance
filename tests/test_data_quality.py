import duckdb

def test_mart_orders_quality():
    con=duckdb.connect("data/warehouse.duckdb", read_only=True)
    rows=con.execute("select count(*) from mart_orders").fetchone()[0]
    nulls=con.execute("select count(*) from mart_orders where order_id is null or customer_id is null").fetchone()[0]
    dupes=con.execute("select count(*)-count(distinct order_id) from mart_orders").fetchone()[0]
    assert rows > 0
    assert nulls == 0
    assert dupes == 0

def test_reconciliation_with_raw_orders():
    con=duckdb.connect("data/warehouse.duckdb", read_only=True)
    source=con.execute("select count(*) from read_parquet('data/raw_batch/orders.parquet')").fetchone()[0]
    target=con.execute("select count(*) from mart_orders").fetchone()[0]
    assert source == target
