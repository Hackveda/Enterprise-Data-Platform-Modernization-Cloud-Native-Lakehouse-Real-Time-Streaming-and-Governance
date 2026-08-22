select order_id, customer_id, cast(amount as double) amount,
       upper(trim(status)) status, updated_at
from read_parquet('data/raw_batch/orders.parquet')
