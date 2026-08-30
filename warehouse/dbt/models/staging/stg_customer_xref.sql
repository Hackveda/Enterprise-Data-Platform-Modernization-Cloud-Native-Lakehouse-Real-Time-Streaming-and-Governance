select
    source_customer_id,
    master_customer_id
from read_parquet('data/mdm/customer_xref.parquet')
