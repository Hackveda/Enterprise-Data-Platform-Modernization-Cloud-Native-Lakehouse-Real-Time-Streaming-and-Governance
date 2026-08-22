select customer_id, full_name, lower(trim(email)) email,
       upper(trim(country)) country, created_at
from read_parquet('data/raw_batch/customers.parquet')
