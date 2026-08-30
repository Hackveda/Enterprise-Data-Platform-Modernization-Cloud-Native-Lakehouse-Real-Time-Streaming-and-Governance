select
    master_customer_id,
    full_name,
    lower(trim(email)) as email,
    phone,
    upper(trim(country)) as country,
    address,
    created_at,
    updated_at,
    match_type,
    match_score,
    mdm_action
from read_parquet('data/mdm/customers_golden.parquet')
