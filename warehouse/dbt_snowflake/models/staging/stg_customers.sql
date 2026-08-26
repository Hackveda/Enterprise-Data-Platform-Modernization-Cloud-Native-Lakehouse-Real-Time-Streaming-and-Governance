select
    customer_id::number as customer_id,
    trim(full_name)::varchar as full_name,
    lower(trim(email))::varchar as email,
    trim(country)::varchar as country,
    source_file,
    loaded_at
from {{ source('raw', 'raw_customers') }}
