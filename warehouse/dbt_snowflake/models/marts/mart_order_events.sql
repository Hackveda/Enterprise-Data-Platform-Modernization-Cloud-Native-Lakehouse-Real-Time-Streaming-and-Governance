select
    event_id,
    event_type,
    event_ts,
    order_id,
    customer_id,
    amount,
    status,
    channel,
    region,
    schema_version,
    loaded_at
from {{ ref('stg_order_events') }}
