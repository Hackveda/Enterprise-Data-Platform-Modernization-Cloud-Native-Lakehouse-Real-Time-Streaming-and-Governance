
  create or replace   view ENTERPRISE_PLATFORM.MARTS.mart_order_events
  
   as (
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
from ENTERPRISE_PLATFORM.STAGING.stg_order_events
  );

