
  create or replace   view ENTERPRISE_PLATFORM.STAGING.stg_orders
  
   as (
    select
    order_id::number as order_id,
    customer_id::number as customer_id,
    amount::number(18,2) as amount,
    upper(trim(status))::varchar as status,
    updated_at::timestamp_tz as updated_at,
    source_file,
    loaded_at
from ENTERPRISE_PLATFORM.RAW.raw_orders
  );

