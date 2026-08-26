with batch_orders as (
    select
        order_id,
        customer_id,
        amount,
        status,
        updated_at,
        null::varchar as event_id,
        null::varchar as channel,
        null::varchar as region,
        'BATCH'::varchar as source_type
    from ENTERPRISE_PLATFORM.STAGING.stg_orders
),
stream_orders as (
    select
        order_id,
        customer_id,
        amount,
        status,
        event_ts as updated_at,
        event_id,
        channel,
        region,
        'EVENT'::varchar as source_type
    from ENTERPRISE_PLATFORM.STAGING.stg_order_events
),
all_versions as (
    select * from batch_orders
    union all
    select * from stream_orders
),
latest_order as (
    select *
    from all_versions
    qualify row_number() over (
        partition by order_id
        order by updated_at desc, source_type desc
    ) = 1
)
select
    o.order_id,
    o.customer_id,
    c.full_name,
    c.email,
    c.country,
    o.amount,
    o.status,
    o.updated_at,
    o.event_id,
    o.channel,
    o.region,
    o.source_type
from latest_order o
left join ENTERPRISE_PLATFORM.STAGING.stg_customers c
  on o.customer_id = c.customer_id