with ranked as (
    select
        event_id::varchar as event_id,
        event_type::varchar as event_type,
        event_ts::timestamp_tz as event_ts,
        order_id::number as order_id,
        customer_id::number as customer_id,
        amount::number(18,2) as amount,
        upper(trim(status))::varchar as status,
        channel::varchar as channel,
        region::varchar as region,
        schema_version::number as schema_version,
        raw_payload,
        loaded_at,
        row_number() over (
            partition by event_id
            order by loaded_at desc
        ) as rn
    from {{ source('raw', 'raw_order_events') }}
)
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
    raw_payload,
    loaded_at
from ranked
where rn = 1
