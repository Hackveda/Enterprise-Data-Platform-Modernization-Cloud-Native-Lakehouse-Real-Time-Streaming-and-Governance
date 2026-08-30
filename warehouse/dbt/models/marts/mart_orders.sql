select
    o.order_id,
    x.master_customer_id,
    c.full_name,
    c.email,
    c.country,
    o.amount,
    o.status,
    o.updated_at
from {{ ref('stg_orders') }} o
join {{ ref('stg_customer_xref') }} x
    on o.customer_id = x.source_customer_id
join {{ ref('stg_customers') }} c
    on x.master_customer_id = c.master_customer_id
