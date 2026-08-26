select
    o.order_id,
    o.customer_id,
    c.full_name,
    c.email,
    c.country,
    o.amount,
    o.status,
    o.updated_at
from {{ ref('stg_orders') }} o
join {{ ref('stg_customers') }} c
  on o.customer_id = c.customer_id
