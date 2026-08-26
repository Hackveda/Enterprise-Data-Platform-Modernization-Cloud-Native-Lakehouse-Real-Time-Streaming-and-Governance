
  
    

        create or replace transient table ENTERPRISE_PLATFORM.MARTS.mart_orders
         as
        (select
    o.order_id,
    o.customer_id,
    c.full_name,
    c.email,
    c.country,
    o.amount,
    o.status,
    o.updated_at
from ENTERPRISE_PLATFORM.STAGING.stg_orders o
join ENTERPRISE_PLATFORM.STAGING.stg_customers c
  on o.customer_id = c.customer_id
        );
      
  