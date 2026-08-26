select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select customer_id
from ENTERPRISE_PLATFORM.STAGING.stg_order_events
where customer_id is null



      
    ) dbt_internal_test