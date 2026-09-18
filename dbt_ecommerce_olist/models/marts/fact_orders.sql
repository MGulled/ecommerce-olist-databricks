-- fact_orders.sql
select
    order_id,
    customer_id,
    order_status,
    cast(purchase_date as date) as purchase_date,
    cast(approved_date as date) as approved_date,
    carrier_delivery_date,
    customer_delivery_date,
    estimated_delivery_date,
    'batch' as source_type
from {{ ref('stg_orders') }}

union all

select distinct
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp as purchase_date,
    order_approved_at as approved_date,
    cast(null as timestamp) as carrier_delivery_date,
    cast(null as timestamp) as customer_delivery_date,
    cast(null as timestamp) as estimated_delivery_date,
    'streaming' as source_type
from {{ ref('stg_new_orders') }}