-- fact_order_items.sql
select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value,
    shipping_date,
    'batch' as source_type
from {{ ref('stg_order_items') }}

union all

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value,
    cast(null as date) as shipping_date,
    'streaming' as source_type
from {{ ref('stg_new_orders') }}