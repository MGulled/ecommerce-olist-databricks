select
    seller_id,
    seller_zip_code,
    seller_city,
    seller_state
from {{ ref('stg_sellers') }}

union all

select distinct
    seller_id,
    cast(null as string) as seller_zip_code,
    cast(null as string) as seller_city,
    cast(null as string) as seller_state
from {{ ref('stg_new_orders') }}
where seller_id not in (select seller_id from {{ ref('stg_sellers') }})