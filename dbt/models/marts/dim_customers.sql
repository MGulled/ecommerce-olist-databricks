select
customer_id,
unique_id,
zip_code,
city,
state 
from {{ref('stg_customers')}}    