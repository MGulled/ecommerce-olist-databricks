select
    p.product_id,
    coalesce(c.category_name_en, 'unknown') as category_name_en,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
from {{ ref('stg_products') }} p
left join {{ ref('stg_category') }} c
    on lower(trim(p.product_category_name)) = c.category_name_pt