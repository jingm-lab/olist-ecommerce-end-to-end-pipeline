{{ config(materialized='incremental', unique_key = 'product_id') }}
with ranked as (
    select
        v:product_id::string as product_id,
        v:product_category_name::VARCHAR as product_category_name,
        v:product_name_length::INT as product_name_length,
        v:product_description_length::INT as product_description_length,
        v:product_photos_qty::INT as product_photos_qty,
        v:product_weight_g::INT as product_weight_g,
        v:product_length_cm::INT as product_length_cm,
        v:product_height_cm::INT as product_height_cm,
        v:product_width_cm::INT as product_width_cm,
        v:is_active::BOOLEAN as is_active,
        v:created_at::TIMESTAMP as created_at,
        v:updated_at::TIMESTAMP as updated_at,
        row_number() over (partition by v:product_id::string
        order by v:updated_at::TIMESTAMP desc) as rn
    from {{ source('raw', 'products') }}

    {% if is_incremental() %}
        where v:updated_at::TIMESTAMP > (select max(updated_at) from {{ this }})
    {% endif %}
)

select 
    product_id,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    is_active,
    created_at,
    updated_at
from ranked
where rn = 1

