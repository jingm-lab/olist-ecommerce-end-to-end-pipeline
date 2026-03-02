{{ config(materialized='incremental') }}

select DISTINCT
    v:order_id::string as order_id,
    v:order_item_id::INT as order_item_id,
    v:product_id::VARCHAR as product_id,
    v:seller_id::VARCHAR as seller_id,
    TO_TIMESTAMP(v:shipping_limit_date::NUMBER, 6) as shipping_limit_date,
    v:price::NUMERIC(10,2) as price,
    v:freight_value::NUMERIC(10,2) as freight_value,
    v:created_at::TIMESTAMP as created_at
from {{ source('raw', 'order_items') }}

{% if is_incremental() %}
    where v:created_at::TIMESTAMP > (select max(created_at) from {{ this }})
{% endif %}

