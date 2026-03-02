{{ config(materialized='incremental', unique_key = 'order_id') }}

with ranked as (select
    v:order_id::string as order_id,
    v:customer_id::string as customer_id,
    v:order_status::VARCHAR as order_status,
    TO_TIMESTAMP(v:order_purchase_timestamp::NUMBER, 6) as order_purchase_timestamp,
    TO_TIMESTAMP(v:order_approved_at::NUMBER, 6) as order_approved_at,
    TO_TIMESTAMP(v:order_delivered_carrier_date::NUMBER, 6) as order_delivered_carrier_date,
    TO_TIMESTAMP(v:order_delivered_customer_date::NUMBER, 6) as order_delivered_customer_date,
    TO_TIMESTAMP(v:order_estimated_delivery_date::NUMBER, 6) as order_estimated_delivery_date,
    v:created_at::TIMESTAMP as created_at,
    COALESCE(TO_TIMESTAMP(v:updated_at::NUMBER, 6), v:created_at::TIMESTAMP) as updated_at,
    row_number() over (partition by v:order_id::string order by COALESCE(TO_TIMESTAMP(v:updated_at::NUMBER, 6), v:created_at::TIMESTAMP) desc) as rn
from {{ source('raw', 'orders') }}

{% if is_incremental() %}
    where COALESCE(TO_TIMESTAMP(v:updated_at::NUMBER, 6), v:created_at::TIMESTAMP) > (select max(updated_at) from {{ this }})
{% endif %})

select 
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    created_at,
    updated_at
from ranked
where rn = 1