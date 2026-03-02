{{ config(materialized='incremental') }}

select DISTINCT
    v:order_id::string as order_id,
    v:payment_sequential::INT as payment_sequential,
    v:payment_type::VARCHAR as payment_type,
    v:payment_installments::INT as payment_installments,
    v:payment_value::NUMERIC as payment_value,
    v:created_at::TIMESTAMP as created_at
from {{ source('raw', 'order_payments') }}

{% if is_incremental() %}
    where v:created_at::TIMESTAMP > (select max(created_at) from {{ this }})
{% endif %}