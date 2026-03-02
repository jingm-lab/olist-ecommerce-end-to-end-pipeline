{{ config(materialized = 'incremental', unique_key=['order_id','payment_sequential']) }}


SELECT
    op.order_id,
    op.payment_sequential,
    od.customer_id,
    op.payment_type,
    op.payment_installments,
    op.payment_value,
    op.created_at
FROM {{ ref('stg_order_payments') }} op
    LEFT JOIN {{ ref('stg_orders') }} od
    ON op.order_id = od.order_id

{% if is_incremental() %}
    where op.created_at > (select max(created_at) from {{ this }})
{% endif %}
