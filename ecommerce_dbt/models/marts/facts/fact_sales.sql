{{ config(materialized = 'incremental', unique_key=['order_id','order_item_id']) }}


SELECT
    oi.order_id,
    oi.order_item_id,
    od.customer_id,
    oi.seller_id,
    oi.product_id,
    od.order_purchase_timestamp::date AS order_purchase_date,
    od.order_delivered_carrier_date::date AS order_delivered_carrier_date,
    od.order_delivered_customer_date::date AS order_delivered_customer_date,
    od.order_estimated_delivery_date::date AS order_estimated_delivery_date,
    oi.price,
    oi.freight_value,
    GREATEST(oi.created_at, od.updated_at) AS last_updated
FROM {{ ref('stg_order_items') }} oi
    LEFT JOIN {{ ref('stg_orders') }} od
    ON oi.order_id = od.order_id

{% if is_incremental() %}
    where GREATEST(oi.created_at, od.updated_at) > (select max(last_updated) from {{ this }})
{% endif %}
