{{ config(materialized = 'table') }} 

WITH installments as (
    SELECT 
        order_id, 
        CASE WHEN max(payment_installments) > 1 THEN True
        ELSE False END AS has_installments
FROM {{ ref('stg_order_payments') }}
GROUP BY order_id
),

most_recent_reviews AS (
    SELECT 
        order_id, 
        review_score, 
        row_number() OVER (PARTITION BY order_id ORDER BY review_answer_timestamp desc) AS rn
    FROM {{ ref('stg_order_reviews') }}
)

SELECT
    o.order_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    i.has_installments,
    m.review_score
FROM
    {{ ref('stg_orders') }} o
    INNER JOIN installments i ON o.order_id = i.order_id
    LEFT JOIN most_recent_reviews m
    ON o.order_id = m.order_id AND m.rn = 1
    
