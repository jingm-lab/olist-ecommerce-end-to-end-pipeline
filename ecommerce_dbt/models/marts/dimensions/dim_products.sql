{{ config(materialized='table') }}

WITH weight_percentile AS (
    SELECT *, PERCENT_RANK() OVER (ORDER BY product_weight_g) as pct_rank
    FROM {{ ref('stg_products') }}
    WHERE product_weight_g > 0

    UNION ALL
    SELECT *, NULL FROM {{ ref('stg_products') }}
    WHERE product_weight_g = 0
)


SELECT
    w.product_id,
    COALESCE(t.product_category_name_english, w.product_category_name) AS product_category_name,
    w.product_weight_g,
    CASE
        WHEN w.product_weight_g = 0 THEN 'unknown'
        WHEN w.pct_rank <= 0.33 THEN 'light'
        WHEN w.pct_rank <= 0.66 THEN 'medium'
        ELSE 'heavy'
    END AS weight_category,
    w.is_active
FROM weight_percentile w
LEFT JOIN {{ ref('stg_product_category_name_translation') }} t
on w.product_category_name = t.product_category_name
