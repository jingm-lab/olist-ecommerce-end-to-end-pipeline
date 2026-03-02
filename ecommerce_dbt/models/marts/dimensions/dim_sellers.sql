{{ config(materialized='table') }}

SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    is_active
FROM {{ ref('stg_sellers') }}