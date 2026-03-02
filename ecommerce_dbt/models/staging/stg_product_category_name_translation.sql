{{ config(materialized='table') }}

select DISTINCT
    v:product_category_name::VARCHAR as product_category_name,
    v:product_category_name_english::VARCHAR as product_category_name_english
from {{ source('raw', 'product_category_name_translation') }}