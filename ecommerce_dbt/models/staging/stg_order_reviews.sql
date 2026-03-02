{{ config(materialized='incremental') }}

select DISTINCT
    v:review_id::string as review_id,
    v:order_id::string as order_id,
    v:review_score::INT as review_score,
    TO_TIMESTAMP(v:review_creation_date::NUMBER, 6) as review_creation_date,
    TO_TIMESTAMP(v:review_answer_timestamp::NUMBER, 6) as review_answer_timestamp,
    v:created_at::TIMESTAMP as created_at
from {{ source('raw', 'order_reviews') }}

{% if is_incremental() %}
    where v:created_at::TIMESTAMP > (select max(created_at) from {{ this }})
{% endif %}