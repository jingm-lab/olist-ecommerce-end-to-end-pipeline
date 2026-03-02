{{ config(materialized='incremental', unique_key = 'seller_id') }}
with ranked as (
    select
        v:seller_id::string as seller_id,
        v:seller_zip_code_prefix::VARCHAR as seller_zip_code_prefix,
        v:seller_city::VARCHAR as seller_city,
        v:seller_state::VARCHAR as seller_state,
        v:is_active::BOOLEAN as is_active,
        v:created_at::TIMESTAMP as created_at,
        v:updated_at::TIMESTAMP as updated_at,
        row_number() over (partition by v:seller_id::string
        order by v:updated_at::TIMESTAMP desc) as rn
    from {{ source('raw', 'sellers') }}

    {% if is_incremental() %}
        where v:updated_at::TIMESTAMP > (select max(updated_at) from {{ this }})
    {% endif %}
)

select 
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    is_active,
    created_at,
    updated_at
from ranked
where rn = 1

