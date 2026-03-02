{{ config(materialized='incremental', unique_key = 'customer_id') }}
with ranked as (
    select
        v:customer_id::string as customer_id,
        v:customer_unique_id::string as customer_unique_id,
        v:customer_zip_code_prefix::VARCHAR as customer_zip_code_prefix,
        v:customer_city::VARCHAR as customer_city,
        v:customer_state::VARCHAR as customer_state,
        v:created_at::TIMESTAMP as created_at,
        v:updated_at::TIMESTAMP as updated_at,
        row_number() over (partition by v:customer_id::string
        order by v:updated_at::TIMESTAMP desc) as rn
    from {{ source('raw', 'customers') }}

    {% if is_incremental() %}
        where v:updated_at::TIMESTAMP > (select max(updated_at) from {{ this }})
    {% endif %}
)

select 
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    created_at,
    updated_at
from ranked
where rn = 1