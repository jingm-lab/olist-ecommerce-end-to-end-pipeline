{{ config(materialized='table') }}

select DISTINCT
    v:geolocation_zip_code_prefix::VARCHAR as geolocation_zip_code_prefix,
    v:geolocation_lat::DOUBLE as geolocation_lat,
    v:geolocation_lng::DOUBLE as geolocation_lng,
    v:geolocation_city::VARCHAR as geolocation_city,
    v:geolocation_state::VARCHAR as geolocation_state,
    v:created_at::TIMESTAMP as created_at
from {{ source('raw', 'geolocations') }}