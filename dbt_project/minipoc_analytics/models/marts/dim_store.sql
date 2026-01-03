{{ config(
    materialized='table'
) }}

select
    store_id,
    store_name,
    city,
    region
from {{ ref('stg_stores') }}
