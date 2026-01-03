{{ config(
    materialized='table'
) }}

select
    customer_id,
    customer_name,
    gender,
    city,
    signup_date
from {{ ref('stg_customers') }}
