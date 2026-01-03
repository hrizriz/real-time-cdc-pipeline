{{ config(
    materialized='table'
) }}

select
    transaction_id,
    customer_id, -- Foreign Key ke dim_customer
    product_id,  -- Foreign Key ke dim_product
    store_id,    -- Foreign Key ke dim_store
    transaction_date,
    quantity,
    unit_price,
    total_amount,
    payment_method
from {{ ref('stg_sales') }}
