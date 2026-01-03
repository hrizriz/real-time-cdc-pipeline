with source as (
    select * from {{ source('retail_source', 'customers_raw') }}
),

cleaned as (
    select
        customer_id,
        customer_name,
        gender,
        city,
        signup_date
    from source
)

select * from cleaned
