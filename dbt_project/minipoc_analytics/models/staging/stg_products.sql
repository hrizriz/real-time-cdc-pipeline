with source as (
    select * from {{ source('retail_source', 'products_raw') }}
),

cleaned as (
    select
        product_id,
        product_name,
        category,
        price
    from source
)

select * from cleaned
