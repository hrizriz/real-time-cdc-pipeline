with source as (
    select * from {{ source('retail_source', 'stores_raw') }}
),

cleaned as (
    select
        store_id,
        store_name,
        city,
        region
    from source
)

select * from cleaned
