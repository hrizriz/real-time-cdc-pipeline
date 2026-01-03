with source as (
    select * from {{ source('retail_source', 'sales_transaction_raw') }}
),

cleaned as (
    select
        transaction_id,
        customer_id,
        product_id,
        store_id,
        transaction_date,
        quantity,
        unit_price,
        payment_method,
        -- Kita buat kolom baru: Total Amount (Quantity * Harga Satuan)
        (quantity * unit_price) as total_amount
    from source
)

select * from cleaned
