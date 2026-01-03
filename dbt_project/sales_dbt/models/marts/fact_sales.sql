{{ config(
    materialized = 'table',
    post_hook = [
        "alter table {{ this }}
           add constraint fk_fact_customer
           foreign key (customer_id)
           references dtmart_lyr.dim_customer(customer_id)",

        "alter table {{ this }}
           add constraint fk_fact_product
           foreign key (product_id)
           references dtmart_lyr.dim_product(product_id)",

        "alter table {{ this }}
           add constraint fk_fact_store
           foreign key (store_id)
           references dtmart_lyr.dim_store(store_id)"
    ]
) }}

select
    transaction_id,
    customer_id,
    product_id,
    store_id,
    transaction_date,
    quantity,
    unit_price,
    quantity * unit_price as sales_amount,
    payment_method
from {{ source('stg_lyr', 'sales_transaction_clean') }}
