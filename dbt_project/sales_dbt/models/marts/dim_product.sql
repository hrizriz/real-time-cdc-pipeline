{{ config(
    materialized = 'table',
    post_hook = [
	"alter table {{ this }}
           drop constraint if exists pk_dim_product",
        "drop index if exists {{ this.schema }}.pk_dim_product",
        "alter table {{ this }}
           add constraint pk_dim_product
           primary key (product_id)"
    ]
) }}

select distinct
    product_id,
    product_name,
    initcap(category) as product_category,
    price             as product_base_price
from {{ source('stg_lyr', 'products_clean') }}
