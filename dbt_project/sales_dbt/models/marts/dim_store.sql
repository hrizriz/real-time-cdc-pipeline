{{ config(
    materialized = 'table',
    post_hook = [
	"alter table {{ this }}
           drop constraint if exists pk_dim_store",
        "drop index if exists {{ this.schema }}.pk_dim_store",
        "alter table {{ this }}
           add constraint pk_dim_store
           primary key (store_id)"
    ]
) }}

select distinct
    store_id,
    store_name,
    city            as store_city,
    initcap(region) as store_region
from {{ source('stg_lyr', 'stores_clean') }}
