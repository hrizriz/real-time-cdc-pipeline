{{ config(
    materialized = 'table',
    post_hook = [
	"alter table {{ this }}
	   drop constraint if exists pk_dim_customer",
	"drop index if exists {{ this.schema }}.pk_dim_customer",
	"alter table {{ this }}
           add constraint pk_dim_customer
           primary key (customer_id)"
    ]
) }}

SELECT DISTINCT
    customer_id,
    customer_name,
    gender         AS customer_gender,
    city           AS customer_city,
    signup_date    AS customer_signup_date
FROM {{ source('stg_lyr', 'customers_clean') }}
