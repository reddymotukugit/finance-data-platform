{{
    config(
        materialized = 'table',
        tags         = ['gold', 'dimensions']
    )
}}

/*
  Gold: dim_customers — SCD Type 2 Customer Dimension
  Full history of billing country and customer attribute changes.
  Required for historical tax accuracy and revenue attribution.
*/

SELECT
    customer_key,
    customer_id,
    created_at,
    created_date,
    currency,
    email,
    name,
    phone,
    billing_country,
    address_city,
    address_postal_code,
    delinquent,
    balance,
    tax_exempt,
    valid_from,
    valid_to,
    is_current
FROM {{ ref('stg_customers_history') }}
