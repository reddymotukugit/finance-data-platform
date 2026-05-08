{{
    config(
        unique_key = 'id',
        tags       = ['bronze', 'stripe', 'customers']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_stripe_customers') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
)

SELECT
    id,
    TO_TIMESTAMP(created)           AS created_at,
    DATE(TO_TIMESTAMP(created))     AS created_date,
    LOWER(COALESCE(currency, 'usd')) AS currency,
    description,
    LOWER(email)                    AS email,
    name,
    phone,
    UPPER(COALESCE(address_country, 'UNKNOWN')) AS billing_country,
    address_city,
    address_line1,
    address_postal_code,
    delinquent,
    balance,
    LOWER(COALESCE(tax_exempt, 'none')) AS tax_exempt,
    source_file,
    ingestion_run_id,
    load_mode,
    loaded_at
FROM source
