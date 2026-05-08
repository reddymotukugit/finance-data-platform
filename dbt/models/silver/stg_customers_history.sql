{{
    config(
        unique_key  = 'customer_key',
        tags        = ['silver', 'customers', 'scd2']
    )
}}

/*
  Silver: Customer History — SCD Type 2
  Tracks changes in billing_country and other key customer attributes over time.
  valid_from / valid_to pattern. is_current = TRUE for the latest record.
  Required for historical tax and revenue attribution accuracy.
*/

WITH customers AS (
    SELECT * FROM {{ ref('brz_stripe_customers') }}
),

-- For incremental runs, get existing records from this model
{% if is_incremental() %}
existing AS (
    SELECT * FROM {{ this }}
    WHERE is_current = TRUE
),
{% endif %}

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) AS rn
    FROM customers
),

latest AS (
    SELECT * FROM ranked WHERE rn = 1
),

scd2 AS (
    SELECT
        -- Surrogate key: customer_id + load timestamp hash
        {{ dbt_utils.generate_surrogate_key(['id', 'loaded_at']) }}  AS customer_key,
        id                                                            AS customer_id,
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
        loaded_at                                                     AS valid_from,
        LEAD(loaded_at) OVER (PARTITION BY id ORDER BY loaded_at)    AS valid_to_raw,
        ingestion_run_id,
        load_mode,
        loaded_at
    FROM customers
)

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
    COALESCE(valid_to_raw, '9999-12-31 00:00:00'::TIMESTAMP_NTZ)    AS valid_to,
    CASE WHEN valid_to_raw IS NULL THEN TRUE ELSE FALSE END          AS is_current,
    ingestion_run_id,
    load_mode,
    loaded_at
FROM scd2
