{{
    config(
        unique_key       = 'id',
        cluster_by       = ['created_date'],
        tags             = ['bronze', 'stripe', 'ledger']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_stripe_balance_transactions') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
)

SELECT
    -- Natural key
    id,
    object,

    -- Amounts (stored as cents in Stripe — keep as-is in Bronze)
    amount,
    fee,
    net,
    exchange_rate,

    -- Timestamps (convert unix → timestamp)
    TO_TIMESTAMP(created)                               AS created_at,
    TO_TIMESTAMP(available_on)                          AS available_on_at,
    DATE(TO_TIMESTAMP(created))                         AS created_date,

    -- Categorical (standardize casing)
    LOWER(currency)                                     AS currency,
    LOWER(COALESCE(status, 'unknown'))                  AS status,
    LOWER(COALESCE(type, 'unknown'))                    AS event_type,
    LOWER(COALESCE(reporting_category, 'unknown'))      AS reporting_category,

    -- Identifiers
    source                                              AS source_id,
    description,

    -- Ingestion metadata
    source_file,
    ingestion_run_id,
    load_mode,
    loaded_at

FROM source
