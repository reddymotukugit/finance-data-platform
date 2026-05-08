{{
    config(
        unique_key  = 'id',
        cluster_by  = ['created_date'],
        tags        = ['bronze', 'stripe', 'charges']
    )
}}

/*
  Bronze: Stripe Charges
  One row per charge attempt. Includes succeeded, failed, and pending charges.
  Amounts kept in original cents — conversion happens in Silver.
  Payment method details flattened from the nested card object.
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_stripe_charges') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
)

SELECT
    -- Natural key
    id,
    object,

    -- Amounts (cents — keep as-is in Bronze)
    amount,
    amount_captured,
    amount_refunded,

    -- Status flags
    LOWER(COALESCE(status, 'unknown'))          AS status,
    COALESCE(paid, FALSE)                        AS paid,
    COALESCE(captured, FALSE)                    AS captured,
    COALESCE(refunded, FALSE)                    AS refunded,

    -- Currency
    LOWER(COALESCE(currency, 'usd'))             AS currency,

    -- Timestamps
    TO_TIMESTAMP(created)                        AS created_at,
    DATE(TO_TIMESTAMP(created))                  AS created_date,

    -- Relationships
    customer                                     AS customer_id,
    invoice                                      AS invoice_id,
    payment_intent                               AS payment_intent_id,
    payment_method                               AS payment_method_id,
    balance_transaction                          AS balance_transaction_id,

    -- Payment method details (flattened from card object)
    LOWER(COALESCE(payment_method_details_type, 'unknown'))         AS payment_method_type,
    LOWER(COALESCE(payment_method_details_card_brand, 'unknown'))   AS card_brand,
    LOWER(COALESCE(payment_method_details_card_funding, 'unknown')) AS card_funding,
    UPPER(COALESCE(payment_method_details_card_country, 'UNKNOWN')) AS card_country,
    payment_method_details_card_last4                               AS card_last4,
    payment_method_details_card_exp_month                           AS card_exp_month,
    payment_method_details_card_exp_year                            AS card_exp_year,

    -- Failure info
    failure_code,
    failure_message,

    -- Descriptive
    description,
    receipt_url,
    statement_descriptor,

    -- Ingestion metadata
    source_file,
    ingestion_run_id,
    load_mode,
    loaded_at

FROM source
