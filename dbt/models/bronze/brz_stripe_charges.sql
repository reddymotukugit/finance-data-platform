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
  Payment method details are extracted from raw_payload VARIANT (full JSON).
  Note: 'object' is a Snowflake reserved keyword and must be double-quoted.
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
    "object"                                        AS stripe_object_type,

    -- Amounts (cents — keep as-is in Bronze)
    amount,
    amount_captured,
    amount_refunded,

    -- Status flags
    LOWER(COALESCE(status, 'unknown'))              AS status,
    COALESCE(paid, FALSE)                           AS paid,
    COALESCE(captured, FALSE)                       AS captured,
    COALESCE(refunded, FALSE)                       AS refunded,
    COALESCE(disputed, FALSE)                       AS disputed,

    -- Currency
    LOWER(COALESCE(currency, 'usd'))               AS currency,

    -- Timestamps
    TO_TIMESTAMP(created)                           AS created_at,
    DATE(TO_TIMESTAMP(created))                     AS created_date,

    -- Relationships
    customer                                        AS customer_id,
    invoice                                         AS invoice_id,
    payment_intent                                  AS payment_intent_id,

    -- Keys sourced from raw_payload (not promoted to flat columns in RAW table)
    raw_payload:payment_method::STRING              AS payment_method_id,
    raw_payload:balance_transaction::STRING         AS balance_transaction_id,
    raw_payload:receipt_url::STRING                 AS receipt_url,
    raw_payload:statement_descriptor::STRING        AS statement_descriptor,

    -- Payment method details (flattened from raw_payload nested card object)
    LOWER(COALESCE(
        raw_payload:payment_method_details:type::STRING, 'unknown'
    ))                                              AS payment_method_type,
    LOWER(COALESCE(
        raw_payload:payment_method_details:card:brand::STRING, 'unknown'
    ))                                              AS card_brand,
    LOWER(COALESCE(
        raw_payload:payment_method_details:card:funding::STRING, 'unknown'
    ))                                              AS card_funding,
    UPPER(COALESCE(
        raw_payload:payment_method_details:card:country::STRING, 'UNKNOWN'
    ))                                              AS card_country,
    raw_payload:payment_method_details:card:last4::STRING
                                                    AS card_last4,
    raw_payload:payment_method_details:card:exp_month::NUMBER
                                                    AS card_exp_month,
    raw_payload:payment_method_details:card:exp_year::NUMBER
                                                    AS card_exp_year,

    -- Failure info
    failure_code,
    failure_message,

    -- Descriptive
    receipt_email,
    description,

    -- Ingestion metadata
    source_file,
    ingestion_run_id,
    load_mode,
    loaded_at

FROM source
