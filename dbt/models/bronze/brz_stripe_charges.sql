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

  Design note: All fields beyond the guaranteed core schema are extracted from
  raw_payload VARIANT. This makes the model resilient to variations in the flat
  column set across different table creation scripts.

  Guaranteed flat columns (always present): id, amount, amount_captured,
  amount_refunded, status, paid, captured, refunded, disputed, currency,
  created, raw_payload, source_file, ingestion_run_id, load_mode, loaded_at.
  Everything else comes from raw_payload.
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

    -- Amounts (cents — keep as-is in Bronze)
    amount,
    amount_captured,
    amount_refunded,

    -- Status flags (flat columns — guaranteed present)
    LOWER(COALESCE(status, 'unknown'))              AS status,
    COALESCE(paid, FALSE)                           AS paid,
    COALESCE(captured, FALSE)                       AS captured,
    COALESCE(refunded, FALSE)                       AS refunded,
    COALESCE(disputed, FALSE)                       AS disputed,

    -- Currency & timestamp (flat columns — guaranteed present)
    LOWER(COALESCE(currency, 'usd'))                AS currency,
    TO_TIMESTAMP(created)                           AS created_at,
    DATE(TO_TIMESTAMP(created))                     AS created_date,

    -- All relationship keys from raw_payload (avoids dependency on optional flat cols)
    raw_payload:customer::STRING                    AS customer_id,
    raw_payload:invoice::STRING                     AS invoice_id,
    raw_payload:payment_intent::STRING              AS payment_intent_id,
    raw_payload:payment_method::STRING              AS payment_method_id,
    raw_payload:balance_transaction::STRING         AS balance_transaction_id,

    -- Failure info from raw_payload
    raw_payload:failure_code::STRING                AS failure_code,
    raw_payload:failure_message::STRING             AS failure_message,

    -- Descriptive from raw_payload
    raw_payload:description::STRING                 AS description,
    raw_payload:receipt_email::STRING               AS receipt_email,
    raw_payload:receipt_url::STRING                 AS receipt_url,
    raw_payload:statement_descriptor::STRING        AS statement_descriptor,

    -- Payment method details (nested card object in raw_payload)
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

    -- Ingestion metadata
    source_file,
    ingestion_run_id,
    load_mode,
    loaded_at

FROM source
