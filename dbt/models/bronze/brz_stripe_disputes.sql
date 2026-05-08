{{
    config(
        unique_key  = 'id',
        cluster_by  = ['created_date'],
        tags        = ['bronze', 'stripe', 'disputes']
    )
}}

/*
  Bronze: Stripe Disputes (Chargebacks)
  One row per dispute raised by a customer via their bank.
  Amounts kept in original cents. Evidence due dates converted from unix.
  Outcome (won/lost) is the most important downstream field for loss-rate reporting.
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_stripe_disputes') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
)

SELECT
    -- Natural key
    id,

    -- Amount (cents — keep as-is in Bronze)
    amount,

    -- Currency
    LOWER(COALESCE(currency, 'usd'))                AS currency,

    -- Status lifecycle: warning_needs_response → warning_under_review → warning_closed
    --                   needs_response → under_review → charge_refunded / won / lost
    LOWER(COALESCE(status, 'unknown'))              AS status,

    -- Reason (bank-supplied): fraudulent, duplicate, product_not_received, etc.
    LOWER(COALESCE(reason, 'unknown'))              AS reason,

    -- Outcome convenience flags
    CASE WHEN LOWER(status) = 'won'  THEN TRUE ELSE FALSE END  AS is_won,
    CASE WHEN LOWER(status) = 'lost' THEN TRUE ELSE FALSE END  AS is_lost,
    COALESCE(is_charge_refundable, FALSE)           AS is_charge_refundable,

    -- Timestamps
    TO_TIMESTAMP(created)                           AS created_at,
    DATE(TO_TIMESTAMP(created))                     AS created_date,
    TO_TIMESTAMP(evidence_details_due_by)           AS evidence_due_at,
    DATE(TO_TIMESTAMP(evidence_details_due_by))     AS evidence_due_date,

    -- Evidence submission flag
    COALESCE(evidence_details_has_evidence, FALSE)  AS has_evidence_submitted,

    -- Relationships
    charge                                          AS charge_id,
    payment_intent                                  AS payment_intent_id,
    balance_transaction                             AS balance_transaction_id,

    -- Ingestion metadata
    source_file,
    ingestion_run_id,
    load_mode,
    loaded_at

FROM source
