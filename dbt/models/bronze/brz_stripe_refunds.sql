{{
    config(
        unique_key  = 'id',
        cluster_by  = ['created_date'],
        tags        = ['bronze', 'stripe', 'refunds']
    )
}}

/*
  Bronze: Stripe Refunds
  One row per refund issued against a charge.
  Amounts kept in original cents — conversion happens in Silver.
  Refunds can be partial or full; amount_refunded on the charge tracks the total refunded.
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_stripe_refunds') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
)

SELECT
    -- Natural key
    id,
    object,

    -- Amount (cents — keep as-is in Bronze)
    amount,

    -- Currency
    LOWER(COALESCE(currency, 'usd'))            AS currency,

    -- Status
    LOWER(COALESCE(status, 'unknown'))          AS status,

    -- Timestamps
    TO_TIMESTAMP(created)                       AS created_at,
    DATE(TO_TIMESTAMP(created))                 AS created_date,

    -- Relationships
    charge                                      AS charge_id,
    payment_intent                              AS payment_intent_id,
    balance_transaction                         AS balance_transaction_id,

    -- Refund reason (requested_by_customer | duplicate | fraudulent | expired_uncaptured_charge)
    LOWER(COALESCE(reason, 'requested_by_customer')) AS reason,

    -- Descriptive
    description,
    receipt_number,

    -- Failure info
    failure_balance_transaction                 AS failure_balance_transaction_id,
    LOWER(failure_reason)                       AS failure_reason,

    -- Ingestion metadata
    source_file,
    ingestion_run_id,
    load_mode,
    loaded_at

FROM source
