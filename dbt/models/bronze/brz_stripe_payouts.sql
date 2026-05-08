{{
    config(
        unique_key  = 'id',
        cluster_by  = ['created_date'],
        tags        = ['bronze', 'stripe', 'payouts']
    )
}}

/*
  Bronze: Stripe Payouts
  One row per payout sent from Stripe to the connected bank account.
  Amounts kept in original cents. arrival_date (unix) represents the bank settlement date.
  method: standard (2 business days) | instant (minutes, higher fee).
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_stripe_payouts') }}
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
    LOWER(COALESCE(currency, 'usd'))            AS currency,

    -- Status: paid | pending | in_transit | canceled | failed
    LOWER(COALESCE(status, 'unknown'))          AS status,

    -- Payout type: bank_account | card
    LOWER(COALESCE(type, 'bank_account'))       AS payout_type,

    -- Method: standard | instant
    LOWER(COALESCE(method, 'standard'))         AS method,

    -- Automatic vs manual payout
    COALESCE(automatic, TRUE)                   AS automatic,

    -- Timestamps
    TO_TIMESTAMP(created)                       AS created_at,
    DATE(TO_TIMESTAMP(created))                 AS created_date,
    TO_TIMESTAMP(arrival_date)                  AS arrival_at,
    DATE(TO_TIMESTAMP(arrival_date))            AS arrival_date_key,

    -- Relationships
    destination                                 AS destination_id,  -- bank account / card
    balance_transaction                         AS balance_transaction_id,

    -- Failure info
    failure_code,
    failure_message,
    failure_balance_transaction                 AS failure_balance_transaction_id,

    -- Descriptive
    description,
    statement_descriptor,

    -- Reconciliation
    source_type,                                -- card | bank_account | fpx

    -- Ingestion metadata
    source_file,
    ingestion_run_id,
    load_mode,
    loaded_at

FROM source
