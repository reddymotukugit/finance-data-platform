{{
    config(
        unique_key  = 'refund_id',
        cluster_by  = ['created_date'],
        tags        = ['silver', 'refunds']
    )
}}

/*
  Silver: Refunds
  Grain: one row per refund issued against a charge.
  Amounts converted from cents to dollars and FX-normalised to USD.
  Links back to the originating charge_id and balance_transaction_id.
*/

WITH refunds AS (
    SELECT * FROM {{ ref('brz_stripe_refunds') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

fx AS (
    SELECT rate_date, from_currency, rate
    FROM {{ ref('brz_fx_rates') }}
    WHERE to_currency = 'USD'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY from_currency, rate_date
        ORDER BY loaded_at DESC
    ) = 1
),

enriched AS (
    SELECT
        -- Keys
        r.id                                                AS refund_id,
        r.charge_id,
        r.payment_intent_id,
        r.balance_transaction_id,

        -- Status & reason
        r.status,
        r.reason,
        CASE WHEN r.status = 'succeeded' THEN TRUE ELSE FALSE END AS is_successful,

        -- Currency
        r.currency,

        -- Amounts: cents → dollars (refunds are always negative in accounting)
        ROUND(r.amount / 100.0, 2)                          AS amount,
        ROUND(r.amount / 100.0, 2) * -1                     AS signed_amount,    -- contra-revenue

        -- FX to USD
        COALESCE(fx.rate, 1.0)                              AS usd_rate,
        ROUND((r.amount / 100.0) * COALESCE(fx.rate, 1.0), 2)      AS amount_usd,
        ROUND((r.amount / 100.0) * COALESCE(fx.rate, 1.0), 2) * -1 AS signed_amount_usd,

        -- Failure info
        r.failure_balance_transaction_id,
        r.failure_reason,
        CASE WHEN r.failure_reason IS NOT NULL THEN TRUE ELSE FALSE END AS is_failed,

        -- Timestamps
        r.created_at,
        r.created_date,

        -- Descriptive
        r.description,
        r.receipt_number,

        -- Audit
        r.ingestion_run_id,
        r.load_mode,
        r.loaded_at

    FROM refunds r
    LEFT JOIN fx
        ON  UPPER(r.currency) = fx.from_currency
        AND r.created_date    = fx.rate_date
)

SELECT * FROM enriched
QUALIFY ROW_NUMBER() OVER (PARTITION BY refund_id ORDER BY loaded_at DESC) = 1
