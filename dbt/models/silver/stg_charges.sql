{{
    config(
        unique_key  = 'charge_id',
        cluster_by  = ['created_date'],
        tags        = ['silver', 'charges']
    )
}}

/*
  Silver: Charges
  Grain: one row per unique charge attempt.
  Amounts converted from cents to dollars and FX-normalised to USD.
  Deduplication via QUALIFY on loaded_at (latest load wins).
  Links to balance_transaction_id for reconciliation with fct_transactions.
*/

WITH charges AS (
    SELECT * FROM {{ ref('brz_stripe_charges') }}
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
        c.id                                                AS charge_id,
        c.customer_id,
        c.invoice_id,
        c.payment_intent_id,
        c.payment_method_id,
        c.balance_transaction_id,

        -- Status & flags
        c.status,
        c.paid,
        c.captured,
        c.refunded,

        -- Currency
        c.currency,

        -- Amounts: cents → dollars
        ROUND(c.amount          / 100.0, 2)                AS amount,
        ROUND(c.amount_captured / 100.0, 2)                AS amount_captured,
        ROUND(c.amount_refunded / 100.0, 2)                AS amount_refunded,
        ROUND((c.amount - c.amount_refunded) / 100.0, 2)   AS net_amount,

        -- FX to USD
        COALESCE(fx.rate, 1.0)                              AS usd_rate,
        ROUND((c.amount          / 100.0) * COALESCE(fx.rate, 1.0), 2) AS amount_usd,
        ROUND((c.amount_captured / 100.0) * COALESCE(fx.rate, 1.0), 2) AS amount_captured_usd,
        ROUND((c.amount_refunded / 100.0) * COALESCE(fx.rate, 1.0), 2) AS amount_refunded_usd,
        ROUND(((c.amount - c.amount_refunded) / 100.0) * COALESCE(fx.rate, 1.0), 2) AS net_amount_usd,

        -- Payment method details
        c.payment_method_type,
        c.card_brand,
        c.card_funding,
        c.card_country,
        c.card_last4,
        c.card_exp_month,
        c.card_exp_year,

        -- Failure info
        c.failure_code,
        c.failure_message,
        CASE WHEN c.failure_code IS NOT NULL THEN TRUE ELSE FALSE END AS is_failed,

        -- Timestamps
        c.created_at,
        c.created_date,

        -- Descriptive
        c.description,
        c.receipt_url,
        c.statement_descriptor,

        -- Audit
        c.ingestion_run_id,
        c.load_mode,
        c.loaded_at

    FROM charges c
    LEFT JOIN fx
        ON  UPPER(c.currency) = fx.from_currency
        AND c.created_date    = fx.rate_date
)

SELECT * FROM enriched
QUALIFY ROW_NUMBER() OVER (PARTITION BY charge_id ORDER BY loaded_at DESC) = 1
