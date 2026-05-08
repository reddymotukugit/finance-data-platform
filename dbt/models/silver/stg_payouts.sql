{{
    config(
        unique_key  = 'payout_id',
        cluster_by  = ['created_date'],
        tags        = ['silver', 'payouts']
    )
}}

/*
  Silver: Payouts
  Grain: one row per payout sent from Stripe to the connected bank account.
  Amounts converted from cents to dollars (payouts are always USD for most accounts).
  Adds transit_days (created → arrival) for cash-flow lag analysis.
  method: standard (~2 business days) | instant (minutes, higher cost).
*/

WITH payouts AS (
    SELECT * FROM {{ ref('brz_stripe_payouts') }}
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
        p.id                                                AS payout_id,
        p.balance_transaction_id,
        p.destination_id,

        -- Status & method
        p.status,
        p.payout_type,
        p.method,
        p.automatic,
        p.source_type,

        -- Outcome flags
        CASE WHEN p.status = 'paid'        THEN TRUE ELSE FALSE END AS is_paid,
        CASE WHEN p.status = 'failed'      THEN TRUE ELSE FALSE END AS is_failed,
        CASE WHEN p.status = 'canceled'    THEN TRUE ELSE FALSE END AS is_canceled,
        CASE WHEN p.method = 'instant'     THEN TRUE ELSE FALSE END AS is_instant,

        -- Currency
        p.currency,

        -- Amounts: cents → dollars
        ROUND(p.amount / 100.0, 2)                          AS amount,

        -- FX to USD (payouts are typically USD; rate=1 if already USD)
        COALESCE(fx.rate, 1.0)                              AS usd_rate,
        ROUND((p.amount / 100.0) * COALESCE(fx.rate, 1.0), 2) AS amount_usd,

        -- Cash-flow lag
        p.created_at,
        p.created_date,
        p.arrival_at,
        p.arrival_date,
        DATEDIFF('day', p.created_date, p.arrival_date)     AS transit_days,

        -- Failure info
        p.failure_code,
        p.failure_message,
        p.failure_balance_transaction_id,

        -- Descriptive
        p.description,
        p.statement_descriptor,

        -- Audit
        p.ingestion_run_id,
        p.load_mode,
        p.loaded_at

    FROM payouts p
    LEFT JOIN fx
        ON  UPPER(p.currency) = fx.from_currency
        AND p.created_date    = fx.rate_date
)

SELECT * FROM enriched
QUALIFY ROW_NUMBER() OVER (PARTITION BY payout_id ORDER BY loaded_at DESC) = 1
