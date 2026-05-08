{{
    config(
        unique_key          = 'transaction_id',
        cluster_by          = ['created_date'],
        tags                = ['silver', 'ledger', 'critical']
    )
}}

/*
  Silver: Finance Ledger Events
  One row per atomic finance event (charge, refund, dispute, payout).
  FX normalized to USD. Amounts converted from cents to dollars.
  This is the canonical accounting ledger for the Gold fct_transactions model.
*/

WITH balance_txns AS (
    SELECT * FROM {{ ref('brz_stripe_balance_transactions') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

fx AS (
    -- Deduplicate: take the latest rate per currency/date in case of multiple loads
    SELECT
        rate_date,
        from_currency,
        rate
    FROM {{ ref('brz_fx_rates') }}
    WHERE to_currency = 'USD'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY from_currency, rate_date
        ORDER BY loaded_at DESC
    ) = 1
),

enriched AS (
    SELECT
        bt.id                                               AS transaction_id,
        bt.source_id,
        bt.event_type,
        bt.reporting_category,
        bt.status,
        bt.currency,
        bt.created_at,
        bt.created_date,
        bt.available_on_at,

        -- Convert cents to dollars
        ROUND(bt.amount   / 100.0, 2)                       AS amount,
        ROUND(bt.fee      / 100.0, 2)                       AS fee,
        ROUND(bt.net      / 100.0, 2)                       AS net_amount,

        -- FX normalization: join on transaction date and currency
        COALESCE(fx.rate, 1.0)                              AS usd_rate,
        ROUND((bt.amount / 100.0) * COALESCE(fx.rate, 1.0), 2)  AS amount_usd,
        ROUND((bt.fee    / 100.0) * COALESCE(fx.rate, 1.0), 2)  AS fee_usd,
        ROUND((bt.net    / 100.0) * COALESCE(fx.rate, 1.0), 2)  AS net_amount_usd,

        -- Standardize sign convention:
        -- charges/payouts = positive, refunds/disputes = negative
        CASE
            WHEN bt.event_type IN ('refund', 'dispute', 'adjustment')
            THEN ABS(ROUND(bt.net / 100.0, 2)) * -1
            ELSE ROUND(bt.net / 100.0, 2)
        END                                                 AS signed_net_amount,

        CASE
            WHEN bt.event_type IN ('refund', 'dispute', 'adjustment')
            THEN ABS(ROUND((bt.net / 100.0) * COALESCE(fx.rate, 1.0), 2)) * -1
            ELSE ROUND((bt.net / 100.0) * COALESCE(fx.rate, 1.0), 2)
        END                                                 AS signed_net_amount_usd,

        bt.description,
        bt.ingestion_run_id,
        bt.load_mode,
        bt.loaded_at

    FROM balance_txns bt
    LEFT JOIN fx
        ON  bt.currency  = fx.from_currency
        AND bt.created_date = fx.rate_date
)

-- Deduplicate: RAW tables may have multiple loads of the same record
SELECT * FROM enriched
QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY loaded_at DESC) = 1
