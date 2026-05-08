{{
    config(
        unique_key  = 'line_item_id',
        cluster_by  = ['period_start_date'],
        tags        = ['silver', 'mrr', 'critical']
    )
}}

/*
  Silver: Invoice Line Items — MRR source of truth.
  Filters to recurring lines only, normalizes to monthly amounts,
  converts to USD. Used by fct_mrr_movements in Gold.
*/

WITH line_items AS (
    SELECT * FROM {{ ref('brz_stripe_invoice_line_items') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

fx AS (
    SELECT rate_date, from_currency, rate
    FROM {{ ref('brz_fx_rates') }}
    WHERE to_currency = 'USD'
),

enriched AS (
    SELECT
        li.id                                               AS line_item_id,
        li.invoice_id,
        li.subscription,
        li.subscription_item,
        li.plan_id,
        li.price_id,
        li.plan_interval,
        li.plan_interval_count,
        li.period_start_date,
        li.period_end_date,
        li.currency,
        li.proration,
        li.quantity,
        li.line_item_type,

        -- Is this a recurring line (not a proration or one-off)?
        CASE
            WHEN li.line_item_type = 'subscription'
             AND (li.proration = FALSE OR li.proration IS NULL)
            THEN TRUE ELSE FALSE
        END                                                 AS is_recurring,

        -- Raw amount in cents → dollars
        ROUND(li.amount / 100.0, 2)                         AS amount,

        -- Normalize to monthly equivalent
        CASE
            WHEN li.plan_interval = 'year'
            THEN ROUND((li.amount / 100.0) / 12.0, 2)
            WHEN li.plan_interval = 'week'
            THEN ROUND((li.amount / 100.0) * 4.333, 2)
            WHEN li.plan_interval = 'day'
            THEN ROUND((li.amount / 100.0) * 30.0, 2)
            ELSE ROUND(li.amount / 100.0, 2)              -- monthly default
        END                                                 AS mrr_amount,

        -- FX to USD
        COALESCE(fx.rate, 1.0)                              AS usd_rate,

        CASE
            WHEN li.plan_interval = 'year'
            THEN ROUND(((li.amount / 100.0) / 12.0) * COALESCE(fx.rate, 1.0), 2)
            WHEN li.plan_interval = 'week'
            THEN ROUND(((li.amount / 100.0) * 4.333) * COALESCE(fx.rate, 1.0), 2)
            ELSE ROUND((li.amount / 100.0) * COALESCE(fx.rate, 1.0), 2)
        END                                                 AS mrr_amount_usd,

        li.ingestion_run_id,
        li.load_mode,
        li.loaded_at

    FROM line_items li
    LEFT JOIN fx
        ON  UPPER(li.currency)   = fx.from_currency
        AND li.period_start_date = fx.rate_date
)

-- Deduplicate: RAW tables may have multiple loads of the same record
SELECT * FROM enriched
QUALIFY ROW_NUMBER() OVER (PARTITION BY line_item_id ORDER BY loaded_at DESC) = 1
