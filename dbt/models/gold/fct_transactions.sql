{{
    config(
        unique_key  = 'transaction_id',
        cluster_by  = ['created_date'],
        tags        = ['gold', 'ledger', 'critical']
    )
}}

/*
  Gold: fct_transactions — The Accounting Ledger
  Grain: one row per atomic finance event.
  Events: charge, refund, dispute, payout.
  All amounts in USD. Links to all dimension tables.
*/

WITH ledger AS (
    SELECT * FROM {{ ref('stg_finance_ledger_events') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

customers AS (
    SELECT customer_key, customer_id, billing_country
    FROM {{ ref('dim_customers') }}
    WHERE is_current = TRUE
),

dates AS (
    SELECT date_day, date_key
    FROM {{ ref('dim_dates') }}
)

SELECT
    -- Keys
    ledger.transaction_id,
    ledger.source_id,
    customers.customer_key,
    dates.date_key,

    -- Event classification
    ledger.event_type,
    ledger.reporting_category,
    ledger.status,
    ledger.currency,

    -- Amounts in original currency
    ledger.amount,
    ledger.fee,
    ledger.net_amount,
    ledger.signed_net_amount,

    -- Amounts in USD
    ledger.usd_rate,
    ledger.amount_usd,
    ledger.fee_usd,
    ledger.net_amount_usd,
    ledger.signed_net_amount_usd,

    -- Dates
    ledger.created_at,
    ledger.created_date,
    ledger.available_on_at,

    -- Descriptive
    ledger.description,

    -- Audit
    ledger.ingestion_run_id,
    ledger.load_mode,
    ledger.loaded_at

FROM ledger
LEFT JOIN customers
    ON ledger.source_id = customers.customer_id   -- charges link to customer_id
LEFT JOIN dates
    ON ledger.created_date = dates.date_day
QUALIFY ROW_NUMBER() OVER (PARTITION BY ledger.transaction_id ORDER BY ledger.loaded_at DESC) = 1
