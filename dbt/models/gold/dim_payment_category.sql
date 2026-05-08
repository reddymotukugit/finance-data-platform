{{
    config(
        materialized = 'table',
        tags         = ['gold', 'dimensions']
    )
}}

/*
  Gold: dim_payment_category — Payment Event Type Reference Dimension
  Grain: one row per unique event_type + reporting_category combination.
  Provides human-readable labels, sign convention, and financial classification
  for every transaction type flowing through the ledger.
  Used by fct_transactions to enrich event type analysis.
*/

WITH event_types AS (
    SELECT DISTINCT
        event_type,
        reporting_category
    FROM {{ ref('stg_finance_ledger_events') }}
    WHERE event_type IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['event_type', 'reporting_category']) }}  AS payment_category_key,
    event_type,
    reporting_category,

    -- Human-readable display label
    CASE event_type
        WHEN 'charge'     THEN 'Payment Received'
        WHEN 'refund'     THEN 'Refund Issued'
        WHEN 'dispute'    THEN 'Dispute / Chargeback'
        WHEN 'payout'     THEN 'Bank Payout'
        WHEN 'transfer'   THEN 'Internal Transfer'
        WHEN 'adjustment' THEN 'Balance Adjustment'
        WHEN 'payment'    THEN 'Payment'
        ELSE INITCAP(REPLACE(event_type, '_', ' '))
    END                                                                            AS event_label,

    -- Financial classification
    CASE event_type
        WHEN 'charge'     THEN 'Revenue'
        WHEN 'refund'     THEN 'Contra-Revenue'
        WHEN 'dispute'    THEN 'Contra-Revenue'
        WHEN 'payout'     THEN 'Cash Movement'
        WHEN 'transfer'   THEN 'Cash Movement'
        WHEN 'adjustment' THEN 'Adjustment'
        ELSE 'Other'
    END                                                                            AS financial_category,

    -- Sign convention: positive = cash inflow, negative = cash outflow
    CASE event_type
        WHEN 'charge'     THEN 'Positive'
        WHEN 'refund'     THEN 'Negative'
        WHEN 'dispute'    THEN 'Negative'
        WHEN 'payout'     THEN 'Negative'
        WHEN 'transfer'   THEN 'Neutral'
        WHEN 'adjustment' THEN 'Negative'
        ELSE 'Neutral'
    END                                                                            AS sign_convention,

    -- Does this event affect revenue recognition?
    CASE event_type
        WHEN 'charge'     THEN TRUE
        WHEN 'refund'     THEN TRUE
        WHEN 'dispute'    THEN TRUE
        WHEN 'adjustment' THEN TRUE
        ELSE FALSE
    END                                                                            AS affects_revenue,

    -- Does this event affect cash balance?
    CASE event_type
        WHEN 'payout'     THEN TRUE
        WHEN 'transfer'   THEN TRUE
        ELSE FALSE
    END                                                                            AS affects_cash,

    CURRENT_TIMESTAMP()                                                            AS dbt_updated_at

FROM event_types
