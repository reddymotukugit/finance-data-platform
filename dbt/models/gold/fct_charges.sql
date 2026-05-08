{{
    config(
        unique_key  = 'charge_id',
        cluster_by  = ['created_date'],
        tags        = ['gold', 'charges']
    )
}}

/*
  Gold: fct_charges — Payment Attempts
  Grain: one row per Stripe charge.
  Links to dim_customers, dim_dates, dim_currencies, dim_payment_category.
  Also cross-references fct_transactions via balance_transaction_id for full
  accounting reconciliation (charge → balance_transaction → ledger event).

  Key metrics surfaced:
    - amount / amount_usd          : gross charge value
    - amount_captured_usd          : actually collected (post auth/capture gap)
    - amount_refunded_usd          : returned to customer
    - net_amount_usd               : amount_captured - amount_refunded
    - is_failed / failure_code     : payment failure analysis
    - payment_method_type          : card, bank_transfer, etc.
*/

WITH charges AS (
    SELECT * FROM {{ ref('stg_charges') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

customers AS (
    SELECT customer_key, customer_id
    FROM {{ ref('dim_customers') }}
    WHERE is_current = TRUE
),

dates AS (
    SELECT date_key, date_day
    FROM {{ ref('dim_dates') }}
),

currencies AS (
    SELECT currency_key, currency_code
    FROM {{ ref('dim_currencies') }}
)

SELECT
    -- Natural key
    c.charge_id,

    -- Surrogate foreign keys
    customers.customer_key,
    dates.date_key                                      AS created_date_key,
    currencies.currency_key,

    -- Pass-through IDs for cross-model joins
    c.customer_id,
    c.invoice_id,
    c.payment_intent_id,
    c.balance_transaction_id,               -- join to fct_transactions

    -- Status & outcome flags
    c.status,
    c.paid,
    c.captured,
    c.refunded,
    c.is_failed,

    -- Payment method details
    c.payment_method_type,
    c.card_brand,
    c.card_funding,
    c.card_country,
    c.card_last4,

    -- Currency
    c.currency,
    c.usd_rate,

    -- Amounts (original currency)
    c.amount,
    c.amount_captured,
    c.amount_refunded,
    c.net_amount,

    -- Amounts (USD)
    c.amount_usd,
    c.amount_captured_usd,
    c.amount_refunded_usd,
    c.net_amount_usd,

    -- Failure details
    c.failure_code,
    c.failure_message,

    -- Timestamps
    c.created_at,
    c.created_date,

    -- Descriptive
    c.description,
    c.receipt_url,

    -- Audit
    c.ingestion_run_id,
    c.loaded_at

FROM charges c
LEFT JOIN customers  ON c.customer_id           = customers.customer_id
LEFT JOIN dates      ON c.created_date          = dates.date_day
LEFT JOIN currencies ON UPPER(c.currency)       = currencies.currency_code

QUALIFY ROW_NUMBER() OVER (PARTITION BY c.charge_id ORDER BY c.loaded_at DESC) = 1
