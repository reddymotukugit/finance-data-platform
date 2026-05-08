{{
    config(
        unique_key  = 'refund_id',
        cluster_by  = ['created_date'],
        tags        = ['gold', 'refunds']
    )
}}

/*
  Gold: fct_refunds — Issued Refunds
  Grain: one row per refund.
  Links to dim_customers (via the parent charge), dim_dates, dim_currencies.
  Cross-references fct_charges via charge_id.

  Key metrics:
    - signed_amount_usd            : always negative (contra-revenue)
    - reason                       : requested_by_customer | duplicate | fraudulent
    - is_successful                : FALSE for pending/failed refunds
    - days_since_charge            : latency from charge to refund (returns policy analysis)
*/

WITH refunds AS (
    SELECT * FROM {{ ref('stg_refunds') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

-- Pull customer linkage through the charge
charges AS (
    SELECT charge_id, customer_id
    FROM {{ ref('stg_charges') }}
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
),

charge_created AS (
    SELECT charge_id, created_date AS charge_created_date
    FROM {{ ref('fct_charges') }}
)

SELECT
    -- Natural key
    r.refund_id,

    -- Surrogate foreign keys
    customers.customer_key,
    dates.date_key                                          AS created_date_key,
    currencies.currency_key,

    -- Pass-through IDs
    r.charge_id,                                           -- join to fct_charges
    r.balance_transaction_id,                              -- join to fct_transactions
    charges.customer_id,

    -- Status & reason
    r.status,
    r.reason,
    r.is_successful,
    r.is_failed,

    -- Currency
    r.currency,
    r.usd_rate,

    -- Amounts (original currency)
    r.amount,
    r.signed_amount,

    -- Amounts (USD)
    r.amount_usd,
    r.signed_amount_usd,

    -- Latency: days from original charge to refund
    DATEDIFF('day', charge_created.charge_created_date, r.created_date) AS days_since_charge,

    -- Failure details
    r.failure_reason,

    -- Timestamps
    r.created_at,
    r.created_date,

    -- Descriptive
    r.description,
    r.receipt_number,

    -- Audit
    r.ingestion_run_id,
    r.loaded_at

FROM refunds r
LEFT JOIN charges       ON r.charge_id              = charges.charge_id
LEFT JOIN customers     ON charges.customer_id       = customers.customer_id
LEFT JOIN dates         ON r.created_date            = dates.date_day
LEFT JOIN currencies    ON UPPER(r.currency)         = currencies.currency_code
LEFT JOIN charge_created ON r.charge_id              = charge_created.charge_id

QUALIFY ROW_NUMBER() OVER (PARTITION BY r.refund_id ORDER BY r.loaded_at DESC) = 1
