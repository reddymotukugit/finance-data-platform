{{
    config(
        unique_key  = 'dispute_id',
        cluster_by  = ['created_date'],
        tags        = ['gold', 'disputes']
    )
}}

/*
  Gold: fct_disputes — Chargebacks
  Grain: one row per dispute filed by a customer via their issuing bank.
  Links to dim_customers (via parent charge), dim_dates, dim_currencies.
  Cross-references fct_charges via charge_id.

  Key metrics:
    - outcome_category             : Won | Lost | In Progress | Warning | Charge Refunded
    - signed_amount_usd            : negative when lost (contra-revenue / write-off)
    - days_to_evidence_due         : SLA window for submitting evidence
    - is_overdue                   : missed evidence deadline without response
    - dispute_rate                 : use COUNT(dispute_id) / COUNT(charge_id) in BI
*/

WITH disputes AS (
    SELECT * FROM {{ ref('stg_disputes') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

charges AS (
    SELECT charge_id, customer_id, created_date AS charge_created_date
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
)

SELECT
    -- Natural key
    d.dispute_id,

    -- Surrogate foreign keys
    customers.customer_key,
    dates.date_key                                          AS created_date_key,
    currencies.currency_key,

    -- Pass-through IDs
    d.charge_id,                                            -- join to fct_charges
    d.balance_transaction_id,                               -- join to fct_transactions
    charges.customer_id,

    -- Status & reason
    d.status,
    d.reason,
    d.outcome_category,
    d.is_won,
    d.is_lost,
    d.is_charge_refundable,
    d.has_evidence_submitted,
    d.is_overdue,

    -- Currency
    d.currency,
    d.usd_rate,

    -- Amounts (original currency)
    d.amount,
    d.signed_amount,                                        -- negative = financial loss

    -- Amounts (USD)
    d.amount_usd,
    d.signed_amount_usd,

    -- Loss exposure: only materialises when lost
    CASE WHEN d.is_lost THEN d.amount_usd ELSE 0 END        AS loss_amount_usd,

    -- SLA tracking
    d.evidence_due_at,
    d.evidence_due_date,
    d.days_to_evidence_due,

    -- Latency: days from charge to dispute filing
    DATEDIFF('day', charges.charge_created_date, d.created_date) AS days_since_charge,

    -- Timestamps
    d.created_at,
    d.created_date,

    -- Audit
    d.ingestion_run_id,
    d.loaded_at

FROM disputes d
LEFT JOIN charges       ON d.charge_id          = charges.charge_id
LEFT JOIN customers     ON charges.customer_id   = customers.customer_id
LEFT JOIN dates         ON d.created_date        = dates.date_day
LEFT JOIN currencies    ON UPPER(d.currency)     = currencies.currency_code

QUALIFY ROW_NUMBER() OVER (PARTITION BY d.dispute_id ORDER BY d.loaded_at DESC) = 1
