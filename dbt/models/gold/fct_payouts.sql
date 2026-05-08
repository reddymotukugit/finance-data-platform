{{
    config(
        unique_key  = 'payout_id',
        cluster_by  = ['created_date'],
        tags        = ['gold', 'payouts']
    )
}}

/*
  Gold: fct_payouts — Bank Disbursements
  Grain: one row per payout sent from Stripe to the connected bank account.
  Links to dim_dates (created and arrival), dim_currencies.

  Key metrics:
    - amount_usd                   : gross cash sent to bank
    - transit_days                 : created → arrival lag (cash-flow forecasting)
    - is_instant                   : TRUE for instant payouts (higher Stripe fee)
    - is_failed / failure_code     : payout failure monitoring
    - automatic                    : Stripe-scheduled vs manually triggered
*/

WITH payouts AS (
    SELECT * FROM {{ ref('stg_payouts') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

created_dates AS (
    SELECT date_key, date_day
    FROM {{ ref('dim_dates') }}
),

arrival_dates AS (
    SELECT date_key, date_day
    FROM {{ ref('dim_dates') }}
),

currencies AS (
    SELECT currency_key, currency_code
    FROM {{ ref('dim_currencies') }}
)

SELECT
    -- Natural key
    p.payout_id,

    -- Surrogate foreign keys
    created_dates.date_key                                  AS created_date_key,
    arrival_dates.date_key                                  AS arrival_date_key,
    currencies.currency_key,

    -- Pass-through IDs
    p.balance_transaction_id,                               -- join to fct_transactions
    p.destination_id,

    -- Status & method
    p.status,
    p.payout_type,
    p.method,
    p.automatic,
    p.source_type,

    -- Outcome flags
    p.is_paid,
    p.is_failed,
    p.is_canceled,
    p.is_instant,

    -- Currency
    p.currency,
    p.usd_rate,

    -- Amounts
    p.amount,
    p.amount_usd,

    -- Cash-flow timing
    p.created_at,
    p.created_date,
    p.arrival_at,
    p.arrival_date,
    p.transit_days,

    -- Failure details
    p.failure_code,
    p.failure_message,

    -- Descriptive
    p.description,
    p.statement_descriptor,

    -- Audit
    p.ingestion_run_id,
    p.loaded_at

FROM payouts p
LEFT JOIN created_dates ON p.created_date       = created_dates.date_day
LEFT JOIN arrival_dates ON p.arrival_date        = arrival_dates.date_day
LEFT JOIN currencies    ON UPPER(p.currency)     = currencies.currency_code

QUALIFY ROW_NUMBER() OVER (PARTITION BY p.payout_id ORDER BY p.loaded_at DESC) = 1
