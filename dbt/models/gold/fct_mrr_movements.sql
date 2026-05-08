{{
    config(
        unique_key  = ['customer_id', 'plan_id', 'billing_month'],
        tags        = ['gold', 'mrr', 'critical']
    )
}}

/*
  Gold: fct_mrr_movements
  Grain: one row per customer + plan + billing_month.
  Movement types: New, Expansion, Contraction, Churn, No Change.
  Source: stg_invoice_line_items (recurring lines only).
*/

WITH recurring AS (
    SELECT
        subscription,
        plan_id,
        DATE_TRUNC('month', period_start_date)  AS billing_month,
        SUM(mrr_amount_usd)                     AS mrr_usd
    FROM {{ ref('stg_invoice_line_items') }}
    WHERE is_recurring = TRUE
      AND mrr_amount_usd IS NOT NULL
    GROUP BY 1, 2, 3
),

-- Join customer to get customer_id from subscription
-- Deduplicate: raw table may have multiple loads of the same subscription record
subscriptions AS (
    SELECT
        id AS subscription_id,
        customer AS customer_id,
        plan_id
    FROM {{ source('raw', 'raw_stripe_subscriptions') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) = 1
),

with_customer AS (
    SELECT
        COALESCE(s.customer_id, r.subscription)     AS customer_id,
        r.plan_id,
        r.billing_month,
        r.mrr_usd
    FROM recurring r
    LEFT JOIN subscriptions s ON r.subscription = s.subscription_id
),

-- Calculate prior month MRR per customer + plan
lagged AS (
    SELECT
        customer_id,
        plan_id,
        billing_month,
        mrr_usd,
        LAG(mrr_usd) OVER (
            PARTITION BY customer_id, plan_id
            ORDER BY billing_month
        )                                           AS prior_mrr_usd,
        LAG(billing_month) OVER (
            PARTITION BY customer_id, plan_id
            ORDER BY billing_month
        )                                           AS prior_billing_month
    FROM with_customer
),

classified AS (
    SELECT
        customer_id,
        plan_id,
        billing_month,
        mrr_usd,
        prior_mrr_usd,

        -- MRR movement delta
        ROUND(mrr_usd - COALESCE(prior_mrr_usd, 0), 2)     AS mrr_change_usd,

        -- Movement classification
        CASE
            WHEN prior_mrr_usd IS NULL                          THEN 'New'
            WHEN mrr_usd = 0 AND prior_mrr_usd > 0             THEN 'Churn'
            WHEN mrr_usd > prior_mrr_usd                       THEN 'Expansion'
            WHEN mrr_usd < prior_mrr_usd AND mrr_usd > 0       THEN 'Contraction'
            ELSE                                                     'No Change'
        END                                                         AS movement_type

    FROM lagged
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'plan_id', 'billing_month']) }}  AS mrr_key,
    customer_id,
    plan_id,
    billing_month,
    mrr_usd,
    prior_mrr_usd,
    mrr_change_usd,
    movement_type,
    CURRENT_TIMESTAMP()                                             AS dbt_updated_at
FROM classified
-- Deduplicate: guard against any remaining fan-out on the surrogate key grain
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, plan_id, billing_month ORDER BY billing_month) = 1
