{{
    config(
        materialized = 'table',
        tags         = ['gold', 'dimensions']
    )
}}

/*
  Gold: dim_plans — Subscription Plan Dimension
  Grain: one row per unique plan_id + price_id combination.
  Derived from Silver invoice line items — captures plan interval,
  pricing tier, and normalized monthly equivalent amount.
  Used by fct_mrr_movements to slice MRR by plan type.
*/

WITH plans AS (
    SELECT
        plan_id,
        price_id,
        plan_interval,
        plan_interval_count,
        currency,
        mrr_amount_usd,
        loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY plan_id, price_id
            ORDER BY loaded_at DESC
        ) AS rn
    FROM {{ ref('stg_invoice_line_items') }}
    WHERE plan_id IS NOT NULL
      AND is_recurring = TRUE
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['plan_id', 'price_id']) }}  AS plan_key,
    plan_id,
    price_id,
    plan_interval,
    plan_interval_count,
    UPPER(currency)                                                   AS currency,

    -- Human-readable billing cadence
    CASE
        WHEN plan_interval = 'month' AND plan_interval_count = 1  THEN 'Monthly'
        WHEN plan_interval = 'month' AND plan_interval_count = 3  THEN 'Quarterly'
        WHEN plan_interval = 'month' AND plan_interval_count = 6  THEN 'Semi-Annual'
        WHEN plan_interval = 'year'  AND plan_interval_count = 1  THEN 'Annual'
        WHEN plan_interval = 'week'                               THEN 'Weekly'
        WHEN plan_interval = 'day'                                THEN 'Daily'
        ELSE plan_interval || ' x' || plan_interval_count
    END                                                               AS billing_cadence,

    -- Pricing tier based on monthly equivalent MRR
    CASE
        WHEN mrr_amount_usd < 50    THEN 'Starter'
        WHEN mrr_amount_usd < 200   THEN 'Growth'
        WHEN mrr_amount_usd < 1000  THEN 'Professional'
        ELSE                             'Enterprise'
    END                                                               AS pricing_tier,

    -- Is this an annual plan? (useful for ARR vs MRR analysis)
    CASE WHEN plan_interval = 'year' THEN TRUE ELSE FALSE END         AS is_annual,

    ROUND(mrr_amount_usd, 2)                                          AS monthly_price_usd,
    ROUND(mrr_amount_usd * 12, 2)                                     AS annual_price_usd

FROM plans
WHERE rn = 1
