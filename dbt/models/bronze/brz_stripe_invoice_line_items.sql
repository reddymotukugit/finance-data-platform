{{
    config(
        unique_key = 'id',
        tags       = ['bronze', 'stripe', 'mrr']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_stripe_invoice_line_items') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
)

SELECT
    id,
    invoice_id,
    amount,
    LOWER(currency)                             AS currency,
    description,
    TO_TIMESTAMP(period_start)                  AS period_start_at,
    TO_TIMESTAMP(period_end)                    AS period_end_at,
    DATE(TO_TIMESTAMP(period_start))            AS period_start_date,
    DATE(TO_TIMESTAMP(period_end))              AS period_end_date,
    plan_id,
    LOWER(COALESCE(plan_interval, 'unknown'))   AS plan_interval,
    plan_interval_count,
    plan_amount,
    price_id,
    proration,
    quantity,
    subscription,
    subscription_item,
    LOWER(COALESCE(type, 'unknown'))            AS line_item_type,
    source_file,
    ingestion_run_id,
    load_mode,
    loaded_at
FROM source
