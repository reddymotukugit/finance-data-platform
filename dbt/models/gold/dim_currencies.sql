{{
    config(
        materialized = 'table',
        tags         = ['gold', 'dimensions']
    )
}}

/*
  Gold: dim_currencies — Currency Reference Dimension
  Grain: one row per currency code.
  Provides latest FX rate to USD, currency metadata, and region grouping.
  Used by fct_transactions for multi-currency revenue analysis.
*/

WITH fx AS (
    SELECT
        from_currency                                               AS currency_code,
        rate                                                        AS usd_rate,
        rate_date                                                   AS latest_rate_date,
        loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY from_currency
            ORDER BY rate_date DESC, loaded_at DESC
        )                                                           AS rn
    FROM {{ ref('brz_fx_rates') }}
    WHERE to_currency = 'USD'
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['currency_code']) }}       AS currency_key,
    currency_code,

    -- Currency full name
    CASE currency_code
        WHEN 'USD' THEN 'US Dollar'
        WHEN 'EUR' THEN 'Euro'
        WHEN 'GBP' THEN 'British Pound'
        WHEN 'CAD' THEN 'Canadian Dollar'
        WHEN 'AUD' THEN 'Australian Dollar'
        WHEN 'JPY' THEN 'Japanese Yen'
        WHEN 'INR' THEN 'Indian Rupee'
        WHEN 'SGD' THEN 'Singapore Dollar'
        WHEN 'CHF' THEN 'Swiss Franc'
        WHEN 'SEK' THEN 'Swedish Krona'
        WHEN 'NOK' THEN 'Norwegian Krone'
        WHEN 'DKK' THEN 'Danish Krone'
        WHEN 'NZD' THEN 'New Zealand Dollar'
        WHEN 'HKD' THEN 'Hong Kong Dollar'
        WHEN 'MXN' THEN 'Mexican Peso'
        WHEN 'BRL' THEN 'Brazilian Real'
        ELSE currency_code || ' (Unknown)'
    END                                                             AS currency_name,

    -- Currency symbol
    CASE currency_code
        WHEN 'USD' THEN '$'
        WHEN 'EUR' THEN '€'
        WHEN 'GBP' THEN '£'
        WHEN 'JPY' THEN '¥'
        WHEN 'INR' THEN '₹'
        ELSE currency_code
    END                                                             AS currency_symbol,

    -- Geographic region
    CASE currency_code
        WHEN 'USD' THEN 'Americas'
        WHEN 'CAD' THEN 'Americas'
        WHEN 'MXN' THEN 'Americas'
        WHEN 'BRL' THEN 'Americas'
        WHEN 'EUR' THEN 'EMEA'
        WHEN 'GBP' THEN 'EMEA'
        WHEN 'CHF' THEN 'EMEA'
        WHEN 'SEK' THEN 'EMEA'
        WHEN 'NOK' THEN 'EMEA'
        WHEN 'DKK' THEN 'EMEA'
        WHEN 'JPY' THEN 'APAC'
        WHEN 'AUD' THEN 'APAC'
        WHEN 'NZD' THEN 'APAC'
        WHEN 'SGD' THEN 'APAC'
        WHEN 'HKD' THEN 'APAC'
        WHEN 'INR' THEN 'APAC'
        ELSE 'Other'
    END                                                             AS region,

    -- Is this already USD? (no conversion needed)
    CASE WHEN currency_code = 'USD' THEN TRUE ELSE FALSE END        AS is_base_currency,

    COALESCE(usd_rate, 1.0)                                         AS latest_usd_rate,
    latest_rate_date

FROM fx
WHERE rn = 1

UNION ALL

-- Always include USD even if not in FX table
SELECT
    {{ dbt_utils.generate_surrogate_key(['\'USD\'']) }}             AS currency_key,
    'USD'                                                           AS currency_code,
    'US Dollar'                                                     AS currency_name,
    '$'                                                             AS currency_symbol,
    'Americas'                                                      AS region,
    TRUE                                                            AS is_base_currency,
    1.0                                                             AS latest_usd_rate,
    CURRENT_DATE()                                                  AS latest_rate_date
WHERE NOT EXISTS (SELECT 1 FROM fx WHERE currency_code = 'USD' AND rn = 1)
