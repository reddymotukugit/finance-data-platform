{{
    config(
        materialized = 'table',
        tags         = ['gold', 'dimensions']
    )
}}

/*
  Gold: dim_geography — Billing Geography Dimension
  Grain: one row per unique billing_country.
  Derived from customer billing addresses.
  Provides region, sub-region, and market classification
  for geographic revenue analysis.
*/

WITH countries AS (
    SELECT DISTINCT
        UPPER(TRIM(billing_country)) AS country_code
    FROM {{ ref('stg_customers_history') }}
    WHERE billing_country IS NOT NULL
      AND billing_country != ''
      AND is_current = TRUE
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['country_code']) }}        AS geography_key,
    country_code,

    -- Country full name
    CASE country_code
        WHEN 'US' THEN 'United States'
        WHEN 'GB' THEN 'United Kingdom'
        WHEN 'CA' THEN 'Canada'
        WHEN 'AU' THEN 'Australia'
        WHEN 'DE' THEN 'Germany'
        WHEN 'FR' THEN 'France'
        WHEN 'IN' THEN 'India'
        WHEN 'JP' THEN 'Japan'
        WHEN 'SG' THEN 'Singapore'
        WHEN 'NL' THEN 'Netherlands'
        WHEN 'SE' THEN 'Sweden'
        WHEN 'NO' THEN 'Norway'
        WHEN 'CH' THEN 'Switzerland'
        WHEN 'IE' THEN 'Ireland'
        WHEN 'NZ' THEN 'New Zealand'
        WHEN 'BR' THEN 'Brazil'
        WHEN 'MX' THEN 'Mexico'
        WHEN 'ES' THEN 'Spain'
        WHEN 'IT' THEN 'Italy'
        WHEN 'PL' THEN 'Poland'
        WHEN 'HK' THEN 'Hong Kong'
        WHEN 'IL' THEN 'Israel'
        WHEN 'ZA' THEN 'South Africa'
        WHEN 'AE' THEN 'United Arab Emirates'
        ELSE country_code || ' (Unknown)'
    END                                                             AS country_name,

    -- Geographic region
    CASE country_code
        WHEN 'US' THEN 'Americas'
        WHEN 'CA' THEN 'Americas'
        WHEN 'MX' THEN 'Americas'
        WHEN 'BR' THEN 'Americas'
        WHEN 'GB' THEN 'EMEA'
        WHEN 'DE' THEN 'EMEA'
        WHEN 'FR' THEN 'EMEA'
        WHEN 'NL' THEN 'EMEA'
        WHEN 'SE' THEN 'EMEA'
        WHEN 'NO' THEN 'EMEA'
        WHEN 'CH' THEN 'EMEA'
        WHEN 'IE' THEN 'EMEA'
        WHEN 'ES' THEN 'EMEA'
        WHEN 'IT' THEN 'EMEA'
        WHEN 'PL' THEN 'EMEA'
        WHEN 'IL' THEN 'EMEA'
        WHEN 'ZA' THEN 'EMEA'
        WHEN 'AE' THEN 'EMEA'
        WHEN 'AU' THEN 'APAC'
        WHEN 'NZ' THEN 'APAC'
        WHEN 'JP' THEN 'APAC'
        WHEN 'SG' THEN 'APAC'
        WHEN 'IN' THEN 'APAC'
        WHEN 'HK' THEN 'APAC'
        ELSE 'Other'
    END                                                             AS region,

    -- Sub-region
    CASE country_code
        WHEN 'US' THEN 'North America'
        WHEN 'CA' THEN 'North America'
        WHEN 'MX' THEN 'Latin America'
        WHEN 'BR' THEN 'Latin America'
        WHEN 'GB' THEN 'Northern Europe'
        WHEN 'IE' THEN 'Northern Europe'
        WHEN 'SE' THEN 'Northern Europe'
        WHEN 'NO' THEN 'Northern Europe'
        WHEN 'DE' THEN 'Western Europe'
        WHEN 'FR' THEN 'Western Europe'
        WHEN 'NL' THEN 'Western Europe'
        WHEN 'CH' THEN 'Western Europe'
        WHEN 'ES' THEN 'Southern Europe'
        WHEN 'IT' THEN 'Southern Europe'
        WHEN 'PL' THEN 'Eastern Europe'
        WHEN 'IL' THEN 'Middle East'
        WHEN 'AE' THEN 'Middle East'
        WHEN 'ZA' THEN 'Africa'
        WHEN 'AU' THEN 'Oceania'
        WHEN 'NZ' THEN 'Oceania'
        WHEN 'JP' THEN 'East Asia'
        WHEN 'HK' THEN 'East Asia'
        WHEN 'SG' THEN 'Southeast Asia'
        WHEN 'IN' THEN 'South Asia'
        ELSE 'Other'
    END                                                             AS sub_region,

    -- Market classification
    CASE country_code
        WHEN 'US' THEN 'Tier 1'
        WHEN 'GB' THEN 'Tier 1'
        WHEN 'CA' THEN 'Tier 1'
        WHEN 'AU' THEN 'Tier 1'
        WHEN 'DE' THEN 'Tier 1'
        WHEN 'FR' THEN 'Tier 1'
        WHEN 'JP' THEN 'Tier 1'
        WHEN 'IN' THEN 'Tier 2'
        WHEN 'SG' THEN 'Tier 2'
        WHEN 'NL' THEN 'Tier 2'
        WHEN 'SE' THEN 'Tier 2'
        WHEN 'CH' THEN 'Tier 2'
        WHEN 'IE' THEN 'Tier 2'
        WHEN 'BR' THEN 'Tier 2'
        ELSE 'Tier 3'
    END                                                             AS market_tier,

    CURRENT_TIMESTAMP()                                             AS dbt_updated_at

FROM countries
