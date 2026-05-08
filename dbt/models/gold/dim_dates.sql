{{
    config(
        materialized = 'table',
        tags         = ['gold', 'dimensions']
    )
}}

/*
  Gold: dim_dates — Fiscal Calendar Dimension
  Fiscal year: July–June (fiscal_year_start_month = 7)
  Covers 2018–2030 to handle historical Stripe data and future dates.
  Uses Snowflake GENERATOR instead of dbt_date.get_date_dimension to avoid
  recursive CTE limitations in Snowflake.
*/

WITH date_spine AS (
    -- Generate one row per day from 2018-01-01 to 2030-12-31 (4748 days)
    SELECT
        DATEADD(day, SEQ4(), '2018-01-01'::DATE) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 4748))
),

fiscal AS (
    SELECT
        date_day,
        {{ dbt_utils.generate_surrogate_key(['date_day']) }}    AS date_key,

        -- Calendar attributes
        YEAR(date_day)                                          AS calendar_year,
        MONTH(date_day)                                         AS calendar_month,
        DAY(date_day)                                           AS calendar_day,
        DAYOFWEEK(date_day)                                     AS day_of_week,
        DAYNAME(date_day)                                       AS day_name,
        WEEKOFYEAR(date_day)                                    AS week_of_year,
        QUARTER(date_day)                                       AS calendar_quarter,

        -- Fiscal year (July = month 1 of fiscal year)
        CASE
            WHEN MONTH(date_day) >= {{ var('fiscal_year_start_month') }}
            THEN YEAR(date_day)
            ELSE YEAR(date_day) - 1
        END                                                     AS fiscal_year,

        CASE
            WHEN MONTH(date_day) >= {{ var('fiscal_year_start_month') }}
            THEN MONTH(date_day) - {{ var('fiscal_year_start_month') }} + 1
            ELSE MONTH(date_day) + (12 - {{ var('fiscal_year_start_month') }} + 1)
        END                                                     AS fiscal_month,

        -- Month start/end flags
        DATE_TRUNC('month', date_day)                           AS month_start_date,
        LAST_DAY(date_day)                                      AS month_end_date,
        CASE WHEN date_day = DATE_TRUNC('month', date_day) THEN TRUE ELSE FALSE END AS is_month_start,
        CASE WHEN date_day = LAST_DAY(date_day) THEN TRUE ELSE FALSE END            AS is_month_end,

        -- Weekend flag
        CASE WHEN DAYOFWEEK(date_day) IN (0, 6) THEN TRUE ELSE FALSE END            AS is_weekend

    FROM date_spine
)

SELECT * FROM fiscal
