{{
    config(
        unique_key = ['rate_date', 'from_currency', 'to_currency'],
        tags       = ['bronze', 'fx']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_fx_rates') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
)

SELECT
    rate_date,
    UPPER(from_currency)    AS from_currency,
    UPPER(to_currency)      AS to_currency,
    rate,
    source,
    source_file,
    ingestion_run_id,
    load_mode,
    loaded_at
FROM source
WHERE rate IS NOT NULL AND rate > 0
