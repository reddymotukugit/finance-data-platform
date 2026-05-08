{{
    config(
        unique_key  = 'dispute_id',
        cluster_by  = ['created_date'],
        tags        = ['silver', 'disputes']
    )
}}

/*
  Silver: Disputes (Chargebacks)
  Grain: one row per dispute filed by a customer via their bank.
  Amounts converted from cents to dollars and FX-normalised to USD.
  Adds days_to_evidence_due and is_overdue for SLA tracking.
  is_lost = the most important flag for finance reporting (contra-revenue).
*/

WITH disputes AS (
    SELECT * FROM {{ ref('brz_stripe_disputes') }}
    {% if is_incremental() %}
    WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

fx AS (
    SELECT rate_date, from_currency, rate
    FROM {{ ref('brz_fx_rates') }}
    WHERE to_currency = 'USD'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY from_currency, rate_date
        ORDER BY loaded_at DESC
    ) = 1
),

enriched AS (
    SELECT
        -- Keys
        d.id                                                AS dispute_id,
        d.charge_id,
        d.payment_intent_id,
        d.balance_transaction_id,

        -- Status & reason
        d.status,
        d.reason,
        d.is_won,
        d.is_lost,
        d.is_charge_refundable,
        d.has_evidence_submitted,

        -- Derived outcome for reporting
        CASE
            WHEN d.status IN ('won')                         THEN 'Won'
            WHEN d.status IN ('lost')                        THEN 'Lost'
            WHEN d.status IN ('charge_refunded')             THEN 'Charge Refunded'
            WHEN d.status LIKE 'warning%'                    THEN 'Warning'
            WHEN d.status IN ('needs_response', 'under_review') THEN 'In Progress'
            ELSE 'Unknown'
        END                                                 AS outcome_category,

        -- Currency
        d.currency,

        -- Amounts: cents → dollars (disputes carry a negative financial impact)
        ROUND(d.amount / 100.0, 2)                          AS amount,
        ROUND(d.amount / 100.0, 2) * -1                     AS signed_amount,  -- contra-revenue when lost

        -- FX to USD
        COALESCE(fx.rate, 1.0)                              AS usd_rate,
        ROUND((d.amount / 100.0) * COALESCE(fx.rate, 1.0), 2)      AS amount_usd,
        ROUND((d.amount / 100.0) * COALESCE(fx.rate, 1.0), 2) * -1 AS signed_amount_usd,

        -- SLA tracking
        d.evidence_due_at,
        d.evidence_due_date,
        DATEDIFF('day', d.created_date, d.evidence_due_date)        AS days_to_evidence_due,
        CASE
            WHEN d.evidence_due_date < CURRENT_DATE()
             AND d.status IN ('needs_response', 'warning_needs_response')
            THEN TRUE ELSE FALSE
        END                                                 AS is_overdue,

        -- Timestamps
        d.created_at,
        d.created_date,

        -- Audit
        d.ingestion_run_id,
        d.load_mode,
        d.loaded_at

    FROM disputes d
    LEFT JOIN fx
        ON  UPPER(d.currency) = fx.from_currency
        AND d.created_date    = fx.rate_date
)

SELECT * FROM enriched
QUALIFY ROW_NUMBER() OVER (PARTITION BY dispute_id ORDER BY loaded_at DESC) = 1
