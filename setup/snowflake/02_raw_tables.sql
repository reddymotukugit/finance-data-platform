-- ============================================================
-- FINANCE DATA PLATFORM — SNOWFLAKE SETUP
-- Script 02: RAW Tables
-- Run as ACCOUNTADMIN or FINANCE_LOADER
-- ============================================================

USE ROLE FINANCE_TRANSFORMER;
USE DATABASE FINANCE_PLATFORM_DEV;
USE SCHEMA RAW;
USE WAREHOUSE FINANCE_INGEST_WH;

-- ────────────────────────────────────────
-- STRIPE: BALANCE TRANSACTIONS
-- The accounting ledger foundation
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS RAW.RAW_STRIPE_BALANCE_TRANSACTIONS (
    -- Business columns
    id                      STRING          NOT NULL,
    object                  STRING,
    amount                  NUMBER(18,0),           -- in cents
    available_on            NUMBER,                 -- unix timestamp
    created                 NUMBER,                 -- unix timestamp
    currency                STRING,
    description             STRING,
    exchange_rate           FLOAT,
    fee                     NUMBER(18,0),           -- in cents
    net                     NUMBER(18,0),           -- in cents
    reporting_category      STRING,
    source                  STRING,
    status                  STRING,
    type                    STRING,
    -- Ingestion metadata
    raw_payload             VARIANT,                -- full JSON for debugging
    source_file             STRING,
    ingestion_run_id        STRING,
    load_mode               STRING DEFAULT 'batch', -- batch | streaming
    loaded_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ────────────────────────────────────────
-- STRIPE: CHARGES
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS RAW.RAW_STRIPE_CHARGES (
    id                      STRING          NOT NULL,
    object                  STRING,
    amount                  NUMBER(18,0),
    amount_captured         NUMBER(18,0),
    amount_refunded         NUMBER(18,0),
    captured                BOOLEAN,
    created                 NUMBER,
    currency                STRING,
    customer                STRING,
    description             STRING,
    disputed                BOOLEAN,
    failure_code            STRING,
    failure_message         STRING,
    invoice                 STRING,
    paid                    BOOLEAN,
    payment_intent          STRING,
    receipt_email           STRING,
    refunded                BOOLEAN,
    status                  STRING,
    raw_payload             VARIANT,
    source_file             STRING,
    ingestion_run_id        STRING,
    load_mode               STRING DEFAULT 'batch',
    loaded_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ────────────────────────────────────────
-- STRIPE: REFUNDS
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS RAW.RAW_STRIPE_REFUNDS (
    id                      STRING          NOT NULL,
    object                  STRING,
    amount                  NUMBER(18,0),
    charge                  STRING,
    created                 NUMBER,
    currency                STRING,
    payment_intent          STRING,
    reason                  STRING,
    status                  STRING,
    raw_payload             VARIANT,
    source_file             STRING,
    ingestion_run_id        STRING,
    load_mode               STRING DEFAULT 'batch',
    loaded_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ────────────────────────────────────────
-- STRIPE: DISPUTES
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS RAW.RAW_STRIPE_DISPUTES (
    id                      STRING          NOT NULL,
    object                  STRING,
    amount                  NUMBER(18,0),
    charge                  STRING,
    created                 NUMBER,
    currency                STRING,
    evidence_due_by         NUMBER,
    is_charge_refundable    BOOLEAN,
    reason                  STRING,
    status                  STRING,
    raw_payload             VARIANT,
    source_file             STRING,
    ingestion_run_id        STRING,
    load_mode               STRING DEFAULT 'batch',
    loaded_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ────────────────────────────────────────
-- STRIPE: PAYOUTS
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS RAW.RAW_STRIPE_PAYOUTS (
    id                      STRING          NOT NULL,
    object                  STRING,
    amount                  NUMBER(18,0),
    arrival_date            NUMBER,
    created                 NUMBER,
    currency                STRING,
    description             STRING,
    failure_code            STRING,
    failure_message         STRING,
    method                  STRING,
    source_type             STRING,
    status                  STRING,
    type                    STRING,
    raw_payload             VARIANT,
    source_file             STRING,
    ingestion_run_id        STRING,
    load_mode               STRING DEFAULT 'batch',
    loaded_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ────────────────────────────────────────
-- STRIPE: CUSTOMERS
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS RAW.RAW_STRIPE_CUSTOMERS (
    id                      STRING          NOT NULL,
    object                  STRING,
    created                 NUMBER,
    currency                STRING,
    description             STRING,
    email                   STRING,
    name                    STRING,
    phone                   STRING,
    address_city            STRING,
    address_country         STRING,
    address_line1           STRING,
    address_postal_code     STRING,
    delinquent              BOOLEAN,
    balance                 NUMBER(18,0),
    tax_exempt              STRING,
    raw_payload             VARIANT,
    source_file             STRING,
    ingestion_run_id        STRING,
    load_mode               STRING DEFAULT 'batch',
    loaded_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ────────────────────────────────────────
-- STRIPE: SUBSCRIPTIONS
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS RAW.RAW_STRIPE_SUBSCRIPTIONS (
    id                      STRING          NOT NULL,
    object                  STRING,
    customer                STRING,
    created                 NUMBER,
    current_period_start    NUMBER,
    current_period_end      NUMBER,
    cancel_at               NUMBER,
    canceled_at             NUMBER,
    ended_at                NUMBER,
    start_date              NUMBER,
    status                  STRING,
    plan_id                 STRING,
    plan_interval           STRING,
    plan_interval_count     NUMBER,
    plan_amount             NUMBER(18,0),
    plan_currency           STRING,
    quantity                NUMBER,
    raw_payload             VARIANT,
    source_file             STRING,
    ingestion_run_id        STRING,
    load_mode               STRING DEFAULT 'batch',
    loaded_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ────────────────────────────────────────
-- STRIPE: INVOICES
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS RAW.RAW_STRIPE_INVOICES (
    id                      STRING          NOT NULL,
    object                  STRING,
    account_country         STRING,
    amount_due              NUMBER(18,0),
    amount_paid             NUMBER(18,0),
    amount_remaining        NUMBER(18,0),
    created                 NUMBER,
    currency                STRING,
    customer                STRING,
    customer_email          STRING,
    due_date                NUMBER,
    period_end              NUMBER,
    period_start            NUMBER,
    status                  STRING,
    subscription            STRING,
    subtotal                NUMBER(18,0),
    tax                     NUMBER(18,0),
    total                   NUMBER(18,0),
    raw_payload             VARIANT,
    source_file             STRING,
    ingestion_run_id        STRING,
    load_mode               STRING DEFAULT 'batch',
    loaded_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ────────────────────────────────────────
-- STRIPE: INVOICE LINE ITEMS
-- MRR source of truth
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS RAW.RAW_STRIPE_INVOICE_LINE_ITEMS (
    id                      STRING          NOT NULL,
    object                  STRING,
    invoice_id              STRING,
    amount                  NUMBER(18,0),
    currency                STRING,
    description             STRING,
    discountable            BOOLEAN,
    invoice_item            STRING,
    period_start            NUMBER,
    period_end              NUMBER,
    plan_id                 STRING,
    plan_interval           STRING,
    plan_interval_count     NUMBER,
    plan_amount             NUMBER(18,0),
    price_id                STRING,
    proration               BOOLEAN,
    quantity                NUMBER,
    subscription            STRING,
    subscription_item       STRING,
    type                    STRING,
    raw_payload             VARIANT,
    source_file             STRING,
    ingestion_run_id        STRING,
    load_mode               STRING DEFAULT 'batch',
    loaded_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ────────────────────────────────────────
-- STRIPE: PRICES
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS RAW.RAW_STRIPE_PRICES (
    id                      STRING          NOT NULL,
    object                  STRING,
    active                  BOOLEAN,
    billing_scheme          STRING,
    created                 NUMBER,
    currency                STRING,
    product                 STRING,
    recurring_interval      STRING,
    recurring_interval_count NUMBER,
    type                    STRING,
    unit_amount             NUMBER(18,0),
    raw_payload             VARIANT,
    source_file             STRING,
    ingestion_run_id        STRING,
    load_mode               STRING DEFAULT 'batch',
    loaded_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ────────────────────────────────────────
-- STRIPE: PRODUCTS
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS RAW.RAW_STRIPE_PRODUCTS (
    id                      STRING          NOT NULL,
    object                  STRING,
    active                  BOOLEAN,
    created                 NUMBER,
    description             STRING,
    name                    STRING,
    updated                 NUMBER,
    raw_payload             VARIANT,
    source_file             STRING,
    ingestion_run_id        STRING,
    load_mode               STRING DEFAULT 'batch',
    loaded_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ────────────────────────────────────────
-- FX RATES
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS RAW.RAW_FX_RATES (
    rate_date               DATE            NOT NULL,
    from_currency           STRING          NOT NULL,
    to_currency             STRING          NOT NULL DEFAULT 'USD',
    rate                    FLOAT           NOT NULL,
    source                  STRING,
    source_file             STRING,
    ingestion_run_id        STRING,
    load_mode               STRING DEFAULT 'batch',
    loaded_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

SELECT 'Script 02 complete — ' || COUNT(*) || ' RAW tables created.'
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'RAW' AND TABLE_CATALOG = 'FINANCE_PLATFORM_DEV';
