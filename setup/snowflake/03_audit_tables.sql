-- ============================================================
-- FINANCE DATA PLATFORM — SNOWFLAKE SETUP
-- Script 03: AUDIT Schema Tables (Operational Metadata)
-- Run as ACCOUNTADMIN or FINANCE_TRANSFORMER
-- ============================================================

USE ROLE FINANCE_TRANSFORMER;
USE DATABASE FINANCE_PLATFORM_DEV;
USE SCHEMA AUDIT;
USE WAREHOUSE FINANCE_TRANSFORM_WH;

-- ────────────────────────────────────────
-- INGESTION CONTROL (Watermarks)
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS AUDIT.INGESTION_CONTROL (
    source_name                 STRING      NOT NULL,
    entity_name                 STRING      NOT NULL,
    load_strategy               STRING      NOT NULL,   -- timestamp_watermark | cursor | full_snapshot
    last_successful_watermark   TIMESTAMP_NTZ,
    last_successful_cursor      STRING,
    last_successful_run_id      STRING,
    last_run_status             STRING,
    updated_at                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_ingestion_control PRIMARY KEY (source_name, entity_name)
);

-- Seed initial watermark values (epoch start — pulls all history on first run)
INSERT INTO AUDIT.INGESTION_CONTROL (source_name, entity_name, load_strategy, last_successful_watermark)
SELECT * FROM VALUES
    ('stripe', 'balance_transactions', 'timestamp_watermark', '2020-01-01 00:00:00'),
    ('stripe', 'charges',              'timestamp_watermark', '2020-01-01 00:00:00'),
    ('stripe', 'refunds',              'timestamp_watermark', '2020-01-01 00:00:00'),
    ('stripe', 'disputes',             'timestamp_watermark', '2020-01-01 00:00:00'),
    ('stripe', 'payouts',              'timestamp_watermark', '2020-01-01 00:00:00'),
    ('stripe', 'customers',            'timestamp_watermark', '2020-01-01 00:00:00'),
    ('stripe', 'subscriptions',        'timestamp_watermark', '2020-01-01 00:00:00'),
    ('stripe', 'invoices',             'timestamp_watermark', '2020-01-01 00:00:00'),
    ('stripe', 'invoice_line_items',   'timestamp_watermark', '2020-01-01 00:00:00'),
    ('stripe', 'prices',               'full_snapshot',       '2020-01-01 00:00:00'),
    ('stripe', 'products',             'full_snapshot',       '2020-01-01 00:00:00'),
    ('fx',     'rates',                'full_snapshot',       '2020-01-01 00:00:00')
AS v(source_name, entity_name, load_strategy, last_successful_watermark);

-- ────────────────────────────────────────
-- PIPELINE RUNS (Operational Log)
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS AUDIT.PIPELINE_RUNS (
    run_id          STRING      NOT NULL,
    dag_id          STRING,
    task_id         STRING,
    status          STRING,         -- running | success | failed | skipped
    started_at      TIMESTAMP_NTZ,
    finished_at     TIMESTAMP_NTZ,
    duration_sec    NUMBER,
    row_count       NUMBER,
    error_message   STRING,
    extra_metadata  VARIANT,
    logged_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ────────────────────────────────────────
-- FAILED FILES (Dead Letter Queue)
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS AUDIT.FAILED_FILES (
    id              STRING DEFAULT UUID_STRING(),
    file_path       STRING      NOT NULL,
    source          STRING,
    entity          STRING,
    error_type      STRING,
    error_message   STRING,
    failed_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    resolved_at     TIMESTAMP_NTZ,
    resolved_by     STRING,
    run_id          STRING
);

-- ────────────────────────────────────────
-- RECONCILIATION RESULTS
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS AUDIT.RECONCILIATION_RESULTS (
    id              STRING DEFAULT UUID_STRING(),
    run_id          STRING      NOT NULL,
    check_name      STRING      NOT NULL,
    layer_from      STRING,
    layer_to        STRING,
    expected_value  NUMBER(18,2),
    actual_value    NUMBER(18,2),
    delta           NUMBER(18,2),
    tolerance_pct   NUMBER(5,2),
    status          STRING,         -- passed | failed | warning
    checked_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ────────────────────────────────────────
-- DBT TEST RESULTS
-- ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS AUDIT.DBT_TEST_RESULTS (
    run_id          STRING,
    test_name       STRING,
    model_name      STRING,
    column_name     STRING,
    status          STRING,         -- pass | fail | error
    failure_count   NUMBER,
    generated_at    TIMESTAMP_NTZ,
    logged_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

SELECT 'Script 03 complete — AUDIT tables and initial watermarks created.' AS STATUS;
