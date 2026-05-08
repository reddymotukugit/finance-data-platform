-- ============================================================
-- FINANCE DATA PLATFORM — SNOWFLAKE SETUP
-- Script 04: Azure Storage Integration + External Stages
-- Run as ACCOUNTADMIN
-- IMPORTANT: Replace placeholders before running
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE FINANCE_PLATFORM_DEV;
USE SCHEMA RAW;

-- ────────────────────────────────────────
-- STEP 1: Create Storage Integration
-- This allows Snowflake to trust your ADLS Gen2 account
-- ────────────────────────────────────────
CREATE STORAGE INTEGRATION IF NOT EXISTS AZURE_FINANCE_STORAGE_INT
    TYPE                        = EXTERNAL_STAGE
    STORAGE_PROVIDER            = 'AZURE'
    ENABLED                     = TRUE
    AZURE_TENANT_ID             = '<YOUR_AZURE_TENANT_ID>'          -- replace
    STORAGE_ALLOWED_LOCATIONS   = ('azure://<YOUR_STORAGE_ACCOUNT>.blob.core.windows.net/finance/');
    -- Example: 'azure://stfindatalakedev001.blob.core.windows.net/finance/'

-- ────────────────────────────────────────
-- STEP 2: Get the Snowflake App registration details
-- Run this and then grant access in Azure IAM
-- ────────────────────────────────────────
DESC INTEGRATION AZURE_FINANCE_STORAGE_INT;
-- Copy the values of:
--   AZURE_CONSENT_URL   → paste in browser to grant Snowflake consent
--   AZURE_MULTI_TENANT_APP_NAME → use this as the service principal in Azure IAM

-- ────────────────────────────────────────
-- STEP 3: Grant the integration to transformer role
-- ────────────────────────────────────────
GRANT USAGE ON INTEGRATION AZURE_FINANCE_STORAGE_INT TO ROLE FINANCE_TRANSFORMER;

-- ────────────────────────────────────────
-- STEP 4: Create External Stages (one per entity)
-- Replace <YOUR_STORAGE_ACCOUNT> with your actual storage account name
-- ────────────────────────────────────────

-- Helper: define a stage URL base
-- Format: azure://<account>.blob.core.windows.net/<container>/landing/<source>/<entity>/

CREATE STAGE IF NOT EXISTS RAW.STG_STRIPE_BALANCE_TRANSACTIONS
    URL                 = 'azure://<YOUR_STORAGE_ACCOUNT>.blob.core.windows.net/finance/landing/stripe/balance_transactions/'
    STORAGE_INTEGRATION = AZURE_FINANCE_STORAGE_INT
    FILE_FORMAT         = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
    COMMENT             = 'Landing zone for Stripe balance_transactions Parquet files';

CREATE STAGE IF NOT EXISTS RAW.STG_STRIPE_CHARGES
    URL                 = 'azure://<YOUR_STORAGE_ACCOUNT>.blob.core.windows.net/finance/landing/stripe/charges/'
    STORAGE_INTEGRATION = AZURE_FINANCE_STORAGE_INT
    FILE_FORMAT         = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE);

CREATE STAGE IF NOT EXISTS RAW.STG_STRIPE_REFUNDS
    URL                 = 'azure://<YOUR_STORAGE_ACCOUNT>.blob.core.windows.net/finance/landing/stripe/refunds/'
    STORAGE_INTEGRATION = AZURE_FINANCE_STORAGE_INT
    FILE_FORMAT         = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE);

CREATE STAGE IF NOT EXISTS RAW.STG_STRIPE_DISPUTES
    URL                 = 'azure://<YOUR_STORAGE_ACCOUNT>.blob.core.windows.net/finance/landing/stripe/disputes/'
    STORAGE_INTEGRATION = AZURE_FINANCE_STORAGE_INT
    FILE_FORMAT         = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE);

CREATE STAGE IF NOT EXISTS RAW.STG_STRIPE_PAYOUTS
    URL                 = 'azure://<YOUR_STORAGE_ACCOUNT>.blob.core.windows.net/finance/landing/stripe/payouts/'
    STORAGE_INTEGRATION = AZURE_FINANCE_STORAGE_INT
    FILE_FORMAT         = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE);

CREATE STAGE IF NOT EXISTS RAW.STG_STRIPE_CUSTOMERS
    URL                 = 'azure://<YOUR_STORAGE_ACCOUNT>.blob.core.windows.net/finance/landing/stripe/customers/'
    STORAGE_INTEGRATION = AZURE_FINANCE_STORAGE_INT
    FILE_FORMAT         = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE);

CREATE STAGE IF NOT EXISTS RAW.STG_STRIPE_SUBSCRIPTIONS
    URL                 = 'azure://<YOUR_STORAGE_ACCOUNT>.blob.core.windows.net/finance/landing/stripe/subscriptions/'
    STORAGE_INTEGRATION = AZURE_FINANCE_STORAGE_INT
    FILE_FORMAT         = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE);

CREATE STAGE IF NOT EXISTS RAW.STG_STRIPE_INVOICES
    URL                 = 'azure://<YOUR_STORAGE_ACCOUNT>.blob.core.windows.net/finance/landing/stripe/invoices/'
    STORAGE_INTEGRATION = AZURE_FINANCE_STORAGE_INT
    FILE_FORMAT         = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE);

CREATE STAGE IF NOT EXISTS RAW.STG_STRIPE_INVOICE_LINE_ITEMS
    URL                 = 'azure://<YOUR_STORAGE_ACCOUNT>.blob.core.windows.net/finance/landing/stripe/invoice_line_items/'
    STORAGE_INTEGRATION = AZURE_FINANCE_STORAGE_INT
    FILE_FORMAT         = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE);

CREATE STAGE IF NOT EXISTS RAW.STG_STRIPE_PRICES
    URL                 = 'azure://<YOUR_STORAGE_ACCOUNT>.blob.core.windows.net/finance/landing/stripe/prices/'
    STORAGE_INTEGRATION = AZURE_FINANCE_STORAGE_INT
    FILE_FORMAT         = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE);

CREATE STAGE IF NOT EXISTS RAW.STG_STRIPE_PRODUCTS
    URL                 = 'azure://<YOUR_STORAGE_ACCOUNT>.blob.core.windows.net/finance/landing/stripe/products/'
    STORAGE_INTEGRATION = AZURE_FINANCE_STORAGE_INT
    FILE_FORMAT         = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE);

CREATE STAGE IF NOT EXISTS RAW.STG_FX_RATES
    URL                 = 'azure://<YOUR_STORAGE_ACCOUNT>.blob.core.windows.net/finance/landing/fx/rates/'
    STORAGE_INTEGRATION = AZURE_FINANCE_STORAGE_INT
    FILE_FORMAT         = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE);

-- Grant stage usage to transformer
GRANT USAGE ON ALL STAGES IN SCHEMA FINANCE_PLATFORM_DEV.RAW TO ROLE FINANCE_TRANSFORMER;
GRANT USAGE ON ALL STAGES IN SCHEMA FINANCE_PLATFORM_DEV.RAW TO ROLE FINANCE_LOADER;

-- Verify stages
SHOW STAGES IN SCHEMA FINANCE_PLATFORM_DEV.RAW;

SELECT 'Script 04 complete — storage integration and stages created.' AS STATUS;
