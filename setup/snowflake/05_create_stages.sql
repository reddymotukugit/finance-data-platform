-- ============================================================
-- FINANCE DATA PLATFORM — SNOWFLAKE SETUP
-- Script 05: Create External Stages (Azure ADLS Gen2 via SAS)
-- Run as ACCOUNTADMIN
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE FINANCE_PLATFORM_DEV;
USE SCHEMA RAW;

-- SAS token valid until 2027-12-31
-- Storage account: stfinancereddy001 | Container: finance

CREATE OR REPLACE STAGE RAW.STG_STRIPE_BALANCE_TRANSACTIONS
    URL         = 'azure://stfinancereddy001.blob.core.windows.net/finance/landing/stripe/balance_transactions/'
    CREDENTIALS = (AZURE_SAS_TOKEN = 'se=2027-12-31&sp=rl&sv=2026-02-06&sr=c&sig=WA3scINDEdfOxUFP1%2Bj7l5QgqJDxOTrXqzB%2BfeyCS2w%3D')
    FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
    COMMENT     = 'Stripe balance_transactions landing zone';

CREATE OR REPLACE STAGE RAW.STG_STRIPE_CHARGES
    URL         = 'azure://stfinancereddy001.blob.core.windows.net/finance/landing/stripe/charges/'
    CREDENTIALS = (AZURE_SAS_TOKEN = 'se=2027-12-31&sp=rl&sv=2026-02-06&sr=c&sig=WA3scINDEdfOxUFP1%2Bj7l5QgqJDxOTrXqzB%2BfeyCS2w%3D')
    FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
    COMMENT     = 'Stripe charges landing zone';

CREATE OR REPLACE STAGE RAW.STG_STRIPE_REFUNDS
    URL         = 'azure://stfinancereddy001.blob.core.windows.net/finance/landing/stripe/refunds/'
    CREDENTIALS = (AZURE_SAS_TOKEN = 'se=2027-12-31&sp=rl&sv=2026-02-06&sr=c&sig=WA3scINDEdfOxUFP1%2Bj7l5QgqJDxOTrXqzB%2BfeyCS2w%3D')
    FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
    COMMENT     = 'Stripe refunds landing zone';

CREATE OR REPLACE STAGE RAW.STG_STRIPE_DISPUTES
    URL         = 'azure://stfinancereddy001.blob.core.windows.net/finance/landing/stripe/disputes/'
    CREDENTIALS = (AZURE_SAS_TOKEN = 'se=2027-12-31&sp=rl&sv=2026-02-06&sr=c&sig=WA3scINDEdfOxUFP1%2Bj7l5QgqJDxOTrXqzB%2BfeyCS2w%3D')
    FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
    COMMENT     = 'Stripe disputes landing zone';

CREATE OR REPLACE STAGE RAW.STG_STRIPE_PAYOUTS
    URL         = 'azure://stfinancereddy001.blob.core.windows.net/finance/landing/stripe/payouts/'
    CREDENTIALS = (AZURE_SAS_TOKEN = 'se=2027-12-31&sp=rl&sv=2026-02-06&sr=c&sig=WA3scINDEdfOxUFP1%2Bj7l5QgqJDxOTrXqzB%2BfeyCS2w%3D')
    FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
    COMMENT     = 'Stripe payouts landing zone';

CREATE OR REPLACE STAGE RAW.STG_STRIPE_CUSTOMERS
    URL         = 'azure://stfinancereddy001.blob.core.windows.net/finance/landing/stripe/customers/'
    CREDENTIALS = (AZURE_SAS_TOKEN = 'se=2027-12-31&sp=rl&sv=2026-02-06&sr=c&sig=WA3scINDEdfOxUFP1%2Bj7l5QgqJDxOTrXqzB%2BfeyCS2w%3D')
    FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
    COMMENT     = 'Stripe customers landing zone';

CREATE OR REPLACE STAGE RAW.STG_STRIPE_SUBSCRIPTIONS
    URL         = 'azure://stfinancereddy001.blob.core.windows.net/finance/landing/stripe/subscriptions/'
    CREDENTIALS = (AZURE_SAS_TOKEN = 'se=2027-12-31&sp=rl&sv=2026-02-06&sr=c&sig=WA3scINDEdfOxUFP1%2Bj7l5QgqJDxOTrXqzB%2BfeyCS2w%3D')
    FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
    COMMENT     = 'Stripe subscriptions landing zone';

CREATE OR REPLACE STAGE RAW.STG_STRIPE_INVOICES
    URL         = 'azure://stfinancereddy001.blob.core.windows.net/finance/landing/stripe/invoices/'
    CREDENTIALS = (AZURE_SAS_TOKEN = 'se=2027-12-31&sp=rl&sv=2026-02-06&sr=c&sig=WA3scINDEdfOxUFP1%2Bj7l5QgqJDxOTrXqzB%2BfeyCS2w%3D')
    FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
    COMMENT     = 'Stripe invoices landing zone';

CREATE OR REPLACE STAGE RAW.STG_STRIPE_INVOICE_LINE_ITEMS
    URL         = 'azure://stfinancereddy001.blob.core.windows.net/finance/landing/stripe/invoice_line_items/'
    CREDENTIALS = (AZURE_SAS_TOKEN = 'se=2027-12-31&sp=rl&sv=2026-02-06&sr=c&sig=WA3scINDEdfOxUFP1%2Bj7l5QgqJDxOTrXqzB%2BfeyCS2w%3D')
    FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
    COMMENT     = 'Stripe invoice_line_items landing zone';

CREATE OR REPLACE STAGE RAW.STG_FX_RATES
    URL         = 'azure://stfinancereddy001.blob.core.windows.net/finance/landing/fx/rates/'
    CREDENTIALS = (AZURE_SAS_TOKEN = 'se=2027-12-31&sp=rl&sv=2026-02-06&sr=c&sig=WA3scINDEdfOxUFP1%2Bj7l5QgqJDxOTrXqzB%2BfeyCS2w%3D')
    FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
    COMMENT     = 'FX rates landing zone';

-- Grant access to transformer and loader roles
GRANT USAGE ON ALL STAGES IN SCHEMA FINANCE_PLATFORM_DEV.RAW TO ROLE FINANCE_TRANSFORMER;
GRANT USAGE ON ALL STAGES IN SCHEMA FINANCE_PLATFORM_DEV.RAW TO ROLE FINANCE_LOADER;

-- Verify all stages created
SHOW STAGES IN SCHEMA FINANCE_PLATFORM_DEV.RAW;

SELECT 'Script 05 complete — ' || COUNT(*) || ' stages created.' AS STATUS
FROM INFORMATION_SCHEMA.STAGES
WHERE STAGE_SCHEMA = 'RAW' AND STAGE_CATALOG = 'FINANCE_PLATFORM_DEV';
