# Finance Data Platform — 2-Day Execution Plan

> **Goal:** Full working pipeline — Stripe → ADLS → Snowflake → dbt → Gold layer  
> **Stack:** Stripe (test API) · Azure ADLS Gen2 · Snowflake · dbt · Airflow (Docker)

---

## Prerequisites (do these first — 30 min)

| Tool | Install |
|------|---------|
| Azure CLI | https://docs.microsoft.com/en-us/cli/azure/install-azure-cli |
| Docker Desktop | https://www.docker.com/products/docker-desktop |
| Python 3.11+ | https://www.python.org/downloads |
| dbt-snowflake | `pip install dbt-snowflake` |
| Stripe CLI (optional) | https://stripe.com/docs/stripe-cli |

### Create your `.env` file
```bash
cd finance-data-platform
cp .env.example .env
# Open .env and fill in all your values
```

---

## DAY 1 — Infrastructure + Raw Data Flowing

### Step 1: Azure Setup (30 min)

```bash
# Login to Azure
az login

# Open setup/azure/setup_azure.sh and replace:
#   <YOUR_AZURE_SUBSCRIPTION_ID>  → your subscription ID (from az account list)
#   stfindatalakedev001           → a globally unique storage name (all lowercase, no hyphens)

# Run the setup script
chmod +x setup/azure/setup_azure.sh
bash setup/azure/setup_azure.sh
```

**After it completes, copy:**
- Storage Account Name → update `AZURE_STORAGE_ACCOUNT` in `.env`
- Storage Account Key  → update `AZURE_STORAGE_KEY` in `.env` (get from Azure Portal → Storage Account → Access Keys)

---

### Step 2: Snowflake Setup (45 min)

Open your Snowflake account and run these scripts **in order** in the Snowflake worksheet:

```
setup/snowflake/01_schemas_warehouses_roles.sql   ← creates roles, warehouses, schemas
setup/snowflake/02_raw_tables.sql                  ← creates all RAW tables
setup/snowflake/03_audit_tables.sql                ← creates audit + watermark tables
setup/snowflake/04_storage_integration.sql         ← links Snowflake to ADLS
```

**Important for Script 04:**
1. Replace `<YOUR_AZURE_TENANT_ID>` with your Azure tenant ID (`az account show --query tenantId`)
2. Replace `<YOUR_STORAGE_ACCOUNT>` with your ADLS account name
3. After running `CREATE STORAGE INTEGRATION`, run `DESC INTEGRATION AZURE_FINANCE_STORAGE_INT`
4. Copy the `AZURE_CONSENT_URL` and paste it in your browser to grant Snowflake access
5. Copy the `AZURE_MULTI_TENANT_APP_NAME` and add it as a Storage Blob Data Contributor in Azure IAM

Update `.env` with your Snowflake account name (format: `xy12345.us-east-1`).

---

### Step 3: Test Stripe Ingestion (30 min)

```bash
# Install Python dependencies
pip install stripe pandas pyarrow azure-storage-file-datalake python-dotenv requests

# Run a test ingestion (customers is safest/fastest to test with)
python scripts/stripe_to_adls.py --entity customers --run-id test-001

# If successful, run all entities
python scripts/stripe_to_adls.py --entity all --run-id test-002
```

**Verify in Azure Portal:** Go to your Storage Account → Containers → finance → landing/stripe/ — you should see Parquet files.

---

### Step 4: Run First Snowflake COPY INTO (30 min)

Open Snowflake and run manually (replace dates with today):

```sql
USE ROLE FINANCE_TRANSFORMER;
USE WAREHOUSE FINANCE_INGEST_WH;

-- Load customers
COPY INTO FINANCE_PLATFORM_DEV.RAW.RAW_STRIPE_CUSTOMERS
FROM @FINANCE_PLATFORM_DEV.RAW.STG_STRIPE_CUSTOMERS
FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';

-- Check the load
SELECT COUNT(*), MAX(loaded_at) FROM FINANCE_PLATFORM_DEV.RAW.RAW_STRIPE_CUSTOMERS;

-- Repeat for balance_transactions, charges, etc.
```

**End of Day 1 checkpoint:** You should have raw data in Snowflake RAW tables. ✅

---

## DAY 2 — dbt Transforms + Airflow Orchestration

### Step 5: dbt Setup (45 min)

```bash
cd dbt

# Install dbt packages
dbt deps

# Test connection to Snowflake
dbt debug

# Run Bronze layer
dbt run --select tag:bronze

# Check Bronze tables in Snowflake
# You should see BRONZE.BRZ_STRIPE_BALANCE_TRANSACTIONS, etc.

# Run Silver layer
dbt run --select tag:silver

# Run tests
dbt test --select tag:silver

# Run Gold layer
dbt run --select tag:gold

# Run Gold tests
dbt test --select tag:gold
```

**Verify in Snowflake:**
```sql
-- Check Gold tables
SELECT COUNT(*) FROM FINANCE_PLATFORM_DEV.GOLD.FCT_TRANSACTIONS;
SELECT COUNT(*), SUM(mrr_usd) FROM FINANCE_PLATFORM_DEV.GOLD.FCT_MRR_MOVEMENTS;
SELECT COUNT(*) FROM FINANCE_PLATFORM_DEV.GOLD.DIM_CUSTOMERS WHERE IS_CURRENT = TRUE;
```

---

### Step 6: Airflow Docker Setup (45 min)

```bash
cd airflow

# Generate Fernet key and add to .env
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Copy the output → AIRFLOW__CORE__FERNET_KEY in .env

# Generate webserver secret
python -c "import secrets; print(secrets.token_hex(32))"
# Copy the output → AIRFLOW__WEBSERVER__SECRET_KEY in .env

# Build and start Airflow
docker compose up airflow-init       # first time only — initializes DB and creates admin user
docker compose up -d                 # start all services

# Wait ~60 seconds, then open http://localhost:8080
# Login: admin / admin
```

---

### Step 7: Configure Airflow Snowflake Connection (15 min)

In Airflow UI → Admin → Connections → Add a new record:

| Field | Value |
|-------|-------|
| Connection ID | `snowflake_finance` |
| Connection Type | `Snowflake` |
| Host | `<your-account>.snowflakecomputing.com` |
| Schema | `RAW` |
| Login | `FINANCE_AIRFLOW_USER` |
| Password | `ChangeMe_airflow_2024!` |
| Extra (JSON) | `{"warehouse": "FINANCE_TRANSFORM_WH", "database": "FINANCE_PLATFORM_DEV", "role": "FINANCE_TRANSFORMER"}` |

---

### Step 8: Trigger the Full Pipeline (30 min)

In Airflow UI:
1. Go to DAGs → `finance_data_platform_daily`
2. Toggle the DAG **on** (unpause it)
3. Click the ▶ **Trigger DAG** button
4. Watch the Graph view — tasks turn green as they complete

**Expected run time:** 15–25 minutes for a full pipeline run.

---

### Step 9: Verify End-to-End (20 min)

```sql
-- Revenue summary
SELECT
    DATE_TRUNC('month', created_date)   AS month,
    event_type,
    COUNT(*)                            AS events,
    SUM(amount_usd)                     AS gross_revenue_usd,
    SUM(signed_net_amount_usd)          AS net_revenue_usd
FROM FINANCE_PLATFORM_DEV.GOLD.FCT_TRANSACTIONS
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- MRR breakdown
SELECT
    billing_month,
    movement_type,
    COUNT(*)                AS customers,
    SUM(mrr_usd)            AS mrr_usd,
    SUM(mrr_change_usd)     AS mrr_change_usd
FROM FINANCE_PLATFORM_DEV.GOLD.FCT_MRR_MOVEMENTS
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- Check audit log
SELECT * FROM FINANCE_PLATFORM_DEV.AUDIT.PIPELINE_RUNS ORDER BY logged_at DESC LIMIT 10;
SELECT * FROM FINANCE_PLATFORM_DEV.AUDIT.INGESTION_CONTROL;
```

---

## Project Complete ✅

**What you have built:**
- Azure ADLS Gen2 with full landing zone directory structure
- 12 RAW Snowflake tables loaded from Stripe test API
- dbt Medallion Architecture: Bronze → Silver → Gold
- fct_transactions (accounting ledger), fct_mrr_movements, dim_customers (SCD2), dim_dates
- Airflow master DAG orchestrating the full pipeline
- Watermark-controlled incremental ingestion
- Reconciliation checks in the AUDIT schema

**To run the pipeline daily:** Airflow is scheduled at 02:00 UTC automatically. Just leave Docker running.

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| COPY INTO returns 0 rows | Check stage URL in Script 04 — must match your storage account name exactly |
| dbt connection fails | Run `dbt debug` and check `profiles.yml` — verify env vars are set |
| Airflow can't reach Snowflake | Check the `snowflake_finance` connection in Airflow UI |
| Stripe returns no data | Your test account may have no data — use Stripe CLI to generate test data: `stripe fixtures stripe/fixtures.json` |
| Storage integration fails | Re-run the consent URL in browser and re-assign IAM role to Snowflake managed identity |
