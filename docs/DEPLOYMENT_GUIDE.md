# Finance Data Platform — Deployment Guide

> **Stack:** Stripe · Azure ADLS Gen2 · Snowflake · dbt · Apache Airflow (Docker)

End-to-end setup guide for deploying the Finance Data Platform from scratch. Follow the phases in order.

---

## Prerequisites

| Tool | Install |
|------|---------|
| Azure CLI | https://docs.microsoft.com/en-us/cli/azure/install-azure-cli |
| Docker Desktop | https://www.docker.com/products/docker-desktop |
| Python 3.11+ | https://www.python.org/downloads |
| dbt-snowflake | `pip install dbt-snowflake` |
| Stripe CLI (optional) | https://stripe.com/docs/stripe-cli |

### Configure environment variables

```bash
cp .env.example .env
# Fill in all values in .env before proceeding
```

---

## Phase 1 — Infrastructure Setup

### Azure Storage

```bash
chmod +x setup/azure/setup_azure.sh
bash setup/azure/setup_azure.sh
```

After completion, update `.env`:
- `AZURE_STORAGE_ACCOUNT` — your storage account name
- `AZURE_STORAGE_KEY` — from Azure Portal → Storage Account → Access Keys

### Snowflake

Run the setup scripts **in order** in a Snowflake worksheet:

```
setup/snowflake/01_schemas_warehouses_roles.sql   — roles, warehouses, schemas
setup/snowflake/02_raw_tables.sql                  — all RAW source tables
setup/snowflake/03_audit_tables.sql                — audit and watermark tables
setup/snowflake/04_storage_integration.sql         — Snowflake ↔ Azure ADLS trust
setup/snowflake/05_create_stages.sql               — external stages per entity
```

**After running Script 04:**
1. Run `DESC INTEGRATION AZURE_FINANCE_STORAGE_INT`
2. Paste the `AZURE_CONSENT_URL` in your browser to grant Snowflake access
3. Assign `Storage Blob Data Contributor` role to the `AZURE_MULTI_TENANT_APP_NAME` in Azure IAM

---

## Phase 2 — Data Ingestion

### Stripe → Azure ADLS Gen2

```bash
pip install stripe pandas pyarrow azure-storage-file-datalake python-dotenv requests

# Test with a single entity first
python scripts/stripe_to_adls.py --entity customers --run-id test-001

# Run all entities
python scripts/stripe_to_adls.py --entity all --run-id test-002
```

Verify in Azure Portal: Storage Account → Containers → `finance` → `landing/stripe/` should contain Parquet files.

### Snowflake COPY INTO

```sql
USE ROLE FINANCE_TRANSFORMER;
USE WAREHOUSE FINANCE_INGEST_WH;

COPY INTO FINANCE_PLATFORM_DEV.RAW.RAW_STRIPE_CUSTOMERS
FROM @FINANCE_PLATFORM_DEV.RAW.STG_STRIPE_CUSTOMERS
FILE_FORMAT = (TYPE = PARQUET, SNAPPY_COMPRESSION = TRUE)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';

-- Verify
SELECT COUNT(*), MAX(loaded_at) FROM FINANCE_PLATFORM_DEV.RAW.RAW_STRIPE_CUSTOMERS;
```

Repeat for: `balance_transactions`, `charges`, `refunds`, `disputes`, `payouts`, `invoices`, `invoice_line_items`, `subscriptions`.

---

## Phase 3 — dbt Transformation

```bash
cd dbt
dbt deps        # install packages
dbt debug       # verify Snowflake connection

dbt run --select tag:bronze    # Bronze layer
dbt run --select tag:silver    # Silver layer — cleaned, FX-normalized
dbt test --select tag:silver   # Run data quality tests

dbt run --select tag:gold      # Gold layer — facts and dimensions
dbt test --select tag:gold     # Run all 26 tests
```

**Verify in Snowflake:**
```sql
SELECT COUNT(*) FROM FINANCE_PLATFORM_DEV.GOLD.FCT_TRANSACTIONS;
SELECT COUNT(*), SUM(mrr_usd) FROM FINANCE_PLATFORM_DEV.GOLD.FCT_MRR_MOVEMENTS;
SELECT COUNT(*) FROM FINANCE_PLATFORM_DEV.GOLD.DIM_CUSTOMERS WHERE IS_CURRENT = TRUE;
```

---

## Phase 4 — Airflow Orchestration

### Start Airflow

```bash
cd airflow

# Generate secrets and add to .env
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# → AIRFLOW__CORE__FERNET_KEY

python -c "import secrets; print(secrets.token_hex(32))"
# → AIRFLOW__WEBSERVER__SECRET_KEY

docker compose up airflow-init    # first-time DB initialization
docker compose up -d              # start all services
```

Airflow UI: http://localhost:8080 (admin / admin)

### Configure Snowflake Connection

Admin → Connections → Add:

| Field | Value |
|-------|-------|
| Connection ID | `snowflake_finance` |
| Connection Type | `Snowflake` |
| Account | your account identifier |
| Login | `FINANCE_AIRFLOW_USER` |
| Password | your password |
| Database | `FINANCE_PLATFORM_DEV` |
| Schema | `RAW` |
| Warehouse | `FINANCE_TRANSFORM_WH` |
| Role | `FINANCE_TRANSFORMER` |

### Trigger the Pipeline

1. Unpause `finance_data_platform_daily` in the DAGs list
2. Click **Trigger DAG**
3. All 21 tasks should complete green in ~5 minutes

---

## Phase 5 — Verify End-to-End

```sql
-- Revenue summary by month
SELECT
    DATE_TRUNC('month', created_date)   AS month,
    event_type,
    COUNT(*)                            AS events,
    SUM(amount_usd)                     AS gross_revenue_usd,
    SUM(signed_net_amount_usd)          AS net_revenue_usd
FROM FINANCE_PLATFORM_DEV.GOLD.FCT_TRANSACTIONS
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- MRR movements
SELECT
    billing_month,
    movement_type,
    COUNT(*)            AS customers,
    SUM(mrr_usd)        AS mrr_usd,
    SUM(mrr_change_usd) AS mrr_change_usd
FROM FINANCE_PLATFORM_DEV.GOLD.FCT_MRR_MOVEMENTS
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- Pipeline audit log
SELECT * FROM FINANCE_PLATFORM_DEV.AUDIT.PIPELINE_RUNS ORDER BY logged_at DESC LIMIT 10;
```

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| COPY INTO returns 0 rows | Check stage URL in `05_create_stages.sql` — must match your storage account name exactly |
| `dbt debug` fails | Verify `profiles.yml` env vars are set and Snowflake account identifier is correct |
| Airflow can't reach Snowflake | Check the `snowflake_finance` connection in Airflow Admin UI |
| Stripe returns no data | Use Stripe CLI to seed test data: `stripe fixtures trigger payment_intent.succeeded` |
| Storage integration fails | Re-run the consent URL in browser and reassign IAM role to the Snowflake managed identity |
| Account temporarily locked | Run `ALTER USER <user> SET MINS_TO_UNLOCK = 0;` as ACCOUNTADMIN in Snowflake |
