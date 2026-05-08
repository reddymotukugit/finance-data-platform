# Finance Data Platform

A production-grade financial data pipeline built on **Azure ADLS Gen2 · Snowflake · dbt · Apache Airflow**.

Ingests Stripe financial events daily, transforms them through a Bronze → Silver → Gold medallion architecture, and delivers clean, analytics-ready fact and dimension tables for revenue reporting and MRR tracking.

---

## Architecture

```
Stripe API
    │
    ▼
Azure ADLS Gen2          (Parquet landing zone — one folder per entity)
    │
    ▼  COPY INTO
Snowflake RAW schema     (Bronze: raw tables, exact copy of source)
    │
    ▼  dbt incremental
Snowflake SILVER schema  (Cleaned, deduplicated, FX-normalized)
    │
    ▼  dbt incremental
Snowflake GOLD schema    (Fact & dimension tables — analytics-ready)
    │
    ▼
BI Tools / Reports       (FCT_TRANSACTIONS · FCT_MRR_MOVEMENTS · DIM_CUSTOMERS)
```

**Orchestration:** Apache Airflow (Docker · LocalExecutor) runs the full pipeline daily at 02:00 UTC.

---

## Stack

| Layer | Technology |
|---|---|
| Source | Stripe API (balance transactions, invoices, customers, subscriptions) |
| Storage | Azure ADLS Gen2 (Parquet, Snappy compression) |
| Warehouse | Snowflake (multi-role RBAC, separate transformer / loader roles) |
| Transformation | dbt 1.7 (incremental models, surrogate keys, FX normalization) |
| Orchestration | Apache Airflow 2.x (Docker Compose, LocalExecutor) |
| CI/CD | GitHub Actions (dbt test on PR · dbt docs on merge) |
| Language | Python 3.11, SQL |

---

## dbt Models

### Bronze (4 models) — raw copy from Snowflake RAW tables
| Model | Source |
|---|---|
| `brz_stripe_balance_transactions` | Stripe balance transactions |
| `brz_stripe_invoice_line_items` | Stripe invoice line items |
| `brz_stripe_customers` | Stripe customers |
| `brz_fx_rates` | FX rates (USD base) |

### Silver (3 models) — cleaned, deduplicated, FX-normalized
| Model | Grain | Key logic |
|---|---|---|
| `stg_finance_ledger_events` | 1 row per transaction | Cents→USD, FX join, sign normalization, dedup |
| `stg_invoice_line_items` | 1 row per line item | MRR normalization (annual→monthly), FX join |
| `stg_customers_history` | 1 row per customer | SCD-style latest snapshot |

### Gold (4 models) — analytics-ready facts and dimensions
| Model | Grain | Description |
|---|---|---|
| `fct_transactions` | 1 row per transaction | Full ledger with customer + date dims |
| `fct_mrr_movements` | 1 row per customer/plan/month | MRR with New / Expansion / Contraction / Churn classification |
| `dim_customers` | 1 row per customer | Current customer attributes |
| `dim_dates` | 1 row per calendar day | Date spine 2018–2031 |

**Tests:** 26 tests covering uniqueness, not-null, and accepted-values across all gold models.

---

## Project Structure

```
finance-data-platform/
├── airflow/
│   ├── dags/
│   │   └── finance_daily_dag.py     # Master DAG — 21 tasks end-to-end
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── requirements.txt
├── dbt/
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   ├── macros/
│   │   └── generate_schema_name.sql # Ensures BRONZE/SILVER/GOLD (not BRONZE_BRONZE)
│   ├── packages.yml
│   └── dbt_project.yml
├── scripts/
│   └── stripe_to_adls.py            # Stripe → Azure ADLS Gen2 ingestion
├── setup/
│   ├── azure/
│   │   └── setup_azure.sh           # Azure storage account + container setup
│   └── snowflake/
│       ├── 01_schemas_warehouses_roles.sql
│       ├── 02_raw_tables.sql
│       ├── 03_audit_tables.sql
│       ├── 04_storage_integration.sql
│       └── 05_create_stages.sql
├── docs/
│   └── 2_DAY_EXECUTION_PLAN.md
├── .env.example
├── .gitignore
└── README.md
```

---

## Airflow DAG — `finance_data_platform_daily`

```
create_run_id
    │
    ├── ingest_balance_transactions ──► copy_into_raw_balance_transactions ──┐
    ├── ingest_charges              ──► copy_into_raw_charges               ──┤
    ├── ingest_customers            ──► copy_into_raw_customers             ──┤──► dbt_run_bronze
    ├── ingest_subscriptions        ──► copy_into_raw_subscriptions         ──┤        │
    ├── ingest_invoices             ──► copy_into_raw_invoices              ──┤    dbt_run_silver
    └── ingest_invoice_line_items   ──► copy_into_raw_invoice_line_items    ──┘        │
                                                                               dbt_test_silver
                                                                                       │
                                                                               dbt_run_gold
                                                                                       │
                                                                               dbt_test_gold
                                                                                       │
                                                                          finance_reconciliation_checks
                                                                                       │
                                                                               update_watermarks
                                                                                       │
                                                                          send_success_notification
```

---

## Local Setup

### Prerequisites
- Docker Desktop
- Python 3.11+
- Snowflake account
- Azure storage account
- Stripe account (test mode is fine)

### 1. Clone the repo

```bash
git clone https://github.com/<your-username>/finance-data-platform.git
cd finance-data-platform
```

### 2. Configure environment variables

```bash
cp .env.example .env
# Edit .env with your actual credentials
```

### 3. Set up Snowflake

Run the setup scripts in order against your Snowflake account:

```bash
# In Snowflake worksheet, run in order:
setup/snowflake/01_schemas_warehouses_roles.sql
setup/snowflake/02_raw_tables.sql
setup/snowflake/03_audit_tables.sql
setup/snowflake/04_storage_integration.sql
setup/snowflake/05_create_stages.sql
```

### 4. Set up Azure storage

```bash
chmod +x setup/azure/setup_azure.sh
./setup/azure/setup_azure.sh
```

### 5. Start Airflow

```bash
cd airflow
cp ../.env .env           # Docker Compose reads .env from this directory
docker compose up -d
```

Airflow UI: http://localhost:8080 (admin / admin)

### 6. Add Snowflake connection in Airflow

Admin → Connections → Add:

| Field | Value |
|---|---|
| Connection ID | `snowflake_finance` |
| Connection Type | `Snowflake` |
| Account | `<your-account>` e.g. `orgname-accountname` |
| Login | `FINANCE_AIRFLOW_USER` |
| Password | your password |
| Database | `FINANCE_PLATFORM_DEV` |
| Schema | `RAW` |
| Warehouse | `FINANCE_TRANSFORM_WH` |
| Role | `FINANCE_TRANSFORMER` |

### 7. Trigger the DAG

In the Airflow UI, unpause `finance_data_platform_daily` and trigger a manual run. All 21 tasks should complete green in ~5 minutes.

### 8. Run dbt manually (optional)

```bash
cd dbt
dbt deps
dbt run --select tag:silver tag:gold
dbt test
```

---

## CI/CD

| Workflow | Trigger | What it does |
|---|---|---|
| `dbt-ci.yml` | Pull request to `main` | Compiles dbt, runs all 26 tests against Snowflake dev target |
| `dbt-docs.yml` | Push to `main` | Generates dbt docs and publishes to GitHub Pages |

CI requires these GitHub secrets:

```
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_ROLE
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
```

---

## Key Design Decisions

**Incremental models with `unique_key`** — All Silver and Gold models use dbt incremental strategy with MERGE, so daily runs only process new records rather than full table scans.

**QUALIFY for deduplication** — Raw tables can accumulate duplicates when COPY INTO stages have no prior load history. All Silver models use `QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) = 1` as a guard.

**FX normalization in Silver** — All amounts are converted to USD in the Silver layer using a deduplicated FX rates table, keeping Gold models simple and currency-agnostic.

**`generate_schema_name` macro** — Overrides dbt's default schema naming to land models in `BRONZE`, `SILVER`, `GOLD` rather than `BRONZE_BRONZE`, `BRONZE_SILVER` etc.

**Signed amount convention** — In `stg_finance_ledger_events`, refunds, disputes, and adjustments are stored as negative amounts. Charges and payouts are positive. This makes aggregations in Gold straightforward.

---

## Environment Variables Reference

See `.env.example` for the full list. Key variables:

| Variable | Description |
|---|---|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier (e.g. `orgname-accountname`) |
| `SNOWFLAKE_USER` | Airflow service account username |
| `SNOWFLAKE_PASSWORD` | Airflow service account password |
| `STRIPE_API_KEY` | Stripe secret key (`sk_test_...` for test mode) |
| `AZURE_STORAGE_ACCOUNT` | Azure storage account name |
| `AZURE_STORAGE_KEY` | Azure storage account access key |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook for pipeline alerts |

---

## License

MIT
