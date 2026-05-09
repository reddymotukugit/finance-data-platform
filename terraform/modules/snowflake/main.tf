# ── Database ──────────────────────────────────────────────────────────────────
resource "snowflake_database" "finance" {
  name    = "FINANCE_PLATFORM_${upper(var.environment)}"
  comment = "Finance Data Platform — ${var.environment} environment"
}

# ── Warehouses ────────────────────────────────────────────────────────────────
resource "snowflake_warehouse" "ingest" {
  name           = "FINANCE_INGEST_WH"
  warehouse_size = var.warehouse_size
  auto_suspend   = var.auto_suspend_sec
  auto_resume    = true
  comment        = "Used for Stripe ingestion COPY INTO operations — ${var.environment}"
}

resource "snowflake_warehouse" "transform" {
  name           = "FINANCE_TRANSFORM_WH"
  warehouse_size = var.warehouse_size
  auto_suspend   = var.auto_suspend_sec
  auto_resume    = true
  comment        = "Used for dbt transformations — ${var.environment}"
}

# ── Roles ─────────────────────────────────────────────────────────────────────
resource "snowflake_account_role" "loader" {
  name    = "FINANCE_LOADER"
  comment = "Loads raw data from ADLS stages into RAW schema"
}

resource "snowflake_account_role" "transformer" {
  name    = "FINANCE_TRANSFORMER"
  comment = "Runs dbt transformations across Bronze, Silver, Gold"
}

resource "snowflake_account_role" "analyst" {
  name    = "FINANCE_ANALYST"
  comment = "Read-only access to Gold schema for BI tools"
}

# ── Schemas ───────────────────────────────────────────────────────────────────
resource "snowflake_schema" "raw" {
  database = snowflake_database.finance.name
  name     = "RAW"
  comment  = "Bronze layer — raw COPY INTO from ADLS"
}

resource "snowflake_schema" "bronze" {
  database = snowflake_database.finance.name
  name     = "BRONZE"
  comment  = "Bronze dbt models — typed views over RAW"
}

resource "snowflake_schema" "silver" {
  database = snowflake_database.finance.name
  name     = "SILVER"
  comment  = "Silver layer — cleaned, deduplicated, FX-normalized"
}

resource "snowflake_schema" "gold" {
  database = snowflake_database.finance.name
  name     = "GOLD"
  comment  = "Gold layer — analytics-ready facts and dimensions"
}

resource "snowflake_schema" "audit" {
  database = snowflake_database.finance.name
  name     = "AUDIT"
  comment  = "Pipeline run logs, watermarks, reconciliation results"
}

# ── Service accounts ──────────────────────────────────────────────────────────
resource "snowflake_user" "dbt" {
  name         = "FINANCE_DBT_USER"
  password     = var.dbt_user_password
  default_role = snowflake_account_role.transformer.name
  comment      = "Service account for dbt transformations"

  must_change_password = false
}

resource "snowflake_user" "airflow" {
  name         = "FINANCE_AIRFLOW_USER"
  password     = var.airflow_user_password
  default_role = snowflake_account_role.transformer.name
  comment      = "Service account for Airflow orchestration"

  must_change_password = false
}

# NOTE: Role grants and privilege assignments are managed via setup/snowflake_setup.sql
# using ACCOUNTADMIN to avoid provider version compatibility issues with grant resources.
