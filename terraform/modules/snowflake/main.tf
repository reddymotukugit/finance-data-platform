# ── Database ──────────────────────────────────────────────────────────────────
resource "snowflake_database" "finance" {
  name    = "FINANCE_PLATFORM_${upper(var.environment)}"
  comment = "Finance Data Platform — ${var.environment} environment"
}

# ── Warehouses ────────────────────────────────────────────────────────────────
resource "snowflake_warehouse" "ingest" {
  name           = "FINANCE_INGEST_WH"
  warehouse_size = "X-SMALL"
  auto_suspend   = 60
  auto_resume    = true
  comment        = "Used for Stripe ingestion COPY INTO operations"
}

resource "snowflake_warehouse" "transform" {
  name           = "FINANCE_TRANSFORM_WH"
  warehouse_size = "X-SMALL"
  auto_suspend   = 60
  auto_resume    = true
  comment        = "Used for dbt transformations"
}

# ── Roles ─────────────────────────────────────────────────────────────────────
resource "snowflake_role" "loader" {
  name    = "FINANCE_LOADER"
  comment = "Loads raw data from ADLS stages into RAW schema"
}

resource "snowflake_role" "transformer" {
  name    = "FINANCE_TRANSFORMER"
  comment = "Runs dbt transformations across Bronze, Silver, Gold"
}

resource "snowflake_role" "analyst" {
  name    = "FINANCE_ANALYST"
  comment = "Read-only access to Gold schema for BI tools"
}

# ── Role hierarchy ────────────────────────────────────────────────────────────
resource "snowflake_role_grants" "transformer_inherits_loader" {
  role_name = snowflake_role.loader.name
  roles     = [snowflake_role.transformer.name]
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
  default_role = snowflake_role.transformer.name
  comment      = "Service account for dbt transformations"

  must_change_password = false
}

resource "snowflake_user" "airflow" {
  name         = "FINANCE_AIRFLOW_USER"
  password     = var.airflow_user_password
  default_role = snowflake_role.transformer.name
  comment      = "Service account for Airflow orchestration"

  must_change_password = false
}

# ── Role assignments ──────────────────────────────────────────────────────────
resource "snowflake_user_grant" "dbt_transformer" {
  privilege = "USAGE"
  roles     = [snowflake_role.transformer.name]
  user_name = snowflake_user.dbt.name
}

resource "snowflake_user_grant" "airflow_transformer" {
  privilege = "USAGE"
  roles     = [snowflake_role.transformer.name]
  user_name = snowflake_user.airflow.name
}

# ── Warehouse grants ──────────────────────────────────────────────────────────
resource "snowflake_warehouse_grant" "loader_ingest" {
  warehouse_name = snowflake_warehouse.ingest.name
  privilege      = "USAGE"
  roles          = [snowflake_role.loader.name]
}

resource "snowflake_warehouse_grant" "transformer_transform" {
  warehouse_name = snowflake_warehouse.transform.name
  privilege      = "USAGE"
  roles          = [snowflake_role.transformer.name]
}

# ── Database grants ───────────────────────────────────────────────────────────
resource "snowflake_database_grant" "loader" {
  database_name = snowflake_database.finance.name
  privilege     = "USAGE"
  roles         = [snowflake_role.loader.name]
}

resource "snowflake_database_grant" "transformer" {
  database_name = snowflake_database.finance.name
  privilege     = "USAGE"
  roles         = [snowflake_role.transformer.name]
}

resource "snowflake_database_grant" "analyst" {
  database_name = snowflake_database.finance.name
  privilege     = "USAGE"
  roles         = [snowflake_role.analyst.name]
}

# ── Schema grants ─────────────────────────────────────────────────────────────
resource "snowflake_schema_grant" "loader_raw" {
  database_name = snowflake_database.finance.name
  schema_name   = snowflake_schema.raw.name
  privilege     = "USAGE"
  roles         = [snowflake_role.loader.name]
}

resource "snowflake_schema_grant" "transformer_all" {
  for_each = {
    raw    = snowflake_schema.raw.name
    bronze = snowflake_schema.bronze.name
    silver = snowflake_schema.silver.name
    gold   = snowflake_schema.gold.name
    audit  = snowflake_schema.audit.name
  }

  database_name = snowflake_database.finance.name
  schema_name   = each.value
  privilege     = "USAGE"
  roles         = [snowflake_role.transformer.name]
}

resource "snowflake_schema_grant" "analyst_gold" {
  database_name = snowflake_database.finance.name
  schema_name   = snowflake_schema.gold.name
  privilege     = "USAGE"
  roles         = [snowflake_role.analyst.name]
}
