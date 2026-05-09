output "database_name" {
  value = snowflake_database.finance.name
}

output "transformer_role_name" {
  value = snowflake_account_role.transformer.name
}

output "loader_role_name" {
  value = snowflake_account_role.loader.name
}

output "analyst_role_name" {
  value = snowflake_account_role.analyst.name
}

output "warehouse_transform_name" {
  value = snowflake_warehouse.transform.name
}

output "warehouse_ingest_name" {
  value = snowflake_warehouse.ingest.name
}

output "schema_raw" {
  value = snowflake_schema.raw.name
}

output "schema_gold" {
  value = snowflake_schema.gold.name
}
