output "snowflake_database" {
  description = "Snowflake database name"
  value       = module.snowflake.database_name
}

output "snowflake_transformer_role" {
  description = "Snowflake transformer role name"
  value       = module.snowflake.transformer_role_name
}

output "snowflake_warehouse_transform" {
  description = "Snowflake transform warehouse name"
  value       = module.snowflake.warehouse_transform_name
}

output "azure_storage_account_name" {
  description = "Azure storage account name"
  value       = module.azure.storage_account_name
}

output "azure_storage_account_id" {
  description = "Azure storage account resource ID"
  value       = module.azure.storage_account_id
}

output "azure_container_name" {
  description = "Azure blob container name"
  value       = module.azure.container_name
}

output "adls_landing_path" {
  description = "Base path for Stripe landing zone in ADLS"
  value       = module.azure.landing_path
}
