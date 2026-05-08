output "storage_account_name" {
  value = azurerm_storage_account.finance.name
}

output "storage_account_id" {
  value = azurerm_storage_account.finance.id
}

output "storage_account_primary_key" {
  value     = azurerm_storage_account.finance.primary_access_key
  sensitive = true
}

output "container_name" {
  value = azurerm_storage_container.finance.name
}

output "landing_path" {
  value = "azure://${azurerm_storage_account.finance.name}.blob.core.windows.net/finance/landing/"
}

output "resource_group_name" {
  value = azurerm_resource_group.finance.name
}
