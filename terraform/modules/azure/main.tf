# ── Resource Group ────────────────────────────────────────────────────────────
resource "azurerm_resource_group" "finance" {
  name     = var.resource_group_name
  location = var.location

  tags = {
    environment = var.environment
    project     = "finance-data-platform"
    managed_by  = "terraform"
  }
}

# ── Storage Account (ADLS Gen2) ───────────────────────────────────────────────
resource "azurerm_storage_account" "finance" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.finance.name
  location                 = azurerm_resource_group.finance.location
  account_tier             = "Standard"
  account_replication_type = var.replication_type
  account_kind             = "StorageV2"

  # Enable hierarchical namespace = ADLS Gen2
  is_hns_enabled = true

  # Security settings
  min_tls_version           = "TLS1_2"
  enable_https_traffic_only = true

  blob_properties {
    delete_retention_policy {
      days = 7
    }
  }

  tags = {
    environment = var.environment
    project     = "finance-data-platform"
    managed_by  = "terraform"
  }
}

# ── Container ─────────────────────────────────────────────────────────────────
resource "azurerm_storage_container" "finance" {
  name                  = "finance"
  storage_account_name  = azurerm_storage_account.finance.name
  container_access_type = "private"
}

# ── Landing Zone Folder Structure ─────────────────────────────────────────────
# ADLS Gen2 folders are created as empty blobs with "/" suffix

locals {
  stripe_entities = [
    "balance_transactions",
    "charges",
    "refunds",
    "disputes",
    "payouts",
    "customers",
    "subscriptions",
    "invoices",
    "invoice_line_items",
    "prices",
    "products",
  ]
}

resource "azurerm_storage_blob" "stripe_folders" {
  for_each = toset(local.stripe_entities)

  name                   = "landing/stripe/${each.value}/.keep"
  storage_account_name   = azurerm_storage_account.finance.name
  storage_container_name = azurerm_storage_container.finance.name
  type                   = "Block"
  source_content         = ""
}

resource "azurerm_storage_blob" "fx_folder" {
  name                   = "landing/fx/rates/.keep"
  storage_account_name   = azurerm_storage_account.finance.name
  storage_container_name = azurerm_storage_container.finance.name
  type                   = "Block"
  source_content         = ""
}

resource "azurerm_storage_blob" "archive_folder" {
  name                   = "archive/.keep"
  storage_account_name   = azurerm_storage_account.finance.name
  storage_container_name = azurerm_storage_container.finance.name
  type                   = "Block"
  source_content         = ""
}
