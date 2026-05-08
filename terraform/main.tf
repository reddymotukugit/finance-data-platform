terraform {
  required_version = ">= 1.5.0"

  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.87"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.90"
    }
  }

  # Uncomment to use remote state (recommended for teams)
  # backend "azurerm" {
  #   resource_group_name  = "rg-terraform-state"
  #   storage_account_name = "stterraformstate001"
  #   container_name       = "tfstate"
  #   key                  = "finance-data-platform.tfstate"
  # }
}

# ── Snowflake Provider ────────────────────────────────────────────────────────
provider "snowflake" {
  account  = var.snowflake_account
  username = var.snowflake_username
  password = var.snowflake_password
  role     = "ACCOUNTADMIN"
}

# ── Azure Provider ────────────────────────────────────────────────────────────
provider "azurerm" {
  features {}
  subscription_id = var.azure_subscription_id
}

# ── Modules ───────────────────────────────────────────────────────────────────
module "snowflake" {
  source = "./modules/snowflake"

  environment        = var.environment
  snowflake_account  = var.snowflake_account
  dbt_user_password     = var.dbt_user_password
  airflow_user_password = var.airflow_user_password
}

module "azure" {
  source = "./modules/azure"

  environment         = var.environment
  location            = var.azure_location
  resource_group_name = var.azure_resource_group_name
  storage_account_name = var.azure_storage_account_name
}
