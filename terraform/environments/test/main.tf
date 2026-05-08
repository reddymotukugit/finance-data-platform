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

  # backend "azurerm" {
  #   resource_group_name  = "rg-terraform-state"
  #   storage_account_name = "stterraformstate001"
  #   container_name       = "tfstate"
  #   key                  = "finance-platform/test/terraform.tfstate"
  # }
}

provider "snowflake" {
  account  = var.snowflake_account
  username = var.snowflake_username
  password = var.snowflake_password
  role     = "ACCOUNTADMIN"
}

provider "azurerm" {
  features {}
  subscription_id = var.azure_subscription_id
}

module "snowflake" {
  source = "../../modules/snowflake"

  environment           = "test"
  snowflake_account     = var.snowflake_account
  dbt_user_password     = var.dbt_user_password
  airflow_user_password = var.airflow_user_password

  # Test: small warehouse, moderate auto-suspend for CI runs
  warehouse_size   = "SMALL"
  auto_suspend_sec = 120
}

module "azure" {
  source = "../../modules/azure"

  environment          = "test"
  location             = var.azure_location
  resource_group_name  = "rg-finance-data-platform-test"
  storage_account_name = var.azure_storage_account_name
  replication_type     = var.replication_type  # LRS — cost-effective for test
}
