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
  #   key                  = "finance-platform/prod/terraform.tfstate"
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

  environment           = "prod"
  snowflake_account     = var.snowflake_account
  dbt_user_password     = var.dbt_user_password
  airflow_user_password = var.airflow_user_password

  # Prod: medium warehouse, longer auto-suspend to handle peak loads
  warehouse_size   = "MEDIUM"
  auto_suspend_sec = 300
}

module "azure" {
  source = "../../modules/azure"

  environment          = "prod"
  location             = var.azure_location
  resource_group_name  = "rg-finance-data-platform-prod"
  storage_account_name = var.azure_storage_account_name

  # Prod: geo-redundant storage for durability
  replication_type = "GRS"
}
