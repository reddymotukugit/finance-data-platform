variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

# ── Snowflake ─────────────────────────────────────────────────────────────────
variable "snowflake_account" {
  description = "Snowflake account identifier (e.g. orgname-accountname)"
  type        = string
}

variable "snowflake_username" {
  description = "Snowflake ACCOUNTADMIN username for Terraform"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake ACCOUNTADMIN password for Terraform"
  type        = string
  sensitive   = true
}

variable "dbt_user_password" {
  description = "Password for the FINANCE_DBT_USER service account"
  type        = string
  sensitive   = true
}

variable "airflow_user_password" {
  description = "Password for the FINANCE_AIRFLOW_USER service account"
  type        = string
  sensitive   = true
}

# ── Azure ─────────────────────────────────────────────────────────────────────
variable "azure_subscription_id" {
  description = "Azure subscription ID"
  type        = string
}

variable "azure_location" {
  description = "Azure region for all resources"
  type        = string
  default     = "eastus"
}

variable "azure_resource_group_name" {
  description = "Azure resource group name"
  type        = string
  default     = "rg-finance-data-platform-dev"
}

variable "azure_storage_account_name" {
  description = "Azure storage account name (globally unique, lowercase, no hyphens)"
  type        = string
  default     = "stfinancereddy001"
}
