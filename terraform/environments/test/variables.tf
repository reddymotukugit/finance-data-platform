variable "snowflake_account"        { type = string }
variable "snowflake_username"       { type = string }
variable "snowflake_password"       { type = string; sensitive = true }
variable "dbt_user_password"        { type = string; sensitive = true }
variable "airflow_user_password"    { type = string; sensitive = true }
variable "azure_subscription_id"   { type = string }
variable "azure_location"          { type = string; default = "eastus" }
variable "azure_storage_account_name" { type = string }
variable "replication_type"          { type = string; default = "LRS" }  # LRS for test — no geo-redundancy needed
