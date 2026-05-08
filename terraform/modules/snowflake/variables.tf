variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "snowflake_account" {
  description = "Snowflake account identifier"
  type        = string
}

variable "dbt_user_password" {
  description = "Password for FINANCE_DBT_USER"
  type        = string
  sensitive   = true
}

variable "airflow_user_password" {
  description = "Password for FINANCE_AIRFLOW_USER"
  type        = string
  sensitive   = true
}
