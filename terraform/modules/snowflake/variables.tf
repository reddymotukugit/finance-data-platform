variable "environment" {
  description = "Deployment environment (dev, test, prod)"
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

variable "warehouse_size" {
  description = "Snowflake warehouse size (X-SMALL, SMALL, MEDIUM, LARGE)"
  type        = string
  default     = "X-SMALL"
}

variable "auto_suspend_sec" {
  description = "Seconds of inactivity before warehouse auto-suspends"
  type        = number
  default     = 60
}
