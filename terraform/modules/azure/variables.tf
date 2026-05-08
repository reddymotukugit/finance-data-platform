variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
}

variable "resource_group_name" {
  description = "Azure resource group name"
  type        = string
}

variable "storage_account_name" {
  description = "Azure storage account name"
  type        = string
}

variable "replication_type" {
  description = "Storage account replication type (LRS for dev/test, GRS for prod)"
  type        = string
  default     = "LRS"
}
