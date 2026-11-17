terraform {
  required_version = ">= 1.5.0"
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = "~> 4.0" }
  }
}

provider "azurerm" { features {} }

variable "location" {
  type    = string
  default = "eastus"
}
variable "name_prefix" {
  type        = string
  description = "Globally unique lowercase prefix, 3-12 chars."
}

resource "azurerm_resource_group" "demo" {
  name     = "rg-${var.name_prefix}-lakehouse-demo"
  location = var.location
}

resource "azurerm_storage_account" "lake" {
  name                     = "${var.name_prefix}lake${substr(sha1(var.name_prefix), 0, 5)}"
  resource_group_name      = azurerm_resource_group.demo.name
  location                 = azurerm_resource_group.demo.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  min_tls_version          = "TLS1_2"
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = false
  blob_properties { versioning_enabled = true }
}

resource "azurerm_storage_container" "landing" {
  name                  = "landing"
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

output "storage_account_name" {
  value = azurerm_storage_account.lake.name
}
