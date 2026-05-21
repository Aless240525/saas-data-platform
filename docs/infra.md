# Infraestructura como Código (Terraform)

## Provisión para Onboarding
Terraform aprovisionaría:
1. **ADLS Gen2:** Rutas físicas en el Data Lake.
2. **Unity Catalog:** Schemas aislados lógicamente (ej. `saas_dev.bronze_hn`).
3. **IAM:** Asignación de grants y roles para los ingenieros.

## Snippet Ilustrativo
```hcl
variable "tenant_id" { type = string }
variable "environment" { type = string }

resource "databricks_schema" "bronze_schema" {
  catalog_name = "saas_${var.environment}"
  name         = "bronze_${var.tenant_id}"
}

resource "azurerm_storage_data_lake_gen2_path" "tenant_dir" {
  path               = "bronze/${var.tenant_id}"
  filesystem_name    = "datalake"
  storage_account_id = data.azurerm_storage_account.main.id
  resource           = "directory"
}
```