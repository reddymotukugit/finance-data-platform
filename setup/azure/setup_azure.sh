#!/bin/bash
# ============================================================
# FINANCE DATA PLATFORM — AZURE SETUP SCRIPT
# Run this with Azure CLI installed and logged in
# Prerequisites: az login
# Replace all <PLACEHOLDER> values before running
# ============================================================

set -e  # exit on error

# ────────────────────────────────────────
# CONFIGURATION — fill these in
# ────────────────────────────────────────
SUBSCRIPTION_ID="<YOUR_AZURE_SUBSCRIPTION_ID>"
RESOURCE_GROUP="rg-finance-data-platform-dev"
LOCATION="eastus"
STORAGE_ACCOUNT="stfindatalakedev001"   # must be globally unique, lowercase, no hyphens
CONTAINER_NAME="finance"
ADF_NAME="adf-finance-platform-dev"

echo "============================================================"
echo "  Finance Data Platform — Azure Setup"
echo "  Resource Group: $RESOURCE_GROUP"
echo "  Location:       $LOCATION"
echo "============================================================"

# ────────────────────────────────────────
# 1. Set subscription
# ────────────────────────────────────────
echo ""
echo "[1/6] Setting active subscription..."
az account set --subscription "$SUBSCRIPTION_ID"
echo "     Active subscription: $(az account show --query name -o tsv)"

# ────────────────────────────────────────
# 2. Create Resource Group
# ────────────────────────────────────────
echo ""
echo "[2/6] Creating resource group: $RESOURCE_GROUP..."
az group create \
    --name "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --tags project=finance-data-platform environment=dev owner=data-engineering
echo "     Resource group created."

# ────────────────────────────────────────
# 3. Create ADLS Gen2 Storage Account
# ────────────────────────────────────────
echo ""
echo "[3/6] Creating ADLS Gen2 storage account: $STORAGE_ACCOUNT..."
az storage account create \
    --name "$STORAGE_ACCOUNT" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --sku Standard_LRS \
    --kind StorageV2 \
    --hierarchical-namespace true \
    --access-tier Hot \
    --min-tls-version TLS1_2 \
    --tags project=finance-data-platform environment=dev

echo "     Storage account created."

# Get storage account key for next steps
STORAGE_KEY=$(az storage account keys list \
    --account-name "$STORAGE_ACCOUNT" \
    --resource-group "$RESOURCE_GROUP" \
    --query "[0].value" -o tsv)

# ────────────────────────────────────────
# 4. Create Containers and Directories
# ────────────────────────────────────────
echo ""
echo "[4/6] Creating ADLS container and directory structure..."

az storage container create \
    --name "$CONTAINER_NAME" \
    --account-name "$STORAGE_ACCOUNT" \
    --account-key "$STORAGE_KEY"

# Create landing zone directories for each entity
ENTITIES=(
    "landing/stripe/balance_transactions"
    "landing/stripe/charges"
    "landing/stripe/refunds"
    "landing/stripe/disputes"
    "landing/stripe/payouts"
    "landing/stripe/customers"
    "landing/stripe/subscriptions"
    "landing/stripe/invoices"
    "landing/stripe/invoice_line_items"
    "landing/stripe/prices"
    "landing/stripe/products"
    "landing/fx/rates"
    "streaming/stripe/charges"
    "streaming/stripe/disputes"
    "failed"
    "backfill"
)

for dir in "${ENTITIES[@]}"; do
    az storage fs directory create \
        --name "$dir" \
        --file-system "$CONTAINER_NAME" \
        --account-name "$STORAGE_ACCOUNT" \
        --account-key "$STORAGE_KEY" \
        --output none
    echo "     Created: $dir"
done

echo "     Directory structure created."

# ────────────────────────────────────────
# 5. Create Azure Data Factory
# ────────────────────────────────────────
echo ""
echo "[5/6] Creating Azure Data Factory: $ADF_NAME..."

az datafactory create \
    --resource-group "$RESOURCE_GROUP" \
    --factory-name "$ADF_NAME" \
    --location "$LOCATION"

echo "     ADF instance created."

# Enable system-assigned managed identity on ADF
az datafactory update \
    --resource-group "$RESOURCE_GROUP" \
    --factory-name "$ADF_NAME" \
    --identity '{"type": "SystemAssigned"}'

# Get ADF managed identity principal ID
ADF_PRINCIPAL_ID=$(az datafactory show \
    --resource-group "$RESOURCE_GROUP" \
    --factory-name "$ADF_NAME" \
    --query "identity.principalId" -o tsv)

echo "     ADF Managed Identity Principal ID: $ADF_PRINCIPAL_ID"

# Assign Storage Blob Data Contributor role to ADF managed identity
STORAGE_ACCOUNT_ID=$(az storage account show \
    --name "$STORAGE_ACCOUNT" \
    --resource-group "$RESOURCE_GROUP" \
    --query id -o tsv)

az role assignment create \
    --assignee "$ADF_PRINCIPAL_ID" \
    --role "Storage Blob Data Contributor" \
    --scope "$STORAGE_ACCOUNT_ID"

echo "     ADF granted Storage Blob Data Contributor on ADLS."

# ────────────────────────────────────────
# 6. Print summary
# ────────────────────────────────────────
echo ""
echo "============================================================"
echo "  SETUP COMPLETE — Save these values for next steps:"
echo "============================================================"
echo ""
echo "  Resource Group:     $RESOURCE_GROUP"
echo "  Storage Account:    $STORAGE_ACCOUNT"
echo "  Storage Container:  $CONTAINER_NAME"
echo "  ADF Name:           $ADF_NAME"
echo ""
echo "  ADF Managed Identity Principal ID: $ADF_PRINCIPAL_ID"
echo ""
echo "  ADLS URL for Snowflake stage:"
echo "  azure://${STORAGE_ACCOUNT}.blob.core.windows.net/${CONTAINER_NAME}/"
echo ""
echo "  Next: Run Snowflake scripts 01-04, then update .env"
echo "============================================================"
