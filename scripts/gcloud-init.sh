#!/usr/bin/env bash

# One-time GCP project setup via gcloud CLI
# Run this BEFORE terraform init/apply
#
# Prerequisites:
# - gcloud CLI installed and authenticated (gcloud auth login)
# - Project 'job-market-lakehouse' already created

set -euo pipefail

PROJECT_ID='job-market-lakehouse'

echo '=== Setting active project ==='
gcloud config set project "$PROJECT_ID"

echo '=== Enabling required APIs ==='
gcloud services enable storage.googleapis.com
gcloud services enable iam.googleapis.com
gcloud services enable cloudresourcemanager.googleapis.com

echo '=== Linking billing (skip if already linked) ==='
BILLING_ACCOUNT=$(gcloud billing accounts list --format="value(name)" --limit=2 2>/dev/null || true)

if [ -n "$BILLING_ACCOUNT" ]; then
  gcloud billing projects link "$PROJECT_ID" --billing-account="$BILLING_ACCOUNT" 2>/dev/null || echo "Billing already linked or requires manual setup"
else
  echo "WARNING: No billing account found. Link one manully"
  echo "  gcloud billing projects link $PROJECT_ID --billing-account=YOUR_BILLING_ID"
fi

echo "=== GCP project ready ==="
echo "Next steps:"
echo "  1. cd infra"
echo "  2. cp terraform.tfvars.example terraform.tfvars"
echo "  3. terraform init"
echo "  4. terraform plan"
echo "  5. terraform apply"
echo ""
echo "After terraform apply:"
echo "  - Copy infra/sa-key.json to a secure location"
echo "  - Set GOOGLE_APPLICATION_CREDENTIALS in .env.secrets"
echo "  - Set GCS_BUCKET_NAME in .env.secrets"
