#!/bin/bash

################################################################################
# Create Service Accounts for Cloud Run Generators
################################################################################

set -e

PROJECT_ID="${GCP_PROJECT_ID:-gcp-sandbox-field-eng}"

echo "=========================================="
echo "Service Account Setup"
echo "=========================================="
echo "Project: $PROJECT_ID"
echo "=========================================="

gcloud config set project $PROJECT_ID

# Create telco generator service account
echo "Creating telco-generator service account..."
if gcloud iam service-accounts describe telco-generator@${PROJECT_ID}.iam.gserviceaccount.com --project=$PROJECT_ID &> /dev/null; then
    echo "Service account telco-generator already exists."
else
    gcloud iam service-accounts create telco-generator \
        --display-name="Telco Data Generator" \
        --description="Service account for telco data generator Cloud Run service" \
        --project=$PROJECT_ID
fi

# Grant permissions to telco generator
echo "Granting permissions to telco-generator..."
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:telco-generator@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/pubsub.publisher" \
    --condition=None

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:telco-generator@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/storage.objectAdmin" \
    --condition=None

# Create retail generator service account
echo "Creating retail-generator service account..."
if gcloud iam service-accounts describe retail-generator@${PROJECT_ID}.iam.gserviceaccount.com --project=$PROJECT_ID &> /dev/null; then
    echo "Service account retail-generator already exists."
else
    gcloud iam service-accounts create retail-generator \
        --display-name="Retail Data Generator" \
        --description="Service account for retail data generator Cloud Run service" \
        --project=$PROJECT_ID
fi

# Grant permissions to retail generator
echo "Granting permissions to retail-generator..."
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:retail-generator@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/storage.objectAdmin" \
    --condition=None

echo ""
echo "=========================================="
echo "Service Accounts Created!"
echo "=========================================="
echo "Telco: telco-generator@${PROJECT_ID}.iam.gserviceaccount.com"
echo "  - roles/pubsub.publisher"
echo "  - roles/storage.objectAdmin"
echo ""
echo "Retail: retail-generator@${PROJECT_ID}.iam.gserviceaccount.com"
echo "  - roles/storage.objectAdmin"
echo "=========================================="
