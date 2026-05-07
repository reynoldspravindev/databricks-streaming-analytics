#!/bin/bash

################################################################################
# Deploy Telco Data Generator to GCE
# This script creates a GCE instance running the telco data generator
################################################################################

set -e

# Configuration
PROJECT_ID="${GCP_PROJECT_ID:-gcp-sandbox-field-eng}"
INSTANCE_NAME="telco-generator"
ZONE="us-central1-a"
MACHINE_TYPE="e2-highmem-8"
SERVICE_ACCOUNT="telco-generator@${PROJECT_ID}.iam.gserviceaccount.com"
STARTUP_SCRIPT="startup_telco_generator.sh"
GENERATOR_SCRIPT="../scripts/telco_data_generator.py"
GCS_STAGING_BUCKET="gs://reynolds-telco-bucket"

echo "=========================================="
echo "Telco Generator GCE Deployment"
echo "=========================================="
echo "Project ID: $PROJECT_ID"
echo "Instance: $INSTANCE_NAME"
echo "Zone: $ZONE"
echo "Machine Type: $MACHINE_TYPE"
echo "Service Account: $SERVICE_ACCOUNT"
echo "=========================================="

# Set project
gcloud config set project $PROJECT_ID

# Enable required APIs
echo ""
echo "[1/5] Enabling required APIs..."
gcloud services enable compute.googleapis.com --project=$PROJECT_ID
gcloud services enable pubsub.googleapis.com --project=$PROJECT_ID
gcloud services enable storage.googleapis.com --project=$PROJECT_ID

# Upload generator script to GCS for the VM to download
echo ""
echo "[2/5] Uploading generator script to GCS..."
if [ -f "$GENERATOR_SCRIPT" ]; then
    gsutil cp "$GENERATOR_SCRIPT" "${GCS_STAGING_BUCKET}/scripts/"
    echo "Uploaded telco_data_generator.py to ${GCS_STAGING_BUCKET}/scripts/"
else
    echo "Warning: Generator script not found at $GENERATOR_SCRIPT"
    echo "Make sure to upload it manually to ${GCS_STAGING_BUCKET}/scripts/"
fi

# Check if service account exists
echo ""
echo "[3/5] Verifying service account..."
if gcloud iam service-accounts describe $SERVICE_ACCOUNT --project=$PROJECT_ID &> /dev/null; then
    echo "Service account $SERVICE_ACCOUNT exists."
else
    echo "ERROR: Service account $SERVICE_ACCOUNT does not exist."
    echo "Please run ./setup_service_accounts.sh first."
    exit 1
fi

# Check if instance already exists
echo ""
echo "[4/5] Checking if instance already exists..."
if gcloud compute instances describe $INSTANCE_NAME --zone=$ZONE --project=$PROJECT_ID &> /dev/null; then
    echo "WARNING: Instance $INSTANCE_NAME already exists in zone $ZONE"
    read -p "Do you want to delete and recreate it? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Deleting existing instance..."
        gcloud compute instances delete $INSTANCE_NAME \
            --zone=$ZONE \
            --project=$PROJECT_ID \
            --quiet
        echo "Instance deleted."
    else
        echo "Deployment cancelled."
        exit 0
    fi
fi

# Create the GCE instance
echo ""
echo "[5/5] Creating GCE instance..."
gcloud compute instances create $INSTANCE_NAME \
    --project=$PROJECT_ID \
    --zone=$ZONE \
    --machine-type=$MACHINE_TYPE \
    --service-account=$SERVICE_ACCOUNT \
    --scopes=cloud-platform \
    --image-family=debian-11 \
    --image-project=debian-cloud \
    --boot-disk-size=200GB \
    --boot-disk-type=pd-standard \
    --metadata-from-file=startup-script=$STARTUP_SCRIPT \
    --tags=telco-generator \
    --labels=app=telco-generator,env=demo

echo ""
echo "=========================================="
echo "Deployment Complete!"
echo "=========================================="
echo "Instance: $INSTANCE_NAME"
echo "Zone: $ZONE"
echo "Service Account: $SERVICE_ACCOUNT"
echo ""
echo "The instance is starting up and will begin generating data automatically."
echo "This may take 2-3 minutes for initial setup."
echo ""
echo "To monitor the startup:"
echo "  gcloud compute instances get-serial-port-output $INSTANCE_NAME --zone=$ZONE --project=$PROJECT_ID"
echo ""
echo "To check if the service is running:"
echo "  gcloud compute ssh $INSTANCE_NAME --zone=$ZONE --project=$PROJECT_ID --command='systemctl status telco-generator'"
echo ""
echo "To view logs:"
echo "  gcloud compute ssh $INSTANCE_NAME --zone=$ZONE --project=$PROJECT_ID --command='journalctl -u telco-generator -f'"
echo ""
echo "To use IAP tunnel (no firewall changes needed):"
echo "  gcloud compute ssh $INSTANCE_NAME --zone=$ZONE --tunnel-through-iap --project=$PROJECT_ID"
echo "=========================================="
