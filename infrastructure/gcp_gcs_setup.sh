#!/bin/bash
#
# GCS Bucket Setup for Telco Network Performance Demo
# Creates a GCS bucket for SNMP data ingestion with Pub/Sub notifications
# for low-latency Auto Loader file notification mode
#

set -e

# Configuration - Update these values
PROJECT_ID="${GCP_PROJECT_ID:-<YOUR_GCP_PROJECT_ID>}"  # Set GCP_PROJECT_ID env var or update this
REGION="us-central1"
BUCKET_NAME="${PROJECT_ID}-telco-snmp"
VM_NAME="telco-sftp-server"
ZONE="us-central1-a"

# Pub/Sub configuration for file notifications (OPTIONAL)
# Set ENABLE_PUBSUB=false to skip Pub/Sub setup (use managed file events instead)
ENABLE_PUBSUB="${ENABLE_PUBSUB:-false}"
PUBSUB_TOPIC="databricks-auto-ingest-${BUCKET_NAME}"
PUBSUB_SUBSCRIPTION="${PUBSUB_TOPIC}-sub"

echo "=========================================="
echo "GCS Bucket Setup for Telco Demo"
echo "=========================================="
echo "Project ID: ${PROJECT_ID}"
echo "Bucket Name: ${BUCKET_NAME}"
echo "Region: ${REGION}"
echo "Pub/Sub Setup: ${ENABLE_PUBSUB}"
if [ "$ENABLE_PUBSUB" = "true" ]; then
  echo "Pub/Sub Topic: ${PUBSUB_TOPIC}"
fi
echo "=========================================="

# Set project
echo "Setting project..."
gcloud config set project ${PROJECT_ID}

# Enable required APIs
echo "Enabling required APIs..."
gcloud services enable storage.googleapis.com
if [ "$ENABLE_PUBSUB" = "true" ]; then
  gcloud services enable pubsub.googleapis.com
fi

# Create GCS bucket
echo "Creating GCS bucket: gs://${BUCKET_NAME}..."
if gsutil ls -b gs://${BUCKET_NAME} 2>/dev/null; then
    echo "Bucket already exists, skipping creation"
else
    gsutil mb -l ${REGION} -p ${PROJECT_ID} gs://${BUCKET_NAME}/
    echo "[OK] Bucket created"
fi

# Create folder structure
echo "Creating folder structure..."
echo "" | gsutil cp - gs://${BUCKET_NAME}/snmp/.keep
echo "[OK] Folder structure created"

# Set lifecycle policy to auto-delete old files (optional, for demo cleanup)
echo "Setting lifecycle policy (delete files older than 7 days)..."
cat > /tmp/lifecycle.json << EOF
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "Delete"},
        "condition": {"age": 7}
      }
    ]
  }
}
EOF
gsutil lifecycle set /tmp/lifecycle.json gs://${BUCKET_NAME}/
rm /tmp/lifecycle.json
echo "[OK] Lifecycle policy set"

# Get the VM's service account
echo "Getting VM service account..."
VM_SA=$(gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --format='get(serviceAccounts[0].email)' 2>/dev/null || echo "")

if [ -z "${VM_SA}" ]; then
    echo "Warning: Could not find VM service account. Make sure the VM is created first."
    echo "You can manually grant access with:"
    echo "  gsutil iam ch serviceAccount:SERVICE_ACCOUNT@PROJECT.iam.gserviceaccount.com:objectCreator gs://${BUCKET_NAME}"
else
    echo "Granting write access to VM service account: ${VM_SA}..."
    gsutil iam ch serviceAccount:${VM_SA}:objectCreator gs://${BUCKET_NAME}/
    gsutil iam ch serviceAccount:${VM_SA}:objectViewer gs://${BUCKET_NAME}/
    echo "[OK] IAM permissions granted"
fi

if [ "$ENABLE_PUBSUB" = "true" ]; then
  echo ""
  echo "=========================================="
  echo "Setting up Pub/Sub for File Notifications"
  echo "=========================================="

  # Create Pub/Sub topic
  echo "Creating Pub/Sub topic: ${PUBSUB_TOPIC}..."
  if gcloud pubsub topics describe ${PUBSUB_TOPIC} --project=${PROJECT_ID} 2>/dev/null; then
      echo "Topic already exists, skipping creation"
  else
      gcloud pubsub topics create ${PUBSUB_TOPIC} --project=${PROJECT_ID}
      echo "[OK] Pub/Sub topic created"
  fi

  # Create Pub/Sub subscription for Auto Loader
  echo "Creating Pub/Sub subscription: ${PUBSUB_SUBSCRIPTION}..."
  if gcloud pubsub subscriptions describe ${PUBSUB_SUBSCRIPTION} --project=${PROJECT_ID} 2>/dev/null; then
      echo "Subscription already exists, skipping creation"
  else
      gcloud pubsub subscriptions create ${PUBSUB_SUBSCRIPTION} \
          --topic=${PUBSUB_TOPIC} \
          --ack-deadline=60 \
          --message-retention-duration=7d \
          --project=${PROJECT_ID}
      echo "[OK] Pub/Sub subscription created"
  fi

  # Create GCS notification to Pub/Sub topic
  echo "Creating GCS notification to Pub/Sub..."
  # Check if notification already exists
  EXISTING_NOTIFICATION=$(gsutil notification list gs://${BUCKET_NAME} 2>/dev/null | grep "${PUBSUB_TOPIC}" || echo "")
  if [ -n "${EXISTING_NOTIFICATION}" ]; then
      echo "Notification already exists, skipping creation"
  else
      gsutil notification create \
          -t projects/${PROJECT_ID}/topics/${PUBSUB_TOPIC} \
          -f json \
          -e OBJECT_FINALIZE \
          -p snmp/ \
          gs://${BUCKET_NAME}
      echo "[OK] GCS notification created"
  fi

  # Get the GCS service account for the project
  echo ""
  echo "Getting GCS service account..."
  GCS_SA=$(gsutil kms serviceaccount -p ${PROJECT_ID})
  echo "GCS Service Account: ${GCS_SA}"

  # Grant GCS service account permission to publish to Pub/Sub topic
  echo "Granting GCS service account permission to publish to Pub/Sub..."
  gcloud pubsub topics add-iam-policy-binding ${PUBSUB_TOPIC} \
      --member="serviceAccount:${GCS_SA}" \
      --role="roles/pubsub.publisher" \
      --project=${PROJECT_ID} 2>/dev/null || echo "Policy binding may already exist"
  echo "[OK] Pub/Sub permissions configured"
else
  echo ""
  echo "=========================================="
  echo "Pub/Sub Setup Skipped"
  echo "=========================================="
  echo "Use Databricks managed file events instead:"
  echo "  .option(\"cloudFiles.useManagedFileEvents\", \"true\")"
  echo ""
  PUBSUB_TOPIC="N/A (using managed file events)"
  PUBSUB_SUBSCRIPTION="N/A (using managed file events)"
  GCS_SA="N/A"
fi

# Save configuration
CONFIG_FILE="gcs_config.env"
echo "Saving configuration to ${CONFIG_FILE}..."
cat > ${CONFIG_FILE} << EOF
# GCS Configuration for Telco Demo
# Generated on $(date)
GCS_BUCKET=${BUCKET_NAME}
GCS_SNMP_PATH=gs://${BUCKET_NAME}/snmp/
GCS_PROJECT_ID=${PROJECT_ID}
GCS_REGION=${REGION}
PUBSUB_TOPIC=${PUBSUB_TOPIC}
PUBSUB_SUBSCRIPTION=${PUBSUB_SUBSCRIPTION}
GCS_SERVICE_ACCOUNT=${GCS_SA}
EOF

echo ""
echo "=========================================="
echo "GCS Setup Complete!"
echo "=========================================="
echo ""
echo "Bucket URL: gs://${BUCKET_NAME}/snmp/"
if [ "$ENABLE_PUBSUB" = "true" ]; then
  echo "Pub/Sub Topic: projects/${PROJECT_ID}/topics/${PUBSUB_TOPIC}"
  echo "Pub/Sub Subscription: ${PUBSUB_SUBSCRIPTION}"
else
  echo "Pub/Sub: Disabled (using managed file events)"
fi
echo ""
echo "=========================================="
echo "Next Steps for Databricks Configuration:"
echo "=========================================="
echo ""
echo "1. Run the Databricks notebook: databricks/00_1_setup_gcs_connection.py"
echo "   This will create:"
echo "   - Storage credential for GCS"
echo "   - External location for the bucket"
echo ""
echo "2. Or manually in Databricks SQL:"
echo ""
echo "   -- Create storage credential (requires service account key)"
echo "   CREATE STORAGE CREDENTIAL telco_gcs_credential"
echo "   WITH (GCP_SERVICE_ACCOUNT_KEY = '<base64-encoded-key>');"
echo ""
echo "   -- Create external location"
echo "   CREATE EXTERNAL LOCATION telco_snmp_gcs"
echo "   URL 'gs://${BUCKET_NAME}'"
echo "   WITH (STORAGE CREDENTIAL telco_gcs_credential);"
echo ""
if [ "$ENABLE_PUBSUB" = "true" ]; then
  echo "3. Update the SNMP ingestion notebook with unmanaged file notifications:"
  echo "   .option(\"cloudFiles.useNotifications\", \"true\")"
  echo "   .option(\"cloudFiles.subscriptionPath\", \"projects/${PROJECT_ID}/subscriptions/${PUBSUB_SUBSCRIPTION}\")"
else
  echo "3. Update the SNMP ingestion notebook with managed file events (recommended):"
  echo "   .option(\"cloudFiles.useManagedFileEvents\", \"true\")"
fi
echo ""
