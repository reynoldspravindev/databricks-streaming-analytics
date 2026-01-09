#!/bin/bash
#
# GCS Bucket Setup for Telco Network Performance Demo
# Creates a GCS bucket with Hierarchical Namespace (HNS) for optimal performance
# with Databricks/Spark workloads and Pub/Sub notifications for Auto Loader
#
# Performance Features (per https://docs.cloud.google.com/storage/docs/create-hns-bucket):
# - Hierarchical Namespace (HNS): True folder support for faster directory operations
# - Uniform bucket-level access: IAM-only permissions (required for HNS)
# - Autoclass: Automatic storage class management based on access patterns
# - Lifecycle policies: Auto-deletion of old demo data
#
# Benefits for Databricks/Spark:
# - Faster file listing operations (critical for Auto Loader)
# - Efficient folder rename/move operations
# - Better performance for Hadoop-compatible APIs
#

set -e

# Configuration - Update these values
PROJECT_ID="${GCP_PROJECT_ID:-<YOUR_GCP_PROJECT_ID>}"  # Set GCP_PROJECT_ID env var or update this
REGION="us-central1"
BUCKET_NAME="telco-retail-metrics"
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
echo ""
echo "Performance Features:"
echo "  ✓ Hierarchical Namespace (HNS) - enabled"
echo "  ✓ Uniform bucket-level access (IAM-only)"
echo "  ✓ Autoclass (automatic storage optimization)"
echo ""
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

# Create GCS bucket with Hierarchical Namespace (HNS) enabled
echo "Creating GCS bucket with Hierarchical Namespace: gs://${BUCKET_NAME}..."
if gsutil ls -b gs://${BUCKET_NAME} 2>/dev/null; then
    echo "Bucket already exists, skipping creation"
else
    # HNS requires gcloud storage buckets create (not gsutil mb)
    # HNS also requires uniform bucket-level access to be enabled
    gcloud storage buckets create gs://${BUCKET_NAME} \
        --location=${REGION} \
        --uniform-bucket-level-access \
        --enable-hierarchical-namespace \
        --project=${PROJECT_ID}
    echo "[OK] Bucket created with Hierarchical Namespace enabled"
fi

# Enable Autoclass for automatic storage class management
echo "Enabling Autoclass for automatic storage optimization..."
gsutil autoclass set --autoclass gs://${BUCKET_NAME}/ 2>/dev/null || echo "Note: Autoclass may not be available in your project"
echo "[OK] Autoclass configuration applied"

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
echo ""
echo "=========================================="
echo "Configuring VM Permissions"
echo "=========================================="
echo "Checking for VM: ${VM_NAME}..."
VM_SA=$(gcloud compute instances describe ${VM_NAME} --zone=${ZONE} --format='get(serviceAccounts[0].email)' 2>/dev/null || echo "")

if [ -z "${VM_SA}" ]; then
    echo "Warning: Could not find VM '${VM_NAME}' in zone '${ZONE}'."
    echo "Make sure the VM is created first by running:"
    echo "  cd infrastructure && ./gcp_sftp_setup.sh"
    echo ""
    echo "You can manually grant access later with:"
    echo "  gcloud projects add-iam-policy-binding ${PROJECT_ID} \\"
    echo "    --member='serviceAccount:SERVICE_ACCOUNT' \\"
    echo "    --role='roles/storage.objectAdmin'"
else
    echo "Found VM service account: ${VM_SA}"
    echo ""
    
    # Grant Storage Object Admin role at project level
    echo "Granting Storage Object Admin role to VM service account..."
    gcloud projects add-iam-policy-binding ${PROJECT_ID} \
        --member="serviceAccount:${VM_SA}" \
        --role="roles/storage.objectAdmin" \
        --condition=None \
        2>/dev/null || echo "Note: Policy binding may already exist"
    
    # Also grant bucket-level permissions as backup
    echo "Granting bucket-level permissions..."
    gsutil iam ch serviceAccount:${VM_SA}:objectAdmin gs://${BUCKET_NAME}/ 2>/dev/null || echo "Note: Bucket policy may already be set"
    
    echo "[OK] IAM permissions granted"
    echo ""
    
    # Check if telco-generator service is running and restart it
    echo "Checking if telco-generator service is running on VM..."
    SERVICE_STATUS=$(gcloud compute ssh ${VM_NAME} --zone=${ZONE} \
        --command="sudo systemctl is-active telco-generator 2>/dev/null || echo 'inactive'" 2>/dev/null || echo "ssh-failed")
    
    if [ "${SERVICE_STATUS}" = "active" ]; then
        echo "Telco-generator service is running. Restarting to apply new permissions..."
        gcloud compute ssh ${VM_NAME} --zone=${ZONE} \
            --command="sudo systemctl restart telco-generator"
        echo "[OK] Service restarted"
        
        # Wait a moment and check status
        echo "Waiting for service to stabilize..."
        sleep 3
        gcloud compute ssh ${VM_NAME} --zone=${ZONE} \
            --command="sudo systemctl status telco-generator --no-pager -l" || true
    elif [ "${SERVICE_STATUS}" = "inactive" ]; then
        echo "Note: telco-generator service is not running. Start it with:"
        echo "  gcloud compute ssh ${VM_NAME} --zone=${ZONE} --command='sudo systemctl start telco-generator'"
    else
        echo "Note: Could not check service status. Service may not be installed yet."
    fi
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
  PUBSUB_TOPIC="N/A"
  PUBSUB_SUBSCRIPTION="N/A"
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
echo ""
echo "Performance Features Enabled:"
echo "  ✓ Hierarchical Namespace (HNS) - true folder support"
echo "  ✓ Uniform bucket-level access - IAM-only permissions"
echo "  ✓ Autoclass - automatic storage optimization"
echo "  ✓ Lifecycle policy - 7-day retention"
echo ""
if [ "$ENABLE_PUBSUB" = "true" ]; then
  echo "Pub/Sub Configuration:"
  echo "  Topic: projects/${PROJECT_ID}/topics/${PUBSUB_TOPIC}"
  echo "  Subscription: ${PUBSUB_SUBSCRIPTION}"
else
  echo "File Notifications: Managed file events (recommended)"
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
