#!/bin/bash
#
# Fix VM OAuth Scopes for GCS Access
# This script stops the VM, updates its scopes to include storage, and restarts it
#

set -e

# Load configuration
if [ -f "sftp_config.env" ]; then
    source sftp_config.env
else
    echo "Error: sftp_config.env not found. Run gcp_sftp_setup.sh first."
    exit 1
fi

echo "=========================================="
echo "Fixing VM OAuth Scopes"
echo "=========================================="
echo "VM Name: ${VM_NAME}"
echo "Zone: ${GCP_ZONE}"
echo ""

# Check current scopes
echo "Checking current VM scopes..."
CURRENT_SCOPES=$(gcloud compute instances describe ${VM_NAME} \
    --zone=${GCP_ZONE} \
    --format='value(serviceAccounts[0].scopes)' | tr ';' '\n')

echo "Current scopes:"
echo "${CURRENT_SCOPES}"
echo ""

# Check if WRITE storage scope is present (not just read-only)
if echo "${CURRENT_SCOPES}" | grep -q "cloud-platform"; then
    echo "Cloud-platform scope is already configured!"
    echo "VM should have full GCS access. The issue might be something else."
    VM_SA=$(gcloud compute instances describe ${VM_NAME} \
        --zone=${GCP_ZONE} \
        --format='get(serviceAccounts[0].email)')
    echo "Service Account: ${VM_SA}"
    echo ""
    echo "Try restarting the service:"
    echo "  gcloud compute ssh ${VM_NAME} --zone=${GCP_ZONE} --command='sudo systemctl restart telco-generator'"
    exit 0
elif echo "${CURRENT_SCOPES}" | grep -q "devstorage.full_control"; then
    echo "Full storage scope is already configured!"
    echo "VM should have full GCS access. The issue might be something else."
    exit 0
elif echo "${CURRENT_SCOPES}" | grep -q "devstorage.read_only"; then
    echo "WARNING: VM has READ-ONLY storage scope!"
    echo "This is the problem - need READ-WRITE or CLOUD-PLATFORM scope."
    echo ""
fi

echo "Storage scope is missing. Will add it now..."
echo ""

# Ask for confirmation
read -p "This will stop the VM temporarily. Continue? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
fi

# Stop the VM
echo "Stopping VM..."
gcloud compute instances stop ${VM_NAME} --zone=${GCP_ZONE}
echo "[OK] VM stopped"
echo ""

# Update scopes to include storage
echo "Updating VM scopes to include storage..."
gcloud compute instances set-service-account ${VM_NAME} \
    --zone=${GCP_ZONE} \
    --scopes=https://www.googleapis.com/auth/cloud-platform

echo "[OK] Scopes updated"
echo ""

# Start the VM
echo "Starting VM..."
gcloud compute instances start ${VM_NAME} --zone=${GCP_ZONE}
echo "[OK] VM started"
echo ""

# Wait for VM to be ready
echo "Waiting for VM to be ready (30 seconds)..."
sleep 30

# Restart the telco-generator service
echo "Restarting telco-generator service..."
gcloud compute ssh ${VM_NAME} --zone=${GCP_ZONE} \
    --command="sudo systemctl restart telco-generator"
echo "[OK] Service restarted"
echo ""

# Check service status
echo "Checking service status..."
gcloud compute ssh ${VM_NAME} --zone=${GCP_ZONE} \
    --command="sudo systemctl status telco-generator --no-pager -l" || true

echo ""
echo "=========================================="
echo "Fix Complete!"
echo "=========================================="
echo ""
echo "Monitor the logs to verify 403 errors are gone:"
echo "  gcloud compute ssh ${VM_NAME} --zone=${GCP_ZONE} --command='sudo journalctl -u telco-generator -f'"
echo ""

