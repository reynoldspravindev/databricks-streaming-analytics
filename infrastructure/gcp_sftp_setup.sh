#!/bin/bash

################################################################################
# GCP SFTP Server Setup Script
# Creates a GCE VM with networking configuration for Databricks SFTP connectivity
################################################################################

set -e

# Configuration Variables
PROJECT_ID="${GCP_PROJECT_ID:-<YOUR_GCP_PROJECT_ID>}"  # Set GCP_PROJECT_ID env var or update this
REGION="us-central1"
ZONE="us-central1-a"
VM_NAME="telco-sftp-server"
MACHINE_TYPE="e2-highmem-8"
BOOT_DISK_SIZE="200GB"
NETWORK_NAME="databricks-telco-network"
SUBNET_NAME="databricks-telco-subnet"
FIREWALL_RULE_NAME="allow-sftp-databricks"

echo "=========================================="
echo "GCP SFTP Server Setup"
echo "=========================================="
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo "VM Name: $VM_NAME"
echo "=========================================="

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo "Error: gcloud CLI is not installed. Please install it first."
    exit 1
fi

# Set the project
echo "Setting GCP project to $PROJECT_ID..."
gcloud config set project $PROJECT_ID

# Create VPC network if it doesn't exist
echo "Creating VPC network: $NETWORK_NAME..."
if ! gcloud compute networks describe $NETWORK_NAME --project=$PROJECT_ID &> /dev/null; then
    gcloud compute networks create $NETWORK_NAME \
        --subnet-mode=custom \
        --project=$PROJECT_ID
else
    echo "Network $NETWORK_NAME already exists."
fi

# Create subnet if it doesn't exist
echo "Creating subnet: $SUBNET_NAME..."
if ! gcloud compute networks subnets describe $SUBNET_NAME --region=$REGION --project=$PROJECT_ID &> /dev/null; then
    gcloud compute networks subnets create $SUBNET_NAME \
        --network=$NETWORK_NAME \
        --region=$REGION \
        --range=10.128.0.0/20 \
        --project=$PROJECT_ID
else
    echo "Subnet $SUBNET_NAME already exists."
fi

# Create firewall rule for SSH/SFTP (port 22)
echo "Creating firewall rule: $FIREWALL_RULE_NAME..."
if ! gcloud compute firewall-rules describe $FIREWALL_RULE_NAME --project=$PROJECT_ID &> /dev/null; then
    gcloud compute firewall-rules create $FIREWALL_RULE_NAME \
        --network=$NETWORK_NAME \
        --allow=tcp:22 \
        --source-ranges=0.0.0.0/0 \
        --description="Allow SSH/SFTP access from Databricks and management" \
        --project=$PROJECT_ID
    
    echo "NOTE: For production, restrict source-ranges to Databricks workspace IP ranges only."
    echo "You can get Databricks IP ranges from your workspace documentation."
else
    echo "Firewall rule $FIREWALL_RULE_NAME already exists."
fi

# Reserve a static external IP address
echo "Reserving static external IP address..."
STATIC_IP_NAME="${VM_NAME}-ip"
if ! gcloud compute addresses describe $STATIC_IP_NAME --region=$REGION --project=$PROJECT_ID &> /dev/null; then
    gcloud compute addresses create $STATIC_IP_NAME \
        --region=$REGION \
        --project=$PROJECT_ID
else
    echo "Static IP $STATIC_IP_NAME already exists."
fi

# Get the reserved IP address
EXTERNAL_IP=$(gcloud compute addresses describe $STATIC_IP_NAME --region=$REGION --project=$PROJECT_ID --format="get(address)")
echo "Static IP Address: $EXTERNAL_IP"

# Create the VM instance
echo "Creating VM instance: $VM_NAME..."
if ! gcloud compute instances describe $VM_NAME --zone=$ZONE --project=$PROJECT_ID &> /dev/null; then
    gcloud compute instances create $VM_NAME \
        --zone=$ZONE \
        --machine-type=$MACHINE_TYPE \
        --network-interface=subnet=$SUBNET_NAME,address=$EXTERNAL_IP \
        --boot-disk-size=$BOOT_DISK_SIZE \
        --boot-disk-type=pd-standard \
        --image-family=ubuntu-2204-lts \
        --image-project=ubuntu-os-cloud \
        --metadata=enable-oslogin=FALSE \
        --tags=sftp-server \
        --project=$PROJECT_ID
    
    echo "Waiting for VM to be ready (60 seconds)..."
    sleep 60
else
    echo "VM $VM_NAME already exists."
fi

# Output connection details
echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo "VM Name: $VM_NAME"
echo "External IP: $EXTERNAL_IP"
echo "Zone: $ZONE"
echo ""
echo "Next Steps:"
echo "1. Copy and run the configuration script on the VM:"
echo "   gcloud compute scp infrastructure/configure_sftp.sh $VM_NAME:~/ --zone=$ZONE"
echo "   gcloud compute ssh $VM_NAME --zone=$ZONE --command='chmod +x ~/configure_sftp.sh && sudo ~/configure_sftp.sh'"
echo ""
echo "2. Update your Databricks connection with this IP: $EXTERNAL_IP"
echo ""
echo "Save this IP address for later use!"
echo "=========================================="

# Save configuration to file
cat > infrastructure/sftp_config.env <<EOF
# SFTP Server Configuration
# Generated on $(date)

SFTP_HOST=$EXTERNAL_IP
SFTP_PORT=22
SFTP_USER=telco_user
SFTP_BASE_PATH=/sftp/telco
SFTP_SYSLOG_PATH=/sftp/telco/syslog
SFTP_SNMP_PATH=/sftp/telco/snmp
GCP_PROJECT=$PROJECT_ID
GCP_ZONE=$ZONE
VM_NAME=$VM_NAME
EOF

echo "Configuration saved to infrastructure/sftp_config.env"

