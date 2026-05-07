#!/bin/bash
################################################################################
# Startup Script for Telco Data Generator on GCE
# This script automatically installs dependencies and starts the data generator
################################################################################

set -e

# Log all output
exec > >(tee /var/log/telco-generator-startup.log)
exec 2>&1

echo "=========================================="
echo "Telco Generator Startup Script"
echo "Starting at: $(date)"
echo "=========================================="

# Configuration
PROJECT_ID="gcp-sandbox-field-eng"
PUBSUB_TOPIC="syslog-events"
GCS_BUCKET="gs://reynolds-telco-bucket/snmp/"
FILES_PER_MINUTE="5000"
SCRIPT_DIR="/opt/telco-generator"

# Install system dependencies
echo "[1/6] Installing system dependencies..."
apt-get update
apt-get install -y python3-pip git wget curl

# Install Python dependencies
echo "[2/6] Installing Python dependencies..."
pip3 install --upgrade pip
pip3 install google-cloud-pubsub google-cloud-storage

# Create working directory
echo "[3/6] Creating working directory..."
mkdir -p $SCRIPT_DIR
cd $SCRIPT_DIR

# Download the generator script from GCS
# Note: Update this to your actual GCS bucket where you store the script
echo "[4/6] Downloading generator script..."
# Option 1: Download from GCS (recommended for production)
if gsutil ls gs://reynolds-telco-bucket/scripts/telco_data_generator.py 2>/dev/null; then
    gsutil cp gs://reynolds-telco-bucket/scripts/telco_data_generator.py $SCRIPT_DIR/
    echo "Downloaded generator from GCS"
else
    echo "Warning: Could not find script in GCS, attempting GitHub clone..."
    # Option 2: Clone from GitHub (if repo is accessible)
    if [ ! -f "$SCRIPT_DIR/telco_data_generator.py" ]; then
        echo "ERROR: Generator script not found. Please upload to GCS or make GitHub accessible."
        echo "Expected location: gs://reynolds-telco-bucket/scripts/telco_data_generator.py"
        exit 1
    fi
fi

# Create systemd service
echo "[5/6] Creating systemd service..."
cat > /etc/systemd/system/telco-generator.service << EOF
[Unit]
Description=Telco Data Generator
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/telco-generator
ExecStart=/usr/bin/python3 /opt/telco-generator/telco_data_generator.py \
  --syslog-pubsub-topic=syslog-events \
  --project-id=gcp-sandbox-field-eng \
  --snmp-gcs-bucket=gs://reynolds-telco-bucket/snmp/ \
  --files-per-minute=${FILES_PER_MINUTE} \
  --log-level=INFO
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# Enable and start the service
echo "[6/6] Starting telco-generator service..."
systemctl daemon-reload
systemctl enable telco-generator
systemctl start telco-generator

# Wait a few seconds and check status
sleep 5
systemctl status telco-generator --no-pager

echo ""
echo "=========================================="
echo "Startup Complete!"
echo "=========================================="
echo "Service Status:"
systemctl is-active telco-generator && echo "  Status: ACTIVE" || echo "  Status: FAILED"
echo ""
echo "To check logs:"
echo "  journalctl -u telco-generator -f"
echo ""
echo "To check service status:"
echo "  systemctl status telco-generator"
echo "=========================================="
