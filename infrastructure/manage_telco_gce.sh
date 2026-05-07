#!/bin/bash

################################################################################
# Manage Telco Data Generator GCE Instance
# This script provides management commands for the telco generator instance
################################################################################

set -e

# Configuration
PROJECT_ID="${GCP_PROJECT_ID:-gcp-sandbox-field-eng}"
INSTANCE_NAME="telco-generator"
ZONE="us-central1-a"
GCS_STAGING_BUCKET="gs://reynolds-telco-bucket"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Helper function to print colored messages
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if instance exists
check_instance() {
    if gcloud compute instances describe $INSTANCE_NAME --zone=$ZONE --project=$PROJECT_ID &> /dev/null; then
        return 0
    else
        return 1
    fi
}

# Show usage
usage() {
    echo "=========================================="
    echo "Telco Generator GCE Management Script"
    echo "=========================================="
    echo "Usage: $0 <command>"
    echo ""
    echo "Commands:"
    echo "  status      - Show instance and service status"
    echo "  logs        - View generator service logs (tail)"
    echo "  logs-full   - View all generator logs"
    echo "  startup     - View startup script output"
    echo "  restart     - Restart the generator service"
    echo "  stop        - Stop the generator service"
    echo "  start       - Start the generator service"
    echo "  update      - Update generator script from GCS and restart"
    echo "  ssh         - SSH into the instance (using IAP tunnel)"
    echo "  delete      - Delete the instance"
    echo "  pubsub      - Check Pub/Sub topic metrics"
    echo "  gcs         - List recent SNMP files in GCS"
    echo ""
    echo "Environment Variables:"
    echo "  GCP_PROJECT_ID - Override project ID (default: gcp-sandbox-field-eng)"
    echo "=========================================="
}

# Show status
cmd_status() {
    print_info "Checking instance status..."
    
    if ! check_instance; then
        print_error "Instance $INSTANCE_NAME does not exist in zone $ZONE"
        exit 1
    fi
    
    echo ""
    echo "=========================================="
    echo "Instance Status"
    echo "=========================================="
    gcloud compute instances describe $INSTANCE_NAME \
        --zone=$ZONE \
        --project=$PROJECT_ID \
        --format="table(name,status,machineType.basename(),networkInterfaces[0].networkIP,networkInterfaces[0].accessConfigs[0].natIP)"
    
    echo ""
    echo "=========================================="
    echo "Service Status"
    echo "=========================================="
    gcloud compute ssh $INSTANCE_NAME \
        --zone=$ZONE \
        --project=$PROJECT_ID \
        --tunnel-through-iap \
        --command="systemctl status telco-generator --no-pager" || true
    
    echo ""
    print_info "To see live logs: $0 logs"
}

# View logs
cmd_logs() {
    print_info "Viewing generator service logs (Ctrl+C to exit)..."
    
    if ! check_instance; then
        print_error "Instance $INSTANCE_NAME does not exist"
        exit 1
    fi
    
    gcloud compute ssh $INSTANCE_NAME \
        --zone=$ZONE \
        --project=$PROJECT_ID \
        --tunnel-through-iap \
        --command="journalctl -u telco-generator -f -n 50"
}

# View full logs
cmd_logs_full() {
    print_info "Viewing all generator service logs..."
    
    if ! check_instance; then
        print_error "Instance $INSTANCE_NAME does not exist"
        exit 1
    fi
    
    gcloud compute ssh $INSTANCE_NAME \
        --zone=$ZONE \
        --project=$PROJECT_ID \
        --tunnel-through-iap \
        --command="journalctl -u telco-generator --no-pager"
}

# View startup logs
cmd_startup() {
    print_info "Viewing startup script output..."
    
    if ! check_instance; then
        print_error "Instance $INSTANCE_NAME does not exist"
        exit 1
    fi
    
    gcloud compute instances get-serial-port-output $INSTANCE_NAME \
        --zone=$ZONE \
        --project=$PROJECT_ID
}

# Restart service
cmd_restart() {
    print_info "Restarting generator service..."
    
    if ! check_instance; then
        print_error "Instance $INSTANCE_NAME does not exist"
        exit 1
    fi
    
    gcloud compute ssh $INSTANCE_NAME \
        --zone=$ZONE \
        --project=$PROJECT_ID \
        --tunnel-through-iap \
        --command="sudo systemctl restart telco-generator"
    
    sleep 3
    
    print_info "Service restarted. Checking status..."
    gcloud compute ssh $INSTANCE_NAME \
        --zone=$ZONE \
        --project=$PROJECT_ID \
        --tunnel-through-iap \
        --command="systemctl status telco-generator --no-pager"
}

# Stop service
cmd_stop() {
    print_info "Stopping generator service..."
    
    if ! check_instance; then
        print_error "Instance $INSTANCE_NAME does not exist"
        exit 1
    fi
    
    gcloud compute ssh $INSTANCE_NAME \
        --zone=$ZONE \
        --project=$PROJECT_ID \
        --tunnel-through-iap \
        --command="sudo systemctl stop telco-generator"
    
    print_info "Service stopped."
}

# Start service
cmd_start() {
    print_info "Starting generator service..."
    
    if ! check_instance; then
        print_error "Instance $INSTANCE_NAME does not exist"
        exit 1
    fi
    
    gcloud compute ssh $INSTANCE_NAME \
        --zone=$ZONE \
        --project=$PROJECT_ID \
        --tunnel-through-iap \
        --command="sudo systemctl start telco-generator"
    
    sleep 3
    
    print_info "Service started. Checking status..."
    gcloud compute ssh $INSTANCE_NAME \
        --zone=$ZONE \
        --project=$PROJECT_ID \
        --tunnel-through-iap \
        --command="systemctl status telco-generator --no-pager"
}

# Update generator script
cmd_update() {
    print_info "Updating generator script from GCS..."
    
    if ! check_instance; then
        print_error "Instance $INSTANCE_NAME does not exist"
        exit 1
    fi
    
    # Check if script exists in GCS
    if ! gsutil ls ${GCS_STAGING_BUCKET}/scripts/telco_data_generator.py &> /dev/null; then
        print_error "Generator script not found in ${GCS_STAGING_BUCKET}/scripts/"
        print_info "Please upload the script first:"
        echo "  gsutil cp ../scripts/telco_data_generator.py ${GCS_STAGING_BUCKET}/scripts/"
        exit 1
    fi
    
    gcloud compute ssh $INSTANCE_NAME \
        --zone=$ZONE \
        --project=$PROJECT_ID \
        --tunnel-through-iap \
        --command="sudo gsutil cp ${GCS_STAGING_BUCKET}/scripts/telco_data_generator.py /opt/telco-generator/"
    
    print_info "Script updated. Restarting service..."
    cmd_restart
}

# SSH into instance
cmd_ssh() {
    print_info "Opening SSH session (using IAP tunnel)..."
    
    if ! check_instance; then
        print_error "Instance $INSTANCE_NAME does not exist"
        exit 1
    fi
    
    gcloud compute ssh $INSTANCE_NAME \
        --zone=$ZONE \
        --project=$PROJECT_ID \
        --tunnel-through-iap
}

# Delete instance
cmd_delete() {
    print_warning "This will permanently delete the instance: $INSTANCE_NAME"
    read -p "Are you sure? (y/N): " -n 1 -r
    echo
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_info "Deletion cancelled."
        exit 0
    fi
    
    if ! check_instance; then
        print_error "Instance $INSTANCE_NAME does not exist"
        exit 1
    fi
    
    print_info "Deleting instance..."
    gcloud compute instances delete $INSTANCE_NAME \
        --zone=$ZONE \
        --project=$PROJECT_ID \
        --quiet
    
    print_info "Instance deleted."
}

# Check Pub/Sub metrics
cmd_pubsub() {
    print_info "Checking Pub/Sub topic metrics..."
    
    echo ""
    echo "=========================================="
    echo "Topic: syslog-events"
    echo "=========================================="
    gcloud pubsub topics describe syslog-events --project=$PROJECT_ID
    
    echo ""
    echo "=========================================="
    echo "Subscription: syslog-events-sub"
    echo "=========================================="
    gcloud pubsub subscriptions describe syslog-events-sub --project=$PROJECT_ID
    
    echo ""
    print_info "To view messages in Cloud Console:"
    echo "  https://console.cloud.google.com/cloudpubsub/topic/detail/syslog-events?project=$PROJECT_ID"
}

# List GCS files
cmd_gcs() {
    print_info "Listing recent SNMP files in GCS..."
    
    echo ""
    echo "=========================================="
    echo "Recent SNMP Files (last 20)"
    echo "=========================================="
    gsutil ls -lh ${GCS_STAGING_BUCKET}/snmp/ | tail -20
    
    echo ""
    print_info "Total files:"
    gsutil ls ${GCS_STAGING_BUCKET}/snmp/*.json 2>/dev/null | wc -l || echo "0"
}

# Main command dispatcher
case "${1:-}" in
    status)
        cmd_status
        ;;
    logs)
        cmd_logs
        ;;
    logs-full)
        cmd_logs_full
        ;;
    startup)
        cmd_startup
        ;;
    restart)
        cmd_restart
        ;;
    stop)
        cmd_stop
        ;;
    start)
        cmd_start
        ;;
    update)
        cmd_update
        ;;
    ssh)
        cmd_ssh
        ;;
    delete)
        cmd_delete
        ;;
    pubsub)
        cmd_pubsub
        ;;
    gcs)
        cmd_gcs
        ;;
    *)
        usage
        exit 1
        ;;
esac
