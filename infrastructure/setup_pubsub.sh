#!/bin/bash

################################################################################
# Pub/Sub Setup Script for Telco Syslog Events
################################################################################

set -e

# Configuration
PROJECT_ID="${GCP_PROJECT_ID:-gcp-sandbox-field-eng}"
TOPIC_NAME="syslog-events"
SUBSCRIPTION_NAME="syslog-events-sub"

echo "=========================================="
echo "Pub/Sub Setup for Telco Syslog"
echo "=========================================="
echo "Project: $PROJECT_ID"
echo "Topic: $TOPIC_NAME"
echo "=========================================="

# Set project
gcloud config set project $PROJECT_ID

# Enable Pub/Sub API
echo "Enabling Pub/Sub API..."
gcloud services enable pubsub.googleapis.com --project=$PROJECT_ID

# Create Pub/Sub topic
echo "Creating Pub/Sub topic: $TOPIC_NAME..."
if gcloud pubsub topics describe $TOPIC_NAME --project=$PROJECT_ID &> /dev/null; then
    echo "Topic $TOPIC_NAME already exists."
else
    gcloud pubsub topics create $TOPIC_NAME --project=$PROJECT_ID
    echo "Topic $TOPIC_NAME created."
fi

# Create subscription
echo "Creating subscription: $SUBSCRIPTION_NAME..."
if gcloud pubsub subscriptions describe $SUBSCRIPTION_NAME --project=$PROJECT_ID &> /dev/null; then
    echo "Subscription $SUBSCRIPTION_NAME already exists."
else
    gcloud pubsub subscriptions create $SUBSCRIPTION_NAME \
        --topic=$TOPIC_NAME \
        --ack-deadline=60 \
        --message-retention-duration=7d \
        --project=$PROJECT_ID
    echo "Subscription $SUBSCRIPTION_NAME created."
fi

echo ""
echo "=========================================="
echo "Pub/Sub Setup Complete!"
echo "=========================================="
echo "Topic: projects/$PROJECT_ID/topics/$TOPIC_NAME"
echo "Subscription: projects/$PROJECT_ID/subscriptions/$SUBSCRIPTION_NAME"
echo ""
echo "Test publishing:"
echo "  gcloud pubsub topics publish $TOPIC_NAME --message='test' --project=$PROJECT_ID"
echo ""
echo "Test pulling:"
echo "  gcloud pubsub subscriptions pull $SUBSCRIPTION_NAME --limit=5 --project=$PROJECT_ID"
echo "=========================================="
