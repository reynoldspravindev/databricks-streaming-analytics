# Quick Fix for GCS 403 Forbidden Error

## Problem
The telco-generator service is getting a 403 error when trying to write SNMP data to GCS:
```
"Provided scope(s) are not authorized"
Request failed with status code 403
```

## Root Cause
The VM was created **without the correct OAuth access scopes** for Google Cloud Storage. 

There are TWO requirements for GCS access:
1. **IAM Permissions** - The service account needs Storage Object Admin role (grants WHAT it can do)
2. **OAuth Scopes** - The VM needs storage scopes enabled (grants WHICH APIs it can access)

The error "Provided scope(s) are not authorized" specifically means the VM is missing OAuth scopes, NOT IAM permissions.

## Solution

### Option 1: Automated Fix (Recommended)

Use the automated fix script that will update the VM's OAuth scopes:

```bash
cd infrastructure

# Run the fix script
./fix_vm_scopes.sh
```

The script will:
1. Check current VM scopes
2. Stop the VM temporarily
3. Update scopes to include cloud-platform (which includes storage)
4. Restart the VM
5. Restart the telco-generator service
6. Verify the service status

**Note:** This will cause a brief downtime (about 1-2 minutes) while the VM stops and restarts.

### Option 2: Manual Fix

If you prefer to do it manually:

```bash
cd infrastructure
source sftp_config.env

# Stop the VM
gcloud compute instances stop ${VM_NAME} --zone=${GCP_ZONE}

# Update the scopes to include full cloud-platform access
gcloud compute instances set-service-account ${VM_NAME} \
  --zone=${GCP_ZONE} \
  --scopes=https://www.googleapis.com/auth/cloud-platform

# Start the VM
gcloud compute instances start ${VM_NAME} --zone=${GCP_ZONE}

# Wait for VM to be ready (about 30 seconds)
sleep 30

# Restart the service
gcloud compute ssh ${VM_NAME} --zone=${GCP_ZONE} \
  --command="sudo systemctl restart telco-generator"
```

## Verification

After applying the fix, verify the service is working:

```bash
# Check service status
gcloud compute ssh ${VM_NAME} --zone=${GCP_ZONE} \
  --command="sudo systemctl status telco-generator"

# View real-time logs (should show successful uploads, not 403 errors)
gcloud compute ssh ${VM_NAME} --zone=${GCP_ZONE} \
  --command="sudo journalctl -u telco-generator -f"

# Check GCS bucket for new files
gsutil ls gs://${GCS_BUCKET}/snmp/ | tail -10
```

## Expected Output (Success)

Before fix (logs show 403 errors):
```
Dec 19 02:10:40 telco-sftp-server telco-generator[3553]: "message": "Provided scope(s) are not authorized"
Dec 19 02:10:40 telco-sftp-server telco-generator[3553]: Request failed with status code 403
```

After fix (logs show successful uploads):
```
Dec 19 02:15:23 telco-sftp-server telco-generator[3553]: Uploaded to GCS: gs://bucket/snmp/snmp_20251219_021523_001234.json (75 entries)
Dec 19 02:15:24 telco-sftp-server telco-generator[3553]: Generated 200 files (syslog: 100, snmp: 100)
```

## Prevention

For future deployments, the updated `gcp_gcs_setup.sh` script now automatically handles IAM permissions, so you shouldn't encounter this error again.

The script runs these checks in sequence:
1. Creates GCS bucket
2. Detects VM service account
3. Grants Storage Object Admin role
4. Restarts service if running
5. Verifies status

## Additional Notes

- The fix is idempotent - safe to run multiple times
- Permissions may take a few seconds to propagate
- The service restart ensures the new permissions are picked up
- Both project-level and bucket-level permissions are granted for redundancy

