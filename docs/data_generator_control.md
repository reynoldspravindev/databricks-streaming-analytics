# Data Generator Control Guide

This document provides all the commands needed to control and manage the telco network data generator running on the GCP VM.

## Architecture Overview

The data generator produces two types of telemetry:
- **Syslog (RFC 5424)** → Written to local SFTP directory → Ingested via Auto Loader SFTP
- **SNMP Metrics (JSON)** → Uploaded to GCS bucket → Ingested via Auto Loader GCS

```
┌─────────────────────────────────────────────────────────────┐
│                      GCE VM                                  │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              telco_data_generator.py                 │    │
│  └─────────────────────┬───────────────┬───────────────┘    │
│                        │               │                     │
│              Syslog (TXT)        SNMP (JSON)                │
│                        │               │                     │
│                        ▼               ▼                     │
│              /sftp/telco/syslog   GCS Bucket                │
│                        │           gs://bucket/snmp/         │
└────────────────────────│───────────────│────────────────────┘
                         │               │
                         ▼               ▼
              ┌──────────────────────────────────────────┐
              │           Databricks on GCP               │
              │  Auto Loader (SFTP)   Auto Loader (GCS)  │
              │         │                    │            │
              │         ▼                    ▼            │
              │   bronze.syslog_raw    bronze.snmp_raw   │
              └──────────────────────────────────────────┘
```

## Basic Service Control

### Stop (Pause) the Data Generator

Stops the data generator service. No new files will be created.

```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='sudo systemctl stop telco-generator'
```

### Start (Resume) the Data Generator

Starts the data generator service. Files will begin generating immediately.

```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='sudo systemctl start telco-generator'
```

### Restart the Data Generator

Stops and then starts the service. Useful after configuration changes.

```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='sudo systemctl restart telco-generator'
```

### Check Service Status

View the current status of the data generator service.

```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='sudo systemctl status telco-generator --no-pager'
```

**Expected Output:**
```
● telco-generator.service - Telco Network Performance Data Generator
     Loaded: loaded (/etc/systemd/system/telco-generator.service; enabled)
     Active: active (running) since Mon 2025-12-02 01:15:30 UTC; 5min ago
   Main PID: 12345 (python3)
      Tasks: 2 (limit: 4915)
     Memory: 45.2M
```

---

## Monitoring & Logs

### View Real-Time Logs

Watch logs as they are generated (live tail).

```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='sudo journalctl -u telco-generator -f'
```

Press **Ctrl+C** to exit.

### View Last 50 Log Lines

```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='sudo journalctl -u telco-generator -n 50 --no-pager'
```

### View Logs Since Last Hour

```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='sudo journalctl -u telco-generator --since "1 hour ago" --no-pager'
```

### View Logs for Specific Time Range

```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='sudo journalctl -u telco-generator --since "2025-12-02 10:00:00" --until "2025-12-02 11:00:00" --no-pager'
```

---

## File Management

### Count Generated Files

**Count syslog files:**
```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='ls -1 /sftp/telco/syslog/ | wc -l'
```

**Count SNMP files (GCS):**
```bash
gsutil ls gs://<YOUR_GCS_BUCKET>/snmp/*.json | wc -l
```

**Count total files:**
```bash
# Syslog (local SFTP)
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='echo "Syslog files: $(ls -1 /sftp/telco/syslog/ 2>/dev/null | wc -l)"'

# SNMP (GCS bucket)
echo "SNMP files: $(gsutil ls gs://<YOUR_GCS_BUCKET>/snmp/*.json 2>/dev/null | wc -l)"
```

### View Recent Files

**Last 10 syslog files:**
```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='ls -lht /sftp/telco/syslog/ | head -11'
```

**Last 10 SNMP files (GCS):**
```bash
gsutil ls -l gs://<YOUR_GCS_BUCKET>/snmp/*.json | tail -10
```

### Check Disk/Storage Usage

**Local syslog disk usage:**
```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='du -sh /sftp/telco/syslog'
```

**GCS bucket size (SNMP):**
```bash
gsutil du -sh gs://<YOUR_GCS_BUCKET>/snmp/
```

### Watch Files Being Created in Real-Time

```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='watch -n 2 "ls -1 /sftp/telco/syslog/ | wc -l"'
```

Press **Ctrl+C** to exit.

### Clear Generated Files

**CAUTION**: This permanently deletes all generated files!

> **Note**: Use `find` instead of `rm *.txt` to avoid "Argument list too long" errors when there are many files.

```bash
# Delete all syslog files
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='sudo find /sftp/telco/syslog/ -type f -name "*.txt" -delete'

# Delete all SNMP files (local, if using SFTP mode)
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='sudo find /sftp/telco/snmp/ -type f -name "*.csv" -delete'

# Delete all syslog and SNMP files
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='sudo find /sftp/telco/syslog/ /sftp/telco/snmp/ -type f -delete'

# Delete SNMP files from GCS bucket
gsutil -m rm gs://<YOUR_GCS_BUCKET>/snmp/**
```

---

## Configuration Management

### View Current Configuration

```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='sudo cat /etc/systemd/system/telco-generator.service'
```

### Change File Generation Rate

**Method 1: Direct Edit**

```bash
# SSH into the VM
gcloud compute ssh telco-sftp-server --zone=us-central1-a

# Edit the service file
sudo nano /etc/systemd/system/telco-generator.service

# Find the line with --files-per-minute and change the value
# Default: --files-per-minute 1000
# Examples: 500, 2000, 5000

# Save and exit (Ctrl+X, Y, Enter)

# Reload systemd configuration
sudo systemctl daemon-reload

# Restart the service
sudo systemctl restart telco-generator

# Exit the VM
exit
```

**Method 2: Automated (Replace 1000 with desired rate)**

```bash
# Stop the service
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='sudo systemctl stop telco-generator'

# Update the rate (example: change to 500 files/min)
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command="sudo sed -i 's/--files-per-minute 1000/--files-per-minute 500/' /etc/systemd/system/telco-generator.service"

# Reload and restart
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='sudo systemctl daemon-reload && sudo systemctl start telco-generator'
```

### Enable/Disable Auto-Start on Boot

**Enable auto-start (default):**
```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='sudo systemctl enable telco-generator'
```

**Disable auto-start:**
```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='sudo systemctl disable telco-generator'
```

**Check if enabled:**
```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='sudo systemctl is-enabled telco-generator'
```

---

## Update the Data Generator Script

If you've made changes to the Python script and need to redeploy:

```bash
# Copy the updated script to the VM
gcloud compute scp scripts/telco_data_generator.py telco-sftp-server:/tmp/ --zone=us-central1-a

# Replace the old script and restart the service
gcloud compute ssh telco-sftp-server --zone=us-central1-a --command='
  sudo mv /tmp/telco_data_generator.py /opt/telco-generator/telco_data_generator.py && 
  sudo chown telco_user:sftpusers /opt/telco-generator/telco_data_generator.py &&
  sudo systemctl restart telco-generator
'

# Verify it's running
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='sudo systemctl status telco-generator --no-pager'
```

---

## Troubleshooting

### Service Won't Start

Check for errors in the logs:
```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='sudo journalctl -u telco-generator -n 100 --no-pager'
```

### Check Python Script Directly

Test the script manually:
```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a

# Activate the virtual environment
source /opt/telco-generator/venv/bin/activate

# Run the script manually (Ctrl+C to stop)
# Syslog → local SFTP, SNMP → GCS bucket
python /opt/telco-generator/telco_data_generator.py \
  --syslog-dir /sftp/telco/syslog \
  --snmp-gcs-bucket gs://<YOUR_GCS_BUCKET>/snmp/ \
  --files-per-minute 100 \
  --log-level DEBUG
```

### Check Directory Permissions

```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='ls -la /sftp/telco/'
```

Expected output:
```
drwxr-xr-x 3 root       root       4096 Dec  2 01:10 .
drwxr-xr-x 3 root       root       4096 Dec  2 01:10 ..
drwxr-xr-x 2 telco_user sftpusers  4096 Dec  2 01:15 syslog
```

### Check GCS Bucket Access

```bash
# Verify VM can write to GCS bucket
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='echo "test" | gsutil cp - gs://<YOUR_GCS_BUCKET>/snmp/test.txt && gsutil rm gs://<YOUR_GCS_BUCKET>/snmp/test.txt && echo "GCS access OK"'
```

### Restart All Services

```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a --command='
  sudo systemctl restart ssh &&
  sudo systemctl restart telco-generator &&
  sudo systemctl status telco-generator --no-pager
'
```

---

## Performance Monitoring

### Check CPU and Memory Usage

```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='top -b -n 1 | head -20'
```

### Check Process Details

```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='ps aux | grep telco_data_generator'
```

### Monitor File Creation Rate

```bash
# Run this and watch the file count increase every 5 seconds
gcloud compute ssh telco-sftp-server --zone=us-central1-a --command='
  echo "Monitoring file creation rate (press Ctrl+C to stop)..."
  while true; do
    count=$(ls -1 /sftp/telco/syslog/ 2>/dev/null | wc -l)
    echo "$(date +"%H:%M:%S") - Total syslog files: $count"
    sleep 5
  done
'
```

---

## Schema Evolution Demo

The schema evolution demo showcases Databricks Auto Loader's ability to automatically detect and add new columns when the source data schema changes.

### How It Works

1. **Original Schema**: Base SNMP data has 10 columns
2. **Extended Schema**: Adds 5 new columns + 3 new metrics
3. **Auto Loader**: Automatically detects new columns and updates the Delta table

### Architecture: Running in Parallel

**Yes, you CAN run `schema_evolution_demo.py` while `telco_data_generator.py` is still running!**

This is actually the **ideal demo scenario**:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           GCE VM                                             │
│                                                                              │
│  ┌─────────────────────────────┐   ┌─────────────────────────────────────┐  │
│  │  telco_data_generator.py    │   │     schema_evolution_demo.py        │  │
│  │  (systemd service)          │   │     (manual execution)              │  │
│  │                             │   │                                     │  │
│  │  • Runs continuously        │   │  • Run on-demand during demo        │  │
│  │  • BASE schema (10 cols)    │   │  • EXTENDED schema (15 cols)        │  │
│  │  • snmp_*.json files        │   │  • snmp_demo_evolved_*.json files   │  │
│  └──────────────┬──────────────┘   └──────────────┬──────────────────────┘  │
│                 │                                  │                         │
│                 └────────────┬─────────────────────┘                         │
│                              │                                               │
│                              ▼                                               │
│                   gs://bucket/snmp/ (both write here)                       │
└──────────────────────────────│──────────────────────────────────────────────┘
                               │
                               ▼
            ┌──────────────────────────────────────────────────┐
            │              Databricks Auto Loader               │
            │                                                   │
            │  • Monitors gs://bucket/snmp/*.json              │
            │  • Processes ALL new files (both scripts)        │
            │  • schemaEvolutionMode: addNewColumns            │
            │  • Automatically merges schemas                  │
            └──────────────────────────────────────────────────┘
                               │
                               ▼
            ┌──────────────────────────────────────────────────┐
            │            bronze.snmp_raw Delta Table           │
            │                                                   │
            │  • Old records: new columns = NULL               │
            │  • New records: new columns populated            │
            │  • No restart required!                          │
            └──────────────────────────────────────────────────┘
```

### Why Parallel Execution Works

| Aspect | Explanation |
|--------|-------------|
| **Unique filenames** | Both scripts use timestamp-based filenames (no collisions) |
| **Same GCS path** | Both write to `gs://bucket/snmp/` - Auto Loader processes all |
| **Schema merge** | Auto Loader automatically merges schemas |
| **Backward compatible** | Old records have NULL for new columns |
| **No restart needed** | Streaming continues uninterrupted |

---

### Live Demo Flow (Step-by-Step)

#### **Pre-Demo Setup**

```bash
# Ensure generator is running
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='sudo systemctl status telco-generator --no-pager'

# Ensure SNMP ingestion is running in Databricks
# (01_2_bronze_ingestion_snmp.py notebook)
```

#### **Step 1: Show Baseline Schema (1 min)**

In Databricks SQL:
```sql
-- Show current schema
DESCRIBE TABLE telus_networkperf.bronze.snmp_raw;

-- Expected columns: timestamp, device_id, device_type, location, vendor, 
--                   model, ip_address, oid, metric_name, value,
--                   ingestion_timestamp, source_file
```

#### **Step 2: Show Data Flowing (30 sec)**

```sql
-- Show recent records with current schema
SELECT 
    timestamp, device_id, metric_name, value
FROM telus_networkperf.bronze.snmp_raw
ORDER BY ingestion_timestamp DESC
LIMIT 5;
```

#### **Step 3: Trigger Schema Evolution (1 min)**

Open a NEW terminal and run (while generator is still running):

```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a --command='
  cd /opt/telco-generator
  source venv/bin/activate
  python schema_evolution_demo.py \
    --gcs-bucket gs://<YOUR_GCS_BUCKET>/snmp/ \
    --num-files 10 \
    --evolve-schema
'
```

**Expected output:**
```
============================================================
SCHEMA EVOLUTION MODE: Extended schema with new columns
============================================================
Original columns: timestamp, device_id, device_type, location,
                  vendor, model, ip_address, oid, metric_name, value
NEW columns: firmware_version, uptime_hours, last_reboot,
             maintenance_window, criticality_tier
NEW metrics: cpu_utilization_pct, memory_usage_pct, connection_count
============================================================
Uploaded: gs://bucket/snmp/snmp_demo_evolved_20251203_... (50 records)
...
Complete! Generated 10 files with 500 total records
Check Databricks to see Auto Loader detect the new columns!
```

#### **Step 4: Watch Schema Evolution (1 min)**

Back in Databricks SQL:
```sql
-- Run this a few times - new columns will appear!
DESCRIBE TABLE telus_networkperf.bronze.snmp_raw;

-- NEW columns should appear:
-- firmware_version (STRING)
-- uptime_hours (BIGINT)
-- last_reboot (STRING)
-- maintenance_window (STRING)
-- criticality_tier (STRING)
```

#### **Step 5: Query New Columns (1 min)**

```sql
-- Show records with new columns populated
SELECT 
    device_id,
    metric_name,
    value,
    firmware_version,      -- NEW!
    uptime_hours,          -- NEW!
    criticality_tier       -- NEW!
FROM telus_networkperf.bronze.snmp_raw
WHERE firmware_version IS NOT NULL
ORDER BY ingestion_timestamp DESC
LIMIT 10;

-- Show mixed data (old schema + new schema)
SELECT 
    device_id,
    metric_name,
    CASE 
        WHEN firmware_version IS NULL THEN 'Original Schema'
        ELSE 'Extended Schema'
    END as schema_version,
    firmware_version,
    criticality_tier
FROM telus_networkperf.bronze.snmp_raw
ORDER BY ingestion_timestamp DESC
LIMIT 20;
```

---

### Key Demo Talking Points

1. **"No pipeline restart required"**
   - Schema evolution happens automatically while data keeps flowing
   - Zero downtime for schema changes

2. **"Backward compatible by design"**
   - Old records have NULL for new columns
   - New records have all columns populated
   - Both coexist in the same table

3. **"Forward compatible"**
   - New columns are immediately queryable
   - No manual DDL (ALTER TABLE) needed
   - Auto Loader handles everything

4. **"Production-ready pattern"**
   - This is how real telco data evolves over time
   - New firmware versions add new telemetry
   - New device types bring new metrics

---

### Generate Original Schema Files (Baseline)

Use this to generate files with the ORIGINAL schema (useful for resetting):

```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a --command='
  cd /opt/telco-generator
  source venv/bin/activate
  python schema_evolution_demo.py \
    --gcs-bucket gs://<YOUR_GCS_BUCKET>/snmp/ \
    --num-files 10
'
```

---

---

### Pre-Demo Testing (Without Affecting Demo Data)

You can test the schema evolution script before the demo without affecting your actual data:

#### Option 1: Dry Run (Preview Only)

```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a --command='
  cd /opt/telco-generator
  source venv/bin/activate
  python schema_evolution_demo.py --dry-run --evolve-schema
'
```

**Output shows:**
- Sample records that would be generated
- New columns that would be added
- No files are uploaded

#### Option 2: Test to Separate Path

Write to a test location that Databricks isn't monitoring:

```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a --command='
  cd /opt/telco-generator
  source venv/bin/activate
  python schema_evolution_demo.py \
    --gcs-bucket gs://<YOUR_GCS_BUCKET>/snmp-test/ \
    --num-files 5 \
    --evolve-schema
'

# Clean up test files after
gsutil -m rm gs://<YOUR_GCS_BUCKET>/snmp-test/**
```

#### Option 3: Local Directory Test

```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a --command='
  cd /opt/telco-generator
  source venv/bin/activate
  python schema_evolution_demo.py \
    --local-dir /tmp/schema_test \
    --num-files 3 \
    --evolve-schema
  
  # View generated files
  cat /tmp/schema_test/*.json | head -5
  
  # Clean up
  rm -rf /tmp/schema_test
'
```

---

### Reset for Demo (Full Reset Procedure)

If you've already tested schema evolution and need to reset for the actual demo:

#### Step 1: Stop Data Generator

```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='sudo systemctl stop telco-generator'
```

#### Step 2: Clear All SNMP Files from GCS

```bash
gsutil -m rm gs://<YOUR_GCS_BUCKET>/snmp/**
```

#### Step 3: Clear Checkpoints and Schema (in Databricks)

Run this in a Databricks notebook:
```python
# Clear Auto Loader checkpoints and schema cache
dbutils.fs.rm("/Volumes/telus_networkperf/bronze/checkpoints/snmp_gcs", recurse=True)
dbutils.fs.rm("/Volumes/telus_networkperf/bronze/schemas/snmp_gcs", recurse=True)
print("Cleared checkpoints and schema cache")
```

#### Step 4: Drop the Bronze Table (in Databricks)

```sql
DROP TABLE IF EXISTS telus_networkperf.bronze.snmp_raw;
```

#### Step 5: Restart Data Generator

```bash
gcloud compute ssh telco-sftp-server --zone=us-central1-a \
  --command='sudo systemctl start telco-generator'
```

#### Step 6: Restart SNMP Ingestion Notebook

Detach and re-run `01_2_bronze_ingestion_snmp.py` to start fresh.

#### Step 7: Verify Reset

```sql
-- Should show original schema (no firmware_version, etc.)
DESCRIBE TABLE telus_networkperf.bronze.snmp_raw;
```

---

### Pre-Demo Checklist

| # | Task | Command/Action |
|---|------|----------------|
| 1 | Test schema evolution (dry run) | `python schema_evolution_demo.py --dry-run --evolve-schema` |
| 2 | Reset if needed | Follow "Reset for Demo" procedure above |
| 3 | Verify generator running | `sudo systemctl status telco-generator` |
| 4 | Verify SNMP ingestion running | Check notebook in Databricks |
| 5 | Verify baseline schema | `DESCRIBE TABLE telus_networkperf.bronze.snmp_raw` |
| 6 | Confirm no extended columns | Should NOT see `firmware_version`, etc. |

---

### Cleanup After Demo

To reset the demo environment:

```bash
# Clear all SNMP files from GCS
gsutil -m rm gs://<YOUR_GCS_BUCKET>/snmp/**

# Clear Auto Loader checkpoints (in Databricks)
# dbutils.fs.rm("/Volumes/telus_networkperf/bronze/checkpoints/snmp_gcs", recurse=True)
# dbutils.fs.rm("/Volumes/telus_networkperf/bronze/schemas/snmp_gcs", recurse=True)

# Drop and recreate the table
# DROP TABLE IF EXISTS telus_networkperf.bronze.snmp_raw;
```

---

## Quick Reference

| Action | Command |
|--------|---------|
| **Start** | `sudo systemctl start telco-generator` |
| **Stop** | `sudo systemctl stop telco-generator` |
| **Restart** | `sudo systemctl restart telco-generator` |
| **Status** | `sudo systemctl status telco-generator` |
| **Logs (live)** | `sudo journalctl -u telco-generator -f` |
| **Count syslog files** | `ls -1 /sftp/telco/syslog/ \| wc -l` |
| **Count SNMP files (GCS)** | `gsutil ls gs://BUCKET/snmp/*.json \| wc -l` |
| **Delete syslog files** | `sudo find /sftp/telco/syslog/ -type f -delete` |
| **Delete SNMP files (GCS)** | `gsutil -m rm gs://BUCKET/snmp/**` |
| **Schema evolution demo** | `python schema_evolution_demo.py --gcs-bucket gs://BUCKET/snmp/ --evolve-schema` |

---

## Tips

1. **Before Demo**: Stop the generator and clear old files for a clean start
2. **During Demo**: Monitor logs in real-time to show data generation
3. **After Demo**: Stop the generator to save costs
4. **Testing**: Use a lower rate (100-500 files/min) for initial testing
5. **Production**: Increase to 1000+ files/min to demonstrate scale

---

## Support

For issues or questions:
- Check the main [README.md](../README.md)
- Review [Architecture Documentation](architecture.md)
- Check GCP logs in Cloud Console
- Verify SFTP connectivity with `sftp telco_user@<IP>`

