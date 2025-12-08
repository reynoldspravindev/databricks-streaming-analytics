# Data Generator Control Guide

This document provides all the commands needed to control and manage the data generator running on the GCP VM.

## Available Flavors

| Flavor | Generator Script | Service Name | Data Types |
|--------|-----------------|--------------|------------|
| **Telco** | `telco_data_generator.py` | `telco-generator` | Syslog (RFC 5424), SNMP metrics |
| **Retail** | `retail_data_generator.py` | `retail-generator` | Store events, sales metrics |

## Architecture Overview

The data generator produces two types of data:
- **Logs/Events** - Written to local SFTP directory - Ingested via Auto Loader SFTP
- **Metrics** - Uploaded to GCS bucket - Ingested via Auto Loader GCS

```
+-------------------------------------------------------------+
|                      GCE VM                                  |
|  +-----------------------------------------------------+    |
|  |              data_generator.py                       |    |
|  +---------------------+---------------+---------------+    |
|                        |               |                     |
|              Logs (TXT/JSON)     Metrics (JSON)             |
|                        |               |                     |
|                        v               v                     |
|              /sftp/<domain>/logs   GCS Bucket               |
|                        |           gs://bucket/metrics/      |
+------------------------|--------------|-----------------------+
                         |              |
                         v              v
              +------------------------------------------+
              |           Databricks on GCP               |
              |  Auto Loader (SFTP)   Auto Loader (GCS)  |
              |         |                    |            |
              |         v                    v            |
              |   bronze.logs_raw    bronze.metrics_raw   |
              +------------------------------------------+
```

## Basic Service Control

Replace `<flavor>` with your chosen flavor (e.g., `telco-generator` or `retail-generator`).
Replace `<vm-name>` with your VM name.

### Stop (Pause) the Data Generator

Stops the data generator service. No new files will be created.

```bash
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='sudo systemctl stop <flavor>-generator'
```

### Start (Resume) the Data Generator

Starts the data generator service. Files will begin generating immediately.

```bash
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='sudo systemctl start <flavor>-generator'
```

### Restart the Data Generator

Stops and then starts the service. Useful after configuration changes.

```bash
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='sudo systemctl restart <flavor>-generator'
```

### Check Service Status

View the current status of the data generator service.

```bash
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='sudo systemctl status <flavor>-generator --no-pager'
```

**Expected Output:**
```
* <flavor>-generator.service - Data Generator Service
     Loaded: loaded (/etc/systemd/system/<flavor>-generator.service; enabled)
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
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='sudo journalctl -u <flavor>-generator -f'
```

Press **Ctrl+C** to exit.

### View Last 50 Log Lines

```bash
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='sudo journalctl -u <flavor>-generator -n 50 --no-pager'
```

### View Logs Since Last Hour

```bash
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='sudo journalctl -u <flavor>-generator --since "1 hour ago" --no-pager'
```

### View Logs for Specific Time Range

```bash
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='sudo journalctl -u <flavor>-generator --since "2025-12-02 10:00:00" --until "2025-12-02 11:00:00" --no-pager'
```

---

## File Management

### Count Generated Files

**Count log files (local SFTP):**
```bash
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='ls -1 /sftp/<domain>/logs/ | wc -l'
```

**Count metrics files (GCS):**
```bash
gsutil ls gs://<YOUR_GCS_BUCKET>/metrics/*.json | wc -l
```

### View Recent Files

**Last 10 log files:**
```bash
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='ls -lht /sftp/<domain>/logs/ | head -11'
```

**Last 10 metrics files (GCS):**
```bash
gsutil ls -l gs://<YOUR_GCS_BUCKET>/metrics/*.json | tail -10
```

### Check Disk/Storage Usage

**Local log disk usage:**
```bash
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='du -sh /sftp/<domain>/logs'
```

**GCS bucket size (metrics):**
```bash
gsutil du -sh gs://<YOUR_GCS_BUCKET>/metrics/
```

### Watch Files Being Created in Real-Time

```bash
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='watch -n 2 "ls -1 /sftp/<domain>/logs/ | wc -l"'
```

Press **Ctrl+C** to exit.

### Clear Generated Files

**CAUTION**: This permanently deletes all generated files!

> **Note**: Use `find` instead of `rm *.txt` to avoid "Argument list too long" errors when there are many files.

```bash
# Delete all log files (local SFTP)
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='sudo find /sftp/<domain>/logs/ -type f -delete'

# Delete all metrics files from GCS bucket
gsutil -m rm gs://<YOUR_GCS_BUCKET>/metrics/**
```

---

## Configuration Management

### View Current Configuration

```bash
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='sudo cat /etc/systemd/system/<flavor>-generator.service'
```

### Change File Generation Rate

**Method 1: Direct Edit**

```bash
# SSH into the VM
gcloud compute ssh <vm-name> --zone=us-central1-a

# Edit the service file
sudo nano /etc/systemd/system/<flavor>-generator.service

# Find the line with --files-per-minute and change the value
# Default: --files-per-minute 1000
# Examples: 500, 2000, 5000

# Save and exit (Ctrl+X, Y, Enter)

# Reload systemd configuration
sudo systemctl daemon-reload

# Restart the service
sudo systemctl restart <flavor>-generator

# Exit the VM
exit
```

**Method 2: Automated (Replace 1000 with desired rate)**

```bash
# Stop the service
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='sudo systemctl stop <flavor>-generator'

# Update the rate (example: change to 500 files/min)
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command="sudo sed -i 's/--files-per-minute 1000/--files-per-minute 500/' /etc/systemd/system/<flavor>-generator.service"

# Reload and restart
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='sudo systemctl daemon-reload && sudo systemctl start <flavor>-generator'
```

### Enable/Disable Auto-Start on Boot

**Enable auto-start (default):**
```bash
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='sudo systemctl enable <flavor>-generator'
```

**Disable auto-start:**
```bash
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='sudo systemctl disable <flavor>-generator'
```

**Check if enabled:**
```bash
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='sudo systemctl is-enabled <flavor>-generator'
```

---

## Update the Data Generator Script

If you've made changes to the Python script and need to redeploy:

```bash
# Copy the updated script to the VM
gcloud compute scp scripts/<flavor>_data_generator.py <vm-name>:/tmp/ --zone=us-central1-a

# Replace the old script and restart the service
gcloud compute ssh <vm-name> --zone=us-central1-a --command='
  sudo mv /tmp/<flavor>_data_generator.py /opt/<flavor>-generator/<flavor>_data_generator.py && 
  sudo chown <sftp_user>:sftpusers /opt/<flavor>-generator/<flavor>_data_generator.py &&
  sudo systemctl restart <flavor>-generator
'

# Verify it's running
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='sudo systemctl status <flavor>-generator --no-pager'
```

---

## Troubleshooting

### Service Won't Start

Check for errors in the logs:
```bash
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='sudo journalctl -u <flavor>-generator -n 100 --no-pager'
```

### Check Python Script Directly

Test the script manually:
```bash
gcloud compute ssh <vm-name> --zone=us-central1-a

# Activate the virtual environment
source /opt/<flavor>-generator/venv/bin/activate

# Run the script manually (Ctrl+C to stop)
python /opt/<flavor>-generator/<flavor>_data_generator.py \
  --logs-dir /sftp/<domain>/logs \
  --gcs-bucket gs://<YOUR_GCS_BUCKET>/metrics/ \
  --files-per-minute 100 \
  --log-level DEBUG
```

### Check Directory Permissions

```bash
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='ls -la /sftp/<domain>/'
```

Expected output:
```
drwxr-xr-x 3 root       root       4096 Dec  2 01:10 .
drwxr-xr-x 3 root       root       4096 Dec  2 01:10 ..
drwxr-xr-x 2 <user>     sftpusers  4096 Dec  2 01:15 logs
```

### Check GCS Bucket Access

```bash
# Verify VM can write to GCS bucket
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='echo "test" | gsutil cp - gs://<YOUR_GCS_BUCKET>/metrics/test.txt && gsutil rm gs://<YOUR_GCS_BUCKET>/metrics/test.txt && echo "GCS access OK"'
```

### Restart All Services

```bash
gcloud compute ssh <vm-name> --zone=us-central1-a --command='
  sudo systemctl restart ssh &&
  sudo systemctl restart <flavor>-generator &&
  sudo systemctl status <flavor>-generator --no-pager
'
```

---

## Performance Monitoring

### Check CPU and Memory Usage

```bash
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='top -b -n 1 | head -20'
```

### Check Process Details

```bash
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='ps aux | grep data_generator'
```

### Monitor File Creation Rate

```bash
# Run this and watch the file count increase every 5 seconds
gcloud compute ssh <vm-name> --zone=us-central1-a --command='
  echo "Monitoring file creation rate (press Ctrl+C to stop)..."
  while true; do
    count=$(ls -1 /sftp/<domain>/logs/ 2>/dev/null | wc -l)
    echo "$(date +"%H:%M:%S") - Total log files: $count"
    sleep 5
  done
'
```

---

## Schema Evolution Demo

The schema evolution demo showcases Databricks Auto Loader's ability to automatically detect and add new columns when the source data schema changes.

### How It Works

1. **Original Schema**: Base metrics data has standard columns
2. **Extended Schema**: Adds new columns and metrics
3. **Auto Loader**: Automatically detects new columns and updates the Delta table

### Architecture: Running in Parallel

**Yes, you CAN run `schema_evolution_demo.py` while the main generator is still running!**

This is actually the **ideal demo scenario**:

```
+-----------------------------------------------------------------------------+
|                           GCE VM                                             |
|                                                                              |
|  +-----------------------------+   +-------------------------------------+  |
|  |  data_generator.py          |   |     schema_evolution_demo.py        |  |
|  |  (systemd service)          |   |     (manual execution)              |  |
|  |                             |   |                                     |  |
|  |  - Runs continuously        |   |  - Run on-demand during demo        |  |
|  |  - BASE schema              |   |  - EXTENDED schema                  |  |
|  |  - metrics_*.json files     |   |  - metrics_demo_evolved_*.json      |  |
|  +-------------+---------------+   +------------------+------------------+  |
|                |                                      |                     |
|                +----------------+---------------------+                     |
|                                 |                                           |
|                                 v                                           |
|                   gs://bucket/metrics/ (both write here)                   |
+--------------------------------|--------------------------------------------+
                                 |
                                 v
            +----------------------------------------------+
            |              Databricks Auto Loader           |
            |                                               |
            |  - Monitors gs://bucket/metrics/*.json       |
            |  - Processes ALL new files (both scripts)    |
            |  - schemaEvolutionMode: addNewColumns        |
            |  - Automatically merges schemas              |
            +----------------------------------------------+
                                 |
                                 v
            +----------------------------------------------+
            |            bronze.metrics_raw Delta Table    |
            |                                               |
            |  - Old records: new columns = NULL           |
            |  - New records: new columns populated        |
            |  - No restart required!                      |
            +----------------------------------------------+
```

### Why Parallel Execution Works

| Aspect | Explanation |
|--------|-------------|
| **Unique filenames** | Both scripts use timestamp-based filenames (no collisions) |
| **Same GCS path** | Both write to `gs://bucket/metrics/` - Auto Loader processes all |
| **Schema merge** | Auto Loader automatically merges schemas |
| **Backward compatible** | Old records have NULL for new columns |
| **No restart needed** | Streaming continues uninterrupted |

---

### Live Demo Flow (Step-by-Step)

#### **Pre-Demo Setup**

```bash
# Ensure generator is running
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='sudo systemctl status <flavor>-generator --no-pager'

# Ensure metrics ingestion is running in Databricks
# (bronze ingestion notebook)
```

#### **Step 1: Show Baseline Schema (1 min)**

In Databricks SQL:
```sql
-- Show current schema
DESCRIBE TABLE <catalog>.bronze.metrics_raw;

-- Expected columns: timestamp, entity_id, entity_type, location,
--                   metric_name, value, ingestion_timestamp, source_file
```

#### **Step 2: Show Data Flowing (30 sec)**

```sql
-- Show recent records with current schema
SELECT 
    timestamp, entity_id, metric_name, value
FROM <catalog>.bronze.metrics_raw
ORDER BY ingestion_timestamp DESC
LIMIT 5;
```

#### **Step 3: Trigger Schema Evolution (1 min)**

Open a NEW terminal and run (while generator is still running):

```bash
gcloud compute ssh <vm-name> --zone=us-central1-a --command='
  cd /opt/<flavor>-generator
  source venv/bin/activate
  python schema_evolution_demo.py \
    --gcs-bucket gs://<YOUR_GCS_BUCKET>/metrics/ \
    --num-files 10 \
    --evolve-schema
'
```

**Expected output:**
```
============================================================
SCHEMA EVOLUTION MODE: Extended schema with new columns
============================================================
Original columns: timestamp, entity_id, entity_type, location,
                  metric_name, value
NEW columns: version, uptime_hours, last_update,
             maintenance_window, priority_tier
============================================================
Uploaded: gs://bucket/metrics/demo_evolved_20251203_... (50 records)
...
Complete! Generated 10 files with 500 total records
Check Databricks to see Auto Loader detect the new columns!
```

#### **Step 4: Watch Schema Evolution (1 min)**

Back in Databricks SQL:
```sql
-- Run this a few times - new columns will appear!
DESCRIBE TABLE <catalog>.bronze.metrics_raw;

-- NEW columns should appear:
-- version (STRING)
-- uptime_hours (BIGINT)
-- last_update (STRING)
-- maintenance_window (STRING)
-- priority_tier (STRING)
```

#### **Step 5: Query New Columns (1 min)**

```sql
-- Show records with new columns populated
SELECT 
    entity_id,
    metric_name,
    value,
    version,           -- NEW!
    uptime_hours,      -- NEW!
    priority_tier      -- NEW!
FROM <catalog>.bronze.metrics_raw
WHERE version IS NOT NULL
ORDER BY ingestion_timestamp DESC
LIMIT 10;

-- Show mixed data (old schema + new schema)
SELECT 
    entity_id,
    metric_name,
    CASE 
        WHEN version IS NULL THEN 'Original Schema'
        ELSE 'Extended Schema'
    END as schema_version,
    version,
    priority_tier
FROM <catalog>.bronze.metrics_raw
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
   - This is how real data evolves over time
   - New versions add new telemetry
   - New entity types bring new metrics

---

### Generate Original Schema Files (Baseline)

Use this to generate files with the ORIGINAL schema (useful for resetting):

```bash
gcloud compute ssh <vm-name> --zone=us-central1-a --command='
  cd /opt/<flavor>-generator
  source venv/bin/activate
  python schema_evolution_demo.py \
    --gcs-bucket gs://<YOUR_GCS_BUCKET>/metrics/ \
    --num-files 10
'
```

---

### Pre-Demo Testing (Without Affecting Demo Data)

You can test the schema evolution script before the demo without affecting your actual data:

#### Option 1: Dry Run (Preview Only)

```bash
gcloud compute ssh <vm-name> --zone=us-central1-a --command='
  cd /opt/<flavor>-generator
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
gcloud compute ssh <vm-name> --zone=us-central1-a --command='
  cd /opt/<flavor>-generator
  source venv/bin/activate
  python schema_evolution_demo.py \
    --gcs-bucket gs://<YOUR_GCS_BUCKET>/metrics-test/ \
    --num-files 5 \
    --evolve-schema
'

# Clean up test files after
gsutil -m rm gs://<YOUR_GCS_BUCKET>/metrics-test/**
```

#### Option 3: Local Directory Test

```bash
gcloud compute ssh <vm-name> --zone=us-central1-a --command='
  cd /opt/<flavor>-generator
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
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='sudo systemctl stop <flavor>-generator'
```

#### Step 2: Clear All Metrics Files from GCS

```bash
gsutil -m rm gs://<YOUR_GCS_BUCKET>/metrics/**
```

#### Step 3: Clear Checkpoints and Schema (in Databricks)

Run this in a Databricks notebook:
```python
# Clear Auto Loader checkpoints and schema cache
dbutils.fs.rm("/Volumes/<catalog>/bronze/checkpoints/metrics_gcs", recurse=True)
dbutils.fs.rm("/Volumes/<catalog>/bronze/schemas/metrics_gcs", recurse=True)
print("Cleared checkpoints and schema cache")
```

#### Step 4: Drop the Bronze Table (in Databricks)

```sql
DROP TABLE IF EXISTS <catalog>.bronze.metrics_raw;
```

#### Step 5: Restart Data Generator

```bash
gcloud compute ssh <vm-name> --zone=us-central1-a \
  --command='sudo systemctl start <flavor>-generator'
```

#### Step 6: Restart Metrics Ingestion Notebook

Detach and re-run the bronze ingestion notebook to start fresh.

#### Step 7: Verify Reset

```sql
-- Should show original schema (no extended columns)
DESCRIBE TABLE <catalog>.bronze.metrics_raw;
```

---

### Pre-Demo Checklist

| # | Task | Command/Action |
|---|------|----------------|
| 1 | Test schema evolution (dry run) | `python schema_evolution_demo.py --dry-run --evolve-schema` |
| 2 | Reset if needed | Follow "Reset for Demo" procedure above |
| 3 | Verify generator running | `sudo systemctl status <flavor>-generator` |
| 4 | Verify ingestion running | Check notebook in Databricks |
| 5 | Verify baseline schema | `DESCRIBE TABLE <catalog>.bronze.metrics_raw` |
| 6 | Confirm no extended columns | Should NOT see extended columns |

---

### Cleanup After Demo

To reset the demo environment:

```bash
# Clear all metrics files from GCS
gsutil -m rm gs://<YOUR_GCS_BUCKET>/metrics/**

# Clear Auto Loader checkpoints (in Databricks)
# dbutils.fs.rm("/Volumes/<catalog>/bronze/checkpoints/metrics_gcs", recurse=True)
# dbutils.fs.rm("/Volumes/<catalog>/bronze/schemas/metrics_gcs", recurse=True)

# Drop and recreate the table
# DROP TABLE IF EXISTS <catalog>.bronze.metrics_raw;
```

---

## Quick Reference

| Action | Command |
|--------|---------|
| **Start** | `sudo systemctl start <flavor>-generator` |
| **Stop** | `sudo systemctl stop <flavor>-generator` |
| **Restart** | `sudo systemctl restart <flavor>-generator` |
| **Status** | `sudo systemctl status <flavor>-generator` |
| **Logs (live)** | `sudo journalctl -u <flavor>-generator -f` |
| **Count log files** | `ls -1 /sftp/<domain>/logs/ \| wc -l` |
| **Count metrics (GCS)** | `gsutil ls gs://BUCKET/metrics/*.json \| wc -l` |
| **Delete log files** | `sudo find /sftp/<domain>/logs/ -type f -delete` |
| **Delete metrics (GCS)** | `gsutil -m rm gs://BUCKET/metrics/**` |
| **Schema evolution demo** | `python schema_evolution_demo.py --gcs-bucket gs://BUCKET/metrics/ --evolve-schema` |

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
- Verify SFTP connectivity with `sftp <user>@<IP>`
