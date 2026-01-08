# Databricks notebook source
# MAGIC %md
# MAGIC # Telco Network Performance Demo - Bronze SNMP Ingestion (GCS)
# MAGIC 
# MAGIC This notebook implements Auto Loader for ingesting SNMP metrics from Google Cloud Storage.
# MAGIC 
# MAGIC ## Features
# MAGIC - Real-time streaming ingestion using Auto Loader
# MAGIC - JSON format with automatic schema inference
# MAGIC - Schema evolution support (automatically adds new columns)
# MAGIC - File notification mode for low-latency ingestion
# MAGIC - Exactly-once processing guarantees
# MAGIC - Metadata tracking for lineage
# MAGIC 
# MAGIC ## Architecture
# MAGIC ```
# MAGIC GCE VM → GCS Bucket (snmp/*.json) → Auto Loader (file notifications) → Bronze Delta Table
# MAGIC ```
# MAGIC 
# MAGIC ## Why GCS instead of SFTP for SNMP?
# MAGIC - Lower latency with file notification mode
# MAGIC - Better scalability for high-volume data
# MAGIC - Native GCP integration
# MAGIC - Syslog remains on SFTP for demo variety

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import *

# GCS configuration
# Update this with your actual bucket name
GCS_BUCKET = dbutils.secrets.get(scope="<YOUR_SECRET_SCOPE>", key="GCS_SNMP_BUCKET")
# Or hardcode for demo: GCS_BUCKET = "<YOUR_GCS_BUCKET_NAME>"

# Unity Catalog configuration
CATALOG_NAME = "telus_networkperf"
SCHEMA_NAME = "bronze"

# Table name
BRONZE_SNMP_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.snmp_raw"

# GCS path for SNMP JSON files
SNMP_GCS_PATH = f"gs://{GCS_BUCKET}/snmp/"

# Checkpoint and schema locations (use Unity Catalog volume paths)
CHECKPOINT_BASE = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/checkpoints"
SCHEMA_BASE = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/schemas"

SNMP_CHECKPOINT = f"{CHECKPOINT_BASE}/snmp_gcs"
SNMP_SCHEMA_LOCATION = f"{SCHEMA_BASE}/snmp_gcs"

print("SNMP Ingestion Configuration (GCS):")
print(f"  GCS Path: {SNMP_GCS_PATH}")
print(f"  Target Table: {BRONZE_SNMP_TABLE}")
print(f"  Checkpoint: {SNMP_CHECKPOINT}")
print(f"  Schema Location: {SNMP_SCHEMA_LOCATION}")
print(f"  Format: JSON Lines (NDJSON) - one JSON object per line")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze SNMP Ingestion
# MAGIC 
# MAGIC Ingest SNMP metrics JSON files from GCS using Auto Loader with file notifications.
# MAGIC 
# MAGIC ### JSON Lines (NDJSON) Format
# MAGIC The data generator produces files in **JSON Lines** format:
# MAGIC - Each line contains ONE complete JSON object (no pretty-printing)
# MAGIC - Example: `{"timestamp":"2025-12-03T...","device_id":"router-001",...}`
# MAGIC - This format is optimal for streaming ingestion
# MAGIC 
# MAGIC ### Schema Evolution
# MAGIC The `schemaEvolutionMode` is set to `addNewColumns` which means:
# MAGIC - New columns in incoming files are automatically added to the table
# MAGIC - Run `schema_evolution_demo.py --evolve-schema` on the VM to see this in action

# COMMAND ----------

snmp_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SNMP_SCHEMA_LOCATION)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Auto-add new columns
    .option("cloudFiles.includeExistingFiles", "true")
    .option("pathGlobFilter", "*.json")
    .option("rescuedDataColumn", "_rescued_data")  # Capture malformed data
    # File notification mode for lowest latency
    # Requires: External location with file events enabled (run 00_1_setup_gcs_connection.py)
    # Reference: https://docs.databricks.com/gcp/en/ingestion/cloud-object-storage/auto-loader/file-notification-mode
    .option("cloudFiles.useManagedFileEvents", "true")  # Recommended for GCS
    .load(SNMP_GCS_PATH)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
)

# Display schema
print("SNMP Stream Schema:")
snmp_stream.printSchema()

# COMMAND ----------

# Write SNMP stream to bronze table (CONTINUOUS MODE)
snmp_query = (
    snmp_stream.writeStream
    .format("delta")
    .option("checkpointLocation", SNMP_CHECKPOINT)
    .option("mergeSchema", "true")  # Allow schema evolution on write
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .toTable(BRONZE_SNMP_TABLE)
)

print(f"[OK] Started SNMP continuous ingestion stream from GCS")
print(f"  Source: {SNMP_GCS_PATH}")
print(f"  Writing to: {BRONZE_SNMP_TABLE}")
print(f"  Checkpoint: {SNMP_CHECKPOINT}")
print(f"  Processing Interval: 5 seconds")
print(f"  Schema Evolution: Enabled (addNewColumns)")

print("\n" + "=" * 80)
print("SNMP STREAMING MODE ACTIVE (GCS → Delta)")
print("=" * 80)
print("SNMP stream is now running continuously.")
print("New JSON files in GCS will be processed every 5 seconds.")
print("Schema changes are automatically detected and applied.")
print("The stream will run until manually stopped.")
print("=" * 80 + "\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Evolution Demo
# MAGIC 
# MAGIC To demonstrate schema evolution:
# MAGIC 
# MAGIC 1. Start this notebook (stream is running)
# MAGIC 2. SSH into the VM and run:
# MAGIC    ```bash
# MAGIC    cd /opt/telco-generator
# MAGIC    python schema_evolution_demo.py --gcs-bucket gs://YOUR_BUCKET/snmp/ --num-files 10 --evolve-schema
# MAGIC    ```
# MAGIC 3. Watch the table schema automatically update with new columns:
# MAGIC    - `firmware_version`
# MAGIC    - `uptime_hours`
# MAGIC    - `last_reboot`
# MAGIC    - `maintenance_window`
# MAGIC    - `criticality_tier`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Current Schema
# MAGIC 
# MAGIC Run this cell periodically to see schema evolution in action.

# COMMAND ----------

# Show current table schema
print("Current Bronze SNMP Table Schema:")
print("=" * 50)
try:
    schema_df = spark.sql(f"DESCRIBE TABLE {BRONZE_SNMP_TABLE}")
    display(schema_df)
except Exception as e:
    print(f"Table not yet created: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Recent Records

# COMMAND ----------

# View recent SNMP records
try:
    display(spark.sql(f"""
        SELECT * FROM {BRONZE_SNMP_TABLE}
        ORDER BY ingestion_timestamp DESC
        LIMIT 20
    """))
except Exception as e:
    print(f"No data yet: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. [OK] SNMP bronze ingestion started (from GCS)
# MAGIC 2. → Ensure `01_1_bronze_ingestion_syslog.py` is running for syslog (from SFTP)
# MAGIC 3. → Use `01_3_bronze_monitor.py` to monitor ingestion progress
# MAGIC 4. → Run `schema_evolution_demo.py --evolve-schema` to demonstrate schema evolution
# MAGIC 5. → Create DLT pipelines for silver and gold layers
# MAGIC 
# MAGIC **Note:** This stream runs continuously. To stop, detach or stop the notebook execution.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternative: Batch Mode (Reference)
# MAGIC 
# MAGIC For one-time batch processing, use `trigger(availableNow=True)`.

# COMMAND ----------

# BATCH MODE CODE (Commented)
"""
snmp_batch = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SNMP_SCHEMA_LOCATION)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("pathGlobFilter", "*.json")
    .option("rescuedDataColumn", "_rescued_data")
    .load(SNMP_GCS_PATH)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
    .writeStream
    .format("delta")
    .option("checkpointLocation", SNMP_CHECKPOINT)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(BRONZE_SNMP_TABLE)
)

snmp_batch.awaitTermination()
print("[OK] SNMP batch ingestion complete")

snmp_count = spark.table(BRONZE_SNMP_TABLE).count()
print(f"Total SNMP records ingested: {snmp_count:,}")
display(spark.table(BRONZE_SNMP_TABLE).limit(10))
"""
