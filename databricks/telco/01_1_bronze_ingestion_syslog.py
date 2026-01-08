# Databricks notebook source
# MAGIC %md
# MAGIC # Telco Network Performance Demo - Bronze Syslog Ingestion
# MAGIC 
# MAGIC This notebook implements Auto Loader for ingesting syslog data from the SFTP server.
# MAGIC 
# MAGIC ## Features
# MAGIC - Real-time streaming ingestion using Auto Loader
# MAGIC - RFC 5424 syslog format support
# MAGIC - Exactly-once processing guarantees
# MAGIC - Metadata tracking for lineage
# MAGIC 
# MAGIC ## Architecture
# MAGIC ```
# MAGIC SFTP Server (syslog/*.txt) → Auto Loader → Bronze Delta Table
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import *

# SFTP connection details
SFTP_HOST = dbutils.secrets.get(scope="<YOUR_SECRET_SCOPE>", key="SFTP_HOST")
SFTP_USERNAME = dbutils.secrets.get(scope="<YOUR_SECRET_SCOPE>", key="SFTP_USERNAME")
SFTP_PORT = "22"

# Unity Catalog configuration
CATALOG_NAME = "telus_networkperf"
SCHEMA_NAME = "bronze"

# Table name
BRONZE_SYSLOG_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.syslog_raw"

# SFTP path (relative to chroot - /sftp is the root for SFTP user)
SYSLOG_SFTP_PATH = f"sftp://{SFTP_USERNAME}@{SFTP_HOST}:{SFTP_PORT}/telco/syslog/"

# Checkpoint and schema locations (use Unity Catalog volume paths)
CHECKPOINT_BASE = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/checkpoints"
SCHEMA_BASE = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/schemas"

SYSLOG_CHECKPOINT = f"{CHECKPOINT_BASE}/syslog"
SYSLOG_SCHEMA_LOCATION = f"{SCHEMA_BASE}/syslog"

print("Syslog Ingestion Configuration:")
print(f"  SFTP Path: {SYSLOG_SFTP_PATH}")
print(f"  Target Table: {BRONZE_SYSLOG_TABLE}")
print(f"  Checkpoint: {SYSLOG_CHECKPOINT}")
print(f"  Schema Location: {SYSLOG_SCHEMA_LOCATION}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Syslog Ingestion
# MAGIC 
# MAGIC Ingest RFC 5424 syslog text files from SFTP server using Auto Loader.

# COMMAND ----------

syslog_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "text")
    .option("cloudFiles.schemaLocation", SYSLOG_SCHEMA_LOCATION)
    .option("cloudFiles.includeExistingFiles", "true")
    .option("pathGlobFilter", "*.txt")
    .load(SYSLOG_SFTP_PATH)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
)

# Display schema
print("Syslog Stream Schema:")
syslog_stream.printSchema()

# COMMAND ----------

# Write syslog stream to bronze table (CONTINUOUS MODE)
syslog_query = (
    syslog_stream.writeStream
    .format("delta")
    .option("checkpointLocation", SYSLOG_CHECKPOINT)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .toTable(BRONZE_SYSLOG_TABLE)
)

print(f"[OK] Started syslog continuous ingestion stream")
print(f"  Writing to: {BRONZE_SYSLOG_TABLE}")
print(f"  Checkpoint: {SYSLOG_CHECKPOINT}")
print(f"  Processing Interval: 5 seconds")

print("\n" + "=" * 80)
print("SYSLOG STREAMING MODE ACTIVE")
print("=" * 80)
print("Syslog stream is now running continuously.")
print("New files will be processed every 5 seconds.")
print("The stream will run until manually stopped.")
print("=" * 80 + "\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. [OK] Syslog bronze ingestion started
# MAGIC 2. → Run `01_2_bronze_ingestion_snmp.py` on a separate compute for SNMP ingestion
# MAGIC 3. → Use `01_3_bronze_monitor.py` to monitor ingestion progress
# MAGIC 4. → Create DLT pipelines for silver and gold layers
# MAGIC 
# MAGIC **Note:** This stream runs continuously. To stop, detach or stop the notebook execution.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Mode (Reference)
# MAGIC 
# MAGIC For one-time batch processing, use the code below.

# COMMAND ----------

# BATCH MODE CODE (Commented)
"""
syslog_batch = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "text")
    .option("cloudFiles.schemaLocation", SYSLOG_SCHEMA_LOCATION)
    .option("cloudFiles.includeExistingFiles", "true")
    .option("pathGlobFilter", "*.txt")
    .load(SYSLOG_SFTP_PATH)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
    .writeStream
    .format("delta")
    .option("checkpointLocation", SYSLOG_CHECKPOINT)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(BRONZE_SYSLOG_TABLE)
)

syslog_batch.awaitTermination()
print("[OK] Syslog batch ingestion complete")

syslog_count = spark.table(BRONZE_SYSLOG_TABLE).count()
print(f"Total syslog records ingested: {syslog_count:,}")
display(spark.table(BRONZE_SYSLOG_TABLE).limit(10))
"""

