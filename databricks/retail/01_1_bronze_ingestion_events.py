# Databricks notebook source
# MAGIC %md
# MAGIC # Retail Store Performance Demo - Bronze Events Ingestion
# MAGIC 
# MAGIC This notebook implements Auto Loader for ingesting transaction/event logs from the SFTP server.
# MAGIC 
# MAGIC ## Features
# MAGIC - Real-time streaming ingestion using Auto Loader
# MAGIC - RFC 5424 event log format support
# MAGIC - Exactly-once processing guarantees
# MAGIC - Metadata tracking for lineage
# MAGIC 
# MAGIC ## Architecture
# MAGIC ```
# MAGIC SFTP Server (events/*.txt) → Auto Loader → Bronze Delta Table
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

# SFTP connection details
SFTP_HOST = dbutils.secrets.get(scope="<YOUR_SECRET_SCOPE>", key="SFTP_HOST")
SFTP_USERNAME = dbutils.secrets.get(scope="<YOUR_SECRET_SCOPE>", key="SFTP_USERNAME")
SFTP_PORT = "22"

# Unity Catalog configuration
CATALOG_NAME = "retail_analytics"
SCHEMA_NAME = "bronze"

# Table name
BRONZE_EVENTS_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.events_raw"

# SFTP path (relative to chroot - /sftp is the root for SFTP user)
EVENTS_SFTP_PATH = f"sftp://{SFTP_USERNAME}@{SFTP_HOST}:{SFTP_PORT}/retail/events/"

# Checkpoint and schema locations (use Unity Catalog volume paths)
CHECKPOINT_BASE = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/checkpoints"
SCHEMA_BASE = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/schemas"

EVENTS_CHECKPOINT = f"{CHECKPOINT_BASE}/events"
EVENTS_SCHEMA_LOCATION = f"{SCHEMA_BASE}/events"

print("Retail Events Ingestion Configuration:")
print(f"  SFTP Path: {EVENTS_SFTP_PATH}")
print(f"  Target Table: {BRONZE_EVENTS_TABLE}")
print(f"  Checkpoint: {EVENTS_CHECKPOINT}")
print(f"  Schema Location: {EVENTS_SCHEMA_LOCATION}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Events Ingestion
# MAGIC 
# MAGIC Ingest RFC 5424 transaction event logs from SFTP server using Auto Loader.

# COMMAND ----------

events_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "text")
    .option("cloudFiles.schemaLocation", EVENTS_SCHEMA_LOCATION)
    .option("cloudFiles.includeExistingFiles", "true")
    .option("pathGlobFilter", "*.txt")
    .load(EVENTS_SFTP_PATH)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
)

# Display schema
print("Events Stream Schema:")
events_stream.printSchema()

# COMMAND ----------

# Write events stream to bronze table (CONTINUOUS MODE)
events_query = (
    events_stream.writeStream
    .format("delta")
    .option("checkpointLocation", EVENTS_CHECKPOINT)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .toTable(BRONZE_EVENTS_TABLE)
)

print(f"[OK] Started events continuous ingestion stream")
print(f"  Writing to: {BRONZE_EVENTS_TABLE}")
print(f"  Checkpoint: {EVENTS_CHECKPOINT}")
print(f"  Processing Interval: 5 seconds")

print("\n" + "=" * 80)
print("EVENTS STREAMING MODE ACTIVE")
print("=" * 80)
print("Transaction events stream is now running continuously.")
print("New files will be processed every 5 seconds.")
print("The stream will run until manually stopped.")
print("=" * 80 + "\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. [OK] Events bronze ingestion started
# MAGIC 2. → Run `01_2_bronze_ingestion_metrics.py` on a separate compute for store metrics
# MAGIC 3. → Create DLT pipelines for silver and gold layers
# MAGIC 
# MAGIC **Note:** This stream runs continuously. To stop, detach or stop the notebook execution.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Mode (Reference)

# COMMAND ----------

# BATCH MODE CODE (Commented)
"""
events_batch = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "text")
    .option("cloudFiles.schemaLocation", EVENTS_SCHEMA_LOCATION)
    .option("cloudFiles.includeExistingFiles", "true")
    .option("pathGlobFilter", "*.txt")
    .load(EVENTS_SFTP_PATH)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
    .writeStream
    .format("delta")
    .option("checkpointLocation", EVENTS_CHECKPOINT)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(BRONZE_EVENTS_TABLE)
)

events_batch.awaitTermination()
print("[OK] Events batch ingestion complete")

events_count = spark.table(BRONZE_EVENTS_TABLE).count()
print(f"Total events records ingested: {events_count:,}")
display(spark.table(BRONZE_EVENTS_TABLE).limit(10))
"""

