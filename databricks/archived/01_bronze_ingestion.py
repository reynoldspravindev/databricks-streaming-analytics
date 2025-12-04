# Databricks notebook source
# MAGIC %md
# MAGIC # Telco Network Performance Demo - Bronze Layer Ingestion
# MAGIC 
# MAGIC This notebook implements Auto Loader for ingesting syslog and SNMP data from the SFTP server.
# MAGIC 
# MAGIC ## Features
# MAGIC - Real-time streaming ingestion using Auto Loader
# MAGIC - Automatic schema inference and evolution
# MAGIC - Exactly-once processing guarantees
# MAGIC - Metadata tracking for lineage
# MAGIC 
# MAGIC ## Architecture
# MAGIC ```
# MAGIC SFTP Server → Auto Loader → Bronze Delta Tables
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import *

# SFTP connection details (must match 00_setup_sftp_connection)
SFTP_HOST = dbutils.secrets.get(scope="<YOUR_SECRET_SCOPE>", key="SFTP_HOST")
SFTP_USERNAME = dbutils.secrets.get(scope="<YOUR_SECRET_SCOPE>", key="SFTP_USERNAME")
SFTP_PORT = "22"

# Unity Catalog configuration
CATALOG_NAME = "telus_networkperf"
SCHEMA_NAME = "bronze"

# Table names
BRONZE_SYSLOG_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.syslog_raw"
BRONZE_SNMP_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.snmp_raw"

# SFTP paths (relative to chroot - /sftp is the root for SFTP user)
SYSLOG_SFTP_PATH = f"sftp://{SFTP_USERNAME}@{SFTP_HOST}:{SFTP_PORT}/telco/syslog/"
SNMP_SFTP_PATH = f"sftp://{SFTP_USERNAME}@{SFTP_HOST}:{SFTP_PORT}/telco/snmp/"

# Checkpoint and schema locations (use Unity Catalog volume paths)
CHECKPOINT_BASE = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/checkpoints"
SCHEMA_BASE = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/schemas"

SYSLOG_CHECKPOINT = f"{CHECKPOINT_BASE}/syslog"
SYSLOG_SCHEMA_LOCATION = f"{SCHEMA_BASE}/syslog"

SNMP_CHECKPOINT = f"{CHECKPOINT_BASE}/snmp"
SNMP_SCHEMA_LOCATION = f"{SCHEMA_BASE}/snmp"

print("Configuration:")
print(f"  Syslog SFTP Path: {SYSLOG_SFTP_PATH}")
print(f"  SNMP SFTP Path: {SNMP_SFTP_PATH}")
print(f"  Bronze Syslog Table: {BRONZE_SYSLOG_TABLE}")
print(f"  Bronze SNMP Table: {BRONZE_SNMP_TABLE}")

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
    .option("cloudFiles.includeExistingFiles", "true")  # Process existing files
    .option("pathGlobFilter", "*.txt")  # Only process .txt files
    .load(SYSLOG_SFTP_PATH)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))  # Unity Catalog compatible
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
    .option("mergeSchema", "true")  # Enable schema evolution
    .outputMode("append")
    .trigger(processingTime="5 seconds")  # Process new files every 30 seconds
    .toTable(BRONZE_SYSLOG_TABLE)
)

print(f"✓ Started syslog continuous ingestion stream")
print(f"  Writing to: {BRONZE_SYSLOG_TABLE}")
print(f"  Checkpoint: {SYSLOG_CHECKPOINT}")
print(f"  Processing Interval: 5 seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze SNMP Ingestion
# MAGIC 
# MAGIC Ingest SNMP metrics CSV files from SFTP server using Auto Loader.

# COMMAND ----------

# Auto Loader will infer the CSV schema automatically
snmp_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SNMP_SCHEMA_LOCATION)
    .option("cloudFiles.inferColumnTypes", "true")  # Infer data types for CSV
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Enable schema evolution
    .option("cloudFiles.includeExistingFiles", "true")  # Process existing files
    .option("header", "true")  # CSV files have headers
    .option("pathGlobFilter", "*.csv")  # Only process .csv files
    .option("rescuedDataColumn", "_rescued_data")  # Store malformed data
    .load(SNMP_SFTP_PATH)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))  # Unity Catalog compatible
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
    .option("mergeSchema", "true")  # Enable schema evolution
    .outputMode("append")
    .trigger(processingTime="5 seconds")  # Process new files every 5 seconds
    .toTable(BRONZE_SNMP_TABLE)
)

print(f"✓ Started SNMP continuous ingestion stream")
print(f"  Writing to: {BRONZE_SNMP_TABLE}")
print(f"  Checkpoint: {SNMP_CHECKPOINT}")
print(f"  Processing Interval: 5 seconds")

print("\n" + "=" * 80)
print("CONTINUOUS STREAMING MODE ACTIVE")
print("=" * 80)
print("Both syslog and SNMP streams are now running continuously.")
print("New files will be processed every 5 seconds.")
print("The streams will run until manually stopped.")
print("=" * 80 + "\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. ✓ Bronze layer continuous ingestion started
# MAGIC 2. → Use `01_bronze_monitor.py` to monitor ingestion progress and enable CDF
# MAGIC 3. → Create DLT pipeline for `02_silver_pipeline.py` to parse and validate the data
# MAGIC 4. → Create DLT pipeline for `03_gold_pipeline.py` to create aggregated metrics
# MAGIC 5. → Run `04_metric_views.sql` to set up monitoring views
# MAGIC 
# MAGIC **Note:** The streams are running continuously. New files will be processed every 30 seconds.
# MAGIC To stop the streams, detach or stop the notebook execution.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Mode (Commented - For Reference)
# MAGIC 
# MAGIC For one-time batch processing instead of continuous streaming, use the code below.
# MAGIC This processes all available files and stops.

# COMMAND ----------

# BATCH MODE CODE (Commented)
"""
# Batch Syslog Ingestion
syslog_batch = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "text")
    .option("cloudFiles.schemaLocation", SYSLOG_SCHEMA_LOCATION)
    .option("cloudFiles.includeExistingFiles", "true")
    .option("pathGlobFilter", "*.txt")
    .load(SYSLOG_SFTP_PATH)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))  # Unity Catalog compatible
    .writeStream
    .format("delta")
    .option("checkpointLocation", SYSLOG_CHECKPOINT)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)  # Process available files and stop
    .toTable(BRONZE_SYSLOG_TABLE)
)

# Wait for completion
syslog_batch.awaitTermination()
print("✓ Syslog batch ingestion complete")

# Batch SNMP Ingestion
snmp_batch = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SNMP_SCHEMA_LOCATION)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("header", "true")
    .option("pathGlobFilter", "*.csv")
    .option("rescuedDataColumn", "_rescued_data")
    .load(SNMP_SFTP_PATH)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))  # Unity Catalog compatible
    .writeStream
    .format("delta")
    .option("checkpointLocation", SNMP_CHECKPOINT)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)  # Process available files and stop
    .toTable(BRONZE_SNMP_TABLE)
)

# Wait for completion
snmp_batch.awaitTermination()
print("✓ SNMP batch ingestion complete")

# Check ingestion results
syslog_count = spark.table(BRONZE_SYSLOG_TABLE).count()
snmp_count = spark.table(BRONZE_SNMP_TABLE).count()

print(f"\nTotal syslog records ingested: {syslog_count:,}")
print(f"Total SNMP records ingested: {snmp_count:,}")

# Show samples
print("\nSample syslog records:")
display(spark.table(BRONZE_SYSLOG_TABLE).limit(10))

print("\nSample SNMP records:")
display(spark.table(BRONZE_SNMP_TABLE).limit(10))
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. ✓ Bronze layer continuous ingestion started
# MAGIC 2. ✓ Monitor ingestion progress using the queries above
# MAGIC 3. → Create DLT pipeline for `02_silver_pipeline.py` to parse and validate the data
# MAGIC 4. → Create DLT pipeline for `03_gold_pipeline.py` to create aggregated metrics
# MAGIC 5. → Run `04_metric_views.sql` to set up monitoring views
# MAGIC 
# MAGIC **Note:** The streams are running continuously. New files will be processed every 30 seconds.
# MAGIC To stop the streams, detach or stop the notebook execution.

