# Databricks notebook source
# MAGIC %md
# MAGIC # Retail Store Performance Demo - Bronze Metrics Ingestion (GCS)
# MAGIC 
# MAGIC This notebook implements Auto Loader for ingesting store operational metrics from Google Cloud Storage.
# MAGIC 
# MAGIC ## Features
# MAGIC - Real-time streaming ingestion using Auto Loader
# MAGIC - JSON format with automatic schema inference
# MAGIC - Schema evolution support (automatically adds new columns)
# MAGIC - File notification mode for low-latency ingestion
# MAGIC - Exactly-once processing guarantees
# MAGIC 
# MAGIC ## Architecture
# MAGIC ```
# MAGIC GCE VM → GCS Bucket (metrics/*.json) → Auto Loader → Bronze Delta Table
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

# GCS configuration
GCS_BUCKET = dbutils.secrets.get(scope="<YOUR_SECRET_SCOPE>", key="GCS_RETAIL_BUCKET")
# Or hardcode for demo: GCS_BUCKET = "<YOUR_GCS_BUCKET_NAME>"

# Unity Catalog configuration
CATALOG_NAME = "retail_analytics"
SCHEMA_NAME = "bronze"

# Table name
BRONZE_METRICS_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.metrics_raw"

# GCS path for store metrics JSON files
METRICS_GCS_PATH = f"gs://{GCS_BUCKET}/metrics/"

# Checkpoint and schema locations (use Unity Catalog volume paths)
CHECKPOINT_BASE = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/checkpoints"
SCHEMA_BASE = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/schemas"

METRICS_CHECKPOINT = f"{CHECKPOINT_BASE}/metrics_gcs"
METRICS_SCHEMA_LOCATION = f"{SCHEMA_BASE}/metrics_gcs"

print("Store Metrics Ingestion Configuration (GCS):")
print(f"  GCS Path: {METRICS_GCS_PATH}")
print(f"  Target Table: {BRONZE_METRICS_TABLE}")
print(f"  Checkpoint: {METRICS_CHECKPOINT}")
print(f"  Schema Location: {METRICS_SCHEMA_LOCATION}")
print(f"  Format: JSON Lines (NDJSON)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Metrics Ingestion
# MAGIC 
# MAGIC Ingest store operational metrics from GCS using Auto Loader with file notifications.
# MAGIC 
# MAGIC ### Schema Evolution
# MAGIC The `schemaEvolutionMode` is set to `addNewColumns` which means:
# MAGIC - New columns in incoming files are automatically added to the table
# MAGIC - Run `retail_data_generator.py --extended-schema` to add new columns

# COMMAND ----------

metrics_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", METRICS_SCHEMA_LOCATION)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("pathGlobFilter", "*.json")
    .option("rescuedDataColumn", "_rescued_data")
    .option("cloudFiles.useManagedFileEvents", "true")
    .load(METRICS_GCS_PATH)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
)

# Display schema
print("Metrics Stream Schema:")
metrics_stream.printSchema()

# COMMAND ----------

# Write metrics stream to bronze table (CONTINUOUS MODE)
metrics_query = (
    metrics_stream.writeStream
    .format("delta")
    .option("checkpointLocation", METRICS_CHECKPOINT)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .toTable(BRONZE_METRICS_TABLE)
)

print(f"[OK] Started store metrics continuous ingestion stream from GCS")
print(f"  Source: {METRICS_GCS_PATH}")
print(f"  Writing to: {BRONZE_METRICS_TABLE}")
print(f"  Checkpoint: {METRICS_CHECKPOINT}")
print(f"  Processing Interval: 5 seconds")
print(f"  Schema Evolution: Enabled (addNewColumns)")

print("\n" + "=" * 80)
print("METRICS STREAMING MODE ACTIVE (GCS → Delta)")
print("=" * 80)
print("Store metrics stream is now running continuously.")
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
# MAGIC    cd /opt/retail-generator
# MAGIC    python retail_data_generator.py --metrics-gcs-bucket gs://YOUR_BUCKET/metrics/ --extended-schema
# MAGIC    ```
# MAGIC 3. Watch the table schema automatically update with new columns:
# MAGIC    - `store_manager`
# MAGIC    - `operating_hours`
# MAGIC    - `last_remodel`
# MAGIC    - `square_footage`
# MAGIC    - `staff_count`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Current Schema

# COMMAND ----------

# Show current table schema
print("Current Bronze Metrics Table Schema:")
print("=" * 50)
try:
    schema_df = spark.sql(f"DESCRIBE TABLE {BRONZE_METRICS_TABLE}")
    display(schema_df)
except Exception as e:
    print(f"Table not yet created: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Recent Records

# COMMAND ----------

# View recent metrics records
try:
    display(spark.sql(f"""
        SELECT * FROM {BRONZE_METRICS_TABLE}
        ORDER BY ingestion_timestamp DESC
        LIMIT 20
    """))
except Exception as e:
    print(f"No data yet: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. [OK] Store metrics bronze ingestion started (from GCS)
# MAGIC 2. → Ensure `01_1_bronze_ingestion_events.py` is running for events (from SFTP)
# MAGIC 3. → Create DLT pipelines for silver and gold layers
# MAGIC 
# MAGIC **Note:** This stream runs continuously. To stop, detach or stop the notebook execution.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternative: Batch Mode (Reference)

# COMMAND ----------

# BATCH MODE CODE (Commented)
"""
metrics_batch = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", METRICS_SCHEMA_LOCATION)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("pathGlobFilter", "*.json")
    .option("rescuedDataColumn", "_rescued_data")
    .load(METRICS_GCS_PATH)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
    .writeStream
    .format("delta")
    .option("checkpointLocation", METRICS_CHECKPOINT)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(BRONZE_METRICS_TABLE)
)

metrics_batch.awaitTermination()
print("[OK] Metrics batch ingestion complete")

metrics_count = spark.table(BRONZE_METRICS_TABLE).count()
print(f"Total metrics records ingested: {metrics_count:,}")
display(spark.table(BRONZE_METRICS_TABLE).limit(10))
"""

