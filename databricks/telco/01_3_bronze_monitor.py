# Databricks notebook source
# MAGIC %md
# MAGIC # Telco Network Performance Demo - Bronze Layer Monitoring
# MAGIC 
# MAGIC This notebook monitors the bronze layer ingestion progress and enables Change Data Feed.
# MAGIC 
# MAGIC Run this notebook after starting `01_1_bronze_ingestion_syslog.py` and `01_2_bronze_ingestion_snmp.py` to:
# MAGIC - Enable Change Data Feed on bronze tables
# MAGIC - Monitor ingestion progress
# MAGIC - View recent records
# MAGIC - Check data quality

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Unity Catalog configuration (must match 01_bronze_ingestion.py)
CATALOG_NAME = "telus_networkperf"
SCHEMA_NAME = "bronze"

# Table names
BRONZE_SYSLOG_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.syslog_raw"
BRONZE_SNMP_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.snmp_raw"

print(f"Monitoring Tables:")
print(f"  Syslog: {BRONZE_SYSLOG_TABLE}")
print(f"  SNMP: {BRONZE_SNMP_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable Change Data Feed (for downstream processing)

# COMMAND ----------

# Enable CDF on bronze tables for downstream Delta Live Tables
try:
    spark.sql(f"ALTER TABLE {BRONZE_SYSLOG_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    print(f"[OK] Change Data Feed enabled on {BRONZE_SYSLOG_TABLE}")
except Exception as e:
    print(f"Note: {e}")

try:
    spark.sql(f"ALTER TABLE {BRONZE_SNMP_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    print(f"[OK] Change Data Feed enabled on {BRONZE_SNMP_TABLE}")
except Exception as e:
    print(f"Note: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor Continuous Ingestion
# MAGIC 
# MAGIC Use these queries to monitor the continuous ingestion progress.

# COMMAND ----------

# Check current ingestion progress
print("Current Ingestion Progress:")
print("=" * 80)

# Syslog progress
try:
    syslog_progress = spark.sql(f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT source_file) as total_files,
            MAX(ingestion_timestamp) as last_ingestion
        FROM {BRONZE_SYSLOG_TABLE}
    """).collect()[0]

    print(f"Syslog: {syslog_progress['total_records']:,} records from {syslog_progress['total_files']:,} files")
    print(f"  Last ingestion: {syslog_progress['last_ingestion']}")
except Exception as e:
    print(f"Syslog table not ready: {e}")

# SNMP progress
try:
    snmp_progress = spark.sql(f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT source_file) as total_files,
            MAX(ingestion_timestamp) as last_ingestion
        FROM {BRONZE_SNMP_TABLE}
    """).collect()[0]

    print(f"\nSNMP: {snmp_progress['total_records']:,} records from {snmp_progress['total_files']:,} files")
    print(f"  Last ingestion: {snmp_progress['last_ingestion']}")
except Exception as e:
    print(f"\nSNMP table not ready: {e}")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Recent Syslog Records

# COMMAND ----------

# View recent syslog records
display(spark.sql(f"""
    SELECT * FROM {BRONZE_SYSLOG_TABLE}
    ORDER BY ingestion_timestamp DESC
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Recent SNMP Records

# COMMAND ----------

# View recent SNMP records
display(spark.sql(f"""
    SELECT * FROM {BRONZE_SNMP_TABLE}
    ORDER BY ingestion_timestamp DESC
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# Check for any rescued data (malformed SNMP records)
from pyspark.sql.functions import col

rescued_data = spark.table(BRONZE_SNMP_TABLE).filter(col("_rescued_data").isNotNull())
rescued_count = rescued_data.count()

if rescued_count > 0:
    print(f"[WARNING] {rescued_count} records with rescued data found")
    display(rescued_data.limit(10))
else:
    print("[OK] No rescued data - all records parsed successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Statistics

# COMMAND ----------

# Get detailed statistics
print("=" * 80)
print("BRONZE LAYER INGESTION SUMMARY")
print("=" * 80)

# Syslog statistics
syslog_stats = spark.sql(f"""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT source_file) as total_files,
        MIN(ingestion_timestamp) as first_ingestion,
        MAX(ingestion_timestamp) as last_ingestion
    FROM {BRONZE_SYSLOG_TABLE}
""").collect()[0]

print(f"\nSyslog Ingestion:")
print(f"  Total Records: {syslog_stats['total_records']:,}")
print(f"  Total Files: {syslog_stats['total_files']:,}")
print(f"  First Ingestion: {syslog_stats['first_ingestion']}")
print(f"  Last Ingestion: {syslog_stats['last_ingestion']}")

# SNMP statistics
snmp_stats = spark.sql(f"""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT source_file) as total_files,
        MIN(ingestion_timestamp) as first_ingestion,
        MAX(ingestion_timestamp) as last_ingestion,
        COUNT(DISTINCT device_id) as unique_devices,
        COUNT(DISTINCT metric_name) as unique_metrics
    FROM {BRONZE_SNMP_TABLE}
""").collect()[0]

print(f"\nSNMP Ingestion:")
print(f"  Total Records: {snmp_stats['total_records']:,}")
print(f"  Total Files: {snmp_stats['total_files']:,}")
print(f"  First Ingestion: {snmp_stats['first_ingestion']}")
print(f"  Last Ingestion: {snmp_stats['last_ingestion']}")
print(f"  Unique Devices: {snmp_stats['unique_devices']:,}")
print(f"  Unique Metrics: {snmp_stats['unique_metrics']}")

print("\n" + "=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SNMP Metrics Distribution

# COMMAND ----------

# Show distribution of SNMP metrics
display(spark.sql(f"""
    SELECT 
        metric_name,
        COUNT(*) as record_count,
        ROUND(AVG(value), 2) as avg_value,
        ROUND(MIN(value), 2) as min_value,
        ROUND(MAX(value), 2) as max_value
    FROM {BRONZE_SNMP_TABLE}
    GROUP BY metric_name
    ORDER BY record_count DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Device Distribution

# COMMAND ----------

# Show distribution of devices
display(spark.sql(f"""
    SELECT 
        device_type,
        location,
        COUNT(DISTINCT device_id) as device_count,
        COUNT(*) as record_count
    FROM {BRONZE_SNMP_TABLE}
    GROUP BY device_type, location
    ORDER BY record_count DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Rate Over Time

# COMMAND ----------

# Show ingestion rate over time (records per minute)
display(spark.sql(f"""
    SELECT 
        date_trunc('minute', ingestion_timestamp) as minute,
        COUNT(*) as records_per_minute
    FROM {BRONZE_SNMP_TABLE}
    GROUP BY date_trunc('minute', ingestion_timestamp)
    ORDER BY minute DESC
    LIMIT 30
"""))

