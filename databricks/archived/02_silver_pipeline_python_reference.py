# Databricks notebook source
# MAGIC %md
# MAGIC # Telco Network Performance Demo - Silver Layer Pipeline (Python Reference)
# MAGIC 
# MAGIC This is the **Python reference implementation** of the Silver Layer DLT pipeline.
# MAGIC 
# MAGIC **For production use, see: `02_silver_pipeline.sql` (SQL syntax recommended)**
# MAGIC 
# MAGIC This notebook implements Lakeflow Declarative Pipelines (Delta Live Tables) for the silver layer.
# MAGIC 
# MAGIC ## Features
# MAGIC - Parse RFC 5424 syslog messages
# MAGIC - Clean and validate SNMP metrics
# MAGIC - Data quality expectations with Databricks Expectations
# MAGIC - Automatic schema evolution
# MAGIC - Event classification and enrichment
# MAGIC 
# MAGIC ## Architecture
# MAGIC ```
# MAGIC Bronze Tables → Silver Transformations → Silver Delta Tables
# MAGIC ```

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Catalog and schema names
CATALOG_NAME = "telus_networkperf"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# Bronze table names
BRONZE_SYSLOG_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.syslog_raw"
BRONZE_SNMP_TABLE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.snmp_raw"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Syslog Parsing
# MAGIC 
# MAGIC Parse RFC 5424 syslog messages and extract structured fields.

# COMMAND ----------

@dlt.table(
    name="silver_syslog_parsed",
    comment="Parsed and enriched syslog events from network devices",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_priority", "priority IS NOT NULL AND priority >= 0 AND priority <= 191")
@dlt.expect_or_drop("valid_device_id", "hostname IS NOT NULL AND hostname != ''")
@dlt.expect("valid_severity", "severity BETWEEN 0 AND 7")
def silver_syslog_parsed():
    """
    Parse RFC 5424 syslog messages from bronze layer.
    
    RFC 5424 Format:
    <PRI>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID [STRUCTURED-DATA] MSG
    """
    
    # Read from bronze syslog table
    bronze_df = dlt.read_stream(BRONZE_SYSLOG_TABLE)
    
    # RFC 5424 regex pattern
    # <134>1 2025-12-02T14:23:45.123Z hostname app procid msgid [sd] message
    rfc5424_pattern = r'^<(\d+)>(\d+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\[.+?\]|\-)\s+(.*)$'
    
    # Parse syslog message
    parsed_df = bronze_df.withColumn(
        "parsed",
        regexp_extract(col("value"), rfc5424_pattern, 0)
    ).filter(
        col("parsed") != ""  # Keep only successfully parsed messages
    )
    
    # Extract individual fields
    result_df = parsed_df.select(
        # Extract priority and calculate facility and severity
        regexp_extract(col("value"), rfc5424_pattern, 1).cast("int").alias("priority"),
        regexp_extract(col("value"), rfc5424_pattern, 2).cast("int").alias("version"),
        regexp_extract(col("value"), rfc5424_pattern, 3).alias("log_timestamp"),
        regexp_extract(col("value"), rfc5424_pattern, 4).alias("hostname"),
        regexp_extract(col("value"), rfc5424_pattern, 5).alias("app_name"),
        regexp_extract(col("value"), rfc5424_pattern, 6).alias("proc_id"),
        regexp_extract(col("value"), rfc5424_pattern, 7).alias("msg_id"),
        regexp_extract(col("value"), rfc5424_pattern, 8).alias("structured_data"),
        regexp_extract(col("value"), rfc5424_pattern, 9).alias("message"),
        col("value").alias("raw_message"),
        col("ingestion_timestamp"),
        col("source_file")
    ).withColumn(
        # Calculate facility (priority / 8)
        "facility",
        (col("priority") / 8).cast("int")
    ).withColumn(
        # Calculate severity (priority % 8)
        "severity",
        (col("priority") % 8).cast("int")
    ).withColumn(
        # Add severity name
        "severity_name",
        when(col("severity") == 0, "Emergency")
        .when(col("severity") == 1, "Alert")
        .when(col("severity") == 2, "Critical")
        .when(col("severity") == 3, "Error")
        .when(col("severity") == 4, "Warning")
        .when(col("severity") == 5, "Notice")
        .when(col("severity") == 6, "Informational")
        .when(col("severity") == 7, "Debug")
        .otherwise("Unknown")
    ).withColumn(
        # Parse timestamp to timestamp type
        "event_timestamp",
        to_timestamp(col("log_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    ).withColumn(
        # Extract device type from hostname (e.g., "router-core-01" -> "router")
        "device_type",
        regexp_extract(col("hostname"), r'^([^-]+)', 1)
    ).withColumn(
        # Extract location from hostname (e.g., "router-core-01" -> "core")
        "location",
        regexp_extract(col("hostname"), r'^[^-]+-([^-]+)', 1)
    ).withColumn(
        # Classify event type based on msg_id and message content
        "event_category",
        when(col("msg_id").isin("LINKUPDOWN", "IFDOWN", "IFUP"), "interface_event")
        .when(col("msg_id").isin("ADJCHANGE", "OSPFADJCHG"), "routing_event")
        .when(col("msg_id") == "SECURITY", "security_event")
        .when(col("msg_id").isin("HASTATE"), "availability_event")
        .when(col("msg_id").isin("FANFAIL", "TEMPWARN"), "hardware_event")
        .when(col("msg_id") == "CFGCHANGE", "configuration_event")
        .otherwise("other")
    ).withColumn(
        # Flag critical events (severity 0-3)
        "is_critical",
        col("severity") <= 3
    ).withColumn(
        # Processing timestamp
        "processed_timestamp",
        current_timestamp()
    )
    
    return result_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver SNMP Metrics
# MAGIC 
# MAGIC Clean, validate, and enrich SNMP metrics data.

# COMMAND ----------

@dlt.table(
    name="silver_snmp_metrics",
    comment="Cleaned and validated SNMP network performance metrics",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_or_drop("valid_device_id", "device_id IS NOT NULL AND device_id != ''")
@dlt.expect_or_drop("valid_metric_name", "metric_name IS NOT NULL")
@dlt.expect_or_drop("valid_value", "value IS NOT NULL AND value >= 0")
@dlt.expect("valid_latency", "metric_name != 'latency_ms' OR (value >= 0 AND value <= 1000)")
@dlt.expect("valid_packet_loss", "metric_name != 'packet_loss_pct' OR (value >= 0 AND value <= 100)")
@dlt.expect("valid_throughput", "metric_name != 'throughput_mbps' OR (value > 0 AND value <= 100000)")
@dlt.expect("valid_jitter", "metric_name != 'jitter_ms' OR (value >= 0 AND value <= 500)")
@dlt.expect("valid_error_rate", "metric_name != 'error_rate' OR (value >= 0 AND value <= 10000)")
def silver_snmp_metrics():
    """
    Clean and validate SNMP metrics from bronze layer.
    Apply data quality rules and detect anomalies.
    """
    
    # Read from bronze SNMP table
    bronze_df = dlt.read_stream(BRONZE_SNMP_TABLE)
    
    # Clean and enrich
    result_df = bronze_df.select(
        # Parse timestamp
        to_timestamp(col("timestamp")).alias("event_timestamp"),
        col("device_id"),
        col("device_type"),
        col("location"),
        col("vendor"),
        col("model"),
        col("ip_address"),
        col("oid"),
        col("metric_name"),
        col("value").cast("double").alias("value"),
        col("ingestion_timestamp"),
        col("source_file")
    ).withColumn(
        # Detect anomalies based on thresholds
        "is_anomaly",
        when(
            (col("metric_name") == "latency_ms") & (col("value") > 150), True
        ).when(
            (col("metric_name") == "packet_loss_pct") & (col("value") > 3), True
        ).when(
            (col("metric_name") == "throughput_mbps") & (col("value") > 9500), True
        ).when(
            (col("metric_name") == "jitter_ms") & (col("value") > 40), True
        ).when(
            (col("metric_name") == "error_rate") & (col("value") > 800), True
        ).otherwise(False)
    ).withColumn(
        # Add metric unit for clarity
        "metric_unit",
        when(col("metric_name").endswith("_ms"), "milliseconds")
        .when(col("metric_name").endswith("_pct"), "percent")
        .when(col("metric_name").endswith("_mbps"), "megabits_per_second")
        .when(col("metric_name") == "error_rate", "count")
        .otherwise("unknown")
    ).withColumn(
        # Add severity level based on metric value
        "severity_level",
        when(col("is_anomaly"), "high")
        .when(
            ((col("metric_name") == "latency_ms") & (col("value") > 100)) |
            ((col("metric_name") == "packet_loss_pct") & (col("value") > 1)) |
            ((col("metric_name") == "jitter_ms") & (col("value") > 20)) |
            ((col("metric_name") == "error_rate") & (col("value") > 400)),
            "medium"
        ).otherwise("normal")
    ).withColumn(
        # Calculate percentile rank within device type (using window function)
        "value_z_score",
        (col("value") - avg("value").over(Window.partitionBy("device_type", "metric_name"))) /
        stddev("value").over(Window.partitionBy("device_type", "metric_name"))
    ).withColumn(
        # Processing timestamp
        "processed_timestamp",
        current_timestamp()
    )
    
    return result_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Network Events Summary
# MAGIC 
# MAGIC Create a combined view of network events from syslog for easier analysis.

# COMMAND ----------

@dlt.table(
    name="silver_network_events",
    comment="Deduplicated and categorized network events",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
def silver_network_events():
    """
    Create a unified view of network events with deduplication and enrichment.
    """
    
    syslog_df = dlt.read("silver_syslog_parsed")
    
    # Select relevant event fields and deduplicate
    events_df = syslog_df.select(
        col("event_timestamp"),
        col("hostname").alias("device_id"),
        col("device_type"),
        col("location"),
        col("app_name"),
        col("msg_id").alias("event_type"),
        col("event_category"),
        col("severity"),
        col("severity_name"),
        col("is_critical"),
        col("message").alias("event_message"),
        col("structured_data"),
        col("processed_timestamp")
    ).dropDuplicates([
        "event_timestamp", "device_id", "event_type", "event_message"
    ])
    
    return events_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Metrics Table
# MAGIC 
# MAGIC Track data quality metrics for monitoring pipeline health.

# COMMAND ----------

@dlt.table(
    name="silver_data_quality_metrics",
    comment="Data quality metrics and statistics for silver layer",
    table_properties={
        "quality": "silver"
    }
)
def silver_data_quality_metrics():
    """
    Aggregate data quality metrics from silver tables.
    """
    
    syslog_df = dlt.read("silver_syslog_parsed")
    snmp_df = dlt.read("silver_snmp_metrics")
    
    # Calculate syslog quality metrics
    syslog_metrics = syslog_df.groupBy(
        window(col("processed_timestamp"), "5 minutes").alias("time_window")
    ).agg(
        lit("syslog").alias("source"),
        count("*").alias("total_records"),
        countDistinct("hostname").alias("unique_devices"),
        sum(when(col("is_critical"), 1).otherwise(0)).alias("critical_events"),
        countDistinct("event_category").alias("event_categories")
    ).select(
        col("time_window.start").alias("window_start"),
        col("time_window.end").alias("window_end"),
        col("source"),
        col("total_records"),
        col("unique_devices"),
        col("critical_events"),
        col("event_categories")
    )
    
    # Calculate SNMP quality metrics
    snmp_metrics = snmp_df.groupBy(
        window(col("processed_timestamp"), "5 minutes").alias("time_window")
    ).agg(
        lit("snmp").alias("source"),
        count("*").alias("total_records"),
        countDistinct("device_id").alias("unique_devices"),
        sum(when(col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count"),
        countDistinct("metric_name").alias("unique_metrics")
    ).select(
        col("time_window.start").alias("window_start"),
        col("time_window.end").alias("window_end"),
        col("source"),
        col("total_records"),
        col("unique_devices"),
        col("anomaly_count").alias("critical_events"),
        col("unique_metrics").alias("event_categories")
    )
    
    # Union both metrics
    return syslog_metrics.union(snmp_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Configuration Notes
# MAGIC 
# MAGIC To run this Delta Live Tables pipeline:
# MAGIC 
# MAGIC 1. Create a new DLT pipeline in the Databricks UI
# MAGIC 2. Set the notebook path to this notebook
# MAGIC 3. Configure the pipeline:
# MAGIC    - **Target**: `telco_network.silver`
# MAGIC    - **Storage Location**: Choose your cloud storage path
# MAGIC    - **Pipeline Mode**: Triggered or Continuous
# MAGIC    - **Enable Auto Scaling**: Yes
# MAGIC    - **Enable Schema Evolution**: Yes
# MAGIC 
# MAGIC 4. Start the pipeline
# MAGIC 
# MAGIC The pipeline will automatically:
# MAGIC - Read from bronze tables
# MAGIC - Apply transformations and expectations
# MAGIC - Write to silver tables in Unity Catalog
# MAGIC - Track data lineage
# MAGIC - Monitor data quality

