# Databricks notebook source
# MAGIC %md
# MAGIC # Telco Network Performance Demo - Gold Layer Pipeline (Python Reference)
# MAGIC 
# MAGIC This is the **Python reference implementation** of the Gold Layer DLT pipeline.
# MAGIC 
# MAGIC **For production use, see: `03_gold_pipeline.sql` (SQL syntax recommended)**
# MAGIC 
# MAGIC This notebook implements Lakeflow Declarative Pipelines (Delta Live Tables) for the gold layer.
# MAGIC 
# MAGIC ## Features
# MAGIC - Time-based aggregations for network KPIs
# MAGIC - Device health monitoring
# MAGIC - Dimension tables for analytics
# MAGIC - Prepared for cross-cloud Delta Sharing integration
# MAGIC - Change Data Feed enabled for downstream consumption
# MAGIC 
# MAGIC ## Architecture
# MAGIC ```
# MAGIC Silver Tables → Gold Aggregations → Gold Delta Tables → Metric Views
# MAGIC ```

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Catalog and schema names
CATALOG_NAME = "telus_networkperf"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Network Performance Metrics (5-Minute Aggregations)

# COMMAND ----------

@dlt.table(
    name="gold_network_performance_5min",
    comment="5-minute aggregated network performance metrics by device",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
def gold_network_performance_5min():
    """
    Aggregate SNMP metrics into 5-minute windows with statistical measures.
    This table is optimized for time-series analysis and dashboards.
    """
    
    snmp_df = dlt.read("silver_snmp_metrics")
    
    # Aggregate by device and metric type in 5-minute windows
    agg_df = snmp_df.groupBy(
        window(col("event_timestamp"), "5 minutes").alias("time_window"),
        col("device_id"),
        col("device_type"),
        col("location"),
        col("vendor"),
        col("metric_name"),
        col("metric_unit")
    ).agg(
        # Statistical aggregations
        avg("value").alias("avg_value"),
        min("value").alias("min_value"),
        max("value").alias("max_value"),
        expr("percentile_approx(value, 0.50)").alias("p50_value"),
        expr("percentile_approx(value, 0.95)").alias("p95_value"),
        expr("percentile_approx(value, 0.99)").alias("p99_value"),
        stddev("value").alias("stddev_value"),
        count("*").alias("sample_count"),
        
        # Anomaly tracking
        sum(when(col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count"),
        max(when(col("is_anomaly"), col("value")).otherwise(0)).alias("max_anomaly_value"),
        
        # Quality metrics
        countDistinct("source_file").alias("source_file_count")
    ).select(
        col("time_window.start").alias("window_start"),
        col("time_window.end").alias("window_end"),
        col("device_id"),
        col("device_type"),
        col("location"),
        col("vendor"),
        col("metric_name"),
        col("metric_unit"),
        col("avg_value"),
        col("min_value"),
        col("max_value"),
        col("p50_value"),
        col("p95_value"),
        col("p99_value"),
        col("stddev_value"),
        col("sample_count"),
        col("anomaly_count"),
        col("max_anomaly_value"),
        col("source_file_count"),
        current_timestamp().alias("processed_timestamp")
    )
    
    return agg_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Device Health Status

# COMMAND ----------

@dlt.table(
    name="gold_device_health",
    comment="Current health status and metrics for each network device",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
def gold_device_health():
    """
    Calculate device health scores based on recent metrics and events.
    Provides real-time health monitoring dashboard data.
    """
    
    # Get latest metrics per device
    snmp_df = dlt.read("silver_snmp_metrics")
    events_df = dlt.read("silver_network_events")
    
    # Latest metric values per device
    window_spec = Window.partitionBy("device_id", "metric_name").orderBy(col("event_timestamp").desc())
    
    latest_metrics = snmp_df.withColumn(
        "row_num",
        row_number().over(window_spec)
    ).filter(
        col("row_num") == 1
    ).select(
        col("device_id"),
        col("device_type"),
        col("location"),
        col("vendor"),
        col("model"),
        col("ip_address"),
        col("metric_name"),
        col("value"),
        col("is_anomaly"),
        col("severity_level"),
        col("event_timestamp").alias("last_metric_timestamp")
    )
    
    # Pivot metrics for easier analysis
    pivoted_metrics = latest_metrics.groupBy(
        "device_id",
        "device_type",
        "location",
        "vendor",
        "model",
        "ip_address"
    ).pivot("metric_name").agg(
        first("value")
    )
    
    # Count recent critical events (last 1 hour)
    one_hour_ago = expr("current_timestamp() - INTERVAL 1 HOUR")
    
    recent_events = events_df.filter(
        col("event_timestamp") >= one_hour_ago
    ).groupBy(
        "device_id"
    ).agg(
        count("*").alias("event_count_1h"),
        sum(when(col("is_critical"), 1).otherwise(0)).alias("critical_event_count_1h"),
        max(col("event_timestamp")).alias("last_event_timestamp")
    )
    
    # Join metrics with events
    health_df = pivoted_metrics.join(
        recent_events,
        "device_id",
        "left"
    ).select(
        col("device_id"),
        col("device_type"),
        col("location"),
        col("vendor"),
        col("model"),
        col("ip_address"),
        
        # Individual metrics
        coalesce(col("latency_ms"), lit(0.0)).alias("current_latency_ms"),
        coalesce(col("packet_loss_pct"), lit(0.0)).alias("current_packet_loss_pct"),
        coalesce(col("throughput_mbps"), lit(0.0)).alias("current_throughput_mbps"),
        coalesce(col("jitter_ms"), lit(0.0)).alias("current_jitter_ms"),
        coalesce(col("error_rate"), lit(0.0)).alias("current_error_rate"),
        
        # Event counts
        coalesce(col("event_count_1h"), lit(0)).alias("event_count_1h"),
        coalesce(col("critical_event_count_1h"), lit(0)).alias("critical_event_count_1h"),
        col("last_event_timestamp")
    ).withColumn(
        # Calculate health score (0-100, higher is better)
        "health_score",
        least(
            lit(100),
            greatest(
                lit(0),
                lit(100) -
                # Deduct points for high latency
                when(col("current_latency_ms") > 150, lit(30))
                .when(col("current_latency_ms") > 100, lit(15))
                .when(col("current_latency_ms") > 50, lit(5))
                .otherwise(lit(0)) -
                # Deduct points for packet loss
                when(col("current_packet_loss_pct") > 3, lit(40))
                .when(col("current_packet_loss_pct") > 1, lit(20))
                .when(col("current_packet_loss_pct") > 0.5, lit(10))
                .otherwise(lit(0)) -
                # Deduct points for high jitter
                when(col("current_jitter_ms") > 40, lit(20))
                .when(col("current_jitter_ms") > 20, lit(10))
                .otherwise(lit(0)) -
                # Deduct points for critical events
                when(col("critical_event_count_1h") > 10, lit(30))
                .when(col("critical_event_count_1h") > 5, lit(15))
                .when(col("critical_event_count_1h") > 0, lit(5))
                .otherwise(lit(0))
            )
        )
    ).withColumn(
        # Health status category
        "health_status",
        when(col("health_score") >= 90, "Excellent")
        .when(col("health_score") >= 75, "Good")
        .when(col("health_score") >= 50, "Fair")
        .when(col("health_score") >= 25, "Poor")
        .otherwise("Critical")
    ).withColumn(
        # Alert flag
        "requires_attention",
        (col("health_score") < 50) | (col("critical_event_count_1h") > 0)
    ).withColumn(
        "processed_timestamp",
        current_timestamp()
    )
    
    return health_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Network Events Aggregated

# COMMAND ----------

@dlt.table(
    name="gold_network_events",
    comment="Aggregated and enriched network events for analysis",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
def gold_network_events():
    """
    Deduplicated and enriched network events with context.
    Optimized for event analysis and correlation.
    """
    
    events_df = dlt.read("silver_network_events")
    
    # Enrich events with additional context
    enriched_df = events_df.select(
        col("event_timestamp"),
        col("device_id"),
        col("device_type"),
        col("location"),
        col("app_name"),
        col("event_type"),
        col("event_category"),
        col("severity"),
        col("severity_name"),
        col("is_critical"),
        col("event_message"),
        col("structured_data")
    ).withColumn(
        # Extract hour of day for pattern analysis
        "hour_of_day",
        hour(col("event_timestamp"))
    ).withColumn(
        # Extract day of week
        "day_of_week",
        dayofweek(col("event_timestamp"))
    ).withColumn(
        # Time since event for aging analysis
        "event_age_minutes",
        (unix_timestamp(current_timestamp()) - unix_timestamp(col("event_timestamp"))) / 60
    ).withColumn(
        "processed_timestamp",
        current_timestamp()
    )
    
    return enriched_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Device Dimension Table
# MAGIC 
# MAGIC This dimension table is structured for future integration with IMS data from AWS via Delta Sharing.

# COMMAND ----------

@dlt.table(
    name="dim_devices",
    comment="Device dimension table for analytics and cross-cloud integration",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true"
    }
)
def dim_devices():
    """
    Create a dimension table of all network devices.
    Includes join keys (device_id, location) for future IMS data integration from AWS.
    """
    
    snmp_df = dlt.read("silver_snmp_metrics")
    
    # Get unique device information
    devices_df = snmp_df.select(
        col("device_id"),
        col("device_type"),
        col("location"),
        col("vendor"),
        col("model"),
        col("ip_address")
    ).distinct()
    
    # Enrich with additional attributes
    enriched_devices = devices_df.withColumn(
        # Generate device key for cross-cloud joins
        "device_key",
        concat(col("device_id"), lit("_"), col("location"))
    ).withColumn(
        # Infer region from location
        "region",
        when(col("location").contains("east"), "east")
        .when(col("location").contains("west"), "west")
        .when(col("location").contains("central"), "central")
        .when(col("location").contains("north"), "north")
        .when(col("location").contains("south"), "south")
        .otherwise("unknown")
    ).withColumn(
        # Infer device tier (core, distribution, access, edge)
        "device_tier",
        when(col("device_id").contains("core"), "core")
        .when(col("device_id").contains("dist"), "distribution")
        .when(col("device_id").contains("access"), "access")
        .when(col("device_id").contains("edge"), "edge")
        .otherwise("unknown")
    ).withColumn(
        # Generate surrogate key
        "device_sk",
        md5(col("device_id"))
    ).withColumn(
        # Effective timestamps for SCD tracking
        "effective_start_date",
        current_timestamp()
    ).withColumn(
        "effective_end_date",
        lit(None).cast("timestamp")
    ).withColumn(
        "is_active",
        lit(True)
    ).withColumn(
        # Ready for IMS integration - these fields will be join keys
        "ims_integration_ready",
        lit(True)
    ).withColumn(
        "last_updated",
        current_timestamp()
    )
    
    return enriched_devices

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Metric Summary by Location

# COMMAND ----------

@dlt.table(
    name="gold_metrics_by_location",
    comment="Network metrics aggregated by location for geographic analysis",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_metrics_by_location():
    """
    Aggregate network performance metrics by location.
    Useful for geographic analysis and capacity planning.
    """
    
    perf_df = dlt.read("gold_network_performance_5min")
    
    # Aggregate by location and metric
    location_metrics = perf_df.groupBy(
        col("window_start"),
        col("window_end"),
        col("location"),
        col("metric_name"),
        col("metric_unit")
    ).agg(
        count("device_id").alias("device_count"),
        avg("avg_value").alias("location_avg_value"),
        max("max_value").alias("location_max_value"),
        min("min_value").alias("location_min_value"),
        avg("p95_value").alias("location_p95_value"),
        sum("anomaly_count").alias("total_anomalies"),
        sum("sample_count").alias("total_samples")
    ).withColumn(
        "processed_timestamp",
        current_timestamp()
    )
    
    return location_metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Hourly KPI Summary

# COMMAND ----------

@dlt.table(
    name="gold_kpi_hourly",
    comment="Hourly aggregated network KPIs for executive dashboards",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_kpi_hourly():
    """
    Create hourly KPI summaries across all network dimensions.
    Optimized for executive dashboards and reporting.
    """
    
    perf_df = dlt.read("gold_network_performance_5min")
    events_df = dlt.read("gold_network_events")
    
    # Aggregate performance metrics to hourly
    hourly_perf = perf_df.groupBy(
        window(col("window_start"), "1 hour").alias("hour_window"),
        col("metric_name")
    ).agg(
        count("device_id").alias("device_count"),
        avg("avg_value").alias("hourly_avg"),
        max("max_value").alias("hourly_max"),
        avg("p95_value").alias("hourly_p95"),
        sum("anomaly_count").alias("hourly_anomalies")
    ).select(
        col("hour_window.start").alias("hour_start"),
        col("metric_name"),
        col("device_count"),
        col("hourly_avg"),
        col("hourly_max"),
        col("hourly_p95"),
        col("hourly_anomalies")
    )
    
    # Aggregate events to hourly
    hourly_events = events_df.groupBy(
        window(col("event_timestamp"), "1 hour").alias("hour_window")
    ).agg(
        count("*").alias("total_events"),
        sum(when(col("is_critical"), 1).otherwise(0)).alias("critical_events"),
        countDistinct("device_id").alias("affected_devices")
    ).select(
        col("hour_window.start").alias("hour_start"),
        col("total_events"),
        col("critical_events"),
        col("affected_devices")
    )
    
    # Combine performance and events
    kpi_df = hourly_perf.join(
        hourly_events,
        "hour_start",
        "left"
    ).withColumn(
        "processed_timestamp",
        current_timestamp()
    )
    
    return kpi_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Configuration Notes
# MAGIC 
# MAGIC To run this Delta Live Tables pipeline:
# MAGIC 
# MAGIC 1. Create a new DLT pipeline in the Databricks UI
# MAGIC 2. Set the notebook path to this notebook
# MAGIC 3. Configure the pipeline:
# MAGIC    - **Target**: `telco_network.gold`
# MAGIC    - **Storage Location**: Choose your cloud storage path
# MAGIC    - **Pipeline Mode**: Triggered or Continuous
# MAGIC    - **Enable Auto Scaling**: Yes
# MAGIC    - **Compute**: Use serverless or dedicated cluster
# MAGIC 
# MAGIC 4. Start the pipeline
# MAGIC 
# MAGIC ## Cross-Cloud Integration Notes
# MAGIC 
# MAGIC The `dim_devices` table is prepared for Delta Sharing integration with AWS:
# MAGIC 
# MAGIC - **Join Keys**: `device_id`, `location`, `device_key`
# MAGIC - **Timestamps**: `effective_start_date`, `effective_end_date`
# MAGIC - **SCD Support**: `is_active` flag for slowly changing dimensions
# MAGIC 
# MAGIC When you receive IMS data from AWS via Delta Share, you can join using:
# MAGIC ```sql
# MAGIC SELECT *
# MAGIC FROM telco_network.gold.dim_devices d
# MAGIC INNER JOIN aws_delta_share.ims_gold_table i
# MAGIC   ON d.device_id = i.device_id
# MAGIC   AND d.location = i.location
# MAGIC WHERE d.is_active = TRUE
# MAGIC ```

