-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Telco Network Performance Demo - Gold Layer Pipeline (SQL)
-- MAGIC 
-- MAGIC This notebook implements Lakeflow Declarative Pipelines (Delta Live Tables) using SQL syntax.
-- MAGIC 
-- MAGIC ## Features
-- MAGIC - Time-based aggregations for network KPIs
-- MAGIC - Device health monitoring
-- MAGIC - Dimension tables for analytics
-- MAGIC - Prepared for cross-cloud Delta Sharing integration
-- MAGIC 
-- MAGIC ## Architecture
-- MAGIC ```
-- MAGIC Silver Tables → Gold Aggregations → Gold Delta Tables → Metric Views
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold: Network Performance Metrics (5-Minute Aggregations)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE telus_networkperf.gold.gold_network_performance_5min
COMMENT '5-minute aggregated network performance metrics by device'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
AS SELECT
  window.start AS window_start,
  window.end AS window_end,
  device_id,
  device_type,
  location,
  vendor,
  metric_name,
  metric_unit,
  
  -- Statistical aggregations
  AVG(value) AS avg_value,
  MIN(value) AS min_value,
  MAX(value) AS max_value,
  PERCENTILE_APPROX(value, 0.50) AS p50_value,
  PERCENTILE_APPROX(value, 0.95) AS p95_value,
  PERCENTILE_APPROX(value, 0.99) AS p99_value,
  STDDEV(value) AS stddev_value,
  COUNT(*) AS sample_count,
  
  -- Anomaly tracking
  SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomaly_count,
  MAX(CASE WHEN is_anomaly THEN value ELSE 0 END) AS max_anomaly_value,
  
  -- Quality metrics
  COUNT(DISTINCT source_file) AS source_file_count,
  current_timestamp() AS processed_timestamp
  
FROM telus_networkperf.silver.silver_snmp_metrics
GROUP BY
  window(event_timestamp, '5 minutes'),
  device_id,
  device_type,
  location,
  vendor,
  metric_name,
  metric_unit;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold: Network Events Aggregated

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE telus_networkperf.gold.gold_network_events
COMMENT 'Aggregated and enriched network events for analysis'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
AS SELECT
  event_timestamp,
  device_id,
  device_type,
  location,
  app_name,
  event_type,
  event_category,
  severity,
  severity_name,
  is_critical,
  event_message,
  structured_data,
  
  -- Extract hour of day for pattern analysis
  HOUR(event_timestamp) AS hour_of_day,
  
  -- Extract day of week
  DAYOFWEEK(event_timestamp) AS day_of_week,
  
  -- Time since event for aging analysis
  (UNIX_TIMESTAMP(current_timestamp()) - UNIX_TIMESTAMP(event_timestamp)) / 60 AS event_age_minutes,
  
  current_timestamp() AS processed_timestamp
  
FROM telus_networkperf.silver.silver_network_events;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold: Device Dimension Table
-- MAGIC 
-- MAGIC Structured for future integration with IMS data from AWS via Delta Sharing.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE telus_networkperf.gold.dim_devices
COMMENT 'Device dimension table for analytics and cross-cloud integration'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
AS SELECT DISTINCT
  device_id,
  device_type,
  location,
  vendor,
  model,
  ip_address,
  
  -- Generate device key for cross-cloud joins
  CONCAT(device_id, '_', location) AS device_key,
  
  -- Infer region from location
  CASE
    WHEN location LIKE '%east%' THEN 'east'
    WHEN location LIKE '%west%' THEN 'west'
    WHEN location LIKE '%central%' THEN 'central'
    WHEN location LIKE '%north%' THEN 'north'
    WHEN location LIKE '%south%' THEN 'south'
    ELSE 'unknown'
  END AS region,
  
  -- Infer device tier
  CASE
    WHEN device_id LIKE '%core%' THEN 'core'
    WHEN device_id LIKE '%dist%' THEN 'distribution'
    WHEN device_id LIKE '%access%' THEN 'access'
    WHEN device_id LIKE '%edge%' THEN 'edge'
    ELSE 'unknown'
  END AS device_tier,
  
  -- Generate surrogate key
  MD5(device_id) AS device_sk,
  
  -- Effective timestamps for SCD tracking
  current_timestamp() AS effective_start_date,
  CAST(NULL AS TIMESTAMP) AS effective_end_date,
  TRUE AS is_active,
  
  -- Ready for IMS integration
  TRUE AS ims_integration_ready,
  current_timestamp() AS last_updated
  
FROM telus_networkperf.silver.silver_snmp_metrics;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold: Metric Summary by Location

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE telus_networkperf.gold.gold_metrics_by_location
COMMENT 'Network metrics aggregated by location for geographic analysis'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
  window_start,
  window_end,
  location,
  metric_name,
  metric_unit,
  COUNT(device_id) AS device_count,
  AVG(avg_value) AS location_avg_value,
  MAX(max_value) AS location_max_value,
  MIN(min_value) AS location_min_value,
  AVG(p95_value) AS location_p95_value,
  SUM(anomaly_count) AS total_anomalies,
  SUM(sample_count) AS total_samples,
  current_timestamp() AS processed_timestamp
FROM telus_networkperf.gold.gold_network_performance_5min
GROUP BY
  window_start,
  window_end,
  location,
  metric_name,
  metric_unit;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold: Hourly KPI Summary

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE telus_networkperf.gold.gold_kpi_hourly
COMMENT 'Hourly aggregated network KPIs for executive dashboards'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true'
)
AS 
WITH hourly_perf AS (
  SELECT
    DATE_TRUNC('hour', window_start) AS hour_start,
    metric_name,
    COUNT(device_id) AS device_count,
    AVG(avg_value) AS hourly_avg,
    MAX(max_value) AS hourly_max,
    AVG(p95_value) AS hourly_p95,
    SUM(anomaly_count) AS hourly_anomalies
  FROM telus_networkperf.gold.gold_network_performance_5min
  GROUP BY DATE_TRUNC('hour', window_start), metric_name
),
hourly_events AS (
  SELECT
    DATE_TRUNC('hour', event_timestamp) AS hour_start,
    COUNT(*) AS total_events,
    SUM(CASE WHEN is_critical THEN 1 ELSE 0 END) AS critical_events,
    COUNT(DISTINCT device_id) AS affected_devices
  FROM telus_networkperf.gold.gold_network_events
  GROUP BY DATE_TRUNC('hour', event_timestamp)
)
SELECT
  p.hour_start,
  p.metric_name,
  p.device_count,
  p.hourly_avg,
  p.hourly_max,
  p.hourly_p95,
  p.hourly_anomalies,
  COALESCE(e.total_events, 0) AS total_events,
  COALESCE(e.critical_events, 0) AS critical_events,
  COALESCE(e.affected_devices, 0) AS affected_devices,
  current_timestamp() AS processed_timestamp
FROM hourly_perf p
LEFT JOIN hourly_events e ON p.hour_start = e.hour_start;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold: Device Health Status
-- MAGIC 
-- MAGIC Calculate device health scores based on recent metrics.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE telus_networkperf.gold.gold_device_health
COMMENT 'Current health status and metrics for each network device'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
AS
WITH latest_metrics AS (
  SELECT
    device_id,
    device_type,
    location,
    vendor,
    metric_name,
    avg_value,
    window_end,
    ROW_NUMBER() OVER (PARTITION BY device_id, metric_name ORDER BY window_end DESC) AS rn
  FROM telus_networkperf.gold.gold_network_performance_5min
),
pivoted AS (
  SELECT
    device_id,
    device_type,
    location,
    MAX(CASE WHEN metric_name = 'latency_ms' THEN avg_value END) AS current_latency_ms,
    MAX(CASE WHEN metric_name = 'packet_loss_pct' THEN avg_value END) AS current_packet_loss_pct,
    MAX(CASE WHEN metric_name = 'throughput_mbps' THEN avg_value END) AS current_throughput_mbps,
    MAX(CASE WHEN metric_name = 'jitter_ms' THEN avg_value END) AS current_jitter_ms,
    MAX(CASE WHEN metric_name = 'error_rate' THEN avg_value END) AS current_error_rate
  FROM latest_metrics
  WHERE rn = 1
  GROUP BY device_id, device_type, location
),
recent_events AS (
  SELECT
    device_id,
    COUNT(*) AS event_count_1h,
    SUM(CASE WHEN is_critical THEN 1 ELSE 0 END) AS critical_event_count_1h
  FROM telus_networkperf.gold.gold_network_events
  WHERE event_timestamp >= current_timestamp() - INTERVAL 1 HOUR
  GROUP BY device_id
)
SELECT
  p.device_id,
  p.device_type,
  p.location,
  COALESCE(p.current_latency_ms, 0) AS current_latency_ms,
  COALESCE(p.current_packet_loss_pct, 0) AS current_packet_loss_pct,
  COALESCE(p.current_throughput_mbps, 0) AS current_throughput_mbps,
  COALESCE(p.current_jitter_ms, 0) AS current_jitter_ms,
  COALESCE(p.current_error_rate, 0) AS current_error_rate,
  COALESCE(e.event_count_1h, 0) AS event_count_1h,
  COALESCE(e.critical_event_count_1h, 0) AS critical_event_count_1h,
  
  -- Calculate health score (0-100, higher is better)
  LEAST(100, GREATEST(0,
    100
    - CASE WHEN COALESCE(p.current_latency_ms, 0) > 150 THEN 30
           WHEN COALESCE(p.current_latency_ms, 0) > 100 THEN 15
           WHEN COALESCE(p.current_latency_ms, 0) > 50 THEN 5
           ELSE 0 END
    - CASE WHEN COALESCE(p.current_packet_loss_pct, 0) > 3 THEN 40
           WHEN COALESCE(p.current_packet_loss_pct, 0) > 1 THEN 20
           WHEN COALESCE(p.current_packet_loss_pct, 0) > 0.5 THEN 10
           ELSE 0 END
    - CASE WHEN COALESCE(p.current_jitter_ms, 0) > 40 THEN 20
           WHEN COALESCE(p.current_jitter_ms, 0) > 20 THEN 10
           ELSE 0 END
    - CASE WHEN COALESCE(e.critical_event_count_1h, 0) > 10 THEN 30
           WHEN COALESCE(e.critical_event_count_1h, 0) > 5 THEN 15
           WHEN COALESCE(e.critical_event_count_1h, 0) > 0 THEN 5
           ELSE 0 END
  )) AS health_score,
  
  -- Health status category
  CASE
    WHEN LEAST(100, GREATEST(0, 100 - CASE WHEN COALESCE(p.current_latency_ms, 0) > 150 THEN 30 WHEN COALESCE(p.current_latency_ms, 0) > 100 THEN 15 ELSE 0 END - CASE WHEN COALESCE(p.current_packet_loss_pct, 0) > 3 THEN 40 ELSE 0 END)) >= 90 THEN 'Excellent'
    WHEN LEAST(100, GREATEST(0, 100 - CASE WHEN COALESCE(p.current_latency_ms, 0) > 150 THEN 30 WHEN COALESCE(p.current_latency_ms, 0) > 100 THEN 15 ELSE 0 END - CASE WHEN COALESCE(p.current_packet_loss_pct, 0) > 3 THEN 40 ELSE 0 END)) >= 75 THEN 'Good'
    WHEN LEAST(100, GREATEST(0, 100 - CASE WHEN COALESCE(p.current_latency_ms, 0) > 150 THEN 30 WHEN COALESCE(p.current_latency_ms, 0) > 100 THEN 15 ELSE 0 END - CASE WHEN COALESCE(p.current_packet_loss_pct, 0) > 3 THEN 40 ELSE 0 END)) >= 50 THEN 'Fair'
    WHEN LEAST(100, GREATEST(0, 100 - CASE WHEN COALESCE(p.current_latency_ms, 0) > 150 THEN 30 WHEN COALESCE(p.current_latency_ms, 0) > 100 THEN 15 ELSE 0 END - CASE WHEN COALESCE(p.current_packet_loss_pct, 0) > 3 THEN 40 ELSE 0 END)) >= 25 THEN 'Poor'
    ELSE 'Critical'
  END AS health_status,
  
  -- Alert flag
  (LEAST(100, GREATEST(0, 100)) < 50 OR COALESCE(e.critical_event_count_1h, 0) > 0) AS requires_attention,
  
  current_timestamp() AS processed_timestamp
  
FROM pivoted p
LEFT JOIN recent_events e ON p.device_id = e.device_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pipeline Configuration Notes
-- MAGIC 
-- MAGIC To run this Delta Live Tables pipeline:
-- MAGIC 
-- MAGIC 1. Create a new DLT pipeline in the Databricks UI
-- MAGIC 2. Set the notebook path to this notebook
-- MAGIC 3. Configure the pipeline:
-- MAGIC    - **Target**: `telus_networkperf.gold`
-- MAGIC    - **Pipeline Mode**: Triggered or Continuous
-- MAGIC    - **Compute**: Use serverless or dedicated cluster
-- MAGIC 
-- MAGIC 4. Start the pipeline
-- MAGIC 
-- MAGIC ## Cross-Cloud Integration Notes
-- MAGIC 
-- MAGIC The `dim_devices` table is prepared for Delta Sharing integration with AWS:
-- MAGIC 
-- MAGIC - **Join Keys**: `device_id`, `location`, `device_key`
-- MAGIC - **Timestamps**: `effective_start_date`, `effective_end_date`
-- MAGIC - **SCD Support**: `is_active` flag for slowly changing dimensions

