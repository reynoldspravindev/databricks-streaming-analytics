-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Telco Network Performance Demo - Silver Layer Pipeline (SQL)
-- MAGIC 
-- MAGIC This notebook implements Lakeflow Declarative Pipelines (Delta Live Tables) using SQL syntax.
-- MAGIC 
-- MAGIC ## Features
-- MAGIC - Parse RFC 5424 syslog messages
-- MAGIC - Clean and validate SNMP metrics
-- MAGIC - Data quality expectations
-- MAGIC - Automatic schema evolution
-- MAGIC - Event classification and enrichment
-- MAGIC 
-- MAGIC ## Architecture
-- MAGIC ```
-- MAGIC Bronze Tables → Silver Transformations → Silver Delta Tables
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver Syslog Parsed
-- MAGIC 
-- MAGIC Parse RFC 5424 syslog messages and extract structured fields.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE telus_networkperf.silver.silver_syslog_parsed (
  CONSTRAINT valid_priority EXPECT (priority IS NOT NULL AND priority >= 0 AND priority <= 191) ON VIOLATION DROP ROW,
  CONSTRAINT valid_device_id EXPECT (hostname IS NOT NULL AND hostname != '') ON VIOLATION DROP ROW,
  CONSTRAINT valid_severity EXPECT (severity BETWEEN 0 AND 7)
)
COMMENT 'Parsed and enriched syslog events from network devices'
TBLPROPERTIES (
  'quality' = 'silver',
  'pipelines.autoOptimize.managed' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
AS SELECT
  -- Extract priority and calculate facility and severity
  CAST(regexp_extract(value, '^<(\\d+)>', 1) AS INT) AS priority,
  CAST(regexp_extract(value, '^<\\d+>(\\d+)', 1) AS INT) AS version,
  regexp_extract(value, '^<\\d+>\\d+\\s+(\\S+)', 1) AS log_timestamp,
  regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+(\\S+)', 1) AS hostname,
  regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) AS app_name,
  regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) AS proc_id,
  regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) AS msg_id,
  regexp_extract(value, '(\\[.+?\\]|\\-)', 1) AS structured_data,
  regexp_extract(value, '\\]\\s+(.*)$|\\-\\s+(.*)$', 1) AS message,
  value AS raw_message,
  
  -- Calculate facility and severity
  CAST(CAST(regexp_extract(value, '^<(\\d+)>', 1) AS INT) / 8 AS INT) AS facility,
  CAST(regexp_extract(value, '^<(\\d+)>', 1) AS INT) % 8 AS severity,
  
  -- Severity name
  CASE CAST(regexp_extract(value, '^<(\\d+)>', 1) AS INT) % 8
    WHEN 0 THEN 'Emergency'
    WHEN 1 THEN 'Alert'
    WHEN 2 THEN 'Critical'
    WHEN 3 THEN 'Error'
    WHEN 4 THEN 'Warning'
    WHEN 5 THEN 'Notice'
    WHEN 6 THEN 'Informational'
    WHEN 7 THEN 'Debug'
    ELSE 'Unknown'
  END AS severity_name,
  
  -- Parse timestamp
  to_timestamp(regexp_extract(value, '^<\\d+>\\d+\\s+(\\S+)', 1), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") AS event_timestamp,
  
  -- Extract device type from hostname
  regexp_extract(regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+(\\S+)', 1), '^([^-]+)', 1) AS device_type,
  
  -- Extract location from hostname
  regexp_extract(regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+(\\S+)', 1), '^[^-]+-([^-]+)', 1) AS location,
  
  -- Classify event category
  CASE 
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) IN ('LINKUPDOWN', 'IFDOWN', 'IFUP') THEN 'interface_event'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) IN ('ADJCHANGE', 'OSPFADJCHG') THEN 'routing_event'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) = 'SECURITY' THEN 'security_event'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) = 'HASTATE' THEN 'availability_event'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) IN ('FANFAIL', 'TEMPWARN') THEN 'hardware_event'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) = 'CFGCHANGE' THEN 'configuration_event'
    ELSE 'other'
  END AS event_category,
  
  -- Flag critical events (severity 0-3)
  CAST(regexp_extract(value, '^<(\\d+)>', 1) AS INT) % 8 <= 3 AS is_critical,
  
  ingestion_timestamp,
  source_file,
  current_timestamp() AS processed_timestamp
  
FROM STREAM(telus_networkperf.bronze.syslog_raw)
WHERE regexp_extract(value, '^<(\\d+)>', 1) IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver SNMP Metrics
-- MAGIC 
-- MAGIC Clean, validate, and enrich SNMP metrics data.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE telus_networkperf.silver.silver_snmp_metrics (
  CONSTRAINT valid_device_id EXPECT (device_id IS NOT NULL AND device_id != '') ON VIOLATION DROP ROW,
  CONSTRAINT valid_metric_name EXPECT (metric_name IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_value EXPECT (value IS NOT NULL AND value >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_latency EXPECT (metric_name != 'latency_ms' OR (value >= 0 AND value <= 1000)),
  CONSTRAINT valid_packet_loss EXPECT (metric_name != 'packet_loss_pct' OR (value >= 0 AND value <= 100)),
  CONSTRAINT valid_throughput EXPECT (metric_name != 'throughput_mbps' OR (value > 0 AND value <= 100000)),
  CONSTRAINT valid_jitter EXPECT (metric_name != 'jitter_ms' OR (value >= 0 AND value <= 500)),
  CONSTRAINT valid_error_rate EXPECT (metric_name != 'error_rate' OR (value >= 0 AND value <= 10000))
)
COMMENT 'Cleaned and validated SNMP network performance metrics'
TBLPROPERTIES (
  'quality' = 'silver',
  'pipelines.autoOptimize.managed' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
AS SELECT
  to_timestamp(timestamp) AS event_timestamp,
  device_id,
  device_type,
  location,
  vendor,
  model,
  ip_address,
  oid,
  metric_name,
  CAST(value AS DOUBLE) AS value,
  
  -- Detect anomalies based on thresholds
  CASE
    WHEN metric_name = 'latency_ms' AND CAST(value AS DOUBLE) > 150 THEN TRUE
    WHEN metric_name = 'packet_loss_pct' AND CAST(value AS DOUBLE) > 3 THEN TRUE
    WHEN metric_name = 'throughput_mbps' AND CAST(value AS DOUBLE) > 9500 THEN TRUE
    WHEN metric_name = 'jitter_ms' AND CAST(value AS DOUBLE) > 40 THEN TRUE
    WHEN metric_name = 'error_rate' AND CAST(value AS DOUBLE) > 800 THEN TRUE
    ELSE FALSE
  END AS is_anomaly,
  
  -- Add metric unit
  CASE
    WHEN metric_name LIKE '%_ms' THEN 'milliseconds'
    WHEN metric_name LIKE '%_pct' THEN 'percent'
    WHEN metric_name LIKE '%_mbps' THEN 'megabits_per_second'
    WHEN metric_name = 'error_rate' THEN 'count'
    ELSE 'unknown'
  END AS metric_unit,
  
  -- Severity level based on metric value
  CASE
    WHEN (metric_name = 'latency_ms' AND CAST(value AS DOUBLE) > 150) OR
         (metric_name = 'packet_loss_pct' AND CAST(value AS DOUBLE) > 3) OR
         (metric_name = 'throughput_mbps' AND CAST(value AS DOUBLE) > 9500) OR
         (metric_name = 'jitter_ms' AND CAST(value AS DOUBLE) > 40) OR
         (metric_name = 'error_rate' AND CAST(value AS DOUBLE) > 800) THEN 'high'
    WHEN (metric_name = 'latency_ms' AND CAST(value AS DOUBLE) > 100) OR
         (metric_name = 'packet_loss_pct' AND CAST(value AS DOUBLE) > 1) OR
         (metric_name = 'jitter_ms' AND CAST(value AS DOUBLE) > 20) OR
         (metric_name = 'error_rate' AND CAST(value AS DOUBLE) > 400) THEN 'medium'
    ELSE 'normal'
  END AS severity_level,
  
  ingestion_timestamp,
  source_file,
  current_timestamp() AS processed_timestamp
  
FROM STREAM(telus_networkperf.bronze.snmp_raw);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver Network Events
-- MAGIC 
-- MAGIC Deduplicated and categorized network events from syslog.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE telus_networkperf.silver.silver_network_events
COMMENT 'Deduplicated and categorized network events'
TBLPROPERTIES (
  'quality' = 'silver',
  'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT DISTINCT
  event_timestamp,
  hostname AS device_id,
  device_type,
  location,
  app_name,
  msg_id AS event_type,
  event_category,
  severity,
  severity_name,
  is_critical,
  message AS event_message,
  structured_data,
  processed_timestamp
FROM telus_networkperf.silver.silver_syslog_parsed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver Data Quality Metrics
-- MAGIC 
-- MAGIC Track data quality metrics for monitoring pipeline health.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE telus_networkperf.silver.silver_data_quality_metrics
COMMENT 'Data quality metrics and statistics for silver layer'
TBLPROPERTIES ('quality' = 'silver')
AS 
-- Syslog metrics
SELECT
  window.start AS window_start,
  window.end AS window_end,
  'syslog' AS source,
  COUNT(*) AS total_records,
  COUNT(DISTINCT hostname) AS unique_devices,
  SUM(CASE WHEN is_critical THEN 1 ELSE 0 END) AS critical_events,
  COUNT(DISTINCT event_category) AS event_categories
FROM telus_networkperf.silver.silver_syslog_parsed
GROUP BY window(processed_timestamp, '5 minutes')

UNION ALL

-- SNMP metrics
SELECT
  window.start AS window_start,
  window.end AS window_end,
  'snmp' AS source,
  COUNT(*) AS total_records,
  COUNT(DISTINCT device_id) AS unique_devices,
  SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS critical_events,
  COUNT(DISTINCT metric_name) AS event_categories
FROM telus_networkperf.silver.silver_snmp_metrics
GROUP BY window(processed_timestamp, '5 minutes');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pipeline Configuration Notes
-- MAGIC 
-- MAGIC To run this Delta Live Tables pipeline:
-- MAGIC 
-- MAGIC 1. Create a new DLT pipeline in the Databricks UI
-- MAGIC 2. Set the notebook path to this notebook
-- MAGIC 3. Configure the pipeline:
-- MAGIC    - **Target**: `telus_networkperf.silver`
-- MAGIC    - **Pipeline Mode**: Triggered or Continuous
-- MAGIC    - **Enable Schema Evolution**: Yes
-- MAGIC 
-- MAGIC 4. Start the pipeline

