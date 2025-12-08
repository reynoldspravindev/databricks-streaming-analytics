-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Retail Store Performance Demo - Silver Layer Pipeline (SQL)
-- MAGIC 
-- MAGIC This notebook implements Lakeflow Declarative Pipelines (Delta Live Tables) using SQL syntax.
-- MAGIC 
-- MAGIC ## Features
-- MAGIC - Parse RFC 5424 transaction event logs
-- MAGIC - Clean and validate store metrics
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
-- MAGIC ## Silver Transaction Events Parsed
-- MAGIC 
-- MAGIC Parse RFC 5424 transaction event logs and extract structured fields.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE retail_analytics.silver.silver_events_parsed (
  CONSTRAINT valid_priority EXPECT (priority IS NOT NULL AND priority >= 0 AND priority <= 191) ON VIOLATION DROP ROW,
  CONSTRAINT valid_store_id EXPECT (hostname IS NOT NULL AND hostname != '') ON VIOLATION DROP ROW,
  CONSTRAINT valid_severity EXPECT (severity BETWEEN 0 AND 7)
)
COMMENT 'Parsed transaction and store events'
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
  
  -- Extract region from hostname (store-region-###)
  regexp_extract(regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+(\\S+)', 1), '^store-([^-]+)', 1) AS region,
  
  -- Classify event category
  CASE 
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) = 'TRANSACTION' THEN 'sales'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) IN ('PAYMENT_DECLINED', 'POS_ERROR', 'CHECKOUT_TIMEOUT') THEN 'checkout_issue'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) = 'REFUND' THEN 'returns'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) IN ('INVENTORY_LOW', 'STOCKOUT') THEN 'inventory'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) = 'THEFT_ALERT' THEN 'security'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) = 'LOYALTY_SIGNUP' THEN 'loyalty'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) = 'PRICE_OVERRIDE' THEN 'pricing'
    ELSE 'other'
  END AS event_category,
  
  -- Flag critical events (severity 0-3)
  CAST(regexp_extract(value, '^<(\\d+)>', 1) AS INT) % 8 <= 3 AS is_critical,
  
  ingestion_timestamp,
  source_file,
  current_timestamp() AS processed_timestamp
  
FROM STREAM(retail_analytics.bronze.events_raw)
WHERE regexp_extract(value, '^<(\\d+)>', 1) IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver Store Metrics
-- MAGIC 
-- MAGIC Clean, validate, and enrich store operational metrics data.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE retail_analytics.silver.silver_store_metrics (
  CONSTRAINT valid_store_id EXPECT (store_id IS NOT NULL AND store_id != '') ON VIOLATION DROP ROW,
  CONSTRAINT valid_metric_name EXPECT (metric_name IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_value EXPECT (value IS NOT NULL AND value >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_sales EXPECT (metric_name != 'hourly_sales' OR value <= 100000),
  CONSTRAINT valid_conversion EXPECT (metric_name != 'conversion_rate_pct' OR (value >= 0 AND value <= 100)),
  CONSTRAINT valid_wait_time EXPECT (metric_name != 'checkout_wait_time_sec' OR value <= 1800),
  CONSTRAINT valid_return_rate EXPECT (metric_name != 'return_rate_pct' OR (value >= 0 AND value <= 100))
)
COMMENT 'Cleaned and validated store operational metrics'
TBLPROPERTIES (
  'quality' = 'silver',
  'pipelines.autoOptimize.managed' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
AS SELECT
  to_timestamp(timestamp) AS event_timestamp,
  store_id,
  store_type,
  region,
  brand,
  district,
  address,
  oid,
  metric_name,
  CAST(value AS DOUBLE) AS value,
  
  -- Detect anomalies based on thresholds
  CASE
    WHEN metric_name = 'hourly_sales' AND CAST(value AS DOUBLE) > 22000 THEN TRUE
    WHEN metric_name = 'checkout_wait_time_sec' AND CAST(value AS DOUBLE) > 300 THEN TRUE
    WHEN metric_name = 'conversion_rate_pct' AND CAST(value AS DOUBLE) > 40 THEN TRUE
    WHEN metric_name = 'return_rate_pct' AND CAST(value AS DOUBLE) > 12 THEN TRUE
    WHEN metric_name = 'customer_traffic' AND CAST(value AS DOUBLE) > 1800 THEN TRUE
    ELSE FALSE
  END AS is_anomaly,
  
  -- Add metric unit
  CASE
    WHEN metric_name LIKE '%_pct' THEN 'percent'
    WHEN metric_name LIKE '%_sec' THEN 'seconds'
    WHEN metric_name = 'hourly_sales' THEN 'dollars'
    WHEN metric_name = 'avg_basket_size' THEN 'dollars'
    WHEN metric_name = 'customer_traffic' THEN 'count'
    WHEN metric_name = 'transactions_per_hour' THEN 'count'
    WHEN metric_name = 'staff_productivity' THEN 'dollars_per_staff'
    ELSE 'unknown'
  END AS metric_unit,
  
  -- Severity level based on metric value
  CASE
    WHEN (metric_name = 'checkout_wait_time_sec' AND CAST(value AS DOUBLE) > 300) OR
         (metric_name = 'return_rate_pct' AND CAST(value AS DOUBLE) > 12) THEN 'high'
    WHEN (metric_name = 'checkout_wait_time_sec' AND CAST(value AS DOUBLE) > 180) OR
         (metric_name = 'return_rate_pct' AND CAST(value AS DOUBLE) > 8) THEN 'medium'
    ELSE 'normal'
  END AS severity_level,
  
  ingestion_timestamp,
  source_file,
  current_timestamp() AS processed_timestamp
  
FROM STREAM(retail_analytics.bronze.metrics_raw);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver Store Events (Deduplicated)
-- MAGIC 
-- MAGIC Deduplicated and categorized store events from transaction logs.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE retail_analytics.silver.silver_store_events
COMMENT 'Deduplicated and categorized store events'
TBLPROPERTIES (
  'quality' = 'silver',
  'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT DISTINCT
  event_timestamp,
  hostname AS store_id,
  region,
  app_name,
  msg_id AS event_type,
  event_category,
  severity,
  severity_name,
  is_critical,
  message AS event_message,
  structured_data,
  processed_timestamp
FROM retail_analytics.silver.silver_events_parsed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver Data Quality Metrics
-- MAGIC 
-- MAGIC Track data quality metrics for monitoring pipeline health.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE retail_analytics.silver.silver_data_quality_metrics
COMMENT 'Data quality metrics and statistics for silver layer'
TBLPROPERTIES ('quality' = 'silver')
AS 
-- Events metrics
SELECT
  window.start AS window_start,
  window.end AS window_end,
  'events' AS source,
  COUNT(*) AS total_records,
  COUNT(DISTINCT hostname) AS unique_stores,
  SUM(CASE WHEN is_critical THEN 1 ELSE 0 END) AS critical_events,
  COUNT(DISTINCT event_category) AS event_categories
FROM retail_analytics.silver.silver_events_parsed
GROUP BY window(processed_timestamp, '5 minutes')

UNION ALL

-- Store metrics
SELECT
  window.start AS window_start,
  window.end AS window_end,
  'metrics' AS source,
  COUNT(*) AS total_records,
  COUNT(DISTINCT store_id) AS unique_stores,
  SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS critical_events,
  COUNT(DISTINCT metric_name) AS event_categories
FROM retail_analytics.silver.silver_store_metrics
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
-- MAGIC    - **Target**: `retail_analytics.silver`
-- MAGIC    - **Pipeline Mode**: Triggered or Continuous
-- MAGIC    - **Enable Schema Evolution**: Yes
-- MAGIC 
-- MAGIC 4. Start the pipeline

