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
  
  -- Extract region from hostname (store-region-category-###)
  regexp_extract(regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+(\\S+)', 1), '^store-([^-]+)', 1) AS region,
  
  -- Extract store category from structured data
  regexp_extract(regexp_extract(value, '(\\[.+?\\]|\\-)', 1), 'storeCategory="([^"]+)"', 1) AS store_category,
  
  -- Classify event category (apparel + fast food)
  CASE 
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) = 'TRANSACTION' THEN 'sales'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) IN ('PAYMENT_DECLINED', 'POS_ERROR', 'CHECKOUT_TIMEOUT', 'KIOSK_ERROR') THEN 'checkout_issue'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) = 'REFUND' THEN 'returns'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) IN ('INVENTORY_LOW', 'STOCKOUT') THEN 'inventory'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) = 'THEFT_ALERT' THEN 'security'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) = 'LOYALTY_SIGNUP' THEN 'loyalty'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) = 'PRICE_OVERRIDE' THEN 'pricing'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) IN ('DRIVE_THROUGH_TIMEOUT', 'ORDER_COMPLETE', 'ORDER_DELAYED') THEN 'drive_through'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) IN ('EQUIPMENT_FAILURE') THEN 'kitchen_ops'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) IN ('FOOD_SAFETY_ALERT') THEN 'food_safety'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) IN ('MOBILE_ORDER_READY', 'ONLINE_PICKUP_READY') THEN 'mobile_order'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) IN ('WRONG_ORDER_REPORTED') THEN 'customer_service'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) IN ('FITTING_ROOM_WAIT') THEN 'fitting_room'
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
  CONSTRAINT valid_return_rate EXPECT (metric_name != 'return_rate_pct' OR (value >= 0 AND value <= 100)),
  CONSTRAINT valid_drive_through_wait EXPECT (metric_name != 'drive_through_wait_time_sec' OR value <= 600),
  CONSTRAINT valid_accuracy_pct EXPECT (metric_name != 'drive_through_order_accuracy_pct' OR (value >= 0 AND value <= 100)),
  CONSTRAINT valid_waste_pct EXPECT (metric_name != 'food_waste_pct' OR (value >= 0 AND value <= 100)),
  CONSTRAINT valid_fitting_room EXPECT (metric_name != 'fitting_room_usage_pct' OR (value >= 0 AND value <= 100)),
  CONSTRAINT valid_pickup_rate EXPECT (metric_name != 'online_pickup_rate_pct' OR (value >= 0 AND value <= 100))
)
COMMENT 'Cleaned and validated store operational metrics for apparel and fast food'
TBLPROPERTIES (
  'quality' = 'silver',
  'pipelines.autoOptimize.managed' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
AS SELECT
  to_timestamp(timestamp) AS event_timestamp,
  store_id,
  COALESCE(store_category, 
    CASE WHEN store_id LIKE '%ff%' THEN 'fast_food' 
         WHEN store_id LIKE '%ap%' THEN 'apparel'
         ELSE 'unknown' END) AS store_category,
  store_type,
  region,
  brand,
  district,
  address,
  CAST(latitude AS DOUBLE) AS latitude,
  CAST(longitude AS DOUBLE) AS longitude,
  oid,
  metric_name,
  CAST(value AS DOUBLE) AS value,
  
  -- Detect anomalies based on thresholds (category-aware)
  CASE
    -- Apparel metrics
    WHEN metric_name = 'hourly_sales' AND CAST(value AS DOUBLE) > 22000 THEN TRUE
    WHEN metric_name = 'checkout_wait_time_sec' AND CAST(value AS DOUBLE) > 300 THEN TRUE
    WHEN metric_name = 'conversion_rate_pct' AND CAST(value AS DOUBLE) > 40 THEN TRUE
    WHEN metric_name = 'return_rate_pct' AND CAST(value AS DOUBLE) > 12 THEN TRUE
    WHEN metric_name = 'customer_traffic' AND CAST(value AS DOUBLE) > 1800 THEN TRUE
    WHEN metric_name = 'fitting_room_usage_pct' AND CAST(value AS DOUBLE) > 80 THEN TRUE
    WHEN metric_name = 'online_pickup_rate_pct' AND CAST(value AS DOUBLE) > 35 THEN TRUE
    -- Fast food metrics
    WHEN metric_name = 'drive_through_wait_time_sec' AND CAST(value AS DOUBLE) > 420 THEN TRUE
    WHEN metric_name = 'drive_through_order_accuracy_pct' AND CAST(value AS DOUBLE) < 90 THEN TRUE
    WHEN metric_name = 'speaker_to_window_time_sec' AND CAST(value AS DOUBLE) > 270 THEN TRUE
    WHEN metric_name = 'kitchen_ticket_time_sec' AND CAST(value AS DOUBLE) > 540 THEN TRUE
    WHEN metric_name = 'peak_hour_service_time_sec' AND CAST(value AS DOUBLE) > 650 THEN TRUE
    WHEN metric_name = 'food_waste_pct' AND CAST(value AS DOUBLE) > 20 THEN TRUE
    WHEN metric_name = 'mobile_order_pickup_time_sec' AND CAST(value AS DOUBLE) > 220 THEN TRUE
    ELSE FALSE
  END AS is_anomaly,
  
  -- Add metric unit
  CASE
    WHEN metric_name LIKE '%_pct' THEN 'percent'
    WHEN metric_name LIKE '%_sec' THEN 'seconds'
    WHEN metric_name IN ('hourly_sales', 'avg_basket_size', 'avg_order_value') THEN 'dollars'
    WHEN metric_name IN ('customer_traffic', 'transactions_per_hour') THEN 'count'
    WHEN metric_name = 'drive_through_throughput_per_hour' THEN 'cars_per_hour'
    WHEN metric_name = 'dine_in_table_turnover_rate' THEN 'tables_per_hour'
    WHEN metric_name = 'staff_productivity' THEN 'dollars_per_staff'
    ELSE 'unknown'
  END AS metric_unit,
  
  -- Severity level based on metric value (category-aware)
  CASE
    -- High severity
    WHEN (metric_name = 'checkout_wait_time_sec' AND CAST(value AS DOUBLE) > 300) OR
         (metric_name = 'return_rate_pct' AND CAST(value AS DOUBLE) > 12) OR
         (metric_name = 'drive_through_wait_time_sec' AND CAST(value AS DOUBLE) > 420) OR
         (metric_name = 'drive_through_order_accuracy_pct' AND CAST(value AS DOUBLE) < 88) OR
         (metric_name = 'food_waste_pct' AND CAST(value AS DOUBLE) > 20) THEN 'high'
    -- Medium severity
    WHEN (metric_name = 'checkout_wait_time_sec' AND CAST(value AS DOUBLE) > 180) OR
         (metric_name = 'return_rate_pct' AND CAST(value AS DOUBLE) > 8) OR
         (metric_name = 'drive_through_wait_time_sec' AND CAST(value AS DOUBLE) > 300) OR
         (metric_name = 'drive_through_order_accuracy_pct' AND CAST(value AS DOUBLE) < 92) OR
         (metric_name = 'kitchen_ticket_time_sec' AND CAST(value AS DOUBLE) > 420) THEN 'medium'
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
COMMENT 'Deduplicated and categorized store events from apparel and fast food stores'
TBLPROPERTIES (
  'quality' = 'silver',
  'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT DISTINCT
  event_timestamp,
  hostname AS store_id,
  COALESCE(store_category, 'unknown') AS store_category,
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

