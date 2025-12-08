-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Retail Store Performance Demo - Gold Layer Pipeline (SQL)
-- MAGIC 
-- MAGIC This notebook implements Lakeflow Declarative Pipelines (Delta Live Tables) using SQL syntax.
-- MAGIC 
-- MAGIC ## Features
-- MAGIC - Time-based aggregations for store KPIs
-- MAGIC - Store health monitoring
-- MAGIC - Dimension tables for analytics
-- MAGIC - Prepared for cross-cloud Delta Sharing integration
-- MAGIC 
-- MAGIC ## Architecture
-- MAGIC ```
-- MAGIC Silver Tables → Gold Aggregations → Gold Delta Tables → Metric Views
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold: Store Performance Metrics (5-Minute Aggregations)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE retail_analytics.gold.gold_store_performance_5min
COMMENT '5-minute aggregated store performance metrics'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
AS SELECT
  window.start AS window_start,
  window.end AS window_end,
  store_id,
  store_type,
  region,
  brand,
  district,
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
  
FROM retail_analytics.silver.silver_store_metrics
GROUP BY
  window(event_timestamp, '5 minutes'),
  store_id,
  store_type,
  region,
  brand,
  district,
  metric_name,
  metric_unit;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold: Store Events Aggregated

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE retail_analytics.gold.gold_store_events
COMMENT 'Aggregated and enriched store events for analysis'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
AS SELECT
  event_timestamp,
  store_id,
  region,
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
  
FROM retail_analytics.silver.silver_store_events;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold: Store Dimension Table
-- MAGIC 
-- MAGIC Structured for future integration with Supply Chain/ERP data via Delta Sharing.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE retail_analytics.gold.dim_stores
COMMENT 'Store dimension table for analytics and supply chain integration'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
AS SELECT DISTINCT
  store_id,
  store_type,
  region,
  brand,
  district,
  address,
  
  -- Generate store key for cross-cloud joins
  CONCAT(store_id, '_', region) AS store_key,
  
  -- Infer store tier from type
  CASE
    WHEN store_type = 'flagship' THEN 'tier1'
    WHEN store_type IN ('mall', 'warehouse') THEN 'tier2'
    WHEN store_type = 'outlet' THEN 'tier3'
    ELSE 'tier4'
  END AS store_tier,
  
  -- Generate surrogate key
  MD5(store_id) AS store_sk,
  
  -- Effective timestamps for SCD tracking
  current_timestamp() AS effective_start_date,
  CAST(NULL AS TIMESTAMP) AS effective_end_date,
  TRUE AS is_active,
  
  -- Ready for supply chain integration
  TRUE AS supply_chain_integration_ready,
  current_timestamp() AS last_updated
  
FROM retail_analytics.silver.silver_store_metrics;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold: Metrics by Region

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE retail_analytics.gold.gold_metrics_by_region
COMMENT 'Store metrics aggregated by region for geographic analysis'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
  window_start,
  window_end,
  region,
  metric_name,
  metric_unit,
  COUNT(store_id) AS store_count,
  AVG(avg_value) AS region_avg_value,
  MAX(max_value) AS region_max_value,
  MIN(min_value) AS region_min_value,
  AVG(p95_value) AS region_p95_value,
  SUM(anomaly_count) AS total_anomalies,
  SUM(sample_count) AS total_samples,
  current_timestamp() AS processed_timestamp
FROM retail_analytics.gold.gold_store_performance_5min
GROUP BY
  window_start,
  window_end,
  region,
  metric_name,
  metric_unit;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold: Hourly KPI Summary

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE retail_analytics.gold.gold_kpi_hourly
COMMENT 'Hourly aggregated retail KPIs for executive dashboards'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true'
)
AS 
WITH hourly_perf AS (
  SELECT
    DATE_TRUNC('hour', window_start) AS hour_start,
    metric_name,
    COUNT(store_id) AS store_count,
    AVG(avg_value) AS hourly_avg,
    MAX(max_value) AS hourly_max,
    AVG(p95_value) AS hourly_p95,
    SUM(anomaly_count) AS hourly_anomalies
  FROM retail_analytics.gold.gold_store_performance_5min
  GROUP BY DATE_TRUNC('hour', window_start), metric_name
),
hourly_events AS (
  SELECT
    DATE_TRUNC('hour', event_timestamp) AS hour_start,
    COUNT(*) AS total_events,
    SUM(CASE WHEN is_critical THEN 1 ELSE 0 END) AS critical_events,
    COUNT(DISTINCT store_id) AS affected_stores
  FROM retail_analytics.gold.gold_store_events
  GROUP BY DATE_TRUNC('hour', event_timestamp)
)
SELECT
  p.hour_start,
  p.metric_name,
  p.store_count,
  p.hourly_avg,
  p.hourly_max,
  p.hourly_p95,
  p.hourly_anomalies,
  COALESCE(e.total_events, 0) AS total_events,
  COALESCE(e.critical_events, 0) AS critical_events,
  COALESCE(e.affected_stores, 0) AS affected_stores,
  current_timestamp() AS processed_timestamp
FROM hourly_perf p
LEFT JOIN hourly_events e ON p.hour_start = e.hour_start;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold: Store Health Status
-- MAGIC 
-- MAGIC Calculate store health scores based on recent metrics.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE retail_analytics.gold.gold_store_health
COMMENT 'Current health status and metrics for each retail store'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
AS
WITH latest_metrics AS (
  SELECT
    store_id,
    store_type,
    region,
    brand,
    metric_name,
    avg_value,
    window_end,
    ROW_NUMBER() OVER (PARTITION BY store_id, metric_name ORDER BY window_end DESC) AS rn
  FROM retail_analytics.gold.gold_store_performance_5min
),
pivoted AS (
  SELECT
    store_id,
    store_type,
    region,
    MAX(CASE WHEN metric_name = 'hourly_sales' THEN avg_value END) AS current_hourly_sales,
    MAX(CASE WHEN metric_name = 'transactions_per_hour' THEN avg_value END) AS current_transactions,
    MAX(CASE WHEN metric_name = 'avg_basket_size' THEN avg_value END) AS current_basket_size,
    MAX(CASE WHEN metric_name = 'checkout_wait_time_sec' THEN avg_value END) AS current_wait_time_sec,
    MAX(CASE WHEN metric_name = 'conversion_rate_pct' THEN avg_value END) AS current_conversion_pct,
    MAX(CASE WHEN metric_name = 'return_rate_pct' THEN avg_value END) AS current_return_rate_pct,
    MAX(CASE WHEN metric_name = 'customer_traffic' THEN avg_value END) AS current_traffic
  FROM latest_metrics
  WHERE rn = 1
  GROUP BY store_id, store_type, region
),
recent_events AS (
  SELECT
    store_id,
    COUNT(*) AS event_count_1h,
    SUM(CASE WHEN is_critical THEN 1 ELSE 0 END) AS critical_event_count_1h,
    SUM(CASE WHEN event_category = 'inventory' THEN 1 ELSE 0 END) AS inventory_issues_1h,
    SUM(CASE WHEN event_category = 'checkout_issue' THEN 1 ELSE 0 END) AS checkout_issues_1h,
    SUM(CASE WHEN event_category = 'security' THEN 1 ELSE 0 END) AS security_issues_1h
  FROM retail_analytics.gold.gold_store_events
  WHERE event_timestamp >= current_timestamp() - INTERVAL 1 HOUR
  GROUP BY store_id
)
SELECT
  p.store_id,
  p.store_type,
  p.region,
  COALESCE(p.current_hourly_sales, 0) AS current_hourly_sales,
  COALESCE(p.current_transactions, 0) AS current_transactions,
  COALESCE(p.current_basket_size, 0) AS current_basket_size,
  COALESCE(p.current_wait_time_sec, 0) AS current_wait_time_sec,
  COALESCE(p.current_conversion_pct, 0) AS current_conversion_pct,
  COALESCE(p.current_return_rate_pct, 0) AS current_return_rate_pct,
  COALESCE(p.current_traffic, 0) AS current_traffic,
  COALESCE(e.event_count_1h, 0) AS event_count_1h,
  COALESCE(e.critical_event_count_1h, 0) AS critical_event_count_1h,
  COALESCE(e.inventory_issues_1h, 0) AS inventory_issues_1h,
  COALESCE(e.checkout_issues_1h, 0) AS checkout_issues_1h,
  COALESCE(e.security_issues_1h, 0) AS security_issues_1h,
  
  -- Calculate health score (0-100, higher is better)
  LEAST(100, GREATEST(0,
    100
    - CASE WHEN COALESCE(p.current_wait_time_sec, 0) > 300 THEN 30
           WHEN COALESCE(p.current_wait_time_sec, 0) > 180 THEN 15
           WHEN COALESCE(p.current_wait_time_sec, 0) > 120 THEN 5
           ELSE 0 END
    - CASE WHEN COALESCE(p.current_return_rate_pct, 0) > 12 THEN 25
           WHEN COALESCE(p.current_return_rate_pct, 0) > 8 THEN 15
           WHEN COALESCE(p.current_return_rate_pct, 0) > 5 THEN 5
           ELSE 0 END
    - CASE WHEN COALESCE(e.critical_event_count_1h, 0) > 10 THEN 30
           WHEN COALESCE(e.critical_event_count_1h, 0) > 5 THEN 15
           WHEN COALESCE(e.critical_event_count_1h, 0) > 0 THEN 5
           ELSE 0 END
    - CASE WHEN COALESCE(e.inventory_issues_1h, 0) > 5 THEN 15
           WHEN COALESCE(e.inventory_issues_1h, 0) > 2 THEN 10
           ELSE 0 END
  )) AS health_score,
  
  -- Health status category
  CASE
    WHEN LEAST(100, GREATEST(0,
      100
      - CASE WHEN COALESCE(p.current_wait_time_sec, 0) > 300 THEN 30 ELSE 0 END
      - CASE WHEN COALESCE(p.current_return_rate_pct, 0) > 12 THEN 25 ELSE 0 END
      - CASE WHEN COALESCE(e.critical_event_count_1h, 0) > 10 THEN 30 ELSE 0 END
    )) >= 90 THEN 'Excellent'
    WHEN LEAST(100, GREATEST(0,
      100
      - CASE WHEN COALESCE(p.current_wait_time_sec, 0) > 300 THEN 30 ELSE 0 END
      - CASE WHEN COALESCE(p.current_return_rate_pct, 0) > 12 THEN 25 ELSE 0 END
      - CASE WHEN COALESCE(e.critical_event_count_1h, 0) > 10 THEN 30 ELSE 0 END
    )) >= 75 THEN 'Good'
    WHEN LEAST(100, GREATEST(0,
      100
      - CASE WHEN COALESCE(p.current_wait_time_sec, 0) > 300 THEN 30 ELSE 0 END
      - CASE WHEN COALESCE(p.current_return_rate_pct, 0) > 12 THEN 25 ELSE 0 END
      - CASE WHEN COALESCE(e.critical_event_count_1h, 0) > 10 THEN 30 ELSE 0 END
    )) >= 50 THEN 'Fair'
    WHEN LEAST(100, GREATEST(0,
      100
      - CASE WHEN COALESCE(p.current_wait_time_sec, 0) > 300 THEN 30 ELSE 0 END
      - CASE WHEN COALESCE(p.current_return_rate_pct, 0) > 12 THEN 25 ELSE 0 END
      - CASE WHEN COALESCE(e.critical_event_count_1h, 0) > 10 THEN 30 ELSE 0 END
    )) >= 25 THEN 'Poor'
    ELSE 'Critical'
  END AS health_status,
  
  -- Alert flag
  (COALESCE(e.critical_event_count_1h, 0) > 0 OR 
   COALESCE(e.inventory_issues_1h, 0) > 3 OR
   COALESCE(e.security_issues_1h, 0) > 0 OR
   COALESCE(p.current_wait_time_sec, 0) > 300) AS requires_attention,
  
  current_timestamp() AS processed_timestamp
  
FROM pivoted p
LEFT JOIN recent_events e ON p.store_id = e.store_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pipeline Configuration Notes
-- MAGIC 
-- MAGIC To run this Delta Live Tables pipeline:
-- MAGIC 
-- MAGIC 1. Create a new DLT pipeline in the Databricks UI
-- MAGIC 2. Set the notebook path to this notebook
-- MAGIC 3. Configure the pipeline:
-- MAGIC    - **Target**: `retail_analytics.gold`
-- MAGIC    - **Pipeline Mode**: Triggered or Continuous
-- MAGIC    - **Compute**: Use serverless or dedicated cluster
-- MAGIC 
-- MAGIC 4. Start the pipeline
-- MAGIC 
-- MAGIC ## Cross-Cloud Integration Notes
-- MAGIC 
-- MAGIC The `dim_stores` table is prepared for Delta Sharing integration with Supply Chain/ERP systems:
-- MAGIC 
-- MAGIC - **Join Keys**: `store_id`, `region`, `store_key`
-- MAGIC - **Timestamps**: `effective_start_date`, `effective_end_date`
-- MAGIC - **SCD Support**: `is_active` flag for slowly changing dimensions

