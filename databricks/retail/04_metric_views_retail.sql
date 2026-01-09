-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Retail Store Performance Demo - Unity Catalog Metric Views
-- MAGIC 
-- MAGIC This notebook creates Unity Catalog **Metric Views** for real-time store KPI monitoring.
-- MAGIC 
-- MAGIC ## What are Metric Views?
-- MAGIC Metric Views abstract complex business logic into centralized definitions, enabling:
-- MAGIC - **Standardized metrics** across teams and tools
-- MAGIC - **Flexible analysis** - define metrics once, query across any dimension
-- MAGIC - **Complex measures** like ratios and distinct counts
-- MAGIC - **SQL transparency** with governance
-- MAGIC 
-- MAGIC ## Reference
-- MAGIC - [Unity Catalog Metric Views](https://docs.databricks.com/gcp/en/metric-views/)
-- MAGIC - [Create Metric Views with SQL](https://docs.databricks.com/gcp/en/metric-views/create/sql)
-- MAGIC 
-- MAGIC ## Prerequisites
-- MAGIC - Databricks Runtime 17.2 or above
-- MAGIC - SELECT privilege on source tables
-- MAGIC - CREATE TABLE and USE SCHEMA privileges on target schema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Configuration

-- COMMAND ----------

USE CATALOG retail_analytics;

-- Create metrics schema if not exists
CREATE SCHEMA IF NOT EXISTS retail_analytics.metrics
COMMENT 'Unity Catalog Metric Views for retail store KPI monitoring';

USE SCHEMA metrics;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Metric View 1: Store Performance Metrics
-- MAGIC 
-- MAGIC Comprehensive store performance metrics with dimensions for flexible analysis.

-- COMMAND ----------

CREATE OR REPLACE VIEW retail_analytics.metrics.mv_store_performance
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Store performance KPIs for apparel and fast food stores"
  source: retail_analytics.gold.gold_store_performance_5min
  
  dimensions:
    - name: Time Window Start
      expr: window_start
      comment: "Start of 5-minute aggregation window"
    
    - name: Time Window End
      expr: window_end
      comment: "End of 5-minute aggregation window"
    
    - name: Store ID
      expr: store_id
      comment: "Unique identifier for retail store"
    
    - name: Store Category
      expr: store_category
      comment: "Store category: fast_food or apparel"
    
    - name: Store Type
      expr: store_type
      comment: "Type of store (varies by category)"
    
    - name: Region
      expr: region
      comment: "Geographic region of the store"
    
    - name: Brand
      expr: brand
      comment: "Brand name of the store"
    
    - name: District
      expr: district
      comment: "District or territory"
    
    - name: Latitude
      expr: latitude
      comment: "Store latitude coordinate for geospatial analysis"
    
    - name: Longitude
      expr: longitude
      comment: "Store longitude coordinate for geospatial analysis"
    
    - name: Metric Name
      expr: metric_name
      comment: "Type of metric (varies by category)"
    
    - name: Metric Unit
      expr: metric_unit
      comment: "Unit of measurement for the metric"
  
  measures:
    - name: Average Value
      expr: AVG(avg_value)
      comment: "Average metric value across samples"
    
    - name: Max Value
      expr: MAX(max_value)
      comment: "Maximum metric value observed"
    
    - name: Min Value
      expr: MIN(min_value)
      comment: "Minimum metric value observed"
    
    - name: P50 Value
      expr: AVG(p50_value)
      comment: "Median (50th percentile) metric value"
    
    - name: P95 Value
      expr: AVG(p95_value)
      comment: "95th percentile metric value"
    
    - name: P99 Value
      expr: AVG(p99_value)
      comment: "99th percentile metric value"
    
    - name: Total Samples
      expr: SUM(sample_count)
      comment: "Total number of samples collected"
    
    - name: Anomaly Count
      expr: SUM(anomaly_count)
      comment: "Number of anomalous readings detected"
    
    - name: Anomaly Rate
      expr: SUM(anomaly_count) / NULLIF(SUM(sample_count), 0) * 100
      comment: "Percentage of samples that are anomalies"
    
    - name: Store Count
      expr: COUNT(DISTINCT store_id)
      comment: "Number of unique stores"
$$;

-- COMMAND ----------

-- Test the metric view
SELECT 
  MEASURE(`Average Value`),
  MEASURE(`Max Value`),
  MEASURE(`Anomaly Count`),
  MEASURE(`Store Count`),
  `Metric Name`,
  `Region`
FROM retail_analytics.metrics.mv_store_performance
WHERE `Metric Name` = 'hourly_sales'
GROUP BY `Metric Name`, `Region`
ORDER BY MEASURE(`Average Value`) DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Metric View 2: Store Health Metrics
-- MAGIC 
-- MAGIC Real-time store health scores and status.

-- COMMAND ----------

CREATE OR REPLACE VIEW retail_analytics.metrics.mv_store_health
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Store health scores and operational metrics (apparel and fast food)"
  source: retail_analytics.gold.gold_store_health
  
  dimensions:
    - name: Store ID
      expr: store_id
      comment: "Unique store identifier"
    
    - name: Store Category
      expr: store_category
      comment: "Store category: fast_food or apparel"
    
    - name: Store Type
      expr: store_type
      comment: "Type of retail store"
    
    - name: Region
      expr: region
      comment: "Store region"
    
    - name: Latitude
      expr: latitude
      comment: "Store latitude for geospatial visualization"
    
    - name: Longitude
      expr: longitude
      comment: "Store longitude for geospatial visualization"
    
    - name: Health Status
      expr: health_status
      comment: "Health category: Excellent, Good, Fair, Poor, Critical"
    
    - name: Requires Attention
      expr: CASE WHEN requires_attention THEN 'Yes' ELSE 'No' END
      comment: "Whether store needs immediate attention"
  
  measures:
    - name: Store Count
      expr: COUNT(DISTINCT store_id)
      comment: "Number of stores"
    
    - name: Average Health Score
      expr: AVG(health_score)
      comment: "Average health score (0-100, higher is better)"
    
    - name: Min Health Score
      expr: MIN(health_score)
      comment: "Lowest health score"
    
    - name: Avg Hourly Sales
      expr: AVG(current_hourly_sales)
      comment: "Average current hourly sales"
    
    - name: Avg Wait Time (sec)
      expr: AVG(current_wait_time_sec)
      comment: "Average checkout wait time in seconds"
    
    - name: Avg Conversion Rate
      expr: AVG(current_conversion_pct)
      comment: "Average conversion rate percentage"
    
    - name: Avg Return Rate
      expr: AVG(current_return_rate_pct)
      comment: "Average return rate percentage"
    
    - name: Total Events (1h)
      expr: SUM(event_count_1h)
      comment: "Total events in the last hour"
    
    - name: Critical Events (1h)
      expr: SUM(critical_event_count_1h)
      comment: "Critical events in the last hour"
    
    - name: Inventory Issues (1h)
      expr: SUM(inventory_issues_1h)
      comment: "Inventory issues in the last hour"
    
    - name: Stores Needing Attention
      expr: SUM(CASE WHEN requires_attention THEN 1 ELSE 0 END)
      comment: "Count of stores requiring attention"
    
    - name: Critical Store Count
      expr: SUM(CASE WHEN health_status = 'Critical' THEN 1 ELSE 0 END)
      comment: "Number of stores in critical state"
$$;

-- COMMAND ----------

-- Test store health metric view
SELECT
  `Health Status`,
  MEASURE(`Store Count`),
  MEASURE(`Average Health Score`),
  MEASURE(`Critical Events (1h)`),
  MEASURE(`Stores Needing Attention`)
FROM retail_analytics.metrics.mv_store_health
GROUP BY `Health Status`
ORDER BY MEASURE(`Average Health Score`) ASC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Metric View 3: Store Events Metrics
-- MAGIC 
-- MAGIC Transaction and operational event analysis.

-- COMMAND ----------

CREATE OR REPLACE VIEW retail_analytics.metrics.mv_store_events
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Store event metrics for apparel and fast food stores"
  source: retail_analytics.gold.gold_store_events
  
  dimensions:
    - name: Event Timestamp
      expr: event_timestamp
      comment: "When the event occurred"
    
    - name: Store ID
      expr: store_id
      comment: "Store that generated the event"
    
    - name: Store Category
      expr: store_category
      comment: "Store category: fast_food or apparel"
    
    - name: Region
      expr: region
      comment: "Store region"
    
    - name: Event Type
      expr: event_type
      comment: "Type of event (varies by store category)"
    
    - name: Event Category
      expr: event_category
      comment: "Event category (includes drive_through, kitchen_ops, food_safety for fast food)"
    
    - name: Severity
      expr: severity
      comment: "Numeric severity level (0-7)"
    
    - name: Severity Name
      expr: severity_name
      comment: "Severity label"
    
    - name: Is Critical
      expr: CASE WHEN is_critical THEN 'Critical' ELSE 'Non-Critical' END
      comment: "Whether event is critical (severity 0-3)"
    
    - name: Hour of Day
      expr: hour_of_day
      comment: "Hour when event occurred (0-23)"
    
    - name: Day of Week
      expr: day_of_week
      comment: "Day of week (1=Sunday, 7=Saturday)"
  
  measures:
    - name: Event Count
      expr: COUNT(*)
      comment: "Total number of events"
    
    - name: Critical Event Count
      expr: SUM(CASE WHEN is_critical THEN 1 ELSE 0 END)
      comment: "Number of critical events"
    
    - name: Critical Event Rate
      expr: SUM(CASE WHEN is_critical THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100
      comment: "Percentage of events that are critical"
    
    - name: Affected Store Count
      expr: COUNT(DISTINCT store_id)
      comment: "Number of unique stores with events"
    
    - name: Average Event Age (min)
      expr: AVG(event_age_minutes)
      comment: "Average age of events in minutes"
$$;

-- COMMAND ----------

-- Test store events metric view
SELECT
  `Event Category`,
  `Severity Name`,
  MEASURE(`Event Count`),
  MEASURE(`Critical Event Count`),
  MEASURE(`Affected Store Count`)
FROM retail_analytics.metrics.mv_store_events
GROUP BY `Event Category`, `Severity Name`
ORDER BY MEASURE(`Event Count`) DESC
LIMIT 15;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Metric View 4: Regional Performance Metrics
-- MAGIC 
-- MAGIC Region-based store performance analysis.

-- COMMAND ----------

CREATE OR REPLACE VIEW retail_analytics.metrics.mv_regional_performance
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Store performance metrics aggregated by geographic region (apparel and fast food)"
  source: retail_analytics.gold.gold_metrics_by_region
  
  dimensions:
    - name: Time Window Start
      expr: window_start
      comment: "Start of aggregation window"
    
    - name: Time Window End
      expr: window_end
      comment: "End of aggregation window"
    
    - name: Region
      expr: region
      comment: "Geographic region"
    
    - name: Metric Name
      expr: metric_name
      comment: "Type of metric being measured"
    
    - name: Metric Unit
      expr: metric_unit
      comment: "Unit of measurement"
  
  measures:
    - name: Store Count
      expr: SUM(store_count)
      comment: "Number of stores in region"
    
    - name: Average Value
      expr: AVG(region_avg_value)
      comment: "Average metric value for region"
    
    - name: Max Value
      expr: MAX(region_max_value)
      comment: "Maximum metric value in region"
    
    - name: Min Value
      expr: MIN(region_min_value)
      comment: "Minimum metric value in region"
    
    - name: P95 Value
      expr: AVG(region_p95_value)
      comment: "95th percentile value for region"
    
    - name: Total Anomalies
      expr: SUM(total_anomalies)
      comment: "Total anomalies detected in region"
    
    - name: Total Samples
      expr: SUM(total_samples)
      comment: "Total samples collected from region"
    
    - name: Anomaly Rate
      expr: SUM(total_anomalies) / NULLIF(SUM(total_samples), 0) * 100
      comment: "Percentage of anomalous samples"
$$;

-- COMMAND ----------

-- Test regional performance metric view
SELECT
  `Region`,
  `Metric Name`,
  MEASURE(`Store Count`),
  MEASURE(`Average Value`),
  MEASURE(`Total Anomalies`),
  MEASURE(`Anomaly Rate`)
FROM retail_analytics.metrics.mv_regional_performance
GROUP BY `Region`, `Metric Name`
ORDER BY `Region`, `Metric Name`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Metric View 5: Hourly KPI Dashboard Metrics
-- MAGIC 
-- MAGIC Executive-level hourly KPI summaries.

-- COMMAND ----------

CREATE OR REPLACE VIEW retail_analytics.metrics.mv_kpi_dashboard
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Hourly KPI summary metrics for executive dashboards (apparel and fast food)"
  source: retail_analytics.gold.gold_kpi_hourly
  
  dimensions:
    - name: Hour
      expr: hour_start
      comment: "Start of the hour"
    
    - name: Metric Name
      expr: metric_name
      comment: "Type of KPI metric"
  
  measures:
    - name: Store Count
      expr: SUM(store_count)
      comment: "Number of stores measured"
    
    - name: Hourly Average
      expr: AVG(hourly_avg)
      comment: "Average value for the hour"
    
    - name: Hourly Max
      expr: MAX(hourly_max)
      comment: "Maximum value for the hour"
    
    - name: Hourly P95
      expr: AVG(hourly_p95)
      comment: "95th percentile for the hour"
    
    - name: Total Anomalies
      expr: SUM(hourly_anomalies)
      comment: "Total anomalies detected"
    
    - name: Total Events
      expr: SUM(total_events)
      comment: "Total events in the hour"
    
    - name: Critical Events
      expr: SUM(critical_events)
      comment: "Critical events in the hour"
    
    - name: Affected Stores
      expr: SUM(affected_stores)
      comment: "Stores affected by events"
$$;

-- COMMAND ----------

-- Test KPI dashboard metric view
SELECT
  `Metric Name`,
  MEASURE(`Hourly Average`),
  MEASURE(`Hourly Max`),
  MEASURE(`Total Anomalies`),
  MEASURE(`Critical Events`)
FROM retail_analytics.metrics.mv_kpi_dashboard
GROUP BY `Metric Name`
ORDER BY `Metric Name`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Metric View 6: HME Drive-Through Speed of Service (Fast Food Only)
-- MAGIC 
-- MAGIC Industry-standard HME Drive-Thru Timer System metrics for fast food stores.
-- MAGIC Includes OTD (Order-To-Delivery), Loop A/B timing, KDS bump time, and throughput.

-- COMMAND ----------

CREATE OR REPLACE VIEW retail_analytics.metrics.mv_hme_speed_of_service
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "HME Drive-Thru Timer System Speed of Service KPIs for fast food stores"
  source: retail_analytics.gold.gold_store_performance_5min
  
  dimensions:
    - name: Time Window Start
      expr: window_start
      comment: "Start of aggregation window"
    
    - name: Time Window End
      expr: window_end
      comment: "End of aggregation window"
    
    - name: Store ID
      expr: store_id
      comment: "Fast food store identifier"
    
    - name: Store Category
      expr: store_category
      comment: "Store category (fast_food or apparel)"
    
    - name: Store Type
      expr: store_type
      comment: "Type of fast food store"
    
    - name: Region
      expr: region
      comment: "Store region"
    
    - name: Brand
      expr: brand
      comment: "Fast food brand"
    
    - name: Latitude
      expr: latitude
      comment: "Store latitude for geospatial heat maps"
    
    - name: Longitude
      expr: longitude
      comment: "Store longitude for geospatial heat maps"
    
    - name: Metric Name
      expr: metric_name
      comment: "HME metric: drive_through_total_experience_time_sec, menu_board_time, window_time, kds_bump_time, cars_per_hour"
    
    - name: Data Source
      expr: data_source
      comment: "HME_TIMER_SYSTEM for fast food stores"
    
    - name: Sensor Type
      expr: sensor_type
      comment: "Hardware source: LOOP_A (speaker), LOOP_B (window), KDS_BUMP (kitchen), LOOP_INTEGRATED"
    
    - name: Hour of Day
      expr: HOUR(window_start)
      comment: "Hour of day (0-23) for peak analysis"
    
    - name: Time Period
      expr: CASE 
              WHEN HOUR(window_start) BETWEEN 11 AND 13 THEN 'Lunch Rush'
              WHEN HOUR(window_start) BETWEEN 17 AND 19 THEN 'Dinner Rush'
              WHEN HOUR(window_start) BETWEEN 7 AND 10 THEN 'Breakfast'
              WHEN HOUR(window_start) BETWEEN 14 AND 16 THEN 'Off-Peak Afternoon'
              WHEN HOUR(window_start) BETWEEN 20 AND 22 THEN 'Off-Peak Evening'
              ELSE 'Overnight'
            END
      comment: "Time period classification for operational analysis"
  
  measures:
    - name: Average Value
      expr: AVG(avg_value)
      comment: "Average metric value"
    
    - name: P50 Value
      expr: AVG(p50_value)
      comment: "Median metric value"
    
    - name: P95 Value
      expr: AVG(p95_value)
      comment: "95th percentile metric value"
    
    - name: P99 Value
      expr: AVG(p99_value)
      comment: "99th percentile metric value"
    
    - name: Max Value
      expr: MAX(max_value)
      comment: "Maximum metric value observed"
    
    - name: Min Value
      expr: MIN(min_value)
      comment: "Minimum metric value observed"
    
    - name: Total Samples
      expr: SUM(sample_count)
      comment: "Total samples collected"
    
    - name: Anomaly Count
      expr: SUM(anomaly_count)
      comment: "Number of anomalous readings"
    
    - name: Anomaly Rate
      expr: SUM(anomaly_count) / NULLIF(SUM(sample_count), 0) * 100
      comment: "Percentage of samples that are anomalies"
    
    - name: Store Count
      expr: COUNT(DISTINCT store_id)
      comment: "Number of stores"
    
    - name: Avg Order Value
      expr: AVG(avg_order_value_context)
      comment: "Average order dollar amount (for normalization analysis)"
    
    - name: Avg Item Count
      expr: AVG(avg_item_count_context)
      comment: "Average items per order (for complexity analysis)"
$$;

-- COMMAND ----------

-- Test HME Speed of Service metric view - Total Experience Time (OTD)
SELECT
  `Time Period`,
  `Brand`,
  MEASURE(`Store Count`),
  MEASURE(`Average Value`) as avg_otd_seconds,
  MEASURE(`P95 Value`) as p95_otd_seconds,
  MEASURE(`Anomaly Rate`) as anomaly_pct
FROM retail_analytics.metrics.mv_hme_speed_of_service
GROUP BY `Time Period`, `Brand`
ORDER BY MEASURE(`Average Value`) DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query Example: Root Cause Analysis
-- MAGIC Find which component is the bottleneck (Menu Board vs KDS vs Window)

-- COMMAND ----------

-- Root cause analysis: identify bottleneck
WITH component_times AS (
  SELECT
    `Store ID`,
    `Time Period`,
    MEASURE(`Average Value`) as avg_time,
    `Metric Name`,
    `Sensor Type`
  FROM retail_analytics.metrics.mv_hme_speed_of_service
  WHERE `Metric Name` IN (
    'drive_through_menu_board_time_sec',
    'drive_through_kds_bump_time_sec', 
    'drive_through_window_time_sec'
  )
  GROUP BY `Store ID`, `Time Period`, `Metric Name`, `Sensor Type`
)
SELECT
  `Store ID`,
  `Time Period`,
  MAX(CASE WHEN `Metric Name` = 'drive_through_menu_board_time_sec' THEN avg_time END) as menu_board_sec,
  MAX(CASE WHEN `Metric Name` = 'drive_through_kds_bump_time_sec' THEN avg_time END) as kds_bump_sec,
  MAX(CASE WHEN `Metric Name` = 'drive_through_window_time_sec' THEN avg_time END) as window_sec,
  CASE 
    WHEN MAX(CASE WHEN `Metric Name` = 'drive_through_menu_board_time_sec' THEN avg_time END) > 60 THEN 'Menu Board Issue - Train Staff'
    WHEN MAX(CASE WHEN `Metric Name` = 'drive_through_kds_bump_time_sec' THEN avg_time END) > 80 THEN 'Kitchen Production Issue'
    WHEN MAX(CASE WHEN `Metric Name` = 'drive_through_window_time_sec' THEN avg_time END) > 90 THEN 'Window Bottleneck'
    ELSE 'Peak Hour Volume'
  END as root_cause_diagnosis
FROM component_times
GROUP BY `Store ID`, `Time Period`
HAVING (menu_board_sec + kds_bump_sec + window_sec) > 360
ORDER BY (menu_board_sec + kds_bump_sec + window_sec) DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Metric View 7: Fast Food Hardware Events & Causality
-- MAGIC 
-- MAGIC Event-to-metric causality for HME system hardware failures and operational issues.

-- COMMAND ----------

CREATE OR REPLACE VIEW retail_analytics.metrics.mv_fast_food_event_causality
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Fast food event causality analysis - correlate hardware failures and operational issues with performance degradation"
  source: retail_analytics.gold.gold_store_events
  
  dimensions:
    - name: Event Timestamp
      expr: event_timestamp
      comment: "When the event occurred"
    
    - name: Store ID
      expr: store_id
      comment: "Store that generated the event"
    
    - name: Region
      expr: region
      comment: "Store region"
    
    - name: Store Category
      expr: store_category
      comment: "Store category (apparel or fast_food)"
    
    - name: Event Type
      expr: event_type
      comment: "Type of event including hardware: LOOP_SENSOR_MALFUNCTION, KDS_SCREEN_DOWN, EQUIPMENT_FAILURE, etc."
    
    - name: Event Category
      expr: event_category
      comment: "Category: hardware_failure, kitchen_ops, drive_through, food_safety, pos_system"
    
    - name: Is Hardware Event
      expr: CASE 
              WHEN event_type IN ('LOOP_SENSOR_MALFUNCTION', 'KDS_SCREEN_DOWN', 'POS_NETWORK_TIMEOUT', 'BUMP_BAR_STUCK', 'HME_SYSTEM_REBOOT')
              THEN 'Hardware Failure'
              ELSE 'Operational Issue'
            END
      comment: "Whether event is hardware-related"
    
    - name: Severity
      expr: severity
      comment: "Numeric severity level (0-7)"
    
    - name: Severity Name
      expr: severity_name
      comment: "Severity label"
    
    - name: Is Critical
      expr: CASE WHEN is_critical THEN 'Critical' ELSE 'Non-Critical' END
      comment: "Whether event is critical (severity 0-3)"
    
    - name: Hour of Day
      expr: hour_of_day
      comment: "Hour when event occurred (0-23)"
    
    - name: Time Period
      expr: CASE 
              WHEN hour_of_day BETWEEN 11 AND 13 THEN 'Lunch Rush'
              WHEN hour_of_day BETWEEN 17 AND 19 THEN 'Dinner Rush'
              ELSE 'Off-Peak'
            END
      comment: "Time period when event occurred"
  
  measures:
    - name: Event Count
      expr: COUNT(*)
      comment: "Total number of events"
    
    - name: Hardware Event Count
      expr: SUM(CASE WHEN event_type IN ('LOOP_SENSOR_MALFUNCTION', 'KDS_SCREEN_DOWN', 'POS_NETWORK_TIMEOUT', 'BUMP_BAR_STUCK', 'HME_SYSTEM_REBOOT') THEN 1 ELSE 0 END)
      comment: "Number of hardware-related events"
    
    - name: Operational Event Count
      expr: SUM(CASE WHEN event_type NOT IN ('LOOP_SENSOR_MALFUNCTION', 'KDS_SCREEN_DOWN', 'POS_NETWORK_TIMEOUT', 'BUMP_BAR_STUCK', 'HME_SYSTEM_REBOOT') THEN 1 ELSE 0 END)
      comment: "Number of operational events"
    
    - name: Critical Event Count
      expr: SUM(CASE WHEN is_critical THEN 1 ELSE 0 END)
      comment: "Number of critical events"
    
    - name: Critical Event Rate
      expr: SUM(CASE WHEN is_critical THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100
      comment: "Percentage of events that are critical"
    
    - name: Affected Store Count
      expr: COUNT(DISTINCT store_id)
      comment: "Number of unique stores with events"
    
    - name: Avg Event Age (min)
      expr: AVG(event_age_minutes)
      comment: "Average age of events in minutes"
    
    - name: Events Per Store
      expr: COUNT(*) / NULLIF(COUNT(DISTINCT store_id), 0)
      comment: "Average events per affected store"
$$;

-- COMMAND ----------

-- Test event causality metric view
SELECT
  `Event Type`,
  `Is Hardware Event`,
  `Severity Name`,
  `Time Period`,
  MEASURE(`Event Count`),
  MEASURE(`Critical Event Count`),
  MEASURE(`Affected Store Count`),
  MEASURE(`Events Per Store`)
FROM retail_analytics.metrics.mv_fast_food_event_causality
WHERE `Store Category` = 'fast_food'
GROUP BY `Event Type`, `Is Hardware Event`, `Severity Name`, `Time Period`
ORDER BY MEASURE(`Event Count`) DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query Example: Event-to-Metric Correlation
-- MAGIC Find which events caused performance degradation

-- COMMAND ----------

-- Event causality analysis: correlate events with metric degradation
WITH events_15min AS (
  SELECT
    store_id,
    event_timestamp,
    event_type,
    severity,
    DATEADD(MINUTE, 15, event_timestamp) as event_window_end
  FROM retail_analytics.gold.gold_store_events
  WHERE store_category = 'fast_food'
    AND event_type IN ('EQUIPMENT_FAILURE', 'KDS_SCREEN_DOWN', 'LOOP_SENSOR_MALFUNCTION', 'POS_NETWORK_TIMEOUT', 'HME_SYSTEM_REBOOT')
    AND is_critical = TRUE
),
metrics_after_events AS (
  SELECT
    m.store_id,
    m.metric_name,
    m.window_start,
    m.avg_value as metric_value,
    e.event_type,
    e.event_timestamp,
    TIMESTAMPDIFF(MINUTE, e.event_timestamp, m.window_start) as minutes_after_event
  FROM retail_analytics.gold.gold_store_performance_5min m
  INNER JOIN events_15min e
    ON m.store_id = e.store_id
    AND m.window_start BETWEEN e.event_timestamp AND e.event_window_end
  WHERE m.store_category = 'fast_food'
    AND m.metric_name IN (
      'drive_through_total_experience_time_sec',
      'drive_through_kds_bump_time_sec',
      'drive_through_window_time_sec',
      'drive_through_cars_per_hour'
    )
)
SELECT
  event_type,
  metric_name,
  COUNT(DISTINCT store_id) as affected_stores,
  AVG(metric_value) as avg_metric_during_incident,
  AVG(minutes_after_event) as avg_minutes_after_event,
  COUNT(*) as incident_count
FROM metrics_after_events
GROUP BY event_type, metric_name
ORDER BY event_type, metric_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Metric View 8: Order Complexity Analysis (Fast Food Only)
-- MAGIC 
-- MAGIC Analyze timing variations based on order complexity and normalization.

-- COMMAND ----------

CREATE OR REPLACE VIEW retail_analytics.metrics.mv_order_complexity
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Order complexity analysis for fast food stores - timing normalization by order size"
  source: retail_analytics.gold.gold_store_performance_5min
  
  dimensions:
    - name: Time Window Start
      expr: window_start
      comment: "Start of aggregation window"
    
    - name: Store ID
      expr: store_id
      comment: "Fast food store identifier"
    
    - name: Store Category
      expr: store_category
      comment: "Store category (fast_food or apparel)"
    
    - name: Region
      expr: region
      comment: "Store region"
    
    - name: Brand
      expr: brand
      comment: "Fast food brand"
    
    - name: Metric Name
      expr: metric_name
      comment: "Timing metric"
    
    - name: Order Size Category
      expr: CASE
              WHEN avg_order_value_context < 15 THEN 'Small ($8-15)'
              WHEN avg_order_value_context < 30 THEN 'Medium ($15-30)'
              WHEN avg_order_value_context < 50 THEN 'Large ($30-50)'
              ELSE 'Mega ($50+)'
            END
      comment: "Order complexity category based on dollar value"
    
    - name: Is Normalized
      expr: CASE WHEN normalized THEN 'Order-Normalized' ELSE 'Raw Timing' END
      comment: "Whether timing was adjusted for order complexity"
    
    - name: Time Period
      expr: CASE 
              WHEN HOUR(window_start) BETWEEN 11 AND 13 THEN 'Lunch Rush'
              WHEN HOUR(window_start) BETWEEN 17 AND 19 THEN 'Dinner Rush'
              ELSE 'Off-Peak'
            END
      comment: "Time period"
  
  measures:
    - name: Average Timing (sec)
      expr: AVG(avg_value)
      comment: "Average timing value"
    
    - name: P95 Timing (sec)
      expr: AVG(p95_value)
      comment: "95th percentile timing"
    
    - name: Average Order Value
      expr: AVG(avg_order_value_context)
      comment: "Average order dollar amount"
    
    - name: Average Item Count
      expr: AVG(avg_item_count_context)
      comment: "Average items per order"
    
    - name: Total Orders
      expr: SUM(sample_count)
      comment: "Total orders measured"
    
    - name: Store Count
      expr: COUNT(DISTINCT store_id)
      comment: "Number of stores"
    
    - name: Normalized vs Baseline Ratio
      expr: AVG(CASE WHEN normalized THEN avg_value ELSE NULL END) / NULLIF(AVG(CASE WHEN NOT normalized THEN avg_value ELSE NULL END), 0)
      comment: "Ratio of normalized to baseline timing"
$$;

-- COMMAND ----------

-- Test order complexity metric view
SELECT
  `Order Size Category`,
  `Metric Name`,
  `Time Period`,
  MEASURE(`Store Count`),
  MEASURE(`Average Timing (sec)`) as avg_seconds,
  MEASURE(`Average Order Value`) as avg_order_dollars,
  MEASURE(`Average Item Count`) as avg_items,
  MEASURE(`Total Orders`)
FROM retail_analytics.metrics.mv_order_complexity
WHERE `Metric Name` = 'drive_through_total_experience_time_sec'
  AND `Store Category` = 'fast_food'
GROUP BY `Order Size Category`, `Metric Name`, `Time Period`
ORDER BY `Order Size Category`, `Time Period`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query Example: Order Normalization Effectiveness
-- MAGIC Check if timing issues are due to order complexity or actual problems

-- COMMAND ----------

-- Order normalization analysis: identify true problems vs complexity
SELECT
  `Store ID`,
  `Brand`,
  MEASURE(`Average Order Value`) as avg_order_value,
  MEASURE(`Average Timing (sec)`) as avg_time,
  MEASURE(`P95 Timing (sec)`) as p95_time,
  CASE 
    WHEN MEASURE(`Average Order Value`) > 40 AND MEASURE(`Average Timing (sec)`) > 300 THEN 'ACCEPTABLE - Large Orders'
    WHEN MEASURE(`Average Order Value`) < 20 AND MEASURE(`Average Timing (sec)`) > 300 THEN 'PROBLEM - Small Orders Slow'
    WHEN MEASURE(`Average Timing (sec)`) > 360 THEN 'CRITICAL - Review Operations'
    ELSE 'NORMAL'
  END as assessment
FROM retail_analytics.metrics.mv_order_complexity
WHERE `Metric Name` = 'drive_through_total_experience_time_sec'
  AND `Store Category` = 'fast_food'
  AND `Is Normalized` = 'Order-Normalized'
GROUP BY `Store ID`, `Brand`
HAVING MEASURE(`Average Timing (sec)`) > 240
ORDER BY MEASURE(`Average Timing (sec)`) DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## List All Metric Views

-- COMMAND ----------

-- Show all metric views created
SHOW VIEWS IN retail_analytics.metrics;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Metric Views Summary
-- MAGIC 
-- MAGIC | Metric View | Source Table | Key Measures | Scope |
-- MAGIC |-------------|--------------|--------------|-------|
-- MAGIC | `mv_store_performance` | `gold_store_performance_5min` | Avg/Max/P95 values, Anomaly count/rate | All stores |
-- MAGIC | `mv_store_health` | `gold_store_health` | Health score, Category-aware metrics | All stores |
-- MAGIC | `mv_store_events` | `gold_store_events` | Event count, Critical rate, Affected stores | All stores |
-- MAGIC | `mv_regional_performance` | `gold_metrics_by_region` | Regional metrics, Anomaly rate | All stores |
-- MAGIC | `mv_kpi_dashboard` | `gold_kpi_hourly` | Hourly KPIs, Event summaries | All stores |
-- MAGIC | `mv_hme_speed_of_service` | `gold_store_performance_5min` | HME Drive-Thru Timer KPIs: OTD, Menu Board, KDS, Window, Cars/hr | Fast food only |
-- MAGIC | `mv_fast_food_event_causality` | `gold_store_events` | Hardware vs operational events, Event-metric correlation | Fast food only |
-- MAGIC | `mv_order_complexity` | `gold_store_performance_5min` | Order normalization, Timing by order size | Fast food only |
-- MAGIC 
-- MAGIC ## Query Examples
-- MAGIC 
-- MAGIC ### Get sales by region:
-- MAGIC ```sql
-- MAGIC SELECT
-- MAGIC   `Region`,
-- MAGIC   MEASURE(`Average Value`),
-- MAGIC   MEASURE(`P95 Value`)
-- MAGIC FROM retail_analytics.metrics.mv_store_performance
-- MAGIC WHERE `Metric Name` = 'hourly_sales'
-- MAGIC GROUP BY `Region`;
-- MAGIC ```
-- MAGIC 
-- MAGIC ### Get store health summary:
-- MAGIC ```sql
-- MAGIC SELECT
-- MAGIC   `Health Status`,
-- MAGIC   MEASURE(`Store Count`),
-- MAGIC   MEASURE(`Average Health Score`)
-- MAGIC FROM retail_analytics.metrics.mv_store_health
-- MAGIC GROUP BY `Health Status`;
-- MAGIC ```
-- MAGIC 
-- MAGIC ### Get critical events by category:
-- MAGIC ```sql
-- MAGIC SELECT
-- MAGIC   `Event Category`,
-- MAGIC   MEASURE(`Critical Event Count`),
-- MAGIC   MEASURE(`Critical Event Rate`)
-- MAGIC FROM retail_analytics.metrics.mv_store_events
-- MAGIC WHERE `Is Critical` = 'Critical'
-- MAGIC GROUP BY `Event Category`;
-- MAGIC ```
-- MAGIC 
-- MAGIC ### Get HME Speed of Service by time period (Fast Food):
-- MAGIC ```sql
-- MAGIC SELECT
-- MAGIC   `Time Period`,
-- MAGIC   `Metric Name`,
-- MAGIC   MEASURE(`Average Value`) as avg_seconds,
-- MAGIC   MEASURE(`P95 Value`) as p95_seconds,
-- MAGIC   MEASURE(`Benchmark Gap (sec)`) as gap_from_industry_standard
-- MAGIC FROM retail_analytics.metrics.mv_hme_speed_of_service
-- MAGIC WHERE `Metric Name` IN (
-- MAGIC   'drive_through_total_experience_time_sec',
-- MAGIC   'drive_through_menu_board_time_sec',
-- MAGIC   'drive_through_kds_bump_time_sec',
-- MAGIC   'drive_through_window_time_sec'
-- MAGIC )
-- MAGIC GROUP BY `Time Period`, `Metric Name`
-- MAGIC ORDER BY `Time Period`, `Metric Name`;
-- MAGIC ```
-- MAGIC 
-- MAGIC ### Correlate hardware events with performance degradation (Fast Food):
-- MAGIC ```sql
-- MAGIC WITH recent_events AS (
-- MAGIC   SELECT
-- MAGIC     `Store ID`,
-- MAGIC     `Event Type`,
-- MAGIC     `Event Timestamp`,
-- MAGIC     DATEADD(MINUTE, 15, `Event Timestamp`) as window_end
-- MAGIC   FROM retail_analytics.metrics.mv_fast_food_event_causality
-- MAGIC   WHERE `Is Hardware Event` = 'Hardware Failure'
-- MAGIC     AND `Is Critical` = 'Critical'
-- MAGIC ),
-- MAGIC metrics_during_events AS (
-- MAGIC   SELECT
-- MAGIC     m.`Store ID`,
-- MAGIC     m.`Metric Name`,
-- MAGIC     MEASURE(m.`Average Value`) as avg_value,
-- MAGIC     e.`Event Type`
-- MAGIC   FROM retail_analytics.metrics.mv_hme_speed_of_service m
-- MAGIC   JOIN recent_events e 
-- MAGIC     ON m.`Store ID` = e.`Store ID`
-- MAGIC     AND m.`Time Window Start` BETWEEN e.`Event Timestamp` AND e.window_end
-- MAGIC   WHERE m.`Metric Name` = 'drive_through_total_experience_time_sec'
-- MAGIC   GROUP BY m.`Store ID`, m.`Metric Name`, e.`Event Type`
-- MAGIC )
-- MAGIC SELECT
-- MAGIC   `Event Type`,
-- MAGIC   COUNT(DISTINCT `Store ID`) as affected_stores,
-- MAGIC   AVG(avg_value) as avg_otd_during_incident
-- MAGIC FROM metrics_during_events
-- MAGIC GROUP BY `Event Type`
-- MAGIC ORDER BY avg_otd_during_incident DESC;
-- MAGIC ```
-- MAGIC 
-- MAGIC ## Fast Food KPI Benchmarks
-- MAGIC 
-- MAGIC Industry-standard benchmarks for HME Drive-Thru Timer System:
-- MAGIC 
-- MAGIC | Metric | Benchmark | Excellent | Poor | Description |
-- MAGIC |--------|-----------|-----------|------|-------------|
-- MAGIC | Total Experience Time (OTD) | 4:30 (270s) | < 4:15 (255s) | > 6:00 (360s) | Loop A to Loop B departure |
-- MAGIC | Menu Board Time | 25s | < 20s | > 60s | Ordering time at speaker (Loop A) |
-- MAGIC | Window Time | 50s | < 45s | > 90s | Service time at pickup (Loop B) |
-- MAGIC | KDS Bump Time | 40s | < 35s | > 80s | Kitchen production time |
-- MAGIC | Cars Per Hour | 100 | > 110 | < 60 | Drive-through throughput |
-- MAGIC 
-- MAGIC ## Next Steps
-- MAGIC 
-- MAGIC 1. Use metric views with **AI/BI Dashboards**
-- MAGIC 2. Configure **Genie spaces** for natural language queries (e.g., "Show me stores with high KDS bump time during lunch rush")
-- MAGIC 3. Set up **Alerts** based on metric thresholds (e.g., alert when OTD > 6 minutes)
-- MAGIC 4. Enable **Materialization** for performance optimization
-- MAGIC 5. Create **Lakeflow** pipelines for real-time metric computation
-- MAGIC 6. Build **Root Cause Analysis** dashboards using event causality views
-- MAGIC 
-- MAGIC ## HME System Integration Notes
-- MAGIC 
-- MAGIC The fast food metrics now include:
-- MAGIC - **Hardware source metadata**: LOOP_A (speaker sensor), LOOP_B (window sensor), KDS_BUMP (kitchen display)
-- MAGIC - **Order complexity normalization**: Large orders get time allowances (1.0x to 1.8x)
-- MAGIC - **Event causality**: Hardware failures (sensor malfunctions, KDS down) correlated with performance degradation
-- MAGIC - **Time-of-day patterns**: Lunch rush (11am-1pm) and dinner rush (5-7pm) show realistic performance impacts
-- MAGIC - **Performance profiling**: Stores assigned tiers (excellent/good/average/poor) with persistent characteristics

