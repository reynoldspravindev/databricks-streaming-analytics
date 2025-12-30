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
-- MAGIC ## Metric View 6: Drive-Through Performance (Fast Food Only)
-- MAGIC 
-- MAGIC Drive-through specific metrics for fast food store analysis.

-- COMMAND ----------

CREATE OR REPLACE VIEW retail_analytics.metrics.mv_drive_through_performance
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Drive-through performance metrics for fast food stores"
  source: retail_analytics.gold.gold_drive_through_performance
  
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
    
    - name: Store Type
      expr: store_type
      comment: "Type of fast food store"
    
    - name: Region
      expr: region
      comment: "Store region"
    
    - name: Brand
      expr: brand
      comment: "Fast food brand"
    
    - name: Time Period
      expr: time_period
      comment: "Time period: breakfast_peak, lunch_peak, dinner_peak, off_peak"
    
    - name: Performance Grade
      expr: performance_grade
      comment: "Performance grade: Excellent, Good, Fair, Needs_Improvement"
  
  measures:
    - name: Avg Wait Time (sec)
      expr: AVG(avg_wait_time_sec)
      comment: "Average drive-through wait time in seconds"
    
    - name: P95 Wait Time (sec)
      expr: AVG(p95_wait_time_sec)
      comment: "95th percentile drive-through wait time"
    
    - name: Avg Throughput
      expr: AVG(avg_throughput_per_hour)
      comment: "Average cars served per hour"
    
    - name: Avg Order Accuracy
      expr: AVG(avg_order_accuracy_pct)
      comment: "Average order accuracy percentage"
    
    - name: Avg Speaker to Window Time
      expr: AVG(avg_speaker_to_window_sec)
      comment: "Average time from speaker to window"
    
    - name: P95 Speaker to Window Time
      expr: AVG(p95_speaker_to_window_sec)
      comment: "95th percentile speaker to window time"
    
    - name: Avg Mobile Pickup Time
      expr: AVG(avg_mobile_pickup_sec)
      comment: "Average mobile order pickup time"
    
    - name: Store Count
      expr: COUNT(DISTINCT store_id)
      comment: "Number of fast food stores"
$$;

-- COMMAND ----------

-- Test drive-through performance metric view
SELECT
  `Performance Grade`,
  `Time Period`,
  MEASURE(`Store Count`),
  MEASURE(`Avg Wait Time (sec)`),
  MEASURE(`Avg Order Accuracy`),
  MEASURE(`Avg Throughput`)
FROM retail_analytics.metrics.mv_drive_through_performance
GROUP BY `Performance Grade`, `Time Period`
ORDER BY `Performance Grade`, `Time Period`;

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
-- MAGIC | Metric View | Source Table | Key Measures |
-- MAGIC |-------------|--------------|--------------|
-- MAGIC | `mv_store_performance` | `gold_store_performance_5min` | Avg/Max/P95 values, Anomaly count/rate (apparel + fast food) |
-- MAGIC | `mv_store_health` | `gold_store_health` | Health score, Category-aware metrics (apparel + fast food) |
-- MAGIC | `mv_store_events` | `gold_store_events` | Event count, Critical rate, Affected stores (apparel + fast food) |
-- MAGIC | `mv_regional_performance` | `gold_metrics_by_region` | Regional metrics, Anomaly rate (apparel + fast food) |
-- MAGIC | `mv_kpi_dashboard` | `gold_kpi_hourly` | Hourly KPIs, Event summaries (apparel + fast food) |
-- MAGIC | `mv_drive_through_performance` | `gold_drive_through_performance` | Drive-through wait time, throughput, accuracy (fast food only) |
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
-- MAGIC ## Next Steps
-- MAGIC 
-- MAGIC 1. Use metric views with **AI/BI Dashboards**
-- MAGIC 2. Configure **Genie spaces** for natural language queries
-- MAGIC 3. Set up **Alerts** based on metric thresholds
-- MAGIC 4. Enable **Materialization** for performance optimization

