-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Telco Network Performance Demo - Unity Catalog Metric Views
-- MAGIC 
-- MAGIC This notebook creates Unity Catalog **Metric Views** for real-time network KPI monitoring.
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

USE CATALOG telus_networkperf;

-- Create metrics schema if not exists
CREATE SCHEMA IF NOT EXISTS telus_networkperf.metrics
COMMENT 'Unity Catalog Metric Views for network KPI monitoring';

USE SCHEMA metrics;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Metric View 1: Network Performance Metrics
-- MAGIC 
-- MAGIC Comprehensive SNMP performance metrics with dimensions for flexible analysis.

-- COMMAND ----------

CREATE OR REPLACE VIEW telus_networkperf.metrics.mv_network_performance
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Network performance KPIs from SNMP metrics - latency, packet loss, throughput, jitter, error rate"
  source: telus_networkperf.gold.gold_network_performance_5min
  
  dimensions:
    - name: Time Window Start
      expr: window_start
      comment: "Start of 5-minute aggregation window"
    
    - name: Time Window End
      expr: window_end
      comment: "End of 5-minute aggregation window"
    
    - name: Device ID
      expr: device_id
      comment: "Unique identifier for network device"
    
    - name: Device Type
      expr: device_type
      comment: "Type of device: router, switch, firewall, load_balancer"
    
    - name: Location
      expr: location
      comment: "Geographic location of the device"
    
    - name: Vendor
      expr: vendor
      comment: "Device manufacturer: Cisco, Juniper, Arista, etc."
    
    - name: Metric Name
      expr: metric_name
      comment: "Type of metric: latency_ms, packet_loss_pct, throughput_mbps, jitter_ms, error_rate"
    
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
    
    - name: Device Count
      expr: COUNT(DISTINCT device_id)
      comment: "Number of unique devices"
$$;

-- COMMAND ----------

-- Test the metric view
SELECT 
  MEASURE(`Average Value`),
  MEASURE(`Max Value`),
  MEASURE(`Anomaly Count`),
  MEASURE(`Device Count`),
  `Metric Name`,
  `Location`
FROM telus_networkperf.metrics.mv_network_performance
WHERE `Metric Name` = 'latency_ms'
GROUP BY `Metric Name`, `Location`
ORDER BY MEASURE(`Average Value`) DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Metric View 2: Device Health Metrics
-- MAGIC 
-- MAGIC Real-time device health scores and status.

-- COMMAND ----------

CREATE OR REPLACE VIEW telus_networkperf.metrics.mv_device_health
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Device health scores and current performance metrics for network monitoring"
  source: telus_networkperf.gold.gold_device_health
  
  dimensions:
    - name: Device ID
      expr: device_id
      comment: "Unique device identifier"
    
    - name: Device Type
      expr: device_type
      comment: "Type of network device"
    
    - name: Location
      expr: location
      comment: "Device location"
    
    - name: Health Status
      expr: health_status
      comment: "Health category: Excellent, Good, Fair, Poor, Critical"
    
    - name: Requires Attention
      expr: CASE WHEN requires_attention THEN 'Yes' ELSE 'No' END
      comment: "Whether device needs immediate attention"
  
  measures:
    - name: Device Count
      expr: COUNT(DISTINCT device_id)
      comment: "Number of devices"
    
    - name: Average Health Score
      expr: AVG(health_score)
      comment: "Average health score (0-100, higher is better)"
    
    - name: Min Health Score
      expr: MIN(health_score)
      comment: "Lowest health score"
    
    - name: Average Latency
      expr: AVG(current_latency_ms)
      comment: "Average current latency in milliseconds"
    
    - name: Average Packet Loss
      expr: AVG(current_packet_loss_pct)
      comment: "Average packet loss percentage"
    
    - name: Average Throughput
      expr: AVG(current_throughput_mbps)
      comment: "Average throughput in Mbps"
    
    - name: Average Jitter
      expr: AVG(current_jitter_ms)
      comment: "Average jitter in milliseconds"
    
    - name: Average Error Rate
      expr: AVG(current_error_rate)
      comment: "Average error rate"
    
    - name: Total Events (1h)
      expr: SUM(event_count_1h)
      comment: "Total events in the last hour"
    
    - name: Critical Events (1h)
      expr: SUM(critical_event_count_1h)
      comment: "Critical events in the last hour"
    
    - name: Devices Needing Attention
      expr: SUM(CASE WHEN requires_attention THEN 1 ELSE 0 END)
      comment: "Count of devices requiring attention"
    
    - name: Critical Device Count
      expr: SUM(CASE WHEN health_status = 'Critical' THEN 1 ELSE 0 END)
      comment: "Number of devices in critical state"
$$;

-- COMMAND ----------

-- Test device health metric view
SELECT
  `Health Status`,
  MEASURE(`Device Count`),
  MEASURE(`Average Health Score`),
  MEASURE(`Critical Events (1h)`),
  MEASURE(`Devices Needing Attention`)
FROM telus_networkperf.metrics.mv_device_health
GROUP BY `Health Status`
ORDER BY MEASURE(`Average Health Score`) ASC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Metric View 3: Network Events Metrics
-- MAGIC 
-- MAGIC Syslog event analysis with severity and category dimensions.

-- COMMAND ----------

CREATE OR REPLACE VIEW telus_networkperf.metrics.mv_network_events
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Network event metrics from syslog data for incident analysis"
  source: telus_networkperf.gold.gold_network_events
  
  dimensions:
    - name: Event Timestamp
      expr: event_timestamp
      comment: "When the event occurred"
    
    - name: Device ID
      expr: device_id
      comment: "Device that generated the event"
    
    - name: Device Type
      expr: device_type
      comment: "Type of network device"
    
    - name: Location
      expr: location
      comment: "Device location"
    
    - name: Event Type
      expr: event_type
      comment: "Type of event: LINKUPDOWN, ADJCHANGE, SECURITY, etc."
    
    - name: Event Category
      expr: event_category
      comment: "Category: interface_event, routing_event, security_event, etc."
    
    - name: Severity
      expr: severity
      comment: "Numeric severity level (0-7)"
    
    - name: Severity Name
      expr: severity_name
      comment: "Severity label: Emergency, Alert, Critical, Error, Warning, Notice, Informational, Debug"
    
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
    
    - name: Affected Device Count
      expr: COUNT(DISTINCT device_id)
      comment: "Number of unique devices with events"
    
    - name: Average Event Age (min)
      expr: AVG(event_age_minutes)
      comment: "Average age of events in minutes"
$$;

-- COMMAND ----------

-- Test network events metric view
SELECT
  `Event Category`,
  `Severity Name`,
  MEASURE(`Event Count`),
  MEASURE(`Critical Event Count`),
  MEASURE(`Affected Device Count`)
FROM telus_networkperf.metrics.mv_network_events
GROUP BY `Event Category`, `Severity Name`
ORDER BY MEASURE(`Event Count`) DESC
LIMIT 15;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Metric View 4: Geographic Performance Metrics
-- MAGIC 
-- MAGIC Location-based network performance analysis.

-- COMMAND ----------

CREATE OR REPLACE VIEW telus_networkperf.metrics.mv_geographic_performance
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Network performance metrics aggregated by geographic location"
  source: telus_networkperf.gold.gold_metrics_by_location
  
  dimensions:
    - name: Time Window Start
      expr: window_start
      comment: "Start of aggregation window"
    
    - name: Time Window End
      expr: window_end
      comment: "End of aggregation window"
    
    - name: Location
      expr: location
      comment: "Geographic location"
    
    - name: Metric Name
      expr: metric_name
      comment: "Type of metric being measured"
    
    - name: Metric Unit
      expr: metric_unit
      comment: "Unit of measurement"
  
  measures:
    - name: Device Count
      expr: SUM(device_count)
      comment: "Number of devices in location"
    
    - name: Average Value
      expr: AVG(location_avg_value)
      comment: "Average metric value for location"
    
    - name: Max Value
      expr: MAX(location_max_value)
      comment: "Maximum metric value in location"
    
    - name: Min Value
      expr: MIN(location_min_value)
      comment: "Minimum metric value in location"
    
    - name: P95 Value
      expr: AVG(location_p95_value)
      comment: "95th percentile value for location"
    
    - name: Total Anomalies
      expr: SUM(total_anomalies)
      comment: "Total anomalies detected in location"
    
    - name: Total Samples
      expr: SUM(total_samples)
      comment: "Total samples collected from location"
    
    - name: Anomaly Rate
      expr: SUM(total_anomalies) / NULLIF(SUM(total_samples), 0) * 100
      comment: "Percentage of anomalous samples"
$$;

-- COMMAND ----------

-- Test geographic performance metric view
SELECT
  `Location`,
  `Metric Name`,
  MEASURE(`Device Count`),
  MEASURE(`Average Value`),
  MEASURE(`Total Anomalies`),
  MEASURE(`Anomaly Rate`)
FROM telus_networkperf.metrics.mv_geographic_performance
GROUP BY `Location`, `Metric Name`
ORDER BY `Location`, `Metric Name`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Metric View 5: Hourly KPI Dashboard Metrics
-- MAGIC 
-- MAGIC Executive-level hourly KPI summaries.

-- COMMAND ----------

CREATE OR REPLACE VIEW telus_networkperf.metrics.mv_kpi_dashboard
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  comment: "Hourly KPI summary metrics for executive dashboards and reporting"
  source: telus_networkperf.gold.gold_kpi_hourly
  
  dimensions:
    - name: Hour
      expr: hour_start
      comment: "Start of the hour"
    
    - name: Metric Name
      expr: metric_name
      comment: "Type of KPI metric"
  
  measures:
    - name: Device Count
      expr: SUM(device_count)
      comment: "Number of devices measured"
    
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
    
    - name: Affected Devices
      expr: SUM(affected_devices)
      comment: "Devices affected by events"
$$;

-- COMMAND ----------

-- Test KPI dashboard metric view
SELECT
  `Metric Name`,
  MEASURE(`Hourly Average`),
  MEASURE(`Hourly Max`),
  MEASURE(`Total Anomalies`),
  MEASURE(`Critical Events`)
FROM telus_networkperf.metrics.mv_kpi_dashboard
GROUP BY `Metric Name`
ORDER BY `Metric Name`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## List All Metric Views

-- COMMAND ----------

-- Show all metric views created
SHOW VIEWS IN telus_networkperf.metrics;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## View Metric View Definition

-- COMMAND ----------

-- Get full YAML definition for a metric view
DESCRIBE TABLE EXTENDED telus_networkperf.metrics.mv_network_performance AS JSON;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Metric Views Summary
-- MAGIC 
-- MAGIC | Metric View | Source Table | Key Measures |
-- MAGIC |-------------|--------------|--------------|
-- MAGIC | `mv_network_performance` | `gold_network_performance_5min` | Avg/Max/P95 values, Anomaly count/rate |
-- MAGIC | `mv_device_health` | `gold_device_health` | Health score, Latency, Packet loss, Critical events |
-- MAGIC | `mv_network_events` | `gold_network_events` | Event count, Critical rate, Affected devices |
-- MAGIC | `mv_geographic_performance` | `gold_metrics_by_location` | Location metrics, Anomaly rate |
-- MAGIC | `mv_kpi_dashboard` | `gold_kpi_hourly` | Hourly KPIs, Event summaries |
-- MAGIC 
-- MAGIC ## Query Examples
-- MAGIC 
-- MAGIC ### Get latency by location:
-- MAGIC ```sql
-- MAGIC SELECT
-- MAGIC   `Location`,
-- MAGIC   MEASURE(`Average Value`),
-- MAGIC   MEASURE(`P95 Value`)
-- MAGIC FROM telus_networkperf.metrics.mv_network_performance
-- MAGIC WHERE `Metric Name` = 'latency_ms'
-- MAGIC GROUP BY `Location`;
-- MAGIC ```
-- MAGIC 
-- MAGIC ### Get device health summary:
-- MAGIC ```sql
-- MAGIC SELECT
-- MAGIC   `Health Status`,
-- MAGIC   MEASURE(`Device Count`),
-- MAGIC   MEASURE(`Average Health Score`)
-- MAGIC FROM telus_networkperf.metrics.mv_device_health
-- MAGIC GROUP BY `Health Status`;
-- MAGIC ```
-- MAGIC 
-- MAGIC ### Get critical events by category:
-- MAGIC ```sql
-- MAGIC SELECT
-- MAGIC   `Event Category`,
-- MAGIC   MEASURE(`Critical Event Count`),
-- MAGIC   MEASURE(`Critical Event Rate`)
-- MAGIC FROM telus_networkperf.metrics.mv_network_events
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
