-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Telco Network Performance - AI/BI Genie Space Setup
-- MAGIC 
-- MAGIC This notebook provides the complete configuration for setting up a **Genie Space** for natural language querying of network performance data.
-- MAGIC 
-- MAGIC ## What is Genie?
-- MAGIC Genie is Databricks' AI-powered assistant that enables business users to ask questions about data in natural language and receive accurate SQL-backed answers.
-- MAGIC 
-- MAGIC ---
-- MAGIC 
-- MAGIC # == Genie Space Configuration
-- MAGIC 
-- MAGIC ## Space Name
-- MAGIC **Telco Network Performance Analytics**
-- MAGIC 
-- MAGIC ## Description
-- MAGIC AI-powered analytics for real-time network performance monitoring, device health, and incident analysis. Ask questions about latency, packet loss, throughput, jitter, error rates, and network events across your telco infrastructure.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 📊 Data Assets to Include
-- MAGIC 
-- MAGIC ## Gold Tables (Add these to the Genie Space)
-- MAGIC 
-- MAGIC | Table | Description | Key Use Cases |
-- MAGIC |-------|-------------|---------------|
-- MAGIC | `telus_networkperf.gold.gold_network_performance_5min` | 5-minute aggregated SNMP metrics | Latency trends, throughput analysis, anomaly detection |
-- MAGIC | `telus_networkperf.gold.gold_device_health` | Current device health scores | Device monitoring, health dashboards, alerts |
-- MAGIC | `telus_networkperf.gold.gold_network_events` | Parsed syslog events | Incident analysis, security monitoring |
-- MAGIC | `telus_networkperf.gold.gold_metrics_by_location` | Location-aggregated metrics | Geographic performance comparison |
-- MAGIC | `telus_networkperf.gold.gold_kpi_hourly` | Hourly KPI summaries | Executive reporting, trend analysis |
-- MAGIC 
-- MAGIC ## Metric Views (Add these to the Genie Space)
-- MAGIC 
-- MAGIC | Metric View | Description | Key Measures |
-- MAGIC |-------------|-------------|--------------|
-- MAGIC | `telus_networkperf.metrics.mv_network_performance` | SNMP performance metrics | Avg/Max/P95 values, Anomaly rates |
-- MAGIC | `telus_networkperf.metrics.mv_device_health` | Device health metrics | Health scores, Critical device counts |
-- MAGIC | `telus_networkperf.metrics.mv_network_events` | Event analysis metrics | Event counts, Critical event rates |
-- MAGIC | `telus_networkperf.metrics.mv_geographic_performance` | Location-based metrics | Regional comparisons |
-- MAGIC | `telus_networkperf.metrics.mv_kpi_dashboard` | Dashboard KPIs | Hourly summaries |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # == Genie Space Instructions
-- MAGIC 
-- MAGIC **Copy and paste the following into the Genie Space "General Instructions" section:**
-- MAGIC 
-- MAGIC ---
-- MAGIC 
-- MAGIC ## About This Data
-- MAGIC 
-- MAGIC This Genie space provides access to **Telco Network Performance** data from a telecommunications company's network infrastructure. The data includes:
-- MAGIC 
-- MAGIC - **SNMP metrics**: Performance measurements from network devices (routers, switches, firewalls, load balancers)
-- MAGIC - **Syslog events**: System logs capturing network events, security alerts, and operational changes
-- MAGIC - **Device health**: Real-time health scores and status for all network equipment
-- MAGIC 
-- MAGIC ## Key Metrics Explained
-- MAGIC 
-- MAGIC | Metric | Description | Good Values | Concerning Values |
-- MAGIC |--------|-------------|-------------|-------------------|
-- MAGIC | **Latency (ms)** | Time for data to travel across the network | < 50ms | > 100ms |
-- MAGIC | **Packet Loss (%)** | Percentage of data packets lost in transit | < 0.1% | > 1% |
-- MAGIC | **Throughput (Mbps)** | Data transfer rate | Depends on link capacity | Sudden drops |
-- MAGIC | **Jitter (ms)** | Variation in packet arrival time | < 30ms | > 50ms |
-- MAGIC | **Error Rate** | Rate of transmission errors | < 0.01 | > 0.05 |
-- MAGIC | **Health Score** | Overall device health (0-100) | > 80 | < 50 |
-- MAGIC 
-- MAGIC ## Device Types
-- MAGIC 
-- MAGIC - **router**: Core network routers handling traffic routing
-- MAGIC - **switch**: Network switches for local traffic
-- MAGIC - **firewall**: Security devices filtering traffic
-- MAGIC - **load_balancer**: Devices distributing traffic across servers
-- MAGIC 
-- MAGIC ## Locations
-- MAGIC 
-- MAGIC Network devices are distributed across: New York, Los Angeles, Chicago, Houston, Seattle, Miami, Denver, Boston, Atlanta, Dallas
-- MAGIC 
-- MAGIC ## Vendors
-- MAGIC 
-- MAGIC Equipment from: Cisco, Juniper, Arista, Palo Alto, F5, Fortinet
-- MAGIC 
-- MAGIC ## Severity Levels (for Events)
-- MAGIC 
-- MAGIC | Level | Name | Description |
-- MAGIC |-------|------|-------------|
-- MAGIC | 0 | Emergency | System unusable |
-- MAGIC | 1 | Alert | Immediate action needed |
-- MAGIC | 2 | Critical | Critical conditions |
-- MAGIC | 3 | Error | Error conditions |
-- MAGIC | 4 | Warning | Warning conditions |
-- MAGIC | 5 | Notice | Normal but significant |
-- MAGIC | 6 | Informational | Informational messages |
-- MAGIC | 7 | Debug | Debug-level messages |
-- MAGIC 
-- MAGIC **Critical events** = Severity 0-3
-- MAGIC 
-- MAGIC ## How to Answer Questions
-- MAGIC 
-- MAGIC 1. **For metric views**: Always use the `MEASURE()` function to aggregate measures
-- MAGIC 2. **For time-based queries**: Use `window_start` and `window_end` columns
-- MAGIC 3. **For current status**: Use `gold_device_health` table
-- MAGIC 4. **For historical analysis**: Use `gold_network_performance_5min` or `gold_kpi_hourly`
-- MAGIC 5. **For incidents**: Use `gold_network_events` table
-- MAGIC 
-- MAGIC ## Query Patterns
-- MAGIC 
-- MAGIC ### Using Metric Views (Preferred for Aggregations)
-- MAGIC ```sql
-- MAGIC SELECT
-- MAGIC   `Dimension Name`,
-- MAGIC   MEASURE(`Measure Name`)
-- MAGIC FROM metric_view_name
-- MAGIC GROUP BY `Dimension Name`
-- MAGIC ```
-- MAGIC 
-- MAGIC ### Direct Table Queries
-- MAGIC ```sql
-- MAGIC SELECT columns FROM gold_table WHERE conditions
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 💬 Sample Questions for Genie
-- MAGIC 
-- MAGIC **Add these as "Sample Questions" in the Genie Space configuration:**
-- MAGIC 
-- MAGIC ## Performance Questions
-- MAGIC 
-- MAGIC 1. **"What is the average latency across all locations?"**
-- MAGIC 2. **"Show me the top 10 devices with highest packet loss"**
-- MAGIC 3. **"Compare throughput between New York and Los Angeles"**
-- MAGIC 4. **"What is the P95 latency for each device type?"**
-- MAGIC 5. **"Which locations have latency above 100ms?"**
-- MAGIC 6. **"Show jitter trends over the last 24 hours"**
-- MAGIC 
-- MAGIC ## Device Health Questions
-- MAGIC 
-- MAGIC 7. **"How many devices are in critical health status?"**
-- MAGIC 8. **"List all devices that require attention"**
-- MAGIC 9. **"What is the average health score by location?"**
-- MAGIC 10. **"Show the 5 devices with lowest health scores"**
-- MAGIC 11. **"Which device types have the most critical devices?"**
-- MAGIC 
-- MAGIC ## Event & Incident Questions
-- MAGIC 
-- MAGIC 12. **"How many critical events occurred today?"**
-- MAGIC 13. **"Show me security events from the last hour"**
-- MAGIC 14. **"Which devices had the most events?"**
-- MAGIC 15. **"What are the most common event types?"**
-- MAGIC 16. **"Show events by severity level"**
-- MAGIC 
-- MAGIC ## Geographic Questions
-- MAGIC 
-- MAGIC 17. **"Which location has the best network performance?"**
-- MAGIC 18. **"Compare error rates across all locations"**
-- MAGIC 19. **"How many devices are in each location?"**
-- MAGIC 20. **"Show anomaly rates by location"**
-- MAGIC 
-- MAGIC ## Trend & Summary Questions
-- MAGIC 
-- MAGIC 21. **"What are the hourly KPI trends?"**
-- MAGIC 22. **"Show me a summary of all network metrics"**
-- MAGIC 23. **"How has latency changed over the past week?"**
-- MAGIC 24. **"What percentage of samples are anomalies?"**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 🔧 Table-Specific Instructions
-- MAGIC 
-- MAGIC **Add these as "Table Instructions" for each table in the Genie Space:**
-- MAGIC 
-- MAGIC ---
-- MAGIC 
-- MAGIC ## gold_network_performance_5min
-- MAGIC 
-- MAGIC **Instructions:**
-- MAGIC ```
-- MAGIC This table contains 5-minute aggregated SNMP metrics from network devices.
-- MAGIC 
-- MAGIC Key columns:
-- MAGIC - window_start, window_end: Time window for aggregation
-- MAGIC - device_id, device_type, location, vendor: Device identification
-- MAGIC - metric_name: Type of metric (latency_ms, packet_loss_pct, throughput_mbps, jitter_ms, error_rate)
-- MAGIC - avg_value, max_value, min_value: Aggregated values
-- MAGIC - p50_value, p95_value, p99_value: Percentile values
-- MAGIC - sample_count: Number of samples in window
-- MAGIC - anomaly_count: Number of anomalous readings
-- MAGIC - breach_type: SLA breach status (normal, warning, critical)
-- MAGIC 
-- MAGIC Use this table for:
-- MAGIC - Trend analysis over time
-- MAGIC - Performance comparisons between devices/locations
-- MAGIC - Anomaly detection
-- MAGIC - SLA monitoring
-- MAGIC 
-- MAGIC Always filter by metric_name when querying specific metrics.
-- MAGIC ```
-- MAGIC 
-- MAGIC ---
-- MAGIC 
-- MAGIC ## gold_device_health
-- MAGIC 
-- MAGIC **Instructions:**
-- MAGIC ```
-- MAGIC This table contains current health status for all network devices.
-- MAGIC 
-- MAGIC Key columns:
-- MAGIC - device_id, device_type, location, vendor: Device identification
-- MAGIC - health_score: 0-100 score (higher is better)
-- MAGIC - health_status: Excellent (>90), Good (80-90), Fair (60-79), Poor (40-59), Critical (<40)
-- MAGIC - current_latency_ms, current_packet_loss_pct, current_throughput_mbps, current_jitter_ms, current_error_rate: Latest metrics
-- MAGIC - event_count_1h, critical_event_count_1h: Recent event counts
-- MAGIC - requires_attention: Boolean flag for devices needing action
-- MAGIC 
-- MAGIC Use this table for:
-- MAGIC - Real-time device monitoring
-- MAGIC - Identifying problematic devices
-- MAGIC - Health dashboards
-- MAGIC - Alerting on device status
-- MAGIC 
-- MAGIC Filter by health_status or requires_attention to find problem devices.
-- MAGIC ```
-- MAGIC 
-- MAGIC ---
-- MAGIC 
-- MAGIC ## gold_network_events
-- MAGIC 
-- MAGIC **Instructions:**
-- MAGIC ```
-- MAGIC This table contains parsed syslog events from network devices.
-- MAGIC 
-- MAGIC Key columns:
-- MAGIC - event_timestamp: When the event occurred
-- MAGIC - device_id, device_type, location: Device identification
-- MAGIC - event_type: LINKUPDOWN, ADJCHANGE, SECURITY_LOGIN, BGP_NEIGHBOR, OSPF_NEIGHBOR, CONFIG_CHANGE, etc.
-- MAGIC - event_category: interface_event, routing_event, security_event, configuration_event
-- MAGIC - severity: Numeric 0-7 (0=Emergency, 7=Debug)
-- MAGIC - severity_name: Human-readable severity
-- MAGIC - is_critical: True for severity 0-3
-- MAGIC - event_message: Full event description
-- MAGIC 
-- MAGIC Use this table for:
-- MAGIC - Incident investigation
-- MAGIC - Security monitoring
-- MAGIC - Change tracking
-- MAGIC - Event correlation
-- MAGIC 
-- MAGIC Filter by is_critical=true for important events.
-- MAGIC Filter by event_category for specific event types.
-- MAGIC ```
-- MAGIC 
-- MAGIC ---
-- MAGIC 
-- MAGIC ## gold_metrics_by_location
-- MAGIC 
-- MAGIC **Instructions:**
-- MAGIC ```
-- MAGIC This table aggregates metrics by geographic location.
-- MAGIC 
-- MAGIC Key columns:
-- MAGIC - window_start, window_end: Time window
-- MAGIC - location: Geographic location
-- MAGIC - metric_name: Type of metric
-- MAGIC - device_count: Number of devices at location
-- MAGIC - location_avg_value, location_max_value, location_min_value, location_p95_value: Location aggregates
-- MAGIC - total_anomalies, total_samples: For anomaly rate calculation
-- MAGIC 
-- MAGIC Use this table for:
-- MAGIC - Geographic comparisons
-- MAGIC - Regional performance analysis
-- MAGIC - Capacity planning by location
-- MAGIC 
-- MAGIC Group by location for comparisons. Filter by metric_name for specific metrics.
-- MAGIC ```
-- MAGIC 
-- MAGIC ---
-- MAGIC 
-- MAGIC ## gold_kpi_hourly
-- MAGIC 
-- MAGIC **Instructions:**
-- MAGIC ```
-- MAGIC This table contains hourly KPI summaries for executive reporting.
-- MAGIC 
-- MAGIC Key columns:
-- MAGIC - hour_start: Start of the hour
-- MAGIC - metric_name: Type of metric
-- MAGIC - device_count: Devices reporting
-- MAGIC - hourly_avg, hourly_max, hourly_min, hourly_p95: Hourly aggregates
-- MAGIC - hourly_anomalies: Anomalies detected in hour
-- MAGIC - total_events, critical_events, affected_devices: Event summaries
-- MAGIC 
-- MAGIC Use this table for:
-- MAGIC - Executive dashboards
-- MAGIC - Hourly trend analysis
-- MAGIC - KPI reporting
-- MAGIC - High-level summaries
-- MAGIC 
-- MAGIC Use ORDER BY hour_start for time-series analysis.
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 📋 Metric View Instructions
-- MAGIC 
-- MAGIC **Add these instructions for each metric view:**
-- MAGIC 
-- MAGIC ---
-- MAGIC 
-- MAGIC ## mv_network_performance
-- MAGIC 
-- MAGIC **Instructions:**
-- MAGIC ```
-- MAGIC Metric view for SNMP network performance analysis.
-- MAGIC 
-- MAGIC IMPORTANT: Always use MEASURE() function for measures!
-- MAGIC 
-- MAGIC Available Dimensions (for GROUP BY):
-- MAGIC - Time Window Start, Time Window End
-- MAGIC - Device ID, Device Type, Location, Vendor
-- MAGIC - Metric Name, Metric Unit
-- MAGIC 
-- MAGIC Available Measures (use with MEASURE()):
-- MAGIC - Average Value, Max Value, Min Value
-- MAGIC - P50 Value, P95 Value, P99 Value
-- MAGIC - Total Samples, Anomaly Count, Anomaly Rate
-- MAGIC - Device Count
-- MAGIC 
-- MAGIC Example query:
-- MAGIC SELECT `Location`, MEASURE(`Average Value`), MEASURE(`Anomaly Rate`)
-- MAGIC FROM mv_network_performance
-- MAGIC WHERE `Metric Name` = 'latency_ms'
-- MAGIC GROUP BY `Location`
-- MAGIC ```
-- MAGIC 
-- MAGIC ---
-- MAGIC 
-- MAGIC ## mv_device_health
-- MAGIC 
-- MAGIC **Instructions:**
-- MAGIC ```
-- MAGIC Metric view for device health analysis.
-- MAGIC 
-- MAGIC IMPORTANT: Always use MEASURE() function for measures!
-- MAGIC 
-- MAGIC Available Dimensions:
-- MAGIC - Device ID, Device Type, Location
-- MAGIC - Health Status, Requires Attention
-- MAGIC 
-- MAGIC Available Measures:
-- MAGIC - Device Count, Average Health Score, Min Health Score
-- MAGIC - Average Latency, Average Packet Loss, Average Throughput
-- MAGIC - Average Jitter, Average Error Rate
-- MAGIC - Total Events (1h), Critical Events (1h)
-- MAGIC - Devices Needing Attention, Critical Device Count
-- MAGIC 
-- MAGIC Example query:
-- MAGIC SELECT `Health Status`, MEASURE(`Device Count`), MEASURE(`Average Health Score`)
-- MAGIC FROM mv_device_health
-- MAGIC GROUP BY `Health Status`
-- MAGIC ```
-- MAGIC 
-- MAGIC ---
-- MAGIC 
-- MAGIC ## mv_network_events
-- MAGIC 
-- MAGIC **Instructions:**
-- MAGIC ```
-- MAGIC Metric view for event analysis.
-- MAGIC 
-- MAGIC IMPORTANT: Always use MEASURE() function for measures!
-- MAGIC 
-- MAGIC Available Dimensions:
-- MAGIC - Event Timestamp, Device ID, Device Type, Location
-- MAGIC - Event Type, Event Category
-- MAGIC - Severity, Severity Name, Is Critical
-- MAGIC - Hour of Day, Day of Week
-- MAGIC 
-- MAGIC Available Measures:
-- MAGIC - Event Count, Critical Event Count, Critical Event Rate
-- MAGIC - Affected Device Count, Average Event Age (min)
-- MAGIC 
-- MAGIC Example query:
-- MAGIC SELECT `Event Category`, MEASURE(`Event Count`), MEASURE(`Critical Event Rate`)
-- MAGIC FROM mv_network_events
-- MAGIC GROUP BY `Event Category`
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # == Step-by-Step Genie Space Creation
-- MAGIC 
-- MAGIC ## Step 1: Navigate to Genie
-- MAGIC 1. In Databricks workspace, click **"New"** in the sidebar
-- MAGIC 2. Select **"Genie space"**
-- MAGIC 
-- MAGIC ## Step 2: Basic Configuration
-- MAGIC 1. **Name**: `Telco Network Performance Analytics`
-- MAGIC 2. **Description**: `AI-powered analytics for real-time network performance monitoring, device health, and incident analysis.`
-- MAGIC 
-- MAGIC ## Step 3: Add Data Assets
-- MAGIC Click **"Add tables"** and add these tables from `telus_networkperf`:
-- MAGIC 
-- MAGIC **Gold Tables:**
-- MAGIC - `telus_networkperf.gold.gold_network_performance_5min`
-- MAGIC - `telus_networkperf.gold.gold_device_health`
-- MAGIC - `telus_networkperf.gold.gold_network_events`
-- MAGIC - `telus_networkperf.gold.gold_metrics_by_location`
-- MAGIC - `telus_networkperf.gold.gold_kpi_hourly`
-- MAGIC 
-- MAGIC **Metric Views:**
-- MAGIC - `telus_networkperf.metrics.mv_network_performance`
-- MAGIC - `telus_networkperf.metrics.mv_device_health`
-- MAGIC - `telus_networkperf.metrics.mv_network_events`
-- MAGIC - `telus_networkperf.metrics.mv_geographic_performance`
-- MAGIC - `telus_networkperf.metrics.mv_kpi_dashboard`
-- MAGIC 
-- MAGIC ## Step 4: Configure General Instructions
-- MAGIC Click the **gear icon** → **"General instructions"**
-- MAGIC 
-- MAGIC Copy the full instructions from the **"Genie Space Instructions"** section above.
-- MAGIC 
-- MAGIC ## Step 5: Add Table-Specific Instructions
-- MAGIC For each table/view:
-- MAGIC 1. Click on the table name
-- MAGIC 2. Click **"Add instructions"**
-- MAGIC 3. Paste the corresponding instructions from **"Table-Specific Instructions"** section
-- MAGIC 
-- MAGIC ## Step 6: Add Sample Questions
-- MAGIC Click the **gear icon** → **"Sample questions"**
-- MAGIC 
-- MAGIC Add 5-10 sample questions from the list above, such as:
-- MAGIC - What is the average latency across all locations?
-- MAGIC - How many devices are in critical health status?
-- MAGIC - Show me the top 10 devices with highest packet loss
-- MAGIC - Which location has the best network performance?
-- MAGIC - How many critical events occurred today?
-- MAGIC 
-- MAGIC ## Step 7: Configure SQL Warehouse
-- MAGIC 1. Select an appropriate **SQL Warehouse** (Serverless recommended)
-- MAGIC 2. Ensure warehouse is running **Databricks Runtime 17.2+** for metric views
-- MAGIC 
-- MAGIC ## Step 8: Save and Share
-- MAGIC 1. Click **"Save"**
-- MAGIC 2. Share with relevant users/groups
-- MAGIC 3. Grant appropriate permissions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # == Verification Queries
-- MAGIC 
-- MAGIC Run these queries to verify data is available for Genie:

-- COMMAND ----------

-- Check gold tables exist and have data
SELECT 'gold_network_performance_5min' as table_name, COUNT(*) as row_count FROM telus_networkperf.gold.gold_network_performance_5min
UNION ALL
SELECT 'gold_device_health', COUNT(*) FROM telus_networkperf.gold.gold_device_health
UNION ALL
SELECT 'gold_network_events', COUNT(*) FROM telus_networkperf.gold.gold_network_events
UNION ALL
SELECT 'gold_metrics_by_location', COUNT(*) FROM telus_networkperf.gold.gold_metrics_by_location
UNION ALL
SELECT 'gold_kpi_hourly', COUNT(*) FROM telus_networkperf.gold.gold_kpi_hourly;

-- COMMAND ----------

-- Check metric views exist
SHOW VIEWS IN telus_networkperf.metrics;

-- COMMAND ----------

-- Test a metric view query (what Genie would generate)
SELECT
  `Location`,
  MEASURE(`Average Value`) as avg_latency,
  MEASURE(`P95 Value`) as p95_latency,
  MEASURE(`Anomaly Rate`) as anomaly_pct
FROM telus_networkperf.metrics.mv_network_performance
WHERE `Metric Name` = 'latency_ms'
GROUP BY `Location`
ORDER BY avg_latency DESC;

-- COMMAND ----------

-- Test device health metric view
SELECT
  `Health Status`,
  MEASURE(`Device Count`) as devices,
  MEASURE(`Average Health Score`) as avg_score,
  MEASURE(`Devices Needing Attention`) as needs_attention
FROM telus_networkperf.metrics.mv_device_health
GROUP BY `Health Status`
ORDER BY avg_score ASC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 📚 Reference Links
-- MAGIC 
-- MAGIC - [AI/BI Genie Documentation](https://docs.databricks.com/gcp/en/genie/)
-- MAGIC - [Use Metric Views with Genie](https://docs.databricks.com/gcp/en/metric-views/consume/genie.html)
-- MAGIC - [Create a Genie Space](https://docs.databricks.com/gcp/en/genie/create-genie-space.html)
-- MAGIC - [Metric Views Overview](https://docs.databricks.com/gcp/en/metric-views/)

