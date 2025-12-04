-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Cross-Cloud IMS Analytics - GCP Workspace
-- MAGIC 
-- MAGIC This notebook demonstrates how to consume Delta Shared IMS data from AWS
-- MAGIC and join it with local GCP network performance gold tables.
-- MAGIC 
-- MAGIC ## Prerequisites
-- MAGIC 1. AWS Databricks workspace has run `01_ims_data_setup.py`
-- MAGIC 2. Delta Share recipient has been created and activated
-- MAGIC 3. GCP workspace has accepted the share

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 1: Create Catalog from Delta Share
-- MAGIC 
-- MAGIC First, create a catalog in GCP from the AWS Delta Share.

-- COMMAND ----------

-- Create catalog from AWS share (run once after accepting share)
-- Replace 'aws_provider' with your actual provider name
-- CREATE CATALOG IF NOT EXISTS aws_ims_shared
-- USING SHARE `aws_provider`.telco_ims_share;

-- For demo purposes, we'll reference the catalog as aws_ims_shared
-- The actual catalog name depends on how you create it from the share

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2: Explore Shared IMS Data

-- COMMAND ----------

-- View available tables in the shared catalog
SHOW TABLES IN aws_ims_shared.ims_data;

-- COMMAND ----------

-- Preview VoLTE CDR data
SELECT 
    cdr_id,
    call_start_time,
    call_duration_seconds,
    caller_msisdn,
    call_type,
    codec,
    is_answered,
    mos_score,
    jitter_ms,
    packet_loss_pct,
    originating_device_id,
    originating_location,
    originating_device_key,
    originating_region
FROM aws_ims_shared.ims_data.ims_volte_cdrs
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 3: Cross-Cloud Joins - IMS + Network Performance

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.1 Correlate Call Quality with Network Device Health

-- COMMAND ----------

-- Join VoLTE CDRs with device health to correlate call quality with network status
CREATE OR REPLACE TEMP VIEW v_call_quality_device_health AS
SELECT 
    c.cdr_id,
    c.call_start_time,
    c.call_duration_seconds,
    c.call_type,
    c.mos_score AS call_mos_score,
    c.jitter_ms AS call_jitter_ms,
    c.packet_loss_pct AS call_packet_loss,
    c.is_answered,
    c.termination_cause,
    
    -- Device info from GCP dim_devices
    d.device_id,
    d.device_type,
    d.vendor,
    d.model,
    d.device_tier,
    
    -- Device health from GCP gold layer
    h.current_latency_ms AS device_latency_ms,
    h.current_packet_loss_pct AS device_packet_loss,
    h.current_jitter_ms AS device_jitter_ms,
    h.health_score AS device_health_score,
    h.health_status AS device_health_status,
    h.event_count_1h AS device_events_1h,
    h.critical_event_count_1h AS device_critical_events_1h,
    
    -- Location context
    c.originating_location,
    c.originating_region
FROM aws_ims_shared.ims_data.ims_volte_cdrs c
INNER JOIN telus_networkperf.gold.dim_devices d 
    ON c.originating_device_id = d.device_id 
    AND c.originating_location = d.location
LEFT JOIN telus_networkperf.gold.gold_device_health h 
    ON d.device_id = h.device_id;

-- View the correlation
SELECT * FROM v_call_quality_device_health LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.2 Analyze Call Quality by Device Health Status

-- COMMAND ----------

-- Aggregate call quality metrics by device health status
SELECT 
    device_health_status,
    device_vendor AS vendor,
    COUNT(*) AS total_calls,
    ROUND(AVG(call_mos_score), 2) AS avg_mos_score,
    ROUND(AVG(call_jitter_ms), 2) AS avg_call_jitter_ms,
    ROUND(AVG(call_packet_loss), 3) AS avg_call_packet_loss,
    ROUND(SUM(CASE WHEN is_answered THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS answer_rate_pct,
    ROUND(AVG(device_latency_ms), 2) AS avg_device_latency_ms,
    ROUND(AVG(device_health_score), 1) AS avg_device_health_score
FROM v_call_quality_device_health
WHERE call_mos_score IS NOT NULL
GROUP BY device_health_status, device_vendor
ORDER BY device_health_status, vendor;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.3 Create Gold Table: IMS Call Quality Analysis

-- COMMAND ----------

CREATE OR REPLACE TABLE telus_networkperf.gold.gold_ims_call_quality_analysis
COMMENT 'Cross-cloud analysis of IMS call quality correlated with network device performance'
TBLPROPERTIES (
    'quality' = 'gold',
    'source' = 'aws_ims_delta_share + gcp_network_perf',
    'delta.enableChangeDataFeed' = 'true'
)
AS
WITH call_metrics AS (
    SELECT 
        DATE_TRUNC('hour', TO_TIMESTAMP(c.call_start_time)) AS hour_start,
        c.call_type,
        c.originating_region AS region,
        d.vendor,
        d.device_type,
        h.health_status AS device_health_status,
        
        -- Call metrics
        c.call_duration_seconds,
        c.mos_score,
        c.jitter_ms,
        c.packet_loss_pct,
        c.is_answered,
        
        -- Device metrics
        h.health_score AS device_health_score,
        h.current_latency_ms,
        h.current_packet_loss_pct,
        h.critical_event_count_1h
        
    FROM aws_ims_shared.ims_data.ims_volte_cdrs c
    INNER JOIN telus_networkperf.gold.dim_devices d 
        ON c.originating_device_id = d.device_id 
        AND c.originating_location = d.location
    LEFT JOIN telus_networkperf.gold.gold_device_health h 
        ON d.device_id = h.device_id
)
SELECT 
    hour_start,
    call_type,
    region,
    vendor,
    device_type,
    device_health_status,
    
    -- Call volume
    COUNT(*) AS total_calls,
    SUM(CASE WHEN is_answered THEN 1 ELSE 0 END) AS answered_calls,
    ROUND(SUM(CASE WHEN is_answered THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS answer_rate_pct,
    
    -- Call duration
    ROUND(AVG(call_duration_seconds), 1) AS avg_call_duration_sec,
    SUM(call_duration_seconds) AS total_call_seconds,
    
    -- Quality metrics
    ROUND(AVG(mos_score), 2) AS avg_mos_score,
    ROUND(MIN(mos_score), 2) AS min_mos_score,
    ROUND(PERCENTILE_APPROX(mos_score, 0.05), 2) AS p5_mos_score,
    ROUND(AVG(jitter_ms), 2) AS avg_jitter_ms,
    ROUND(AVG(packet_loss_pct), 3) AS avg_packet_loss_pct,
    
    -- Device health context
    ROUND(AVG(device_health_score), 1) AS avg_device_health_score,
    ROUND(AVG(current_latency_ms), 2) AS avg_device_latency_ms,
    SUM(critical_event_count_1h) AS total_critical_device_events,
    
    current_timestamp() AS processed_timestamp
    
FROM call_metrics
GROUP BY 
    hour_start, call_type, region, vendor, device_type, device_health_status;

-- COMMAND ----------

-- Preview the analysis table
SELECT * FROM telus_networkperf.gold.gold_ims_call_quality_analysis
ORDER BY hour_start DESC, total_calls DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.4 IMS SIP Sessions + Network Events Correlation

-- COMMAND ----------

-- Correlate SIP session failures with network events
CREATE OR REPLACE TEMP VIEW v_sip_network_events AS
SELECT 
    s.session_id,
    s.session_start_time,
    s.sip_method,
    s.final_response_code,
    s.final_response_desc,
    s.is_successful,
    s.invite_response_time_ms,
    s.session_setup_time_ms,
    s.ims_node_type,
    
    -- Network events from GCP
    e.event_type,
    e.event_category,
    e.severity_name,
    e.is_critical AS network_event_critical,
    
    -- Device context
    s.device_id,
    s.location,
    s.region
FROM aws_ims_shared.ims_data.ims_sip_sessions s
LEFT JOIN telus_networkperf.gold.gold_network_events e
    ON s.device_id = e.device_id
    AND TO_TIMESTAMP(s.session_start_time) BETWEEN 
        e.event_timestamp - INTERVAL 5 MINUTES 
        AND e.event_timestamp + INTERVAL 5 MINUTES;

-- Analyze SIP failures with concurrent network events
SELECT 
    sip_method,
    final_response_code,
    final_response_desc,
    COUNT(*) AS session_count,
    SUM(CASE WHEN is_successful THEN 0 ELSE 1 END) AS failed_sessions,
    COUNT(DISTINCT CASE WHEN network_event_critical THEN session_id END) AS sessions_with_critical_events,
    ROUND(AVG(invite_response_time_ms), 2) AS avg_invite_response_ms,
    ROUND(AVG(session_setup_time_ms), 2) AS avg_setup_time_ms
FROM v_sip_network_events
GROUP BY sip_method, final_response_code, final_response_desc
ORDER BY failed_sessions DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 4: Create Cross-Cloud Analytics Views

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4.1 Regional Performance Dashboard View

-- COMMAND ----------

CREATE OR REPLACE VIEW telus_networkperf.gold.v_regional_ims_network_kpis AS
SELECT 
    q.region,
    q.hour_start,
    
    -- IMS KPIs (from AWS via Delta Share)
    q.total_calls,
    q.answer_rate_pct,
    q.avg_mos_score,
    q.avg_jitter_ms AS ims_avg_jitter_ms,
    
    -- Network KPIs (from GCP)
    n.location_avg_value AS network_avg_latency_ms,
    n.location_p95_value AS network_p95_latency_ms,
    n.total_anomalies AS network_anomaly_count,
    n.device_count AS monitored_devices,
    
    -- Combined health indicator
    CASE 
        WHEN q.avg_mos_score >= 4.0 AND n.total_anomalies = 0 THEN 'Excellent'
        WHEN q.avg_mos_score >= 3.5 AND n.total_anomalies < 5 THEN 'Good'
        WHEN q.avg_mos_score >= 3.0 OR n.total_anomalies < 10 THEN 'Fair'
        ELSE 'Poor'
    END AS combined_health_status

FROM telus_networkperf.gold.gold_ims_call_quality_analysis q
LEFT JOIN telus_networkperf.gold.gold_metrics_by_location n
    ON q.region = n.location
    AND q.hour_start = n.window_start
    AND n.metric_name = 'latency_ms';

-- Preview regional KPIs
SELECT * FROM telus_networkperf.gold.v_regional_ims_network_kpis
WHERE hour_start >= current_timestamp() - INTERVAL 24 HOURS
ORDER BY hour_start DESC, region
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4.2 Subscriber Quality Experience View

-- COMMAND ----------

CREATE OR REPLACE VIEW telus_networkperf.gold.v_subscriber_quality_experience AS
SELECT 
    sub.subscription_type,
    sub.home_location,
    sub.region,
    sub.access_technology,
    
    -- Subscriber session metrics (from AWS)
    COUNT(*) AS total_sessions,
    ROUND(AVG(sub.voice_minutes_used), 2) AS avg_voice_minutes,
    ROUND(AVG(sub.video_minutes_used), 2) AS avg_video_minutes,
    ROUND(AVG(sub.data_mb_used), 2) AS avg_data_mb,
    ROUND(AVG(sub.avg_call_mos_score), 2) AS avg_subscriber_mos,
    SUM(sub.dropped_calls) AS total_dropped_calls,
    SUM(sub.failed_call_setups) AS total_failed_setups,
    
    -- Network context (from GCP)
    ROUND(AVG(h.health_score), 1) AS avg_network_health_score,
    COUNT(DISTINCT CASE WHEN h.health_status = 'Critical' THEN sub.device_id END) AS critical_device_count

FROM aws_ims_shared.ims_data.ims_subscriber_sessions sub
LEFT JOIN telus_networkperf.gold.gold_device_health h
    ON sub.device_id = h.device_id
GROUP BY 
    sub.subscription_type, 
    sub.home_location, 
    sub.region, 
    sub.access_technology;

-- Preview subscriber experience
SELECT * FROM telus_networkperf.gold.v_subscriber_quality_experience
ORDER BY total_sessions DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 5: IMS Node Performance Analysis

-- COMMAND ----------

-- Analyze IMS node performance with underlying network metrics
SELECT 
    n.ims_node_type,
    n.ims_node_location,
    n.region,
    
    -- IMS node metrics (from AWS)
    COUNT(*) AS metric_samples,
    ROUND(AVG(n.active_sessions), 0) AS avg_active_sessions,
    ROUND(AVG(n.cpu_utilization_pct), 2) AS avg_cpu_pct,
    ROUND(AVG(n.memory_utilization_pct), 2) AS avg_memory_pct,
    ROUND(AVG(n.avg_response_time_ms), 2) AS avg_response_time_ms,
    ROUND(AVG(n.error_rate_pct), 3) AS avg_error_rate_pct,
    SUM(CASE WHEN n.is_anomaly THEN 1 ELSE 0 END) AS ims_anomaly_count,
    
    -- Network device health (from GCP)
    ROUND(AVG(h.health_score), 1) AS avg_underlying_device_health,
    SUM(h.critical_event_count_1h) AS total_network_critical_events

FROM aws_ims_shared.ims_data.ims_node_metrics n
LEFT JOIN telus_networkperf.gold.gold_device_health h
    ON n.device_id = h.device_id
GROUP BY n.ims_node_type, n.ims_node_location, n.region
ORDER BY ims_anomaly_count DESC, avg_error_rate_pct DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Summary
-- MAGIC 
-- MAGIC ### Cross-Cloud Data Integration Complete!
-- MAGIC 
-- MAGIC **AWS Delta Shared Tables (IMS Data):**
-- MAGIC - `aws_ims_shared.ims_data.ims_volte_cdrs` - VoLTE Call Detail Records
-- MAGIC - `aws_ims_shared.ims_data.ims_sip_sessions` - SIP Session Metrics
-- MAGIC - `aws_ims_shared.ims_data.ims_node_metrics` - IMS Core Network Metrics
-- MAGIC - `aws_ims_shared.ims_data.ims_subscriber_sessions` - Subscriber Analytics
-- MAGIC 
-- MAGIC **GCP Gold Tables (Network Performance):**
-- MAGIC - `telus_networkperf.gold.dim_devices` - Device Dimension
-- MAGIC - `telus_networkperf.gold.gold_device_health` - Device Health Status
-- MAGIC - `telus_networkperf.gold.gold_network_events` - Network Events
-- MAGIC - `telus_networkperf.gold.gold_network_performance_5min` - Performance Metrics
-- MAGIC 
-- MAGIC **New Cross-Cloud Analytics Tables/Views:**
-- MAGIC - `telus_networkperf.gold.gold_ims_call_quality_analysis` - Call Quality + Device Health
-- MAGIC - `telus_networkperf.gold.v_regional_ims_network_kpis` - Regional Dashboard KPIs
-- MAGIC - `telus_networkperf.gold.v_subscriber_quality_experience` - Subscriber QoE Analysis
-- MAGIC 
-- MAGIC ### Join Keys Used
-- MAGIC | AWS IMS Field | GCP Gold Field | Join Type |
-- MAGIC |--------------|----------------|-----------|
-- MAGIC | `device_id` | `dim_devices.device_id` | Primary |
-- MAGIC | `location` | `dim_devices.location` | Composite |
-- MAGIC | `device_key` | `dim_devices.device_key` | Alternative |
-- MAGIC | `region` | `dim_devices.region` | Grouping |

