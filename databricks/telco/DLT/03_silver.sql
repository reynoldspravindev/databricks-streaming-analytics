-- Telco Network Performance — Silver Layer
-- Parses syslog and validates SNMP metrics into clean silver tables.
-- All CREATE statements use fully-qualified names so silver lands in `silver` schema.

CREATE OR REFRESH STREAMING TABLE telco_networkperf.silver.silver_syslog_parsed (
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
  CAST(CAST(regexp_extract(value, '^<(\\d+)>', 1) AS INT) / 8 AS INT) AS facility,
  CAST(regexp_extract(value, '^<(\\d+)>', 1) AS INT) % 8 AS severity,
  CASE CAST(regexp_extract(value, '^<(\\d+)>', 1) AS INT) % 8
    WHEN 0 THEN 'Emergency' WHEN 1 THEN 'Alert' WHEN 2 THEN 'Critical' WHEN 3 THEN 'Error'
    WHEN 4 THEN 'Warning' WHEN 5 THEN 'Notice' WHEN 6 THEN 'Informational' WHEN 7 THEN 'Debug'
    ELSE 'Unknown'
  END AS severity_name,
  to_timestamp(regexp_extract(value, '^<\\d+>\\d+\\s+(\\S+)', 1), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") AS event_timestamp,
  regexp_extract(regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+(\\S+)', 1), '^([^-]+)', 1) AS device_type,
  regexp_extract(regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+(\\S+)', 1), '^[^-]+-([^-]+)', 1) AS location,
  CASE
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) IN ('LINKUPDOWN', 'IFDOWN', 'IFUP') THEN 'interface_event'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) IN ('ADJCHANGE', 'OSPFADJCHG') THEN 'routing_event'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) = 'SECURITY' THEN 'security_event'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) = 'HASTATE' THEN 'availability_event'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) IN ('FANFAIL', 'TEMPWARN') THEN 'hardware_event'
    WHEN regexp_extract(value, '^<\\d+>\\d+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+(\\S+)', 1) = 'CFGCHANGE' THEN 'configuration_event'
    ELSE 'other'
  END AS event_category,
  CAST(regexp_extract(value, '^<(\\d+)>', 1) AS INT) % 8 <= 3 AS is_critical,
  ingestion_timestamp,
  current_timestamp() AS processed_timestamp
FROM STREAM(telco_networkperf.bronze.syslog_raw)
WHERE regexp_extract(value, '^<(\\d+)>', 1) IS NOT NULL;


CREATE OR REFRESH STREAMING TABLE telco_networkperf.silver.silver_snmp_metrics (
  CONSTRAINT valid_device_id EXPECT (device_id IS NOT NULL AND device_id != '') ON VIOLATION DROP ROW,
  CONSTRAINT valid_metric_name EXPECT (metric_name IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_value EXPECT (value IS NOT NULL AND value >= 0) ON VIOLATION DROP ROW
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
  CASE
    WHEN metric_name = 'latency_ms' AND CAST(value AS DOUBLE) > 150 THEN TRUE
    WHEN metric_name = 'packet_loss_pct' AND CAST(value AS DOUBLE) > 3 THEN TRUE
    WHEN metric_name = 'throughput_mbps' AND CAST(value AS DOUBLE) > 9500 THEN TRUE
    WHEN metric_name = 'jitter_ms' AND CAST(value AS DOUBLE) > 40 THEN TRUE
    WHEN metric_name = 'error_rate' AND CAST(value AS DOUBLE) > 800 THEN TRUE
    ELSE FALSE
  END AS is_anomaly,
  CASE
    WHEN metric_name LIKE '%_ms' THEN 'milliseconds'
    WHEN metric_name LIKE '%_pct' THEN 'percent'
    WHEN metric_name LIKE '%_mbps' THEN 'megabits_per_second'
    WHEN metric_name = 'error_rate' THEN 'count'
    ELSE 'unknown'
  END AS metric_unit,
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
FROM STREAM(telco_networkperf.bronze.snmp_raw);


CREATE OR REFRESH MATERIALIZED VIEW telco_networkperf.silver.silver_network_events
COMMENT 'Deduplicated and categorized network events'
TBLPROPERTIES ('quality' = 'silver', 'pipelines.autoOptimize.managed' = 'true')
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
FROM telco_networkperf.silver.silver_syslog_parsed;
