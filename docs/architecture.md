# Telco Network Performance Demo - Architecture

## Overview

This document provides a detailed technical architecture of the Telco Network Performance monitoring solution built on Databricks GCP.

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        GCP Cloud Environment                         │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                  GCE SFTP Server (us-central1)               │  │
│  │  ┌────────────────────────────────────────────────────────┐  │  │
│  │  │  Python Data Generator (Systemd Service)               │  │  │
│  │  │  - RFC 5424 Syslog (text files)                       │  │  │
│  │  │  - SNMP Metrics (CSV files)                           │  │  │
│  │  │  - Rate: 1000 files/minute                            │  │  │
│  │  │  - 100 simulated network devices                      │  │  │
│  │  └────────────────────────────────────────────────────────┘  │  │
│  │                            ↓                                  │  │
│  │  ┌────────────────────────────────────────────────────────┐  │  │
│  │  │  SFTP Directories                                      │  │  │
│  │  │  /sftp/telco/syslog/*.txt  (500 files/min)            │  │  │
│  │  │  /sftp/telco/snmp/*.csv    (500 files/min)            │  │  │
│  │  └────────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                            ↓                                        │
│                    Port 22 (SFTP/SSH)                              │
│                    Password Authentication                          │
│                            ↓                                        │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │              Databricks Workspace (GCP)                      │  │
│  │                  Unity Catalog Enabled                       │  │
│  │                                                              │  │
│  │  ┌────────────────────────────────────────────────────────┐ │  │
│  │  │  Unity Catalog Connection                              │ │  │
│  │  │  - Type: SFTP                                          │ │  │
│  │  │  - Credentials: Secured                                │ │  │
│  │  │  - Governance: Full lineage tracking                   │ │  │
│  │  └────────────────────────────────────────────────────────┘ │  │
│  │                            ↓                                 │  │
│  │  ┌────────────────────────────────────────────────────────┐ │  │
│  │  │         BRONZE LAYER (telco_network.bronze)            │ │  │
│  │  │                                                        │ │  │
│  │  │  Auto Loader (SFTP Connector)                         │ │  │
│  │  │  ┌──────────────────┐  ┌──────────────────┐          │ │  │
│  │  │  │ syslog_raw       │  │ snmp_raw         │          │ │  │
│  │  │  │ - Format: text   │  │ - Format: csv    │          │ │  │
│  │  │  │ - Schema: infer  │  │ - Schema: infer  │          │ │  │
│  │  │  │ - Evolution: on  │  │ - Evolution: on  │          │ │  │
│  │  │  │ - CDF: enabled   │  │ - CDF: enabled   │          │ │  │
│  │  │  └──────────────────┘  └──────────────────┘          │ │  │
│  │  └────────────────────────────────────────────────────────┘ │  │
│  │                            ↓                                 │  │
│  │  ┌────────────────────────────────────────────────────────┐ │  │
│  │  │         SILVER LAYER (telco_network.silver)            │ │  │
│  │  │              Delta Live Tables (DLT)                   │ │  │
│  │  │                                                        │ │  │
│  │  │  ┌──────────────────────────────────────────────────┐ │ │  │
│  │  │  │ silver_syslog_parsed                             │ │ │  │
│  │  │  │ - Parse RFC 5424 format                          │ │ │  │
│  │  │  │ - Extract: priority, severity, hostname, etc.    │ │ │  │
│  │  │  │ - Classify event categories                      │ │ │  │
│  │  │  │ - Expectations: device_id NOT NULL, etc.         │ │ │  │
│  │  │  └──────────────────────────────────────────────────┘ │ │  │
│  │  │                                                        │ │  │
│  │  │  ┌──────────────────────────────────────────────────┐ │ │  │
│  │  │  │ silver_snmp_metrics                              │ │ │  │
│  │  │  │ - Type casting & validation                      │ │ │  │
│  │  │  │ - Anomaly detection                              │ │ │  │
│  │  │  │ - Expectations: value >= 0, ranges, etc.         │ │ │  │
│  │  │  └──────────────────────────────────────────────────┘ │ │  │
│  │  │                                                        │ │  │
│  │  │  ┌──────────────────────────────────────────────────┐ │ │  │
│  │  │  │ silver_network_events (deduplicated)             │ │ │  │
│  │  │  └──────────────────────────────────────────────────┘ │ │  │
│  │  └────────────────────────────────────────────────────────┘ │  │
│  │                            ↓                                 │  │
│  │  ┌────────────────────────────────────────────────────────┐ │  │
│  │  │           GOLD LAYER (telco_network.gold)              │ │  │
│  │  │              Delta Live Tables (DLT)                   │ │  │
│  │  │                                                        │ │  │
│  │  │  ┌──────────────────────────────────────────────────┐ │ │  │
│  │  │  │ gold_network_performance_5min                    │ │ │  │
│  │  │  │ - 5-minute aggregations (avg, min, max, p95)     │ │ │  │
│  │  │  │ - By device, metric, location                    │ │ │  │
│  │  │  └──────────────────────────────────────────────────┘ │ │  │
│  │  │                                                        │ │  │
│  │  │  ┌──────────────────────────────────────────────────┐ │ │  │
│  │  │  │ gold_device_health                               │ │ │  │
│  │  │  │ - Current health per device                      │ │ │  │
│  │  │  │ - Health score (0-100)                           │ │ │  │
│  │  │  │ - Alert flags                                    │ │ │  │
│  │  │  └──────────────────────────────────────────────────┘ │ │  │
│  │  │                                                        │ │  │
│  │  │  ┌──────────────────────────────────────────────────┐ │ │  │
│  │  │  │ gold_network_events (enriched)                   │ │ │  │
│  │  │  └──────────────────────────────────────────────────┘ │ │  │
│  │  │                                                        │ │  │
│  │  │  ┌──────────────────────────────────────────────────┐ │ │  │
│  │  │  │ dim_devices (SCD Type 1)                         │ │ │  │
│  │  │  │ - Device master data                             │ │ │  │
│  │  │  │ - Join keys: device_id, location                 │ │ │  │
│  │  │  │ - Ready for IMS integration (Delta Share)        │ │ │  │
│  │  │  └──────────────────────────────────────────────────┘ │ │  │
│  │  │                                                        │ │  │
│  │  │  ┌──────────────────────────────────────────────────┐ │ │  │
│  │  │  │ gold_metrics_by_location                         │ │ │  │
│  │  │  │ gold_kpi_hourly                                  │ │ │  │
│  │  │  └──────────────────────────────────────────────────┘ │ │  │
│  │  └────────────────────────────────────────────────────────┘ │  │
│  │                            ↓                                 │  │
│  │  ┌────────────────────────────────────────────────────────┐ │  │
│  │  │       METRICS LAYER (telco_network.metrics)            │ │  │
│  │  │           Unity Catalog Metric Views                   │ │  │
│  │  │                                                        │ │  │
│  │  │  - mv_realtime_latency                                │ │  │
│  │  │  - mv_packet_loss_rate                                │ │  │
│  │  │  - mv_throughput_utilization                          │ │  │
│  │  │  - mv_jitter_monitoring                               │ │  │
│  │  │  - mv_error_rate_alerts                               │ │  │
│  │  │  - mv_network_health_score                            │ │  │
│  │  │  - mv_geographic_performance                          │ │  │
│  │  │  - mv_kpi_hourly_dashboard                            │ │  │
│  │  │  - mv_top_problem_devices                             │ │  │
│  │  └────────────────────────────────────────────────────────┘ │  │
│  │                            ↓                                 │  │
│  │  ┌────────────────────────────────────────────────────────┐ │  │
│  │  │              Databricks SQL & Dashboards               │ │  │
│  │  │  - Real-time monitoring dashboards                     │ │  │
│  │  │  - Executive KPI reports                               │ │  │
│  │  │  - Alerting and notifications                          │ │  │
│  │  └────────────────────────────────────────────────────────┘ │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                     Future Integration (Delta Share)                 │
│                                                                      │
│  AWS Databricks Workspace                                           │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │  IMS Gold Table (Delta Share Provider)                         │ │
│  │  - Subscriber data                                             │ │
│  │  - Session information                                         │ │
│  │  - Join keys: device_id, location                             │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                             ↓ Delta Share                           │
│  GCP Databricks (Consumer)                                          │
│  Join with dim_devices for unified telco analytics                 │
└─────────────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. Data Generation Layer

**GCE VM Configuration:**
- Machine Type: e2-standard-4 (4 vCPUs, 16 GB RAM)
- Region: us-central1 (Iowa)
- OS: Ubuntu 22.04 LTS
- Disk: 100 GB standard persistent disk

**Python Data Generator:**
- Language: Python 3
- Framework: Custom script with threading
- Libraries: Standard library (no external dependencies)
- Deployment: Systemd service for reliability

**Syslog Generation (RFC 5424):**
```
Format: <PRI>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID [STRUCTURED-DATA] MSG
Example: <134>1 2025-12-02T14:23:45.123Z router-core-01 BGP 2341 ADJCHANGE [network@32473 peer="10.20.30.40"] neighbor Down
```

**SNMP Generation (CSV):**
```csv
timestamp,device_id,device_type,location,vendor,model,ip_address,oid,metric_name,value
2025-12-02T14:23:45.123Z,router-core-01,router,datacenter-east,Cisco,ASR-9000,10.45.12.3,1.3.6.1.2.1.1.1,latency_ms,45.23
```

### 2. Networking Layer

**VPC Configuration:**
- Network: `databricks-telco-network`
- Subnet: `databricks-telco-subnet` (10.128.0.0/20)
- Firewall Rules:
  - Allow SSH (port 22) from 0.0.0.0/0 (restrict in production)
  - Allow internal traffic

**Static IP:**
- Reserved external IP for stable SFTP endpoint
- Supports Databricks IP allowlisting

### 3. Ingestion Layer (Bronze)

**Auto Loader Configuration:**

| Feature | Syslog | SNMP |
|---------|--------|------|
| Format | text | csv |
| Schema Inference | Disabled (text) | Enabled |
| Schema Evolution | addNewColumns | addNewColumns |
| File Pattern | *.txt | *.csv |
| Trigger Mode | availableNow / continuous | availableNow / continuous |
| Checkpoint | /tmp/telco_demo/checkpoints/syslog | /tmp/telco_demo/checkpoints/snmp |

**Data Governance:**
- Unity Catalog: All tables registered
- Change Data Feed: Enabled
- Lineage: Automatic tracking
- Access Control: Fine-grained permissions

### 4. Transformation Layer (Silver)

**Delta Live Tables Features:**
- Streaming tables for continuous processing
- Data quality expectations
- Automatic schema evolution
- Built-in monitoring and observability

**Syslog Parsing:**
- Regex extraction of RFC 5424 fields
- Severity calculation: `priority % 8`
- Facility calculation: `priority / 8`
- Event categorization (routing, security, hardware, etc.)

**SNMP Validation:**
- Type casting and range validation
- Anomaly detection using thresholds
- Z-score calculation for outliers
- Severity level assignment

**Data Quality Expectations:**
```python
@dlt.expect_or_drop("valid_device_id", "device_id IS NOT NULL")
@dlt.expect("valid_latency", "metric_name != 'latency_ms' OR value <= 1000")
@dlt.expect("valid_packet_loss", "metric_name != 'packet_loss_pct' OR value <= 100")
```

### 5. Analytics Layer (Gold)

**Aggregation Strategy:**
- Time Windows: 5-minute, 1-hour
- Statistical Measures: avg, min, max, p50, p95, p99, stddev
- Dimensions: device_id, device_type, location, metric_name
- Watermarking: Handle late-arriving data

**Device Health Scoring Algorithm:**
```
Health Score = 100 - (
  latency_penalty +
  packet_loss_penalty +
  jitter_penalty +
  critical_events_penalty
)

Where penalties are based on threshold ranges
```

**SCD (Slowly Changing Dimensions):**
- Type 1 for device attributes
- Effective dates for historical tracking
- Active flag for current records

### 6. Metrics Layer

**Unity Catalog Metric Views:**
- Near real-time refresh (1-minute windows)
- Pre-aggregated for dashboard performance
- SQL-based for easy consumption
- Support for time-series analysis

**KPIs Monitored:**
1. Latency (response time)
2. Packet Loss (reliability)
3. Throughput (capacity)
4. Jitter (QoS)
5. Error Rate (stability)

### 7. Data Flow & Latency

**End-to-End Latency:**
```
File Generation → SFTP Write → Auto Loader Detection → Bronze Ingestion → Silver Parsing → Gold Aggregation → Metric View
     ~0s              ~1s              ~10s                 ~15s              ~30s              ~45s             ~60s

Total: ~2 minutes from event generation to queryable metric
```

**Processing Guarantees:**
- Exactly-once semantics (Auto Loader + Delta)
- ACID transactions (Delta Lake)
- Schema evolution without data loss
- Automatic retry on failure

### 8. Unity Catalog Lineage

```
SFTP Connection → bronze.syslog_raw → silver.silver_syslog_parsed → gold.gold_network_events → metrics.mv_*
                                                                  ↘→ gold.gold_device_health
                
SFTP Connection → bronze.snmp_raw → silver.silver_snmp_metrics → gold.gold_network_performance_5min → metrics.mv_*
                                                               ↘→ gold.gold_device_health
                                                               ↘→ gold.dim_devices (for IMS join)
```

## Scalability Considerations

### Current Configuration
- **Data Volume**: 1000 files/min = ~1.44M files/day
- **File Size**: ~10-50 KB average
- **Daily Data**: ~50-200 GB/day
- **Devices**: 100 simulated devices

### Scale-Out Potential
- **10x Scale**: 10,000 files/min with cluster auto-scaling
- **100x Scale**: Add Databricks serverless for unlimited scale
- **Multi-Region**: Deploy additional SFTP servers in different GCP regions

### Performance Optimizations
- Delta Lake Z-ordering on (device_id, event_timestamp)
- Partitioning by date for time-series queries
- Auto-compaction for small file optimization
- Liquid clustering (Delta Lake 3.0+) for adaptive optimization

## Security & Compliance

### Authentication & Authorization
- SFTP: Password-based (demo) / SSH key (production)
- Databricks: Unity Catalog RBAC
- GCP: IAM roles and service accounts

### Encryption
- In-transit: TLS for SFTP, HTTPS for Databricks
- At-rest: GCP default encryption for storage
- Delta Lake: Support for customer-managed keys

### Audit & Compliance
- Unity Catalog audit logs
- GCP Cloud Audit Logs
- Delta Lake transaction log
- Data lineage tracking

## Future Enhancements

1. **Cross-Cloud Integration**
   - Delta Share with AWS for IMS data
   - Multi-cloud analytics federation

2. **Advanced Analytics**
   - ML-based anomaly detection
   - Predictive maintenance models
   - Capacity planning algorithms

3. **Real-Time Alerting**
   - Integration with PagerDuty/Slack
   - Anomaly-based triggers
   - Automated remediation workflows

4. **Additional Data Sources**
   - Direct API ingestion (REST/gRPC)
   - Kafka/Pub-Sub streaming
   - Cloud Storage (GCS) integration

## Technology Stack

| Layer | Technology | Version |
|-------|-----------|---------|
| Compute | GCE | Ubuntu 22.04 |
| Data Generation | Python | 3.10+ |
| SFTP Server | OpenSSH | 8.9+ |
| Data Platform | Databricks | DBR 17.3+ |
| Storage Format | Delta Lake | 3.0+ |
| Catalog | Unity Catalog | - |
| Orchestration | Delta Live Tables | - |
| Visualization | Databricks SQL | - |
| Cloud Provider | Google Cloud Platform | - |

## References

- [Databricks SFTP Connector](https://docs.databricks.com/gcp/en/ingestion/sftp)
- [Delta Live Tables](https://docs.databricks.com/gcp/en/ldp/develop)
- [Unity Catalog Metric Views](https://docs.databricks.com/gcp/en/metric-views/)
- [RFC 5424 Syslog Protocol](https://datatracker.ietf.org/doc/html/rfc5424)

