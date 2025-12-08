# Real-Time Performance Monitoring Demo - Architecture

## Overview

This document provides a detailed technical architecture of the real-time performance monitoring solution built on Databricks GCP. The architecture is industry-agnostic and supports multiple data domains.

## Available Industry Flavors

| Flavor | Use Case | Data Types |
|--------|----------|------------|
| **Telco Network Performance** | Network device monitoring | Syslog (RFC 5424), SNMP metrics |
| **Retail Store Performance** | Store operations monitoring | POS events, sales metrics, inventory |

## High-Level Architecture

![Architecture Flow](images/ArchitectureFlow.png)

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
- Libraries: Standard library (minimal dependencies)
- Deployment: Systemd service for reliability

**Industry-Specific Data Formats:**

| Flavor | Log Format | Metrics Format |
|--------|------------|----------------|
| Telco | RFC 5424 Syslog | CSV/JSON SNMP metrics |
| Retail | JSON event logs | JSON sales/inventory metrics |

### 2. Networking Layer

**VPC Configuration:**
- Network: Custom VPC for isolation
- Subnet: Private subnet (10.128.0.0/20)
- Firewall Rules:
  - Allow SSH (port 22) from authorized IPs
  - Allow internal traffic

**Static IP:**
- Reserved external IP for stable SFTP endpoint
- Supports Databricks IP allowlisting

### 3. Ingestion Layer (Bronze)

**Auto Loader Configuration:**

| Feature | Logs | Metrics |
|---------|------|---------|
| Format | text/json | csv/json |
| Schema Inference | Configurable | Enabled |
| Schema Evolution | addNewColumns | addNewColumns |
| Trigger Mode | availableNow / continuous | availableNow / continuous |
| Checkpoint | Unity Catalog Volume | Unity Catalog Volume |

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

**Log Parsing:**
- Format-specific extraction (RFC 5424, JSON, etc.)
- Timestamp normalization
- Event categorization
- Entity ID extraction

**Metrics Validation:**
- Type casting and range validation
- Anomaly detection using thresholds
- Z-score calculation for outliers
- Severity level assignment

**Data Quality Expectations:**
```python
@dlt.expect_or_drop("valid_entity_id", "entity_id IS NOT NULL")
@dlt.expect("valid_metric_value", "value >= 0")
@dlt.expect("valid_timestamp", "event_timestamp IS NOT NULL")
```

### 5. Analytics Layer (Gold)

**Aggregation Strategy:**
- Time Windows: 5-minute, 1-hour
- Statistical Measures: avg, min, max, p50, p95, p99, stddev
- Dimensions: entity_id, entity_type, location, metric_name
- Watermarking: Handle late-arriving data

**Entity Health Scoring Algorithm:**
```
Health Score = 100 - (
  primary_metric_penalty +
  secondary_metric_penalty +
  critical_events_penalty
)

Where penalties are based on threshold ranges
```

**SCD (Slowly Changing Dimensions):**
- Type 1 for entity attributes
- Effective dates for historical tracking
- Active flag for current records

### 6. Metrics Layer

**Unity Catalog Metric Views:**
- Near real-time refresh (1-minute windows)
- Pre-aggregated for dashboard performance
- SQL-based for easy consumption
- Support for time-series analysis

### 7. Data Flow & Latency

**End-to-End Latency:**
```
File Generation → SFTP/GCS Write → Auto Loader Detection → Bronze Ingestion → Silver Parsing → Gold Aggregation → Metric View
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
Connection → bronze.logs_raw → silver.silver_logs_parsed → gold.gold_events → metrics.mv_*
                                                         ↘→ gold.gold_entity_health
                
Connection → bronze.metrics_raw → silver.silver_metrics_validated → gold.gold_performance_5min → metrics.mv_*
                                                                   ↘→ gold.gold_entity_health
                                                                   ↘→ gold.dim_entities (for cross-cloud join)
```

## Scalability Considerations

### Current Configuration
- **Data Volume**: 5,000 files/min = ~7.2M files/day
- **File Size**: ~10-50 KB average
- **Daily Data**: ~50-200 GB/day
- **Entities**: 100+ simulated entities

### Scale-Out Potential
- **10x Scale**: 10,000 files/min with cluster auto-scaling
- **100x Scale**: Add Databricks serverless for unlimited scale
- **Multi-Region**: Deploy additional servers in different GCP regions

### Performance Optimizations
- Delta Lake Z-ordering on (entity_id, event_timestamp)
- Partitioning by date for time-series queries
- Auto-compaction for small file optimization
- Liquid clustering (Delta Lake 3.0+) for adaptive optimization

## Security & Compliance

### Authentication & Authorization
- SFTP: SSH key-based authentication (recommended for production)
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
   - Delta Share with AWS for related data
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
   - Additional Cloud Storage integration

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
