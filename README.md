# Telco Network Performance Demo on Databricks GCP

A comprehensive demonstration of real-time telco network performance monitoring using Databricks on Google Cloud Platform (GCP), showcasing SFTP ingestion, medallion architecture, and Unity Catalog governance.

## Demo Objectives

This demo showcases:

1. **Reduced Latency**: Near real-time network telemetry processing using Databricks Auto Loader and streaming
2. **Reduced TCO**: Cost optimization compared to GCP BigQuery for large-scale analytics workloads
3. **Unified Platform**: Single platform for ingestion, transformation, analytics, and serving
4. **Enterprise Governance**: Unity Catalog for lineage, security, and cross-cloud data sharing

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                         GCE VM                                │
│  ┌────────────────────────────────────────────────────────┐  │
│  │              telco_data_generator.py                    │  │
│  └────────────────────┬─────────────────┬─────────────────┘  │
│                       │                 │                     │
│             Syslog (TXT)          SNMP (JSON)                │
│                       │                 │                     │
│                       ▼                 ▼                     │
│            /sftp/telco/syslog    GCS Bucket                  │
│                       │          gs://bucket/snmp/           │
└───────────────────────│─────────────────│────────────────────┘
                        │                 │
                        │    1000 files/minute                  
                        ▼                 ▼
┌─────────────────────────────────────────────────────────────┐
│              Databricks on GCP (Unity Catalog)              │
│                                                              │
│  ┌─────────────┐  ┌─────────────┐                           │
│  │ Auto Loader │  │ Auto Loader │                           │
│  │   (SFTP)    │  │   (GCS)     │                           │
│  │  Syslog     │  │   SNMP      │                           │
│  └──────┬──────┘  └──────┬──────┘                           │
│         │                │                                   │
│         ▼                ▼                                   │
│  ┌──────────────────────────────┐                           │
│  │          BRONZE              │                           │
│  │  syslog_raw  │  snmp_raw     │                           │
│  └──────────────┬───────────────┘                           │
│                 │                                            │
│                 ▼                                            │
│  ┌──────────────────────────────┐                           │
│  │          SILVER              │ ← Lakeflow DLT            │
│  │  Parsed, Validated, Enriched │ ← Expectations            │
│  └──────────────┬───────────────┘                           │
│                 │                                            │
│                 ▼                                            │
│  ┌──────────────────────────────┐                           │
│  │           GOLD               │                           │
│  │  Aggregations, KPIs, Health  │ → Metric Views            │
│  └──────────────────────────────┘                           │
└─────────────────────────────────────────────────────────────┘
                        │
                        │ Delta Share (Future)
                        ▼
              ┌─────────────────┐
              │  AWS Databricks │
              │   IMS Data      │
              └─────────────────┘
```

## Repository Structure

```
GCPNetworkPerfETLDemo/
├── infrastructure/           # GCP infrastructure setup
│   ├── gcp_sftp_setup.sh    # GCE VM and networking setup
│   ├── gcp_gcs_setup.sh     # GCS bucket setup for SNMP
│   ├── configure_sftp.sh    # SFTP server configuration
│   ├── telco-generator.service  # Systemd service for data generator
│   └── sftp_config.env      # Generated config (after setup)
│
├── scripts/
│   ├── telco_data_generator.py  # Synthetic data generator (syslog→SFTP, SNMP→GCS)
│   └── schema_evolution_demo.py # Schema evolution demo script
│
├── databricks/                              # Databricks notebooks
│   ├── 00_setup_sftp_connection.py          # Unity Catalog SFTP connection
│   ├── 00_1_setup_gcs_connection.py         # Unity Catalog GCS connection + file events
│   ├── 01_0_bronze_ingestion_combined_reference.py  # Combined ingestion (reference)
│   ├── 01_1_bronze_ingestion_syslog.py      # Syslog Auto Loader from SFTP
│   ├── 01_2_bronze_ingestion_snmp.py        # SNMP Auto Loader from GCS (JSON)
│   ├── 01_3_bronze_monitor.py               # Bronze layer monitoring
│   ├── 02_silver_pipeline.sql               # Lakeflow DLT Silver (SQL)
│   ├── 02_silver_pipeline_python_reference.py  # Silver pipeline (Python reference)
│   ├── 03_gold_pipeline.sql                 # Lakeflow DLT Gold (SQL)
│   ├── 03_gold_pipeline_python_reference.py # Gold pipeline (Python reference)
│   └── 04_metric_views.sql                  # Unity Catalog metric views
│
├── docs/                     # Documentation
│   ├── architecture.md       # Architecture details
│   ├── demo_script.md        # Demo presentation guide
│   ├── tco_analysis.md       # TCO comparison
│   └── performance_metrics.md  # Performance benchmarks
│
└── README.md                 # This file
```

## Quick Start

### Prerequisites

- GCP project with billing enabled
- Databricks workspace on GCP with Unity Catalog enabled
- `gcloud` CLI installed and configured
- Databricks CLI configured (optional)

### Step 1: Deploy SFTP Server on GCP

```bash
# Clone the repository
git clone <repository-url>
cd GCPNetworkPerfETLDemo

# Set your GCP project ID
export GCP_PROJECT_ID="your-gcp-project-id"

# Make scripts executable
chmod +x infrastructure/*.sh

# Deploy GCE VM and configure networking
cd infrastructure
./gcp_sftp_setup.sh

# Note the external IP address from the output
# Update the IP in all Databricks notebooks
```

### Step 2: Configure SFTP Server

```bash
# Copy configuration script to the VM
gcloud compute scp configure_sftp.sh telco-sftp-server:~/ --zone=us-central1-a

# SSH into the VM and run configuration
gcloud compute ssh telco-sftp-server --zone=us-central1-a --command='chmod +x ~/configure_sftp.sh && sudo ~/configure_sftp.sh'

# Copy data generator to the VM
gcloud compute scp ../scripts/telco_data_generator.py telco-sftp-server:/opt/telco-generator/ --zone=us-central1-a

# Install systemd service
gcloud compute scp telco-generator.service telco-sftp-server:/tmp/ --zone=us-central1-a
gcloud compute ssh telco-sftp-server --zone=us-central1-a --command='sudo mv /tmp/telco-generator.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl enable telco-generator && sudo systemctl start telco-generator'
```

### Step 3: Set Up Databricks

#### 3.1: Update Configuration

Configure secrets in Databricks for SFTP connection:
```python
# Set secrets (via Databricks CLI or UI)
# Scope: <YOUR_SECRET_SCOPE> (create your own scope)
# Keys: SFTP_HOST, SFTP_USERNAME, SFTP_PASSWORD
```

#### 3.2: Run Notebooks in Order

1. **00_setup_sftp_connection.py**: Creates Unity Catalog connection and schemas
2. **01_1_bronze_ingestion_syslog.py**: Starts syslog Auto Loader (separate compute)
3. **01_2_bronze_ingestion_snmp.py**: Starts SNMP Auto Loader (separate compute)
4. **01_3_bronze_monitor.py**: Monitor ingestion progress, enable CDF
5. **02_silver_pipeline.sql**: Create DLT pipeline for silver layer (SQL syntax)
6. **03_gold_pipeline.sql**: Create DLT pipeline for gold layer (SQL syntax)
7. **04_metric_views.sql**: Create Unity Catalog metric views

> **Note**: Python reference notebooks (`*_python_reference.py`) are available for developers who prefer Python syntax for DLT pipelines.

#### 3.3: Create Delta Live Tables Pipelines

**Silver Pipeline:**
1. Go to Workflows → Delta Live Tables → Create Pipeline
2. Pipeline Name: `telco_silver_pipeline`
3. Notebook: `databricks/02_silver_pipeline.sql`
4. Target: `telus_networkperf.silver`
5. Enable: Auto Scaling, Schema Evolution
6. Start Pipeline

**Gold Pipeline:**
1. Create Pipeline: `telco_gold_pipeline`
2. Notebook: `databricks/03_gold_pipeline.sql`
3. Target: `telus_networkperf.gold`
4. Enable: Auto Scaling
5. Start Pipeline

> **Note**: For Python-based DLT pipelines, use `02_silver_pipeline_python_reference.py` and `03_gold_pipeline_python_reference.py` instead.

### Step 4: Monitor and Visualize

1. Run `04_metric_views.sql` to create metric views
2. Create Databricks SQL Dashboard using the metric views
3. Monitor Unity Catalog lineage
4. Check data quality metrics

## Key Features Demonstrated

### 1. Auto Loader for SFTP
- Native SFTP connector with Unity Catalog integration
- Automatic schema inference and evolution
- Exactly-once processing guarantees
- Support for structured (CSV) and unstructured (text) data

### 2. Medallion Architecture
- **Bronze**: Raw data with full lineage
- **Silver**: Parsed, cleaned, validated data with expectations
- **Gold**: Business-ready aggregations and metrics

### 3. Data Quality with Expectations
- NOT NULL constraints on critical fields
- Range validation on metrics
- Anomaly detection
- Rescued data handling

### 4. Unity Catalog Governance
- End-to-end lineage tracking
- Fine-grained access control
- Audit logging
- Ready for cross-cloud Delta Sharing

### 5. Real-Time Monitoring
- Network KPI metrics (latency, packet loss, throughput, jitter, error rate)
- Device health scoring
- Geographic performance analysis
- Automated alerting thresholds

## Demo Script

See [docs/demo_script.md](docs/demo_script.md) for a complete demo walkthrough including:
- Narrative flow
- Key talking points
- Live demonstrations
- Q&A preparation

## TCO Analysis

Detailed cost comparison between Databricks and BigQuery:
- See [docs/tco_analysis.md](docs/tco_analysis.md)
- 40-60% cost reduction for typical telco workloads
- Reduced operational overhead
- Unified platform savings

## Performance Metrics

End-to-end latency benchmarks:
- See [docs/performance_metrics.md](docs/performance_metrics.md)
- File landing to query: < 2 minutes
- Compare to traditional ETL: 15-30 minutes
- Real-time dashboard updates

## Cross-Cloud Integration

The gold layer is prepared for Delta Sharing with AWS Databricks:

```sql
-- Example join with IMS data from AWS (via Delta Share)
SELECT 
  d.device_id,
  d.location,
  d.current_latency_ms,
  i.ims_session_id,
  i.subscriber_id
FROM telus_networkperf.gold.dim_devices d
INNER JOIN aws_delta_share.ims_gold_table i
  ON d.device_id = i.device_id
  AND d.location = i.location
WHERE d.is_active = TRUE;
```

## Documentation

- [Architecture Details](docs/architecture.md)
- [Demo Script](docs/demo_script.md)
- [TCO Analysis](docs/tco_analysis.md)
- [Performance Metrics](docs/performance_metrics.md)
- [Data Generator Control Guide](docs/data_generator_control.md)

## Configuration

### Data Generator Configuration

Adjust the data generation rate:

```bash
# SSH into the SFTP VM
gcloud compute ssh telco-sftp-server --zone=us-central1-a

# Edit the systemd service
sudo nano /etc/systemd/system/telco-generator.service

# Change --files-per-minute parameter (default: 1000)
# Restart the service
sudo systemctl restart telco-generator

# Check status
sudo systemctl status telco-generator
```

### Network KPIs Monitored

1. **Latency** (ms): Network round-trip time
   - Normal: < 50ms
   - Warning: 50-100ms
   - Critical: > 150ms

2. **Packet Loss** (%): Percentage of lost packets
   - Acceptable: < 0.5%
   - Warning: 0.5-1%
   - Critical: > 3%

3. **Throughput** (Mbps): Data transfer rate
   - Low: < 1000 Mbps
   - Normal: 1000-5000 Mbps
   - High: > 9000 Mbps

4. **Jitter** (ms): Variation in latency
   - Good QoS: < 10ms
   - Degraded: 20-40ms
   - Poor: > 40ms

5. **Error Rate**: Errors per time window
   - Low: < 100
   - Medium: 100-400
   - Critical: > 800

## Troubleshooting

### SFTP Connection Issues

```bash
# Test SFTP connectivity
sftp telco_user@<SFTP_IP>

# Check SFTP server logs
gcloud compute ssh telco-sftp-server --zone=us-central1-a --command='sudo journalctl -u sshd -f'

# Check data generator logs
gcloud compute ssh telco-sftp-server --zone=us-central1-a --command='sudo journalctl -u telco-generator -f'
```

### Databricks Pipeline Issues

```bash
# Check Auto Loader progress
# In Databricks notebook:
display(spark.sql("DESCRIBE HISTORY telus_networkperf.bronze.syslog_raw"))

# Check DLT pipeline events
# Go to: Workflows → Delta Live Tables → [Pipeline] → Events

# Verify Unity Catalog lineage
# Go to: Catalog → telus_networkperf → [table] → Lineage
```

## Support

For issues or questions:
- Check the [docs/](docs/) directory for detailed documentation
- Review Databricks logs and DLT pipeline events
- Verify GCP firewall rules and SFTP connectivity

## License

This demo is provided as-is for educational and demonstration purposes.

## Acknowledgments

- RFC 5424 Syslog Protocol
- Databricks Auto Loader and DLT documentation
- GCP Compute Engine documentation

# databricks-telco-network-perf
