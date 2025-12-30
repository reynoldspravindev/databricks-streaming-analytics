# Real-Time Performance Monitoring Demo on Databricks GCP

A comprehensive demonstration of real-time performance monitoring using Databricks on Google Cloud Platform (GCP), showcasing SFTP ingestion, medallion architecture, and Unity Catalog governance.

## Available Industry Flavors

This demo supports multiple industry use cases. Choose the one that best fits your needs:

| Flavor | Description | Data Generator | Notebooks |
|--------|-------------|----------------|-----------|
| **Telco Network Performance** | Monitor network devices, latency, packet loss, throughput | `telco_data_generator.py` | `databricks/*.py/sql` |
| **Retail Store Performance** | Track store metrics for fast food chains (with drive-through) and apparel retail | `retail_data_generator.py` | `databricks/retail/*.py/sql` |

## Demo Objectives

This demo showcases:

1. **Reduced Latency**: Near real-time telemetry processing using Databricks Auto Loader and streaming
2. **Reduced TCO**: Cost optimization compared to GCP BigQuery for large-scale analytics workloads
3. **Unified Platform**: Single platform for ingestion, transformation, analytics, and serving
4. **Enterprise Governance**: Unity Catalog for lineage, security, and cross-cloud data sharing

## Architecture

![Architecture Diagram](docs/images/ReadmeMainFlow.png)

## Repository Structure

```
GCPNetworkPerfETLDemo/
├── infrastructure/           # GCP infrastructure setup
│   ├── gcp_sftp_setup.sh    # GCE VM and networking setup
│   ├── gcp_gcs_setup.sh     # GCS bucket setup for metrics
│   ├── configure_sftp.sh    # SFTP server configuration
│   ├── telco-generator.service   # Systemd service (Telco)
│   ├── retail-generator.service  # Systemd service (Retail)
│   └── sftp_config.env      # Generated config (after setup)
│
├── scripts/
│   ├── telco_data_generator.py   # Telco: Network performance data
│   ├── retail_data_generator.py  # Retail: Store performance data
│   ├── ims_data_generator.py     # Cross-cloud IMS data
│   └── schema_evolution_demo.py  # Schema evolution demo script
│
├── databricks/                              # Telco Databricks notebooks
│   ├── 00_setup_sftp_connection.py          # Unity Catalog SFTP connection
│   ├── 01_1_bronze_ingestion_syslog.py      # Syslog Auto Loader from SFTP
│   ├── 01_2_bronze_ingestion_snmp.py        # SNMP Auto Loader from GCS
│   ├── 01_3_bronze_monitor.py               # Bronze layer monitoring
│   ├── 02_silver_pipeline.sql               # Lakeflow DLT Silver
│   ├── 03_gold_pipeline.sql                 # Lakeflow DLT Gold
│   ├── 04_metric_views.sql                  # Unity Catalog metric views
│   ├── 05_genie_space_setup.sql             # Genie AI assistant setup
│   ├── 06_ims_cross_cloud_analytics.sql     # Cross-cloud analytics
│   ├── aws/                                 # AWS cross-cloud notebooks
│   ├── retail/                              # Retail industry notebooks
│   └── archived/                            # Reference implementations
│
├── docs/                     # Documentation
│   ├── architecture.md       # Architecture details
│   ├── data_generator_control.md  # Generator control guide
│   └── performance_metrics.md     # Performance benchmarks
│
└── README.md                 # This file
```

## Quick Start

### Prerequisites

- GCP project with billing enabled
- Databricks workspace on GCP with Unity Catalog enabled
- `gcloud` CLI installed and configured
- Databricks CLI configured (optional)
- **Unity Catalog External Location** for GCS bucket access (required for SNMP/GCS ingestion):
  - Create a storage credential using a GCP service account with `storage.objectViewer` permissions on your GCS bucket
  - Create an external location pointing to your GCS bucket path (e.g., `gs://your-bucket/snmp/`)
  - **Optional**: Enable file notifications for low-latency ingestion:
    - **Managed file events** (recommended): Set `cloudFiles.useManagedFileEvents=true` - Databricks manages Pub/Sub automatically
    - **Unmanaged file events**: Configure your own GCS Pub/Sub notification and provide the subscription path
  - Reference: [Databricks File Notification Mode](https://docs.databricks.com/gcp/en/ingestion/cloud-object-storage/auto-loader/file-notification-mode)

### Shell Environment Setup

To avoid repeatedly entering passphrases and ensure `gcloud` is available in all terminal sessions, configure your shell:

```bash
# Add Google Cloud SDK to PATH and enable ssh-agent
cat >> ~/.zshrc << 'EOF'

# Google Cloud SDK
export PATH="$HOME/google-cloud-sdk/bin:$PATH"
source $HOME/google-cloud-sdk/path.zsh.inc 2>/dev/null
source $HOME/google-cloud-sdk/completion.zsh.inc 2>/dev/null

# SSH Agent - Auto-start and add keys
if [ -z "$SSH_AUTH_SOCK" ]; then
  eval "$(ssh-agent -s)" > /dev/null
  ssh-add ~/.ssh/google_compute_engine 2>/dev/null
fi
EOF

# Reload your shell configuration
source ~/.zshrc

# Verify setup
which gcloud
gcloud version
```

**What this does:**
- **gcloud PATH**: Makes `gcloud` command available in all terminal sessions
- **Auto-completion**: Enables tab completion for gcloud commands
- **ssh-agent**: Automatically starts SSH agent and loads your key
- **Passphrase caching**: You'll only enter your SSH passphrase once per session

### Step 1: Deploy GCP Infrastructure

```bash
# Clone the repository
git clone <repository-url>
cd GCPNetworkPerfETLDemo

# Set your GCP project ID
export GCP_PROJECT_ID="<your-gcp-project-id>"

# Make scripts executable
chmod +x infrastructure/*.sh

# Navigate to infrastructure directory
cd infrastructure

# Step 1a: Deploy SFTP Server (GCE VM and networking)
./gcp_sftp_setup.sh
# This creates: infrastructure/sftp_config.env with SFTP connection details

# Step 1b: Set up GCS bucket for SNMP metrics
./gcp_gcs_setup.sh
# This creates: infrastructure/gcs_config.env with GCS bucket details
# Note: By default, Pub/Sub is disabled (uses Databricks managed file events)
# To enable Pub/Sub notifications: ENABLE_PUBSUB=true ./gcp_gcs_setup.sh
```

**GCS Setup Options:**

The GCS setup script supports two file notification modes:

1. **Managed File Events (Default - Recommended)**
   - Databricks automatically manages Pub/Sub for you
   - No additional GCP configuration needed
   - Simpler setup and maintenance
   - Run: `./gcp_gcs_setup.sh`

2. **Unmanaged File Events (Optional)**
   - You manage your own Pub/Sub topic and subscription
   - More control but requires additional setup
   - Run: `ENABLE_PUBSUB=true ./gcp_gcs_setup.sh`

**Automated IAM Permission Setup:**

The `gcp_gcs_setup.sh` script automatically handles IAM permissions for the VM:

- Detects the VM's service account (from `telco-sftp-server`)
- Grants `Storage Object Admin` role at both project and bucket level
- Automatically restarts the `telco-generator` service if running to apply new permissions
- Provides clear status messages if VM is not found or service is not installed yet

**If you see 403 errors:**
```bash
# The setup script should have already handled this, but if needed:
cd infrastructure
source sftp_config.env
source gcs_config.env

# Re-run the GCS setup to reapply permissions
./gcp_gcs_setup.sh

# Or manually restart the generator service:
gcloud compute ssh ${VM_NAME} --zone=${GCP_ZONE} \
  --command="sudo systemctl restart telco-generator"
```

```

**Understanding the .env Configuration Files**

The repository includes template `.env` files with placeholders. The setup scripts automatically populate these with your actual values:

| File | Purpose | Generated By |
|------|---------|--------------|
| `sftp_config.env` | SFTP server details (IP, port, user, paths, VM info) | `gcp_sftp_setup.sh` |
| `gcs_config.env` | GCS bucket info (bucket name, paths, Pub/Sub topics if enabled) | `gcp_gcs_setup.sh` |

**Before running scripts** (template with placeholders):
```bash
SFTP_HOST=<YOUR_VM_EXTERNAL_IP>
GCS_BUCKET=<YOUR_GCS_BUCKET_NAME>
```

**After running scripts** (populated with real values):
```bash
SFTP_HOST=203.0.113.100  # Example IP (yours will be different)
GCS_BUCKET=my-project-telco-snmp
GCS_SNMP_PATH=gs://my-project-telco-snmp/snmp/
PUBSUB_TOPIC=N/A (using managed file events)  # Or actual topic if ENABLE_PUBSUB=true
```

**How to use these files:**
- **Don't edit manually** - Let the scripts populate them
- **Source them** to load values as environment variables:
  ```bash
  source infrastructure/sftp_config.env
  echo $SFTP_HOST  # Displays your actual SFTP server IP
  ```
- **Reference them** when configuring Databricks connections or other services

### Step 2: Configure SFTP Server

```bash
# Load configuration from generated .env files
source sftp_config.env
source gcs_config.env

# Set the SFTP password (REQUIRED - choose a secure password)
export SFTP_PASSWORD="<your-secure-password>"

# VM details are now loaded from sftp_config.env
# You can verify with: echo $VM_NAME $GCP_ZONE $SFTP_HOST

# Copy configuration script to the VM
gcloud compute scp configure_sftp.sh ${VM_NAME}:~/ --zone=${GCP_ZONE}

# Run the configuration script (password passed as argument)
gcloud compute ssh ${VM_NAME} --zone=${GCP_ZONE} --command="chmod +x ~/configure_sftp.sh && sudo ~/configure_sftp.sh '${SFTP_PASSWORD}'"

# Copy data generator to home directory first, then move to /opt
# For Telco:
gcloud compute scp ../scripts/telco_data_generator.py ${VM_NAME}:~/ --zone=${GCP_ZONE}
gcloud compute ssh ${VM_NAME} --zone=${GCP_ZONE} --command="sudo mv ~/telco_data_generator.py /opt/telco-generator/ && sudo chmod 755 /opt/telco-generator/telco_data_generator.py"

# For Retail (alternative):
# Supports both fast food chains and apparel retail with configurable mix
gcloud compute scp ../scripts/retail_data_generator.py ${VM_NAME}:~/ --zone=${GCP_ZONE}
gcloud compute ssh ${VM_NAME} --zone=${GCP_ZONE} --command="sudo mv ~/retail_data_generator.py /opt/retail-generator/ && sudo chmod 755 /opt/retail-generator/retail_data_generator.py"

# Configure and install systemd service
# The GCS_SNMP_PATH is already loaded from gcs_config.env
sed "s|gs://<YOUR_GCS_BUCKET_NAME>/snmp/|${GCS_SNMP_PATH}|g" telco-generator.service > telco-generator.service.tmp
mv telco-generator.service.tmp telco-generator.service

# Copy and install the configured service
gcloud compute scp telco-generator.service ${VM_NAME}:~/ --zone=${GCP_ZONE}
gcloud compute ssh ${VM_NAME} --zone=${GCP_ZONE} --command="sudo mv ~/telco-generator.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl enable telco-generator && sudo systemctl start telco-generator"

# Verify the service is running
gcloud compute ssh ${VM_NAME} --zone=${GCP_ZONE} --command="sudo systemctl status telco-generator"

# Check the logs to ensure data is being generated
gcloud compute ssh ${VM_NAME} --zone=${GCP_ZONE} --command="sudo journalctl -u telco-generator -f"
```

**For Retail Generator (Alternative):**

```bash
# Configure and install retail systemd service
# The GCS_RETAIL_BUCKET uses the metrics path (different from SNMP)
export GCS_RETAIL_BUCKET="${GCS_SNMP_PATH%snmp/}metrics/"

# Update the service file with your GCS bucket path
sed "s|gs://gcp-sandbox-field-eng-telco-snmp/metrics/|${GCS_RETAIL_BUCKET}|g" retail-generator.service > retail-generator.service.tmp
mv retail-generator.service.tmp retail-generator.service

# Copy and install the configured service
gcloud compute scp retail-generator.service ${VM_NAME}:~/ --zone=${GCP_ZONE}
gcloud compute ssh ${VM_NAME} --zone=${GCP_ZONE} --command="sudo mv ~/retail-generator.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl enable retail-generator && sudo systemctl start retail-generator"

# Verify the service is running
gcloud compute ssh ${VM_NAME} --zone=${GCP_ZONE} --command="sudo systemctl status retail-generator"

# Check the logs to ensure data is being generated
gcloud compute ssh ${VM_NAME} --zone=${GCP_ZONE} --command="sudo journalctl -u retail-generator -f"
```

**Important Notes:**
- **Telco generator**: The systemd service file contains a placeholder `<YOUR_GCS_BUCKET_NAME>` that must be replaced with your actual bucket name
- **Retail generator**: The service file contains a placeholder bucket path that must be replaced with your actual GCS metrics path
- The bucket name is available in `gcs_config.env` after running `gcp_gcs_setup.sh`
- **Telco data generator writes:**
  - **Syslog data** → Local SFTP directory (`/sftp/telco/syslog`)
  - **SNMP metrics** → GCS bucket at `/snmp/` path (JSON format for Auto Loader)
- **Retail data generator writes:**
  - **Events data** → Local SFTP directory (`/sftp/retail/events`)
  - **Metrics data** → GCS bucket at `/metrics/` path (JSON format for Auto Loader)
- Both services run as the `telco_user` with automatic restart on failure


### Step 3: Set Up Databricks

#### 3.1: Configure Secrets

Configure secrets in Databricks for SFTP connection:
```python
# Set secrets (via Databricks CLI or UI)
# Scope: <YOUR_SECRET_SCOPE> (create your own scope)
# Keys: SFTP_HOST, SFTP_USERNAME, SFTP_PASSWORD
```

#### 3.2: Run Notebooks in Order

**For Telco flavor:**
1. `00_setup_sftp_connection.py`: Creates Unity Catalog connection and schemas
2. `01_1_bronze_ingestion_syslog.py`: Starts syslog Auto Loader
3. `01_2_bronze_ingestion_snmp.py`: Starts SNMP Auto Loader
4. `01_3_bronze_monitor.py`: Monitor ingestion progress
5. `02_silver_pipeline.sql`: Create DLT pipeline for silver layer
6. `03_gold_pipeline.sql`: Create DLT pipeline for gold layer
7. `04_metric_views.sql`: Create Unity Catalog metric views

**For Retail flavor:**
1. `retail/00_setup_retail_connection.py`: Creates Unity Catalog connection
2. `retail/01_1_bronze_ingestion_events.py`: Starts events Auto Loader
3. `retail/01_2_bronze_ingestion_metrics.py`: Starts metrics Auto Loader
4. `retail/02_silver_pipeline_retail.sql`: Silver DLT pipeline
5. `retail/03_gold_pipeline_retail.sql`: Gold DLT pipeline
6. `retail/04_metric_views_retail.sql`: Retail metric views

#### 3.3: Create Delta Live Tables Pipelines

**Silver Pipeline:**
1. Go to Workflows → Delta Live Tables → Create Pipeline
2. Pipeline Name: `<flavor>_silver_pipeline`
3. Notebook: `databricks/02_silver_pipeline.sql` (or retail variant)
4. Target: `<catalog>.<schema>.silver`
5. Enable: Auto Scaling, Schema Evolution
6. Start Pipeline

**Gold Pipeline:**
1. Create Pipeline: `<flavor>_gold_pipeline`
2. Notebook: `databricks/03_gold_pipeline.sql` (or retail variant)
3. Target: `<catalog>.<schema>.gold`
4. Enable: Auto Scaling
5. Start Pipeline

### Step 4: Monitor and Visualize

1. Run `04_metric_views.sql` to create metric views
2. Create Databricks SQL Dashboard using the metric views
3. Monitor Unity Catalog lineage
4. Check data quality metrics

## Key Features Demonstrated

### 1. Auto Loader for SFTP - Simplified File Ingestion

Auto Loader with native SFTP support dramatically simplifies file ingestion from legacy systems:

**Before Auto Loader (Traditional Approach):**
- Custom scripts to poll SFTP servers for new files
- Manual file tracking to avoid duplicate processing
- Complex error handling and retry logic
- Separate orchestration (Airflow/Composer) to schedule jobs
- Schema changes require code updates and redeployment

**With Databricks Auto Loader for SFTP:**
- **Zero custom code**: Just point to the SFTP path and start streaming
- **Automatic file tracking**: Built-in checkpointing ensures exactly-once processing
- **Schema inference & evolution**: Automatically detects new columns without pipeline restarts
- **Native Unity Catalog integration**: Full lineage, governance, and access control
- **Continuous or batch modes**: Run continuously or trigger on-demand
- **Built-in retry logic**: Handles transient failures automatically

```python
# That's all you need - no polling scripts, no file tracking, no orchestration!
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.connectionName", "my_sftp_connection")  # Unity Catalog connection
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("sftp://server/path/to/files/")
)
```

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

**Telco KPIs:**
- Network latency, packet loss, throughput, jitter, error rate
- Device health scoring
- Geographic performance analysis

**Retail KPIs:**
- **Apparel**: Sales metrics, inventory levels, customer flow, return rates, fitting room usage
- **Fast Food**: Drive-through performance, order accuracy, kitchen efficiency, food waste
- Category-aware store health scoring
- Regional analytics across both categories

## Industry-Specific Metrics

### Telco Network Performance KPIs

| Metric | Description | Thresholds |
|--------|-------------|------------|
| **Latency** (ms) | Network round-trip time | Normal: <50ms, Warning: 50-100ms, Critical: >150ms |
| **Packet Loss** (%) | Percentage of lost packets | Acceptable: <0.5%, Warning: 0.5-1%, Critical: >3% |
| **Throughput** (Mbps) | Data transfer rate | Low: <1000, Normal: 1000-5000, High: >9000 |
| **Jitter** (ms) | Variation in latency | Good: <10ms, Degraded: 20-40ms, Poor: >40ms |
| **Error Rate** | Errors per time window | Low: <100, Medium: 100-400, Critical: >800 |

### Retail Store Performance KPIs

The retail generator supports two categories with category-specific metrics:

#### Apparel Retail

| Metric | Description | Thresholds |
|--------|-------------|------------|
| **Hourly Sales** | Revenue per hour | Normal: $500-25k, Anomaly: >$22k |
| **Conversion Rate** (%) | Visitors to buyers | Low: <5%, Normal: 5-45%, Anomaly: >40% |
| **Checkout Wait Time** (sec) | Average checkout time | Good: <120, Fair: 120-300, Poor: >300 |
| **Return Rate** (%) | Returns percentage | Normal: 0-15%, Anomaly: >12% |
| **Fitting Room Usage** (%) | Fitting room utilization | Normal: 10-85%, High: >80% |
| **Online Pickup Rate** (%) | BOPIS/Curbside pickup | Normal: 5-40%, High: >35% |

#### Fast Food Chains

| Metric | Description | Thresholds |
|--------|-------------|------------|
| **Drive-Through Wait Time** (sec) | Order to pickup time | Good: <200, Fair: 200-420, Poor: >420 |
| **Drive-Through Throughput** (cars/hr) | Cars served per hour | Low: <15, Normal: 15-120, High: >110 |
| **Order Accuracy** (%) | Correct orders percentage | Critical: <88%, Fair: 88-95%, Good: >95% |
| **Speaker to Window Time** (sec) | Speaker to pickup window | Good: <180, Fair: 180-270, Poor: >270 |
| **Kitchen Ticket Time** (sec) | Food prep time | Good: <300, Fair: 300-540, Poor: >540 |
| **Food Waste** (%) | Waste percentage | Good: <10%, Fair: 10-20%, High: >20% |
| **Mobile Order Pickup** (sec) | Mobile order fulfillment | Good: <120, Fair: 120-220, Poor: >220 |

## Documentation

- [Architecture Details](docs/architecture.md)
- [Data Generator Control Guide](docs/data_generator_control.md)
- [Performance Metrics](docs/performance_metrics.md)

## Configuration

### Data Generator Configuration

Adjust the data generation rate:

```bash
# SSH into the VM (use environment variables from sftp_config.env)
gcloud compute ssh ${VM_NAME} --zone=${GCP_ZONE}

# Edit the systemd service
sudo nano /etc/systemd/system/<flavor>-generator.service

# Change --files-per-minute parameter (default: 1000)
# Restart the service
sudo systemctl restart <flavor>-generator

# Check status
sudo systemctl status <flavor>-generator
```

#### Retail Generator Store Mix Configuration

The retail data generator supports configurable ratios of fast food vs apparel stores:

```bash
# Edit the retail-generator.service file
sudo nano /etc/systemd/system/retail-generator.service

# Add or modify the --fast-food-ratio parameter in ExecStart:
# ExecStart=/usr/bin/python3 /opt/retail-generator/retail_data_generator.py \
#   --events-dir /sftp/retail/events \
#   --metrics-gcs-bucket gs://your-bucket/metrics/ \
#   --files-per-minute 1000 \
#   --fast-food-ratio 0.5

# Restart to apply changes
sudo systemctl daemon-reload
sudo systemctl restart retail-generator
```

**Store Mix Examples:**
- `--fast-food-ratio 0.5` - 50% fast food, 50% apparel (default)
- `--fast-food-ratio 0.7` - 70% fast food, 30% apparel
- `--fast-food-ratio 1.0` - 100% fast food (drive-through focus)
- `--fast-food-ratio 0.0` - 100% apparel retail

**Store Categories Generated:**
- **Fast Food**: BurgerKing, TimHortons, QuickBite, DriveThruExpress
  - Store Types: drive_through, dine_in, express_counter, food_court, flagship_restaurant
- **Apparel**: Lululemon, Arcteryx, AthleticWear, OutdoorGear
  - Store Types: flagship, mall, outlet, express

## Troubleshooting

### SSH Access Issues

If you cannot SSH into the VM from the Google Cloud Console (browser SSH keeps spinning), this is typically caused by missing firewall rules.

**Quick Fix for Demo (Temporary):**

For demo purposes, you can allow SSH from all IP addresses:

```bash
gcloud compute firewall-rules create allow-ssh-from-anywhere \
  --direction=INGRESS \
  --action=allow \
  --rules=tcp:22 \
  --source-ranges=0.0.0.0/0
```

**SECURITY WARNING:** This configuration opens SSH access from any IP address on the internet. This is acceptable for temporary demos but should NEVER be used in production environments.

**Production-Ready SSH Access (Recommended):**

For production or long-term deployments, restrict SSH access to specific sources:

**Option 1: Identity-Aware Proxy (IAP) for Cloud Console SSH**
```bash
# Allow SSH only from Google Cloud Console via IAP
gcloud compute firewall-rules create allow-ssh-from-iap \
  --direction=INGRESS \
  --action=allow \
  --rules=tcp:22 \
  --source-ranges=35.235.240.0/20 \
  --description="Allow SSH from Cloud Console via Identity-Aware Proxy"
```

**Option 2: Your Specific IP Address**
```bash
# Get your current IP
MY_IP=$(curl -s ifconfig.me)

# Allow SSH only from your IP
gcloud compute firewall-rules create allow-ssh-from-my-ip \
  --direction=INGRESS \
  --action=allow \
  --rules=tcp:22 \
  --source-ranges=${MY_IP}/32
```

**Option 3: Combine Both (Best Practice)**
```bash
# Allow both IAP (Cloud Console) and your specific IP
gcloud compute firewall-rules create allow-ssh-secure \
  --direction=INGRESS \
  --action=allow \
  --rules=tcp:22 \
  --source-ranges=35.235.240.0/20,<YOUR_IP>/32
```

**Verify Current Firewall Rules:**
```bash
gcloud compute firewall-rules list --filter="allowed.ports:22" \
  --format="table(name,sourceRanges.list(),allowed.ports)"
```

**Important:** After your demo is complete, remove the open firewall rule:
```bash
gcloud compute firewall-rules delete allow-ssh-from-anywhere
```

### SFTP Connection Issues

```bash
# Test SFTP connectivity (use values from sftp_config.env)
sftp ${SFTP_USER}@${SFTP_HOST}

# Check SFTP server logs
gcloud compute ssh ${VM_NAME} --zone=${GCP_ZONE} --command='sudo journalctl -u sshd -f'

# Check data generator logs
gcloud compute ssh ${VM_NAME} --zone=${GCP_ZONE} --command='sudo journalctl -u <flavor>-generator -f'
```

### GCS Permission Issues (403 Forbidden)

If you see errors like "Provided scope(s) are not authorized" or "403 Forbidden" when the data generator tries to write to GCS:

**Root Cause:** The VM was created without the correct **OAuth access scopes** for Google Cloud Storage.

GCS access requires TWO things:
1. **IAM Permissions** - Service account needs Storage Object Admin role
2. **OAuth Scopes** - VM needs storage API scopes enabled

The error "Provided scope(s) are not authorized" means the VM is missing OAuth scopes.

**Solution:** Use the automated fix script:

```bash
cd infrastructure

# Run the fix script (will stop/start the VM to update scopes)
./fix_vm_scopes.sh
```

The script will:
1. Check current VM OAuth scopes
2. Stop the VM temporarily
3. Update scopes to include cloud-platform (includes storage)
4. Restart the VM and service
5. Verify everything is working

**Check logs after the fix:**

```bash
# View real-time logs (should show successful uploads)
gcloud compute ssh ${VM_NAME} --zone=${GCP_ZONE} \
  --command="sudo journalctl -u telco-generator -f"

# You should see:
# "Uploaded to GCS: gs://bucket/snmp/snmp_xxx.json (75 entries)"
# instead of 403 errors
```

**Manual fix (if needed):**

```bash
cd infrastructure
source sftp_config.env

# Stop VM, update scopes, and restart
gcloud compute instances stop ${VM_NAME} --zone=${GCP_ZONE}
gcloud compute instances set-service-account ${VM_NAME} \
  --zone=${GCP_ZONE} \
  --scopes=https://www.googleapis.com/auth/cloud-platform
gcloud compute instances start ${VM_NAME} --zone=${GCP_ZONE}

# Wait 30 seconds for VM to be ready, then restart service
sleep 30
gcloud compute ssh ${VM_NAME} --zone=${GCP_ZONE} \
  --command="sudo systemctl restart telco-generator"
```

**Note:** Future VMs created with the updated `gcp_sftp_setup.sh` will have the correct scopes from the start.

### Databricks Pipeline Issues

```bash
# Check Auto Loader progress
# In Databricks notebook:
display(spark.sql("DESCRIBE HISTORY <catalog>.<schema>.bronze.<table>"))

# Check DLT pipeline events
# Go to: Workflows → Delta Live Tables → [Pipeline] → Events

# Verify Unity Catalog lineage
# Go to: Catalog → <catalog> → [table] → Lineage
```

## Support

For issues or questions:
- Check the [docs/](docs/) directory for detailed documentation
- Review Databricks logs and DLT pipeline events
- Verify GCP firewall rules and SFTP connectivity

## License

This demo is provided as-is for educational and demonstration purposes.

