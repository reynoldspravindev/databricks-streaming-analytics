# Real-Time Network Performance Monitoring on Databricks GCP

End-to-end demo of real-time telemetry monitoring on Databricks (GCP) using a Lakeflow Declarative Pipeline that ingests:

- **Syslog** (RFC 5424) events from a **GCP Pub/Sub** topic
- **SNMP** metrics (NDJSON) from a **GCS** bucket via Auto Loader (file-event mode)

…into a `bronze → silver → gold` medallion in Unity Catalog. A separate **Retail** flavor (fast food / apparel) is included for comparison; this README focuses on the Telco flavor since that is the deployed pipeline.

## Available flavors

| Flavor | Generator | Notebooks |
|---|---|---|
| **Telco network performance** | `scripts/telco_data_generator.py` | `databricks/telco/` |
| **Retail store performance** | `scripts/retail_data_generator.py` | `databricks/retail/` |

## Architecture

```
┌────────────────────────────────────────────────┐
│  GCE VM (or local) running telco_data_generator│
│   ├── Syslog → Pub/Sub topic syslog-events     │
│   └── SNMP   → gs://reynolds-telco-bucket/snmp/│
└────────────────────────────────────────────────┘
                 │                │
                 ▼                ▼
        ┌────────────────┐  ┌────────────────┐
        │ Pub/Sub        │  │ GCS bucket     │
        │ syslog-events  │  │ (file events)  │
        └────────────────┘  └────────────────┘
                 │                │
                 ▼                ▼
┌────────────────────────────────────────────────────────┐
│ Lakeflow Declarative Pipeline (serverless, Photon)     │
│  bronze.syslog_raw  ←  read_pubsub                     │
│  bronze.snmp_raw    ←  Auto Loader (managedFileEvents) │
│  silver.silver_syslog_parsed   (RFC 5424 regex parse)  │
│  silver.silver_snmp_metrics    (DQ expectations)       │
│  silver.silver_network_events  (deduped events)        │
│  gold.gold_network_performance_5min                    │
│  gold.gold_network_events                              │
│  gold.gold_metrics_by_location                         │
│  gold.gold_device_health                               │
│  gold.dim_devices                                      │
└────────────────────────────────────────────────────────┘
```

Detailed architecture diagram: `docs/architecture.md` and `docs/images/ReadmeMainFlow.png`.

## Repository layout

```
GCPNetworkPerfETLDemo/
├── databricks/
│   ├── telco/
│   │   ├── DLT/                              # Source of truth for the deployed pipeline
│   │   │   ├── 01_bronze_pubsub_syslog.py    # bronze.syslog_raw  ← Pub/Sub
│   │   │   ├── 02_bronze_gcs_snmp.py         # bronze.snmp_raw    ← GCS Auto Loader
│   │   │   ├── 03_silver.sql                 # silver_* tables
│   │   │   └── 04_gold.sql                   # gold_* tables + dim_devices
│   │   ├── 04_metric_views.sql               # UC metric views
│   │   ├── 05_genie_space_setup.sql          # Genie AI space setup
│   │   ├── 06_ims_cross_cloud_analytics.sql  # Cross-cloud joins with IMS data
│   │   ├── telco_network_perf_dash.lvdash.json  # Lakeview dashboard
│   │   └── aws/                              # AWS-side IMS notebooks for cross-cloud demo
│   └── retail/                               # Retail flavor (separate pipeline)
│
├── scripts/
│   ├── telco_data_generator.py   # Generates syslog (Pub/Sub) + SNMP (GCS)
│   ├── retail_data_generator.py  # Retail flavor generator
│   └── ims_data_generator.py     # Cross-cloud IMS data
│
├── infrastructure/                       # GCP setup + GCE deployment of the generator
│   ├── setup_pubsub.sh                   # Creates topic + subscription
│   ├── setup_service_accounts.sh         # Creates telco-generator SA with required roles
│   ├── gcp_gcs_setup.sh                  # Creates the GCS bucket
│   ├── gcs_config.env                    # Sample env for the bucket
│   ├── deploy_telco_gce.sh               # One-shot GCE VM deploy (calls startup script)
│   ├── manage_telco_gce.sh               # status/logs/restart/delete helpers
│   └── startup_telco_generator.sh        # Systemd service startup on the VM
│
├── docs/                       # Architecture, performance benchmarks, dashboard SQL
└── README.md                   # This file
```

## Quick start

### Prerequisites

- GCP project with billing (default in this repo: `gcp-sandbox-field-eng`)
- Databricks workspace on GCP with **Unity Catalog**
- `gcloud` and `databricks` CLIs authenticated (`gcloud auth login`, `databricks auth login -p <profile>`)
- Catalog `telco_networkperf` exists in the workspace
- A **Storage Credential + External Location** in Unity Catalog over the SNMP bucket, with **file events enabled** (the demo uses `telco_gcs_credential` + external location `reynolds-telco-bucket`)
- A Databricks **secret scope** (e.g. `reynolds-pravindev-scope`) with these keys for `read_pubsub` auth:
  - `sa-client-email`, `sa-client-id`, `sa-private-key-id`, `sa-private-key`

### 1. Provision GCP infra (one-time)

```bash
export GCP_PROJECT_ID=gcp-sandbox-field-eng
cd infrastructure/

./setup_service_accounts.sh   # creates telco-generator SA
./setup_pubsub.sh             # creates topic + subscription
gcloud storage buckets create gs://reynolds-telco-bucket \
  --project=$GCP_PROJECT_ID --location=us-central1 --uniform-bucket-level-access

# IAM the generator SA needs (publisher + GCS write + subscription create)
gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
  --member=serviceAccount:telco-generator@$GCP_PROJECT_ID.iam.gserviceaccount.com \
  --role=roles/pubsub.editor --condition=None
gcloud storage buckets add-iam-policy-binding gs://reynolds-telco-bucket \
  --member=serviceAccount:telco-generator@$GCP_PROJECT_ID.iam.gserviceaccount.com \
  --role=roles/storage.objectAdmin

# IAM the Databricks UC managed SA needs on the bucket (for Auto Loader file events)
DBX_SA=$(databricks credentials get-credential telco_gcs_credential -o json \
  | jq -r .databricks_gcp_service_account.email)
gcloud storage buckets add-iam-policy-binding gs://reynolds-telco-bucket \
  --member=serviceAccount:$DBX_SA --role=roles/storage.objectAdmin
gcloud storage buckets add-iam-policy-binding gs://reynolds-telco-bucket \
  --member=serviceAccount:$DBX_SA --role=roles/storage.legacyBucketReader
```

### 2. Stash a fresh SA key in Databricks secrets

`read_pubsub()` uses the JSON SA key passed via secrets — re-mint cleanly to avoid PEM-newline corruption:

```bash
gcloud iam service-accounts keys create /tmp/telco-sa-key.json \
  --iam-account=telco-generator@$GCP_PROJECT_ID.iam.gserviceaccount.com

python3 - <<'PY'
import json, requests, subprocess
key = json.load(open('/tmp/telco-sa-key.json'))
host = subprocess.check_output(['databricks','auth','env','-p','<your-profile>']).decode()
# (or hardcode host + token from env)
# For each: sa-client-email, sa-client-id, sa-private-key-id, sa-private-key
PY

# Easier: use the databricks CLI for short values, and the REST API directly for the PEM.
# See infrastructure/ for example helper if you want to script it.
```

### 3. Deploy the Lakeflow Declarative Pipeline

The four pipeline files live in `databricks/telco/DLT/`. Upload them to your workspace (e.g. via `databricks workspace import-dir`), then create/update the pipeline pointing at:

- **Catalog**: `telco_networkperf`
- **Schema** (default): `bronze`
- **Libraries (file paths)**: all four files in `databricks/telco/DLT/`
- **Mode**: serverless, Photon, channel CURRENT

The deployed pipeline ID in this workspace: `bc64f0c1-170f-40db-9b58-a53b9362b1c0`.

Trigger the first run as a **full refresh** so all bronze tables and the silver/gold MVs get rebuilt. Subsequent runs can be incremental (or switch to `continuous: true` for a true streaming pipeline).

### 4. Start the data generator

Locally (good for ad-hoc testing):

```bash
python3 -m pip install --user google-cloud-pubsub google-cloud-storage
python3 scripts/telco_data_generator.py \
  --syslog-pubsub-topic syslog-events \
  --project-id $GCP_PROJECT_ID \
  --snmp-gcs-bucket gs://reynolds-telco-bucket/snmp/ \
  --files-per-minute 1000
```

On a GCE VM (sustained scale demo, ~10K events/min):

```bash
cd infrastructure/
./deploy_telco_gce.sh           # creates e2-highmem-8 VM, installs deps, starts the systemd service
./manage_telco_gce.sh status    # also: logs / restart / pubsub / gcs / ssh / delete
```

The startup script (`startup_telco_generator.sh`) downloads the latest `telco_data_generator.py` from GCS and runs it as a systemd service. Adjust `FILES_PER_MINUTE` in the startup script for higher rates.

### 5. Validate

```sql
SELECT 'bronze.syslog_raw' AS t, COUNT(*) FROM telco_networkperf.bronze.syslog_raw
UNION ALL SELECT 'bronze.snmp_raw',  COUNT(*) FROM telco_networkperf.bronze.snmp_raw
UNION ALL SELECT 'silver.silver_syslog_parsed', COUNT(*) FROM telco_networkperf.silver.silver_syslog_parsed
UNION ALL SELECT 'silver.silver_snmp_metrics',  COUNT(*) FROM telco_networkperf.silver.silver_snmp_metrics
UNION ALL SELECT 'silver.silver_network_events', COUNT(*) FROM telco_networkperf.silver.silver_network_events
UNION ALL SELECT 'gold.gold_network_performance_5min', COUNT(*) FROM telco_networkperf.gold.gold_network_performance_5min
UNION ALL SELECT 'gold.gold_network_events', COUNT(*) FROM telco_networkperf.gold.gold_network_events
UNION ALL SELECT 'gold.dim_devices', COUNT(*) FROM telco_networkperf.gold.dim_devices
UNION ALL SELECT 'gold.gold_metrics_by_location', COUNT(*) FROM telco_networkperf.gold.gold_metrics_by_location
UNION ALL SELECT 'gold.gold_device_health', COUNT(*) FROM telco_networkperf.gold.gold_device_health
ORDER BY 1;
```

## Sustained throughput (measured)

Generator at `--files-per-minute 1000` (split 60/40 syslog/SNMP, 1K-5K events per syslog batch, 500-2K records per SNMP file), local laptop publisher:

| Source | Total ingested | Span | Rate |
|---|---|---|---|
| Syslog (Pub/Sub → Bronze) | 6.07M events | 1,134s | **5,356 events/sec (321K/min)** |
| SNMP (GCS → Bronze) | 1.99M records over 1,599 files | 1,434s | **1,392 records/sec (83K/min)** |

The Lakeflow pipeline easily kept up — the generator was the bottleneck (~17% of the 1000-batches/min target on a laptop). On the recommended `e2-highmem-8` VM the generator hits its target rate and the pipeline scales linearly.

## Generator CLI reference

```
--syslog-pubsub-topic     Pub/Sub topic for syslog (e.g. syslog-events)
--project-id              GCP project ID (required when using --syslog-pubsub-topic)
--snmp-gcs-bucket         GCS path for SNMP NDJSON files (e.g. gs://bucket/snmp/)
--syslog-dir              (legacy) Local syslog directory if not using Pub/Sub
--snmp-dir                (legacy) Local SNMP directory if not using GCS
--files-per-minute        Total batches/min, split 60% syslog / 40% SNMP (default 100)
--extended-schema         Adds firmware_version / uptime_hours / last_reboot to SNMP records
--log-level               DEBUG | INFO | WARNING | ERROR (default INFO)
```

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `PS_UNABLE_TO_CREATE_SUBSCRIPTION` in pipeline | Generator SA needs `roles/pubsub.editor` (or at least `pubsub.subscriptions.create` on the topic). `read_pubsub` creates a subscription on first run. |
| `UNAUTHENTICATED: Request had invalid authentication credentials` | The `sa-private-key` secret got mangled (PEM newlines stripped). Re-upload via the REST API with `string_value` (Python `requests`) — not bash arg passing. |
| `[TABLE_OR_VIEW_NOT_FOUND]` in silver/gold | Pipeline schema defaults silver/gold CREATEs into `bronze`. Use fully-qualified names: `CREATE OR REFRESH … telco_networkperf.silver.<name>`. |
| `gold_network_events` empty after first refresh | Run the pipeline incrementally once more — the MV refreshed before the upstream silver table finished its first commit. |
| Auto Loader can't list / get file events | The Databricks UC GCP SA (look it up via `databricks credentials get-credential <name>`) needs `roles/storage.objectAdmin` + `roles/storage.legacyBucketReader` on the bucket, and the external location must have `enable_file_events=true`. |

## Cost (rough, monthly, sustained)

| Resource | Notes | $/mo |
|---|---|---|
| GCE `e2-highmem-8` (730h) | Generator host | ~$260 |
| Pub/Sub | At ~150K msgs/sec | ~$1,550 |
| GCS storage + egress | ~100 GB + 500 GB egress | ~$50 |
| Databricks LDP (serverless, advanced) | Variable on compute usage | — |
| **Generator infra subtotal** | | **~$1,860** |

Demo can be turned right down by reducing `--files-per-minute`. For a 1-hour demo the cost is negligible.

## Cross-cloud / IMS analytics

The `databricks/telco/aws/` folder + `06_ims_cross_cloud_analytics.sql` show how to join the GCP-side telemetry with IMS records shared from an AWS workspace via Delta Sharing. `gold.dim_devices` carries the join keys (`device_id`, `device_key`, surrogate `device_sk`) needed for the cross-cloud dimension model.
