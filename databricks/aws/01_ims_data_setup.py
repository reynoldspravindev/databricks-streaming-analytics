# Databricks notebook source
# MAGIC %md
# MAGIC # AWS Databricks - IMS Data Generation & Delta Sharing Setup
# MAGIC 
# MAGIC This notebook generates synthetic Telco IMS (IP Multimedia Subsystem) data on AWS Databricks
# MAGIC and prepares it for Delta Sharing to a GCP Databricks workspace.
# MAGIC 
# MAGIC ## Data Tables Generated
# MAGIC - **ims_volte_cdrs**: VoLTE Call Detail Records
# MAGIC - **ims_sip_sessions**: SIP Session signaling metrics  
# MAGIC - **ims_node_metrics**: IMS core network node performance
# MAGIC - **ims_subscriber_sessions**: Subscriber usage analytics
# MAGIC 
# MAGIC ## Join Keys for GCP Integration
# MAGIC All tables include these fields for joining with GCP gold tables:
# MAGIC - `device_id`, `device_type`, `location`, `device_key`, `region`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configuration

# COMMAND ----------

# Configuration - Update these values for your environment
CATALOG_NAME = "telco_ims_aws"
SCHEMA_NAME = "ims_data"

# Delta Sharing configuration
SHARE_NAME = "telco_ims_share"
RECIPIENT_NAME = "gcp_workspace_recipient"

# Data generation settings
VOLTE_CDR_COUNT = 100000
SIP_SESSION_COUNT = 50000
IMS_NODE_METRICS_COUNT = 25000
SUBSCRIBER_SESSION_COUNT = 75000

print(f"Catalog: {CATALOG_NAME}")
print(f"Schema: {SCHEMA_NAME}")
print(f"Share: {SHARE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Unity Catalog and Schema

# COMMAND ----------

# Create catalog if not exists
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"USE CATALOG {CATALOG_NAME}")

# Create schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

print(f"Using {CATALOG_NAME}.{SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Initialize IMS Data Generator

# COMMAND ----------

# MAGIC %pip install -q faker

# COMMAND ----------

# IMS Data Generator inline (alternatively, upload ims_data_generator.py to DBFS or Repos)
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

# =============================================================================
# Configuration - Must match GCP network performance data for joins
# =============================================================================

DEVICE_TYPES = ["router", "switch", "firewall", "load_balancer"]
LOCATIONS = ["datacenter-east", "datacenter-west", "datacenter-central", "edge-north", "edge-south"]
VENDORS = ["Cisco", "Juniper", "Arista", "Palo Alto", "F5"]
DEVICE_COUNT = 100

IMS_NODE_TYPES = ["P-CSCF", "I-CSCF", "S-CSCF", "HSS", "MGCF", "MRF", "BGCF", "AS"]
CODEC_TYPES = ["AMR-WB", "AMR-NB", "EVS", "G.711", "G.729", "OPUS"]
CALL_TYPES = ["VoLTE", "VoWiFi", "ViLTE", "VoNR"]
TERMINATION_CAUSES = [
    "Normal", "User Busy", "No Answer", "Call Rejected", "Number Changed",
    "Network Congestion", "Bearer Capability Not Available", "Facility Rejected",
    "Normal Clearing", "Destination Out of Order"
]

SIP_RESPONSES = {
    100: "Trying", 180: "Ringing", 183: "Session Progress", 200: "OK",
    302: "Moved Temporarily", 400: "Bad Request", 401: "Unauthorized",
    403: "Forbidden", 404: "Not Found", 408: "Request Timeout",
    480: "Temporarily Unavailable", 486: "Busy Here", 487: "Request Terminated",
    500: "Server Internal Error", 503: "Service Unavailable", 504: "Server Time-out"
}

QOS_CLASSES = ["QCI_1", "QCI_2", "QCI_3", "QCI_4", "QCI_5"]


class IMSDataGenerator:
    """Generates synthetic IMS data for telco analytics."""
    
    def __init__(self, seed: Optional[int] = None):
        if seed:
            random.seed(seed)
        self.devices = self._generate_network_devices()
        self.ims_nodes = self._generate_ims_nodes()
        self.subscribers = self._generate_subscribers(num_subscribers=5000)
    
    def _derive_region(self, location: str) -> str:
        if "east" in location: return "east"
        elif "west" in location: return "west"
        elif "central" in location: return "central"
        elif "north" in location: return "north"
        elif "south" in location: return "south"
        return "unknown"
    
    def _generate_network_devices(self):
        devices = []
        models = {
            "Cisco": {"router": "ASR-9000", "switch": "Nexus-9300", "firewall": "ASA-5525", "load_balancer": "ACE-4710"},
            "Juniper": {"router": "MX960", "switch": "EX4650", "firewall": "SRX5800", "load_balancer": "LB-8"},
            "Arista": {"router": "7280R3", "switch": "7050X3", "firewall": "7280R3", "load_balancer": "7280R3"},
            "Palo Alto": {"router": "PA-5250", "switch": "PA-5250", "firewall": "PA-5250", "load_balancer": "PA-5250"},
            "F5": {"router": "BIG-IP-i15800", "switch": "BIG-IP-i15800", "firewall": "BIG-IP-i15800", "load_balancer": "BIG-IP-i15800"},
        }
        for i in range(DEVICE_COUNT):
            device_type = DEVICE_TYPES[i % len(DEVICE_TYPES)]
            location = LOCATIONS[i % len(LOCATIONS)]
            vendor = VENDORS[i % len(VENDORS)]
            model = models[vendor][device_type]
            device_id = f"{device_type}-{location}-{i:03d}"
            ip_address = f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
            devices.append({
                "device_id": device_id, "device_type": device_type, "location": location,
                "vendor": vendor, "model": model, "ip_address": ip_address,
                "device_key": f"{device_id}_{location}", "region": self._derive_region(location)
            })
        return devices
    
    def _generate_ims_nodes(self):
        nodes = []
        for location in LOCATIONS:
            for node_type in IMS_NODE_TYPES:
                nodes.append({
                    "node_id": f"ims-{node_type.lower()}-{location}-001",
                    "node_type": node_type, "location": location,
                    "region": self._derive_region(location),
                    "ip_address": f"10.100.{random.randint(1, 254)}.{random.randint(1, 254)}",
                    "status": "active", "capacity_pct": random.uniform(30, 85)
                })
        return nodes
    
    def _generate_subscribers(self, num_subscribers: int):
        subscribers = []
        subscription_types = ["Prepaid", "Postpaid", "Enterprise", "MVNO"]
        for i in range(num_subscribers):
            home_location = random.choice(LOCATIONS)
            imsi = f"310{random.randint(100, 999)}{random.randint(1000000000, 9999999999)}"
            msisdn = f"+1{random.randint(200, 999)}{random.randint(1000000, 9999999)}"
            subscribers.append({
                "imsi": imsi, "msisdn": msisdn,
                "impu": f"sip:{msisdn}@ims.telco.com", "impi": f"{imsi}@ims.telco.com",
                "subscription_type": random.choice(subscription_types),
                "home_location": home_location,
                "roaming_status": random.choices(["Home", "Roaming"], weights=[0.85, 0.15])[0]
            })
        return subscribers
    
    def _generate_timestamp(self, hours_back: int = 24):
        now = datetime.now(timezone.utc)
        return now - timedelta(seconds=random.randint(0, hours_back * 3600))
    
    def generate_volte_cdr(self):
        caller = random.choice(self.subscribers)
        callee = random.choice(self.subscribers)
        while callee["msisdn"] == caller["msisdn"]:
            callee = random.choice(self.subscribers)
        
        orig_device = random.choice(self.devices)
        term_device = random.choice(self.devices)
        call_start = self._generate_timestamp(hours_back=72)
        call_duration = random.choices(
            [random.randint(0, 10), random.randint(10, 120), random.randint(120, 1800), random.randint(1800, 7200)],
            weights=[0.15, 0.35, 0.40, 0.10]
        )[0]
        
        is_answered = random.random() > 0.15
        termination_cause = "Normal Clearing" if is_answered else random.choice(TERMINATION_CAUSES[1:])
        
        return {
            "cdr_id": str(uuid.uuid4()), "session_id": str(uuid.uuid4()),
            "call_start_time": call_start.isoformat(),
            "call_end_time": (call_start + timedelta(seconds=call_duration)).isoformat() if is_answered else None,
            "call_duration_seconds": call_duration if is_answered else 0,
            "caller_imsi": caller["imsi"], "caller_msisdn": caller["msisdn"], "caller_impu": caller["impu"],
            "caller_subscription_type": caller["subscription_type"], "caller_home_location": caller["home_location"],
            "caller_roaming_status": caller["roaming_status"],
            "callee_msisdn": callee["msisdn"], "callee_impu": callee["impu"],
            "callee_subscription_type": callee["subscription_type"], "callee_home_location": callee["home_location"],
            "call_type": random.choice(CALL_TYPES), "codec": random.choice(CODEC_TYPES),
            "is_video": random.random() < 0.1, "is_emergency": random.random() < 0.001,
            "is_international": random.random() < 0.05, "is_answered": is_answered,
            "termination_cause": termination_cause, "qos_class": random.choice(QOS_CLASSES),
            "mos_score": round(random.uniform(3.2, 4.5), 2) if is_answered else None,
            "jitter_ms": round(random.uniform(0, 50), 2) if is_answered else None,
            "packet_loss_pct": round(random.uniform(0, 3), 3) if is_answered else None,
            "rtt_ms": round(random.uniform(20, 200), 2) if is_answered else None,
            "originating_device_id": orig_device["device_id"], "originating_device_type": orig_device["device_type"],
            "originating_location": orig_device["location"], "originating_device_key": orig_device["device_key"],
            "originating_region": orig_device["region"],
            "terminating_device_id": term_device["device_id"], "terminating_device_type": term_device["device_type"],
            "terminating_location": term_device["location"], "terminating_device_key": term_device["device_key"],
            "terminating_region": term_device["region"],
            "billable_duration_seconds": call_duration if is_answered else 0,
            "rate_plan": random.choice(["Unlimited", "Metered", "Corporate", "Family"]),
            "record_timestamp": datetime.now(timezone.utc).isoformat(), "data_source": "aws_ims_generator"
        }
    
    def generate_sip_session(self):
        subscriber = random.choice(self.subscribers)
        ims_node = random.choice(self.ims_nodes)
        device = random.choice(self.devices)
        session_start = self._generate_timestamp(hours_back=48)
        is_successful = random.random() > 0.05
        final_response_code = 200 if is_successful else random.choice([400, 401, 403, 404, 408, 480, 486, 500, 503])
        
        return {
            "session_id": str(uuid.uuid4()), "call_id": f"{uuid.uuid4()}@{ims_node['ip_address']}",
            "session_start_time": session_start.isoformat(),
            "session_end_time": (session_start + timedelta(seconds=random.randint(1, 3600))).isoformat(),
            "subscriber_imsi": subscriber["imsi"], "subscriber_msisdn": subscriber["msisdn"],
            "subscriber_impu": subscriber["impu"],
            "ims_node_id": ims_node["node_id"], "ims_node_type": ims_node["node_type"],
            "ims_node_location": ims_node["location"],
            "device_id": device["device_id"], "device_type": device["device_type"],
            "location": device["location"], "device_key": device["device_key"], "region": device["region"],
            "sip_method": random.choice(["INVITE", "REGISTER", "SUBSCRIBE", "OPTIONS", "MESSAGE", "NOTIFY"]),
            "final_response_code": final_response_code,
            "final_response_desc": SIP_RESPONSES.get(final_response_code, "Unknown"),
            "is_successful": is_successful,
            "invite_response_time_ms": round(random.uniform(50, 500), 2),
            "registration_time_ms": round(random.uniform(100, 2000), 2),
            "session_setup_time_ms": round(random.uniform(200, 3000), 2),
            "invite_retransmissions": random.choices([0, 1, 2, 3], weights=[0.7, 0.2, 0.08, 0.02])[0],
            "total_retransmissions": random.randint(0, 5),
            "auth_method": random.choice(["AKA", "Digest", "TLS-Client-Cert"]),
            "auth_success": is_successful,
            "record_timestamp": datetime.now(timezone.utc).isoformat(), "data_source": "aws_ims_generator"
        }
    
    def generate_ims_node_metrics(self):
        ims_node = random.choice(self.ims_nodes)
        device = random.choice([d for d in self.devices if d["location"] == ims_node["location"]])
        timestamp = self._generate_timestamp(hours_back=24)
        is_anomaly = random.random() < 0.05
        
        if ims_node["node_type"] == "P-CSCF":
            active_sessions, reg_per_sec = random.randint(1000, 50000), random.uniform(10, 500)
        elif ims_node["node_type"] == "S-CSCF":
            active_sessions, reg_per_sec = random.randint(5000, 100000), random.uniform(50, 1000)
        elif ims_node["node_type"] == "HSS":
            active_sessions, reg_per_sec = random.randint(10000, 500000), random.uniform(100, 2000)
        else:
            active_sessions, reg_per_sec = random.randint(500, 20000), random.uniform(5, 200)
        
        return {
            "metric_id": str(uuid.uuid4()), "timestamp": timestamp.isoformat(),
            "ims_node_id": ims_node["node_id"], "ims_node_type": ims_node["node_type"],
            "ims_node_location": ims_node["location"], "ims_node_region": ims_node["region"],
            "device_id": device["device_id"], "device_type": device["device_type"],
            "location": device["location"], "device_key": device["device_key"], "region": device["region"],
            "active_sessions": active_sessions, "max_sessions": active_sessions * 2,
            "session_utilization_pct": round((active_sessions / (active_sessions * 2)) * 100, 2),
            "cpu_utilization_pct": round(random.uniform(20, 95) if is_anomaly else random.uniform(20, 70), 2),
            "memory_utilization_pct": round(random.uniform(30, 98) if is_anomaly else random.uniform(30, 75), 2),
            "disk_io_ops_per_sec": random.randint(100, 10000),
            "registrations_per_sec": round(reg_per_sec, 2),
            "invites_per_sec": round(reg_per_sec * 0.3, 2),
            "messages_per_sec": round(reg_per_sec * 0.5, 2),
            "avg_response_time_ms": round(random.uniform(50, 500) if is_anomaly else random.uniform(20, 150), 2),
            "p95_response_time_ms": round(random.uniform(200, 2000) if is_anomaly else random.uniform(50, 300), 2),
            "p99_response_time_ms": round(random.uniform(500, 5000) if is_anomaly else random.uniform(100, 500), 2),
            "error_rate_pct": round(random.uniform(1, 10) if is_anomaly else random.uniform(0, 1), 3),
            "failed_registrations": random.randint(10, 500) if is_anomaly else random.randint(0, 20),
            "timeout_count": random.randint(5, 100) if is_anomaly else random.randint(0, 10),
            "is_anomaly": is_anomaly, "health_status": "Degraded" if is_anomaly else "Healthy",
            "record_timestamp": datetime.now(timezone.utc).isoformat(), "data_source": "aws_ims_generator"
        }
    
    def generate_subscriber_session(self):
        subscriber = random.choice(self.subscribers)
        device = random.choice(self.devices)
        session_start = self._generate_timestamp(hours_back=168)
        session_duration = random.choices(
            [random.randint(1, 5), random.randint(5, 30), random.randint(30, 120), random.randint(120, 480)],
            weights=[0.20, 0.35, 0.30, 0.15]
        )[0]
        
        return {
            "session_id": str(uuid.uuid4()),
            "session_start_time": session_start.isoformat(),
            "session_end_time": (session_start + timedelta(minutes=session_duration)).isoformat(),
            "session_duration_minutes": session_duration,
            "subscriber_imsi": subscriber["imsi"], "subscriber_msisdn": subscriber["msisdn"],
            "subscription_type": subscriber["subscription_type"],
            "home_location": subscriber["home_location"], "roaming_status": subscriber["roaming_status"],
            "device_id": device["device_id"], "device_type": device["device_type"],
            "location": device["location"], "device_key": device["device_key"], "region": device["region"],
            "voice_minutes_used": round(random.uniform(0, session_duration * 0.5), 2),
            "video_minutes_used": round(random.uniform(0, session_duration * 0.2), 2),
            "data_mb_used": round(random.uniform(0, 500), 2),
            "sms_count": random.randint(0, 50),
            "voice_calls_made": random.randint(0, 10), "voice_calls_received": random.randint(0, 10),
            "video_calls_made": random.randint(0, 3), "missed_calls": random.randint(0, 5),
            "avg_call_mos_score": round(random.uniform(3.0, 4.5), 2),
            "dropped_calls": random.randint(0, 2), "failed_call_setups": random.randint(0, 3),
            "device_manufacturer": random.choice(["Apple", "Samsung", "Google", "OnePlus", "Xiaomi"]),
            "device_model": random.choice(["iPhone 15 Pro", "Galaxy S24", "Pixel 8", "OnePlus 12", "14 Ultra"]),
            "os_version": random.choice(["iOS 17.4", "Android 14", "Android 13"]),
            "access_technology": random.choice(["5G-SA", "5G-NSA", "LTE", "LTE-A"]),
            "cell_id": f"CELL-{device['location']}-{random.randint(1000, 9999)}",
            "signal_strength_dbm": random.randint(-110, -60),
            "record_timestamp": datetime.now(timezone.utc).isoformat(), "data_source": "aws_ims_generator"
        }


# Initialize generator
generator = IMSDataGenerator(seed=42)
print("IMS Data Generator initialized successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Generate and Write IMS Data Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 VoLTE Call Detail Records (CDRs)

# COMMAND ----------

print(f"Generating {VOLTE_CDR_COUNT:,} VoLTE CDRs...")
volte_cdrs = [generator.generate_volte_cdr() for _ in range(VOLTE_CDR_COUNT)]
volte_df = spark.createDataFrame(volte_cdrs)

# Write as Delta table with optimizations
volte_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.enableChangeDataFeed", "true") \
    .partitionBy("originating_region", "call_type") \
    .saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.ims_volte_cdrs")

print(f"✅ Created table: {CATALOG_NAME}.{SCHEMA_NAME}.ims_volte_cdrs")
display(spark.sql(f"SELECT COUNT(*) as record_count FROM {CATALOG_NAME}.{SCHEMA_NAME}.ims_volte_cdrs"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 SIP Session Metrics

# COMMAND ----------

print(f"Generating {SIP_SESSION_COUNT:,} SIP sessions...")
sip_sessions = [generator.generate_sip_session() for _ in range(SIP_SESSION_COUNT)]
sip_df = spark.createDataFrame(sip_sessions)

sip_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.enableChangeDataFeed", "true") \
    .partitionBy("region", "ims_node_type") \
    .saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.ims_sip_sessions")

print(f"✅ Created table: {CATALOG_NAME}.{SCHEMA_NAME}.ims_sip_sessions")
display(spark.sql(f"SELECT COUNT(*) as record_count FROM {CATALOG_NAME}.{SCHEMA_NAME}.ims_sip_sessions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 IMS Node Performance Metrics

# COMMAND ----------

print(f"Generating {IMS_NODE_METRICS_COUNT:,} IMS node metrics...")
node_metrics = [generator.generate_ims_node_metrics() for _ in range(IMS_NODE_METRICS_COUNT)]
node_df = spark.createDataFrame(node_metrics)

node_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.enableChangeDataFeed", "true") \
    .partitionBy("ims_node_type", "region") \
    .saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.ims_node_metrics")

print(f"✅ Created table: {CATALOG_NAME}.{SCHEMA_NAME}.ims_node_metrics")
display(spark.sql(f"SELECT COUNT(*) as record_count FROM {CATALOG_NAME}.{SCHEMA_NAME}.ims_node_metrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Subscriber Session Analytics

# COMMAND ----------

print(f"Generating {SUBSCRIBER_SESSION_COUNT:,} subscriber sessions...")
sub_sessions = [generator.generate_subscriber_session() for _ in range(SUBSCRIBER_SESSION_COUNT)]
sub_df = spark.createDataFrame(sub_sessions)

sub_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.enableChangeDataFeed", "true") \
    .partitionBy("region", "subscription_type") \
    .saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.ims_subscriber_sessions")

print(f"✅ Created table: {CATALOG_NAME}.{SCHEMA_NAME}.ims_subscriber_sessions")
display(spark.sql(f"SELECT COUNT(*) as record_count FROM {CATALOG_NAME}.{SCHEMA_NAME}.ims_subscriber_sessions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Optimize Tables for Sharing

# COMMAND ----------

# Optimize all tables for better query performance
tables = ["ims_volte_cdrs", "ims_sip_sessions", "ims_node_metrics", "ims_subscriber_sessions"]

for table in tables:
    full_table = f"{CATALOG_NAME}.{SCHEMA_NAME}.{table}"
    print(f"Optimizing {table}...")
    spark.sql(f"OPTIMIZE {full_table}")
    spark.sql(f"ANALYZE TABLE {full_table} COMPUTE STATISTICS FOR ALL COLUMNS")

print("✅ All tables optimized!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Delta Share for GCP

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Create the Share

# COMMAND ----------

# Create Delta Share
spark.sql(f"""
CREATE SHARE IF NOT EXISTS {SHARE_NAME}
COMMENT 'Telco IMS data share for cross-cloud analytics with GCP'
""")

print(f"✅ Created share: {SHARE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Add Tables to Share

# COMMAND ----------

# Add all IMS tables to the share
tables_to_share = [
    ("ims_volte_cdrs", "VoLTE Call Detail Records for cross-cloud analysis"),
    ("ims_sip_sessions", "SIP Session signaling metrics"),
    ("ims_node_metrics", "IMS core network node performance metrics"),
    ("ims_subscriber_sessions", "Subscriber usage analytics")
]

for table_name, comment in tables_to_share:
    full_table = f"{CATALOG_NAME}.{SCHEMA_NAME}.{table_name}"
    try:
        spark.sql(f"""
        ALTER SHARE {SHARE_NAME}
        ADD TABLE {full_table}
        COMMENT '{comment}'
        """)
        print(f"✅ Added {table_name} to share")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"ℹ️ {table_name} already in share")
        else:
            print(f"❌ Error adding {table_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Verify Share Contents

# COMMAND ----------

# Show share details
display(spark.sql(f"SHOW ALL IN SHARE {SHARE_NAME}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create Recipient for GCP Workspace
# MAGIC 
# MAGIC **Important**: Complete this step after getting the sharing identifier from your GCP Databricks workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option A: Open Sharing (for demo/dev environments)

# COMMAND ----------

# Create recipient with open sharing (no authentication required)
# USE WITH CAUTION - only for demo/dev environments
# spark.sql(f"""
# CREATE RECIPIENT IF NOT EXISTS {RECIPIENT_NAME}
# COMMENT 'GCP Databricks workspace recipient for IMS data'
# """)

# # Grant access to the share
# spark.sql(f"""
# GRANT SELECT ON SHARE {SHARE_NAME} TO RECIPIENT {RECIPIENT_NAME}
# """)

# # Get the activation link
# display(spark.sql(f"DESCRIBE RECIPIENT {RECIPIENT_NAME}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option B: Databricks-to-Databricks Sharing (Recommended for Production)

# COMMAND ----------

# For Databricks-to-Databricks sharing, use the GCP workspace's sharing identifier
# Replace GCP_METASTORE_ID with your actual GCP metastore ID

# GCP_METASTORE_ID = "your-gcp-metastore-sharing-identifier"

# spark.sql(f"""
# CREATE RECIPIENT IF NOT EXISTS {RECIPIENT_NAME}
# USING ID '{GCP_METASTORE_ID}'
# COMMENT 'GCP Databricks workspace - Databricks-to-Databricks sharing'
# """)

# spark.sql(f"""
# GRANT SELECT ON SHARE {SHARE_NAME} TO RECIPIENT {RECIPIENT_NAME}
# """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Preview Data for Validation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample VoLTE CDR Data

# COMMAND ----------

display(spark.sql(f"""
SELECT 
    cdr_id,
    call_start_time,
    call_duration_seconds,
    caller_msisdn,
    callee_msisdn,
    call_type,
    is_answered,
    mos_score,
    originating_device_id,
    originating_location,
    originating_device_key,
    originating_region
FROM {CATALOG_NAME}.{SCHEMA_NAME}.ims_volte_cdrs
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Join Keys Match GCP Schema

# COMMAND ----------

# Show distinct join keys that will match GCP gold tables
display(spark.sql(f"""
SELECT DISTINCT
    originating_device_id as device_id,
    originating_location as location,
    originating_device_key as device_key,
    originating_region as region
FROM {CATALOG_NAME}.{SCHEMA_NAME}.ims_volte_cdrs
ORDER BY device_id
LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC ### Tables Created
# MAGIC | Table | Records | Description |
# MAGIC |-------|---------|-------------|
# MAGIC | `ims_volte_cdrs` | 100,000 | VoLTE Call Detail Records |
# MAGIC | `ims_sip_sessions` | 50,000 | SIP Session Metrics |
# MAGIC | `ims_node_metrics` | 25,000 | IMS Node Performance |
# MAGIC | `ims_subscriber_sessions` | 75,000 | Subscriber Analytics |
# MAGIC 
# MAGIC ### Join Keys for GCP Integration
# MAGIC - `device_id` → matches `dim_devices.device_id`
# MAGIC - `location` → matches `dim_devices.location`
# MAGIC - `device_key` → matches `dim_devices.device_key`
# MAGIC - `region` → matches `dim_devices.region`
# MAGIC 
# MAGIC ### Next Steps
# MAGIC 1. Complete recipient setup (Option A or B above)
# MAGIC 2. Get activation link and share with GCP workspace admin
# MAGIC 3. On GCP, create catalog from share
# MAGIC 4. Join IMS data with network performance gold tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appendix: SQL Queries for GCP Side (After Delta Share Setup)
# MAGIC 
# MAGIC ```sql
# MAGIC -- On GCP Databricks, after accepting the share:
# MAGIC 
# MAGIC -- Create catalog from the shared data
# MAGIC CREATE CATALOG IF NOT EXISTS aws_ims_shared
# MAGIC USING SHARE `aws_provider`.telco_ims_share;
# MAGIC 
# MAGIC -- Join IMS CDRs with Network Performance data
# MAGIC SELECT 
# MAGIC     c.cdr_id,
# MAGIC     c.call_start_time,
# MAGIC     c.call_duration_seconds,
# MAGIC     c.mos_score,
# MAGIC     c.originating_device_id,
# MAGIC     d.device_type,
# MAGIC     d.vendor,
# MAGIC     p.avg_value as network_latency_ms,
# MAGIC     p.anomaly_count as network_anomalies
# MAGIC FROM aws_ims_shared.ims_data.ims_volte_cdrs c
# MAGIC JOIN telus_networkperf.gold.dim_devices d 
# MAGIC     ON c.originating_device_id = d.device_id 
# MAGIC     AND c.originating_location = d.location
# MAGIC LEFT JOIN telus_networkperf.gold.gold_network_performance_5min p
# MAGIC     ON c.originating_device_id = p.device_id
# MAGIC     AND p.metric_name = 'latency_ms'
# MAGIC     AND p.window_start <= c.call_start_time
# MAGIC     AND p.window_end > c.call_start_time
# MAGIC LIMIT 100;
# MAGIC ```

