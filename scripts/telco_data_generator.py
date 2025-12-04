#!/usr/bin/env python3
"""
Telco Network Performance Data Generator
Generates synthetic syslog (RFC 5424) and SNMP trap data for network performance monitoring demo.

Supports two output modes for SNMP:
- Local directory (SFTP): --snmp-dir /path/to/dir
- GCS bucket: --snmp-gcs-bucket gs://bucket-name/path/
"""

import argparse
import csv
import json
import logging
import os
import random
import signal
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import List, Optional

# Configuration
DEVICE_TYPES = ["router", "switch", "firewall", "load_balancer"]
LOCATIONS = ["datacenter-east", "datacenter-west", "datacenter-central", "edge-north", "edge-south"]
VENDORS = ["Cisco", "Juniper", "Arista", "Palo Alto", "F5"]
DEVICE_COUNT = 100  # Total number of devices

# Network event types for syslog
EVENT_TYPES = {
    "LINKUPDOWN": ["Interface {interface} changed state to {state}", 3],
    "ADJCHANGE": ["BGP neighbor {peer} {state} - {reason}", 5],
    "OSPFADJCHG": ["OSPF neighbor {peer} on {interface} from {old_state} to {new_state}", 5],
    "SECURITY": ["{action} {protocol} connection from {src_ip}:{src_port} to {dst_ip}:{dst_port}", 4],
    "IFDOWN": ["Interface {interface} is down, {reason}", 3],
    "IFUP": ["Interface {interface} is up", 6],
    "HASTATE": ["HA state changed to {state}", 5],
    "FANFAIL": ["Fan {fan_id} failure detected", 2],
    "TEMPWARN": ["Temperature sensor {sensor_id} warning: {temp}C", 4],
    "CFGCHANGE": ["Configuration changed by {user} from {source_ip}", 5],
}

# SNMP OIDs for network metrics
SNMP_METRICS = {
    "latency_ms": {"oid": "1.3.6.1.2.1.1.1", "min": 1, "max": 200, "anomaly_threshold": 150},
    "packet_loss_pct": {"oid": "1.3.6.1.2.1.1.2", "min": 0, "max": 5, "anomaly_threshold": 3},
    "throughput_mbps": {"oid": "1.3.6.1.2.1.2.1", "min": 10, "max": 10000, "anomaly_threshold": 9500},
    "jitter_ms": {"oid": "1.3.6.1.2.1.3.1", "min": 0, "max": 50, "anomaly_threshold": 40},
    "error_rate": {"oid": "1.3.6.1.2.1.4.1", "min": 0, "max": 1000, "anomaly_threshold": 800},
}

# Extended metrics for schema evolution demo
EXTENDED_SNMP_METRICS = {
    "cpu_utilization_pct": {"oid": "1.3.6.1.2.1.5.1", "min": 0, "max": 100, "anomaly_threshold": 90},
    "memory_usage_pct": {"oid": "1.3.6.1.2.1.5.2", "min": 0, "max": 100, "anomaly_threshold": 95},
    "connection_count": {"oid": "1.3.6.1.2.1.5.3", "min": 0, "max": 50000, "anomaly_threshold": 45000},
}

# Global flag for graceful shutdown
shutdown_flag = False


@dataclass
class NetworkDevice:
    """Represents a network device"""
    device_id: str
    device_type: str
    location: str
    vendor: str
    model: str
    ip_address: str


class GCSClient:
    """Wrapper for Google Cloud Storage operations"""
    
    def __init__(self, bucket_path: str):
        """
        Initialize GCS client.
        
        Args:
            bucket_path: GCS path like gs://bucket-name/prefix/
        """
        try:
            from google.cloud import storage
            self.storage = storage
        except ImportError:
            raise ImportError(
                "google-cloud-storage is required for GCS support. "
                "Install with: pip install google-cloud-storage"
            )
        
        # Parse bucket path
        if not bucket_path.startswith("gs://"):
            raise ValueError(f"Invalid GCS path: {bucket_path}. Must start with gs://")
        
        path_parts = bucket_path[5:].split("/", 1)
        self.bucket_name = path_parts[0]
        self.prefix = path_parts[1] if len(path_parts) > 1 else ""
        
        # Remove trailing slash from prefix
        self.prefix = self.prefix.rstrip("/")
        if self.prefix:
            self.prefix += "/"
        
        # Initialize client
        self.client = storage.Client()
        self.bucket = self.client.bucket(self.bucket_name)
        
        logging.info(f"GCS client initialized: bucket={self.bucket_name}, prefix={self.prefix}")
    
    def upload_json(self, filename: str, data: List[dict]):
        """
        Upload JSON data to GCS in JSON Lines (NDJSON) format.
        
        Each record is a single-line JSON object, separated by newlines.
        This format is optimal for streaming ingestion with Auto Loader.
        """
        blob_name = f"{self.prefix}{filename}"
        blob = self.bucket.blob(blob_name)
        
        # Create newline-delimited JSON (NDJSON) - each record on ONE line
        # Using separators to ensure compact output with no extra whitespace
        json_lines = []
        for record in data:
            # Compact JSON: no indentation, minimal separators
            compact_json = json.dumps(record, separators=(',', ':'))
            json_lines.append(compact_json)
        
        json_content = "\n".join(json_lines)
        
        blob.upload_from_string(
            json_content,
            content_type="application/x-ndjson"  # Proper MIME type for NDJSON
        )
        
        return f"gs://{self.bucket_name}/{blob_name}"


class TelcoDataGenerator:
    """Generates synthetic telco network performance data"""
    
    def __init__(
        self,
        syslog_dir: str,
        snmp_dir: Optional[str],
        snmp_gcs_bucket: Optional[str],
        files_per_minute: int,
        extended_schema: bool = False
    ):
        self.syslog_dir = Path(syslog_dir)
        self.snmp_dir = Path(snmp_dir) if snmp_dir else None
        self.snmp_gcs_client = GCSClient(snmp_gcs_bucket) if snmp_gcs_bucket else None
        self.files_per_minute = files_per_minute
        self.extended_schema = extended_schema
        self.devices = self._generate_devices()
        
        # Determine SNMP output mode
        self.snmp_output_mode = "gcs" if snmp_gcs_bucket else "local"
        
        # Ensure directories exist
        self.syslog_dir.mkdir(parents=True, exist_ok=True)
        if self.snmp_dir:
            self.snmp_dir.mkdir(parents=True, exist_ok=True)
        
        # Calculate timing
        self.syslog_files_per_min = files_per_minute // 2
        self.snmp_files_per_min = files_per_minute // 2
        self.syslog_interval = 60.0 / self.syslog_files_per_min if self.syslog_files_per_min > 0 else 1.0
        self.snmp_interval = 60.0 / self.snmp_files_per_min if self.snmp_files_per_min > 0 else 1.0
        
        logging.info(f"Initialized generator: {files_per_minute} files/min")
        logging.info(f"Syslog: {self.syslog_files_per_min} files/min (interval: {self.syslog_interval:.3f}s) -> local")
        logging.info(f"SNMP: {self.snmp_files_per_min} files/min (interval: {self.snmp_interval:.3f}s) -> {self.snmp_output_mode}")
        logging.info(f"Extended schema: {self.extended_schema}")
        logging.info(f"Generated {len(self.devices)} network devices")
    
    def _generate_devices(self) -> List[NetworkDevice]:
        """Generate a list of network devices"""
        devices = []
        for i in range(DEVICE_COUNT):
            device_type = random.choice(DEVICE_TYPES)
            location = random.choice(LOCATIONS)
            vendor = random.choice(VENDORS)
            
            # Generate device model based on vendor and type
            models = {
                "Cisco": {"router": "ASR-9000", "switch": "Nexus-9300", "firewall": "ASA-5525", "load_balancer": "ACE-4710"},
                "Juniper": {"router": "MX960", "switch": "EX4650", "firewall": "SRX5800", "load_balancer": "LB-8"},
                "Arista": {"router": "7280R3", "switch": "7050X3", "firewall": "n/a", "load_balancer": "n/a"},
                "Palo Alto": {"router": "n/a", "switch": "n/a", "firewall": "PA-5250", "load_balancer": "n/a"},
                "F5": {"router": "n/a", "switch": "n/a", "firewall": "n/a", "load_balancer": "BIG-IP-i15800"},
            }
            
            model = models.get(vendor, {}).get(device_type, "Unknown")
            if model == "n/a":
                # Skip invalid combinations
                vendor = "Cisco"
                model = models["Cisco"][device_type]
            
            device_id = f"{device_type}-{location}-{i:03d}"
            ip_address = f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
            
            devices.append(NetworkDevice(
                device_id=device_id,
                device_type=device_type,
                location=location,
                vendor=vendor,
                model=model,
                ip_address=ip_address
            ))
        
        return devices
    
    def generate_rfc5424_syslog(self, device: NetworkDevice) -> str:
        """
        Generate RFC 5424 compliant syslog message
        
        Format: <PRI>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID [STRUCTURED-DATA] MSG
        """
        # Priority = (Facility * 8) + Severity
        # Facility 16 (local0) for network devices
        # Severity: 0=Emergency, 1=Alert, 2=Critical, 3=Error, 4=Warning, 5=Notice, 6=Info, 7=Debug
        facility = 16
        
        # Select random event type
        event_type = random.choice(list(EVENT_TYPES.keys()))
        message_template, default_severity = EVENT_TYPES[event_type]
        
        # Occasionally make errors/warnings more severe
        if random.random() < 0.1:  # 10% chance of critical events
            severity = random.choice([0, 1, 2, 3])
        else:
            severity = default_severity
        
        priority = (facility * 8) + severity
        version = 1
        
        # ISO 8601 timestamp with timezone
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        
        hostname = device.device_id
        
        # App name based on event type
        app_names = {
            "LINKUPDOWN": "IFMGR",
            "ADJCHANGE": "BGP",
            "OSPFADJCHG": "OSPF",
            "SECURITY": "FIREWALL",
            "IFDOWN": "IFMGR",
            "IFUP": "IFMGR",
            "HASTATE": "HAMGR",
            "FANFAIL": "ENVMON",
            "TEMPWARN": "ENVMON",
            "CFGCHANGE": "CONFMGR",
        }
        app_name = app_names.get(event_type, "SYSTEM")
        
        procid = str(random.randint(1000, 9999))
        msgid = event_type
        
        # Structured data with network-specific information
        sd_elements = []
        sd_id = "network@32473"  # Enterprise ID
        
        # Generate context-specific structured data
        if event_type in ["LINKUPDOWN", "IFDOWN", "IFUP"]:
            interface = f"GigabitEthernet{random.randint(0, 3)}/{random.randint(0, 1)}/{random.randint(1, 48)}"
            state = random.choice(["up", "down"])
            sd_elements.append(f'[{sd_id} interface="{interface}" state="{state}" speed="1000"]')
        elif event_type == "ADJCHANGE":
            peer = f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
            state = random.choice(["Up", "Down"])
            reason = random.choice(["Hold Timer Expired", "Admin", "Peer closed session", "Configuration change"])
            sd_elements.append(f'[{sd_id} peer="{peer}" state="{state}" as="{random.randint(64512, 65535)}"]')
        elif event_type == "OSPFADJCHG":
            peer = f"172.16.{random.randint(0, 255)}.{random.randint(1, 254)}"
            interface = f"GigabitEthernet{random.randint(0, 3)}/{random.randint(0, 1)}"
            old_state = random.choice(["FULL", "2WAY", "INIT"])
            new_state = random.choice(["DOWN", "INIT", "2WAY"])
            sd_elements.append(f'[{sd_id} peer="{peer}" interface="{interface}" area="0.0.0.0"]')
        elif event_type == "SECURITY":
            action = random.choice(["Denied", "Allowed", "Blocked"])
            protocol = random.choice(["TCP", "UDP", "ICMP"])
            src_ip = f"192.168.{random.randint(0, 255)}.{random.randint(1, 254)}"
            src_port = random.randint(1024, 65535)
            dst_ip = f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
            dst_port = random.choice([80, 443, 22, 3389, 8080])
            sd_elements.append(f'[{sd_id} action="{action}" protocol="{protocol}" src="{src_ip}:{src_port}" dst="{dst_ip}:{dst_port}"]')
        else:
            sd_elements.append(f'[{sd_id} deviceType="{device.device_type}" location="{device.location}"]')
        
        structured_data = " ".join(sd_elements) if sd_elements else "-"
        
        # Fill in message template with appropriate values
        message_params = {}
        if "{interface}" in message_template:
            message_params["interface"] = f"GigabitEthernet{random.randint(0, 3)}/{random.randint(0, 1)}/{random.randint(1, 48)}"
        if "{state}" in message_template:
            message_params["state"] = random.choice(["up", "down", "ACTIVE", "STANDBY"])
        if "{peer}" in message_template:
            message_params["peer"] = f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
        if "{reason}" in message_template:
            message_params["reason"] = random.choice(["Hold Timer Expired", "Admin", "Configuration change"])
        if "{old_state}" in message_template:
            message_params["old_state"] = "FULL"
        if "{new_state}" in message_template:
            message_params["new_state"] = "DOWN"
        if "{action}" in message_template:
            message_params["action"] = random.choice(["Denied", "Allowed"])
        if "{protocol}" in message_template:
            message_params["protocol"] = random.choice(["TCP", "UDP"])
        if "{src_ip}" in message_template:
            message_params["src_ip"] = f"192.168.{random.randint(0, 255)}.{random.randint(1, 254)}"
        if "{src_port}" in message_template:
            message_params["src_port"] = random.randint(1024, 65535)
        if "{dst_ip}" in message_template:
            message_params["dst_ip"] = f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
        if "{dst_port}" in message_template:
            message_params["dst_port"] = random.choice([80, 443, 22])
        if "{fan_id}" in message_template:
            message_params["fan_id"] = random.randint(1, 4)
        if "{sensor_id}" in message_template:
            message_params["sensor_id"] = random.randint(1, 8)
        if "{temp}" in message_template:
            message_params["temp"] = random.randint(65, 85)
        if "{user}" in message_template:
            message_params["user"] = random.choice(["admin", "netops", "automation"])
        if "{source_ip}" in message_template:
            message_params["source_ip"] = f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
        
        message = message_template.format(**message_params)
        
        # Assemble RFC 5424 message
        syslog_message = f"<{priority}>{version} {timestamp} {hostname} {app_name} {procid} {msgid} {structured_data} {message}"
        
        return syslog_message
    
    def _generate_metric_value(self, metric_info: dict) -> float:
        """Generate a metric value with occasional anomalies"""
        metric_name = metric_info.get("name", "unknown")
        
        # 5% chance of anomaly
        if random.random() < 0.05:
            return random.uniform(metric_info["anomaly_threshold"], metric_info["max"])
        
        # Normal distribution for most metrics
        mid = (metric_info["min"] + metric_info["max"]) / 2
        std = (metric_info["max"] - metric_info["min"]) / 6
        value = random.gauss(mid, std)
        return max(metric_info["min"], min(metric_info["max"], value))
    
    def generate_snmp_metrics(self, device: NetworkDevice) -> dict:
        """Generate SNMP metric data"""
        timestamp = datetime.now(timezone.utc).isoformat()
        
        # Select metrics pool based on schema mode
        metrics_pool = SNMP_METRICS.copy()
        if self.extended_schema:
            metrics_pool.update(EXTENDED_SNMP_METRICS)
        
        # Randomly select a metric
        metric_name = random.choice(list(metrics_pool.keys()))
        metric_info = metrics_pool[metric_name]
        
        # Generate value
        value = self._generate_metric_value(metric_info)
        
        record = {
            "timestamp": timestamp,
            "device_id": device.device_id,
            "device_type": device.device_type,
            "location": device.location,
            "vendor": device.vendor,
            "model": device.model,
            "ip_address": device.ip_address,
            "oid": metric_info["oid"],
            "metric_name": metric_name,
            "value": round(value, 2),
        }
        
        # Add extended fields if using extended schema
        if self.extended_schema:
            record["firmware_version"] = f"{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 99)}"
            record["uptime_hours"] = random.randint(1, 8760)  # Up to 1 year
            record["last_reboot"] = (datetime.now(timezone.utc).replace(
                hour=random.randint(0, 23),
                minute=random.randint(0, 59)
            )).isoformat()
        
        return record
    
    def write_syslog_file(self, file_count: int):
        """Write a syslog file with multiple log entries"""
        timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")[:-3]
        filename = self.syslog_dir / f"syslog_{timestamp_str}_{file_count:06d}.txt"
        
        # Generate 10-50 log entries per file
        num_entries = random.randint(10, 50)
        
        try:
            with open(filename, 'w') as f:
                for _ in range(num_entries):
                    device = random.choice(self.devices)
                    log_entry = self.generate_rfc5424_syslog(device)
                    f.write(log_entry + "\n")
            
            logging.debug(f"Created syslog file: {filename} ({num_entries} entries)")
        except Exception as e:
            logging.error(f"Error writing syslog file {filename}: {e}")
    
    def write_snmp_file(self, file_count: int):
        """Write an SNMP metrics file (JSON to GCS or CSV to local)"""
        timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")[:-3]
        
        # Generate 20-100 metric samples per file
        num_entries = random.randint(20, 100)
        records = []
        
        for _ in range(num_entries):
            device = random.choice(self.devices)
            metrics = self.generate_snmp_metrics(device)
            records.append(metrics)
        
        try:
            if self.snmp_gcs_client:
                # Write JSON to GCS
                filename = f"snmp_{timestamp_str}_{file_count:06d}.json"
                gcs_path = self.snmp_gcs_client.upload_json(filename, records)
                logging.debug(f"Uploaded to GCS: {gcs_path} ({num_entries} entries)")
            else:
                # Write CSV to local directory
                filename = self.snmp_dir / f"snmp_{timestamp_str}_{file_count:06d}.csv"
                with open(filename, 'w', newline='') as f:
                    if records:
                        fieldnames = list(records[0].keys())
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(records)
                logging.debug(f"Created SNMP file: {filename} ({num_entries} entries)")
        except Exception as e:
            logging.error(f"Error writing SNMP file: {e}")
    
    def run(self):
        """Main generation loop"""
        logging.info("Starting data generation...")
        logging.info(f"Syslog directory: {self.syslog_dir}")
        if self.snmp_gcs_client:
            logging.info(f"SNMP destination: GCS (gs://{self.snmp_gcs_client.bucket_name}/{self.snmp_gcs_client.prefix})")
        else:
            logging.info(f"SNMP directory: {self.snmp_dir}")
        logging.info("Press Ctrl+C to stop")
        
        syslog_count = 0
        snmp_count = 0
        last_syslog_time = time.time()
        last_snmp_time = time.time()
        
        try:
            while not shutdown_flag:
                current_time = time.time()
                
                # Generate syslog file if interval has passed
                if current_time - last_syslog_time >= self.syslog_interval:
                    self.write_syslog_file(syslog_count)
                    syslog_count += 1
                    last_syslog_time = current_time
                
                # Generate SNMP file if interval has passed
                if current_time - last_snmp_time >= self.snmp_interval:
                    self.write_snmp_file(snmp_count)
                    snmp_count += 1
                    last_snmp_time = current_time
                
                # Log progress every 100 files
                total_files = syslog_count + snmp_count
                if total_files > 0 and total_files % 100 == 0:
                    logging.info(f"Generated {total_files} files (syslog: {syslog_count}, snmp: {snmp_count})")
                
                # Small sleep to prevent busy waiting
                time.sleep(0.001)
        
        except KeyboardInterrupt:
            logging.info("Received interrupt signal")
        finally:
            logging.info(f"Shutting down. Total files generated: {syslog_count + snmp_count}")
            logging.info(f"  Syslog files: {syslog_count}")
            logging.info(f"  SNMP files: {snmp_count}")


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_flag
    logging.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_flag = True


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Generate synthetic telco network performance data (syslog and SNMP)"
    )
    parser.add_argument(
        "--syslog-dir",
        type=str,
        default="/sftp/telco/syslog",
        help="Directory for syslog files (default: /sftp/telco/syslog)"
    )
    parser.add_argument(
        "--snmp-dir",
        type=str,
        default=None,
        help="Directory for SNMP files (local mode, default: /sftp/telco/snmp)"
    )
    parser.add_argument(
        "--snmp-gcs-bucket",
        type=str,
        default=None,
        help="GCS bucket path for SNMP files (e.g., gs://bucket-name/snmp/)"
    )
    parser.add_argument(
        "--files-per-minute",
        type=int,
        default=1000,
        help="Total files to generate per minute (split between syslog and SNMP) (default: 1000)"
    )
    parser.add_argument(
        "--extended-schema",
        action="store_true",
        help="Use extended schema with additional columns (for schema evolution demo)"
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)"
    )
    
    args = parser.parse_args()
    
    # Validate SNMP output configuration
    if not args.snmp_gcs_bucket and not args.snmp_dir:
        args.snmp_dir = "/sftp/telco/snmp"  # Default to local
    
    if args.snmp_gcs_bucket and args.snmp_dir:
        logging.warning("Both --snmp-gcs-bucket and --snmp-dir specified. Using GCS bucket.")
        args.snmp_dir = None
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and run generator
    generator = TelcoDataGenerator(
        syslog_dir=args.syslog_dir,
        snmp_dir=args.snmp_dir,
        snmp_gcs_bucket=args.snmp_gcs_bucket,
        files_per_minute=args.files_per_minute,
        extended_schema=args.extended_schema
    )
    
    generator.run()


if __name__ == "__main__":
    main()
