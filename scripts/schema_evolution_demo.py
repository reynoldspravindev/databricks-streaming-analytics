#!/usr/bin/env python3
"""
Schema Evolution Demo Script

This script demonstrates Databricks Auto Loader's schema evolution capability
by generating SNMP files with additional columns that weren't in the original schema.

Usage:
    # Generate files with original schema (5 metrics)
    python schema_evolution_demo.py --gcs-bucket gs://bucket/snmp/ --num-files 10

    # Generate files with extended schema (8 metrics + new columns)
    python schema_evolution_demo.py --gcs-bucket gs://bucket/snmp/ --num-files 10 --evolve-schema

    # DRY RUN - Preview without uploading (for testing before demo)
    python schema_evolution_demo.py --dry-run --evolve-schema

    # TEST MODE - Write to separate test path (won't affect demo)
    python schema_evolution_demo.py --gcs-bucket gs://bucket/snmp-test/ --num-files 5 --evolve-schema

Demo Flow:
    1. Start Databricks SNMP ingestion with original schema
    2. Run this script without --evolve-schema to generate baseline files
    3. Run this script WITH --evolve-schema to add new columns
    4. Observe Auto Loader automatically detecting and adding new columns to Delta table

Pre-Demo Testing:
    1. Use --dry-run to preview generated data without uploading
    2. Or use a separate --gcs-bucket path like gs://bucket/snmp-test/
    3. Before demo, reset using: gsutil -m rm gs://bucket/snmp/** (clear files)
       and clear checkpoints in Databricks
"""

import argparse
import json
import logging
import random
import sys
from datetime import datetime, timezone
from typing import List, Dict

# Device configuration
DEVICE_TYPES = ["router", "switch", "firewall", "load_balancer"]
LOCATIONS = ["datacenter-east", "datacenter-west", "datacenter-central", "edge-north", "edge-south"]
VENDORS = ["Cisco", "Juniper", "Arista", "Palo Alto", "F5"]

# Original SNMP metrics (base schema)
ORIGINAL_METRICS = {
    "latency_ms": {"oid": "1.3.6.1.2.1.1.1", "min": 1, "max": 200},
    "packet_loss_pct": {"oid": "1.3.6.1.2.1.1.2", "min": 0, "max": 5},
    "throughput_mbps": {"oid": "1.3.6.1.2.1.2.1", "min": 10, "max": 10000},
    "jitter_ms": {"oid": "1.3.6.1.2.1.3.1", "min": 0, "max": 50},
    "error_rate": {"oid": "1.3.6.1.2.1.4.1", "min": 0, "max": 1000},
}

# New metrics for schema evolution
NEW_METRICS = {
    "cpu_utilization_pct": {"oid": "1.3.6.1.2.1.5.1", "min": 0, "max": 100},
    "memory_usage_pct": {"oid": "1.3.6.1.2.1.5.2", "min": 0, "max": 100},
    "connection_count": {"oid": "1.3.6.1.2.1.5.3", "min": 0, "max": 50000},
}


class GCSClient:
    """Wrapper for Google Cloud Storage operations"""
    
    def __init__(self, bucket_path: str):
        try:
            from google.cloud import storage
            self.storage = storage
        except ImportError:
            raise ImportError(
                "google-cloud-storage is required. Install with: pip install google-cloud-storage"
            )
        
        if not bucket_path.startswith("gs://"):
            raise ValueError(f"Invalid GCS path: {bucket_path}. Must start with gs://")
        
        path_parts = bucket_path[5:].split("/", 1)
        self.bucket_name = path_parts[0]
        self.prefix = path_parts[1].rstrip("/") + "/" if len(path_parts) > 1 else ""
        
        self.client = storage.Client()
        self.bucket = self.client.bucket(self.bucket_name)
    
    def upload_json(self, filename: str, data: List[dict]) -> str:
        """Upload JSON Lines (NDJSON) - one compact JSON object per line"""
        blob_name = f"{self.prefix}{filename}"
        blob = self.bucket.blob(blob_name)
        # Compact JSON: no indentation, minimal separators - one record per line
        json_content = "\n".join(json.dumps(record, separators=(',', ':')) for record in data)
        blob.upload_from_string(json_content, content_type="application/x-ndjson")
        return f"gs://{self.bucket_name}/{blob_name}"


def generate_device() -> Dict:
    """Generate a random device"""
    device_type = random.choice(DEVICE_TYPES)
    location = random.choice(LOCATIONS)
    vendor = random.choice(VENDORS)
    device_num = random.randint(0, 99)
    
    return {
        "device_id": f"{device_type}-{location}-{device_num:03d}",
        "device_type": device_type,
        "location": location,
        "vendor": vendor,
        "model": f"{vendor}-Model-{random.randint(1000, 9999)}",
        "ip_address": f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}",
    }


def generate_record(evolve_schema: bool) -> Dict:
    """Generate a single SNMP record"""
    device = generate_device()
    
    # Select metrics pool
    metrics = ORIGINAL_METRICS.copy()
    if evolve_schema:
        metrics.update(NEW_METRICS)
    
    metric_name = random.choice(list(metrics.keys()))
    metric_info = metrics[metric_name]
    value = random.uniform(metric_info["min"], metric_info["max"])
    
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device_id": device["device_id"],
        "device_type": device["device_type"],
        "location": device["location"],
        "vendor": device["vendor"],
        "model": device["model"],
        "ip_address": device["ip_address"],
        "oid": metric_info["oid"],
        "metric_name": metric_name,
        "value": round(value, 2),
    }
    
    # Add new columns only when evolving schema
    if evolve_schema:
        record["firmware_version"] = f"{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 99)}"
        record["uptime_hours"] = random.randint(1, 8760)
        record["last_reboot"] = datetime.now(timezone.utc).replace(
            day=random.randint(1, 28),
            hour=random.randint(0, 23)
        ).isoformat()
        record["maintenance_window"] = random.choice(["weekday_night", "weekend", "anytime", None])
        record["criticality_tier"] = random.choice(["tier1", "tier2", "tier3"])
    
    return record


def generate_file_records(num_records: int, evolve_schema: bool) -> List[Dict]:
    """Generate records for a single file"""
    return [generate_record(evolve_schema) for _ in range(num_records)]


def main():
    parser = argparse.ArgumentParser(
        description="Schema Evolution Demo - Generate SNMP files with evolving schema",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # DRY RUN - Preview what would be generated (no upload)
    python schema_evolution_demo.py --dry-run --evolve-schema

    # Generate 10 files with original schema
    python schema_evolution_demo.py --gcs-bucket gs://my-bucket/snmp/ --num-files 10

    # Generate 10 files with extended schema (new columns)
    python schema_evolution_demo.py --gcs-bucket gs://my-bucket/snmp/ --num-files 10 --evolve-schema

    # TEST MODE - Write to separate test bucket (won't affect demo data)
    python schema_evolution_demo.py --gcs-bucket gs://my-bucket/snmp-test/ --num-files 5 --evolve-schema

    # Generate to local directory instead of GCS
    python schema_evolution_demo.py --local-dir /tmp/snmp_demo --num-files 10 --evolve-schema

Pre-Demo Workflow:
    1. Test with: --dry-run --evolve-schema
    2. Or test to separate path: --gcs-bucket gs://bucket/snmp-test/
    3. Before demo, reset: gsutil -m rm gs://bucket/snmp/**
    4. Clear Databricks checkpoints and drop table
    5. Demo with: --gcs-bucket gs://bucket/snmp/ --evolve-schema
        """
    )
    parser.add_argument(
        "--gcs-bucket",
        type=str,
        help="GCS bucket path (e.g., gs://bucket-name/snmp/)"
    )
    parser.add_argument(
        "--local-dir",
        type=str,
        help="Local directory for output (alternative to GCS)"
    )
    parser.add_argument(
        "--num-files",
        type=int,
        default=10,
        help="Number of files to generate (default: 10)"
    )
    parser.add_argument(
        "--records-per-file",
        type=int,
        default=50,
        help="Records per file (default: 50)"
    )
    parser.add_argument(
        "--evolve-schema",
        action="store_true",
        help="Generate files with extended schema (new columns)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview generated data without uploading (for testing before demo)"
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )
    
    args = parser.parse_args()
    
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    if not args.gcs_bucket and not args.local_dir and not args.dry_run:
        parser.error("Either --gcs-bucket, --local-dir, or --dry-run must be specified")
    
    # Display schema info
    if args.evolve_schema:
        logging.info("=" * 60)
        logging.info("SCHEMA EVOLUTION MODE: Extended schema with new columns")
        logging.info("=" * 60)
        logging.info("Original columns: timestamp, device_id, device_type, location,")
        logging.info("                  vendor, model, ip_address, oid, metric_name, value")
        logging.info("NEW columns: firmware_version, uptime_hours, last_reboot,")
        logging.info("             maintenance_window, criticality_tier")
        logging.info("NEW metrics: cpu_utilization_pct, memory_usage_pct, connection_count")
        logging.info("=" * 60)
    else:
        logging.info("=" * 60)
        logging.info("ORIGINAL SCHEMA MODE: Base columns only")
        logging.info("=" * 60)
        logging.info("Columns: timestamp, device_id, device_type, location,")
        logging.info("         vendor, model, ip_address, oid, metric_name, value")
        logging.info("Metrics: latency_ms, packet_loss_pct, throughput_mbps, jitter_ms, error_rate")
        logging.info("=" * 60)
    
    # Handle dry-run mode
    if args.dry_run:
        logging.info("=" * 60)
        logging.info("DRY RUN MODE - No files will be uploaded")
        logging.info("=" * 60)
        logging.info("")
        logging.info("This is a PREVIEW of what would be generated.")
        logging.info("Use this to test before your demo without affecting the actual data.")
        logging.info("")
        
        # Generate sample records
        sample_records = generate_file_records(3, args.evolve_schema)
        
        logging.info("Sample records that would be generated:")
        logging.info("-" * 60)
        for i, record in enumerate(sample_records, 1):
            logging.info(f"\nRecord {i}:")
            for key, value in record.items():
                logging.info(f"  {key}: {value}")
        
        logging.info("")
        logging.info("-" * 60)
        logging.info(f"Would generate {args.num_files} files × {args.records_per_file} records = {args.num_files * args.records_per_file} total records")
        
        if args.evolve_schema:
            logging.info("")
            logging.info("NEW COLUMNS that would be added:")
            logging.info("  • firmware_version (e.g., '3.2.45')")
            logging.info("  • uptime_hours (e.g., 2456)")
            logging.info("  • last_reboot (e.g., '2025-12-01T08:30:00+00:00')")
            logging.info("  • maintenance_window (e.g., 'weekend', 'weekday_night')")
            logging.info("  • criticality_tier (e.g., 'tier1', 'tier2', 'tier3')")
            logging.info("")
            logging.info("NEW METRICS that would be added:")
            logging.info("  • cpu_utilization_pct")
            logging.info("  • memory_usage_pct")
            logging.info("  • connection_count")
        
        logging.info("")
        logging.info("=" * 60)
        logging.info("To actually upload files, remove --dry-run flag")
        logging.info("=" * 60)
        return
    
    # Initialize output
    gcs_client = None
    local_dir = None
    
    if args.gcs_bucket:
        gcs_client = GCSClient(args.gcs_bucket)
        logging.info(f"Output: GCS bucket {args.gcs_bucket}")
    elif args.local_dir:
        from pathlib import Path
        local_dir = Path(args.local_dir)
        local_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"Output: Local directory {local_dir}")
    
    # Generate files
    total_records = 0
    schema_suffix = "_evolved" if args.evolve_schema else "_original"
    
    for i in range(args.num_files):
        timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")[:-3]
        filename = f"snmp_demo{schema_suffix}_{timestamp_str}_{i:04d}.json"
        
        records = generate_file_records(args.records_per_file, args.evolve_schema)
        total_records += len(records)
        
        if gcs_client:
            path = gcs_client.upload_json(filename, records)
            logging.info(f"Uploaded: {path} ({len(records)} records)")
        else:
            filepath = local_dir / filename
            with open(filepath, 'w') as f:
                # Compact JSON: one record per line
                f.write("\n".join(json.dumps(r, separators=(',', ':')) for r in records))
            logging.info(f"Created: {filepath} ({len(records)} records)")
    
    logging.info("=" * 60)
    logging.info(f"Complete! Generated {args.num_files} files with {total_records} total records")
    if args.evolve_schema:
        logging.info("Check Databricks to see Auto Loader detect the new columns!")
    logging.info("=" * 60)


if __name__ == "__main__":
    main()

