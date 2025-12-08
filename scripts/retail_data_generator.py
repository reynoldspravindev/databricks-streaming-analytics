#!/usr/bin/env python3
"""
Retail Store Performance Data Generator
Generates synthetic transaction events and operational metrics for retail analytics demo.

Supports two output modes for Metrics:
- Local directory (SFTP): --metrics-dir /path/to/dir
- GCS bucket: --metrics-gcs-bucket gs://bucket-name/path/
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
from datetime import datetime, timezone, timedelta
from io import BytesIO
from pathlib import Path
from typing import List, Optional

# Configuration
STORE_TYPES = ["flagship", "mall", "outlet", "express", "warehouse"]
REGIONS = ["west-coast", "east-coast", "midwest", "southeast", "northeast", "southwest"]
BRANDS = ["RetailCo", "ValueMart", "StyleHub", "FreshGoods", "TechZone"]
STORE_COUNT = 100  # Total number of stores

# Transaction event types
EVENT_TYPES = {
    "TRANSACTION": ["Sale completed ${amount:.2f} - {items} items", 6],
    "PAYMENT_DECLINED": ["Payment declined - {reason}", 4],
    "REFUND": ["Refund processed ${amount:.2f} - {reason}", 5],
    "INVENTORY_LOW": ["SKU-{sku} below reorder threshold ({qty} remaining)", 4],
    "STOCKOUT": ["SKU-{sku} out of stock", 3],
    "PRICE_OVERRIDE": ["Manager override on item {sku} - {reason}", 5],
    "THEFT_ALERT": ["RFID exit without checkout - {sku}", 2],
    "POS_ERROR": ["POS terminal {terminal} error - {error_type}", 3],
    "CHECKOUT_TIMEOUT": ["Checkout timeout at terminal {terminal}", 4],
    "LOYALTY_SIGNUP": ["New loyalty member enrolled - {tier}", 6],
}

# Operational metrics
STORE_METRICS = {
    "hourly_sales": {"oid": "1.3.6.1.4.1.1", "min": 500, "max": 25000, "anomaly_threshold": 22000},
    "transactions_per_hour": {"oid": "1.3.6.1.4.1.2", "min": 10, "max": 300, "anomaly_threshold": 280},
    "avg_basket_size": {"oid": "1.3.6.1.4.1.3", "min": 15, "max": 150, "anomaly_threshold": 130},
    "checkout_wait_time_sec": {"oid": "1.3.6.1.4.1.4", "min": 30, "max": 600, "anomaly_threshold": 300},
    "conversion_rate_pct": {"oid": "1.3.6.1.4.1.5", "min": 5, "max": 45, "anomaly_threshold": 40},
}

# Extended metrics for schema evolution demo
EXTENDED_METRICS = {
    "customer_traffic": {"oid": "1.3.6.1.4.1.6", "min": 50, "max": 2000, "anomaly_threshold": 1800},
    "return_rate_pct": {"oid": "1.3.6.1.4.1.7", "min": 0, "max": 15, "anomaly_threshold": 12},
    "staff_productivity": {"oid": "1.3.6.1.4.1.8", "min": 100, "max": 500, "anomaly_threshold": 450},
}

# Global flag for graceful shutdown
shutdown_flag = False


@dataclass
class Store:
    """Represents a retail store"""
    store_id: str
    store_type: str
    region: str
    brand: str
    district: str
    address: str


class GCSClient:
    """Wrapper for Google Cloud Storage operations"""
    
    def __init__(self, bucket_path: str):
        try:
            from google.cloud import storage
            self.storage = storage
        except ImportError:
            raise ImportError(
                "google-cloud-storage is required for GCS support. "
                "Install with: pip install google-cloud-storage"
            )
        
        if not bucket_path.startswith("gs://"):
            raise ValueError(f"Invalid GCS path: {bucket_path}. Must start with gs://")
        
        path_parts = bucket_path[5:].split("/", 1)
        self.bucket_name = path_parts[0]
        self.prefix = path_parts[1] if len(path_parts) > 1 else ""
        
        self.prefix = self.prefix.rstrip("/")
        if self.prefix:
            self.prefix += "/"
        
        self.client = storage.Client()
        self.bucket = self.client.bucket(self.bucket_name)
        
        logging.info(f"GCS client initialized: bucket={self.bucket_name}, prefix={self.prefix}")
    
    def upload_json(self, filename: str, data: List[dict]):
        """Upload JSON Lines (NDJSON) format."""
        blob_name = f"{self.prefix}{filename}"
        blob = self.bucket.blob(blob_name)
        
        json_lines = []
        for record in data:
            compact_json = json.dumps(record, separators=(',', ':'))
            json_lines.append(compact_json)
        
        json_content = "\n".join(json_lines)
        blob.upload_from_string(json_content, content_type="application/x-ndjson")
        
        return f"gs://{self.bucket_name}/{blob_name}"


class RetailDataGenerator:
    """Generates synthetic retail store performance data"""
    
    def __init__(
        self,
        events_dir: str,
        metrics_dir: Optional[str],
        metrics_gcs_bucket: Optional[str],
        files_per_minute: int,
        extended_schema: bool = False
    ):
        self.events_dir = Path(events_dir)
        self.metrics_dir = Path(metrics_dir) if metrics_dir else None
        self.metrics_gcs_client = GCSClient(metrics_gcs_bucket) if metrics_gcs_bucket else None
        self.files_per_minute = files_per_minute
        self.extended_schema = extended_schema
        self.stores = self._generate_stores()
        
        self.metrics_output_mode = "gcs" if metrics_gcs_bucket else "local"
        
        self.events_dir.mkdir(parents=True, exist_ok=True)
        if self.metrics_dir:
            self.metrics_dir.mkdir(parents=True, exist_ok=True)
        
        self.events_files_per_min = files_per_minute // 2
        self.metrics_files_per_min = files_per_minute // 2
        self.events_interval = 60.0 / self.events_files_per_min if self.events_files_per_min > 0 else 1.0
        self.metrics_interval = 60.0 / self.metrics_files_per_min if self.metrics_files_per_min > 0 else 1.0
        
        logging.info(f"Initialized generator: {files_per_minute} files/min")
        logging.info(f"Events: {self.events_files_per_min} files/min -> local SFTP")
        logging.info(f"Metrics: {self.metrics_files_per_min} files/min -> {self.metrics_output_mode}")
        logging.info(f"Extended schema: {self.extended_schema}")
        logging.info(f"Generated {len(self.stores)} retail stores")
    
    def _generate_stores(self) -> List[Store]:
        """Generate a list of retail stores"""
        stores = []
        for i in range(STORE_COUNT):
            store_type = random.choice(STORE_TYPES)
            region = random.choice(REGIONS)
            brand = random.choice(BRANDS)
            
            store_id = f"store-{region[:4]}-{i:03d}"
            district = f"district-{random.randint(1, 20)}"
            address = f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Cedar', 'Park', 'Market'])} {random.choice(['St', 'Ave', 'Blvd', 'Dr'])}"
            
            stores.append(Store(
                store_id=store_id,
                store_type=store_type,
                region=region,
                brand=brand,
                district=district,
                address=address
            ))
        
        return stores
    
    def generate_transaction_event(self, store: Store) -> str:
        """Generate RFC 5424 compliant transaction/event log"""
        facility = 16
        
        event_type = random.choice(list(EVENT_TYPES.keys()))
        message_template, default_severity = EVENT_TYPES[event_type]
        
        if random.random() < 0.1:
            severity = random.choice([2, 3])
        else:
            severity = default_severity
        
        priority = (facility * 8) + severity
        version = 1
        
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        hostname = store.store_id
        
        app_names = {
            "TRANSACTION": "POS",
            "PAYMENT_DECLINED": "PAYMENT",
            "REFUND": "POS",
            "INVENTORY_LOW": "INVENTORY",
            "STOCKOUT": "INVENTORY",
            "PRICE_OVERRIDE": "POS",
            "THEFT_ALERT": "SECURITY",
            "POS_ERROR": "POS",
            "CHECKOUT_TIMEOUT": "POS",
            "LOYALTY_SIGNUP": "LOYALTY",
        }
        app_name = app_names.get(event_type, "STORE")
        
        procid = str(random.randint(1000, 9999))
        msgid = event_type
        
        # Structured data
        sd_id = "retail@32473"
        terminal = f"POS-{random.randint(1, 12):02d}"
        
        if event_type == "TRANSACTION":
            amount = random.uniform(5, 500)
            items = random.randint(1, 15)
            sd = f'[{sd_id} terminal="{terminal}" payment="{random.choice(["credit", "debit", "cash", "mobile"])}" items="{items}"]'
            msg = message_template.format(amount=amount, items=items)
        elif event_type == "PAYMENT_DECLINED":
            reason = random.choice(["insufficient_funds", "expired_card", "fraud_alert", "network_error"])
            sd = f'[{sd_id} terminal="{terminal}" reason="{reason}"]'
            msg = message_template.format(reason=reason)
        elif event_type == "REFUND":
            amount = random.uniform(10, 200)
            reason = random.choice(["defective", "wrong_size", "changed_mind", "price_match"])
            sd = f'[{sd_id} terminal="{terminal}" amount="{amount:.2f}"]'
            msg = message_template.format(amount=amount, reason=reason)
        elif event_type in ["INVENTORY_LOW", "STOCKOUT"]:
            sku = f"{random.randint(10000, 99999)}"
            qty = random.randint(0, 10) if event_type == "INVENTORY_LOW" else 0
            sd = f'[{sd_id} sku="{sku}" category="{random.choice(["apparel", "electronics", "home", "grocery"])}"]'
            msg = message_template.format(sku=sku, qty=qty)
        elif event_type == "THEFT_ALERT":
            sku = f"{random.randint(10000, 99999)}"
            sd = f'[{sd_id} sku="{sku}" exit="{random.choice(["main", "side", "emergency"])}"]'
            msg = message_template.format(sku=sku)
        elif event_type == "POS_ERROR":
            error_type = random.choice(["printer_jam", "scanner_fail", "network_timeout", "cash_drawer_stuck"])
            sd = f'[{sd_id} terminal="{terminal}" error="{error_type}"]'
            msg = message_template.format(terminal=terminal, error_type=error_type)
        else:
            sd = f'[{sd_id} terminal="{terminal}" storeType="{store.store_type}"]'
            msg = message_template.format(terminal=terminal, sku=random.randint(10000, 99999), 
                                          reason="manager_approval", tier=random.choice(["silver", "gold", "platinum"]))
        
        return f"<{priority}>{version} {timestamp} {hostname} {app_name} {procid} {msgid} {sd} {msg}"
    
    def _generate_metric_value(self, metric_info: dict) -> float:
        """Generate a metric value with occasional anomalies"""
        if random.random() < 0.05:
            return random.uniform(metric_info["anomaly_threshold"], metric_info["max"])
        
        mid = (metric_info["min"] + metric_info["max"]) / 2
        std = (metric_info["max"] - metric_info["min"]) / 6
        value = random.gauss(mid, std)
        return max(metric_info["min"], min(metric_info["max"], value))
    
    def generate_store_metrics(self, store: Store) -> dict:
        """Generate operational metrics for a store"""
        timestamp = datetime.now(timezone.utc).isoformat()
        
        metrics_pool = STORE_METRICS.copy()
        if self.extended_schema:
            metrics_pool.update(EXTENDED_METRICS)
        
        metric_name = random.choice(list(metrics_pool.keys()))
        metric_info = metrics_pool[metric_name]
        
        value = self._generate_metric_value(metric_info)
        
        record = {
            "timestamp": timestamp,
            "store_id": store.store_id,
            "store_type": store.store_type,
            "region": store.region,
            "brand": store.brand,
            "district": store.district,
            "address": store.address,
            "oid": metric_info["oid"],
            "metric_name": metric_name,
            "value": round(value, 2),
        }
        
        if self.extended_schema:
            record["store_manager"] = f"manager-{random.randint(100, 999)}"
            record["operating_hours"] = random.choice(["7am-10pm", "8am-9pm", "24hr", "9am-8pm"])
            record["last_remodel"] = (datetime.now(timezone.utc) - timedelta(days=random.randint(30, 1825))).strftime("%Y-%m-%d")
            record["square_footage"] = random.choice([5000, 10000, 25000, 50000, 100000])
            record["staff_count"] = random.randint(5, 100)
        
        return record
    
    def write_events_file(self, file_count: int):
        """Write a transaction events file"""
        timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")[:-3]
        filename = self.events_dir / f"events_{timestamp_str}_{file_count:06d}.txt"
        
        num_entries = random.randint(10, 50)
        
        try:
            with open(filename, 'w') as f:
                for _ in range(num_entries):
                    store = random.choice(self.stores)
                    log_entry = self.generate_transaction_event(store)
                    f.write(log_entry + "\n")
            
            logging.debug(f"Created events file: {filename} ({num_entries} entries)")
        except Exception as e:
            logging.error(f"Error writing events file {filename}: {e}")
    
    def write_metrics_file(self, file_count: int):
        """Write a store metrics file"""
        timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")[:-3]
        
        num_entries = random.randint(20, 100)
        records = []
        
        for _ in range(num_entries):
            store = random.choice(self.stores)
            metrics = self.generate_store_metrics(store)
            records.append(metrics)
        
        try:
            if self.metrics_gcs_client:
                filename = f"metrics_{timestamp_str}_{file_count:06d}.json"
                gcs_path = self.metrics_gcs_client.upload_json(filename, records)
                logging.debug(f"Uploaded to GCS: {gcs_path} ({num_entries} entries)")
            else:
                filename = self.metrics_dir / f"metrics_{timestamp_str}_{file_count:06d}.csv"
                with open(filename, 'w', newline='') as f:
                    if records:
                        fieldnames = list(records[0].keys())
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(records)
                logging.debug(f"Created metrics file: {filename} ({num_entries} entries)")
        except Exception as e:
            logging.error(f"Error writing metrics file: {e}")
    
    def run(self):
        """Main generation loop"""
        logging.info("Starting retail data generation...")
        logging.info(f"Events directory: {self.events_dir}")
        if self.metrics_gcs_client:
            logging.info(f"Metrics destination: GCS (gs://{self.metrics_gcs_client.bucket_name}/{self.metrics_gcs_client.prefix})")
        else:
            logging.info(f"Metrics directory: {self.metrics_dir}")
        logging.info("Press Ctrl+C to stop")
        
        events_count = 0
        metrics_count = 0
        last_events_time = time.time()
        last_metrics_time = time.time()
        
        try:
            while not shutdown_flag:
                current_time = time.time()
                
                if current_time - last_events_time >= self.events_interval:
                    self.write_events_file(events_count)
                    events_count += 1
                    last_events_time = current_time
                
                if current_time - last_metrics_time >= self.metrics_interval:
                    self.write_metrics_file(metrics_count)
                    metrics_count += 1
                    last_metrics_time = current_time
                
                total_files = events_count + metrics_count
                if total_files > 0 and total_files % 100 == 0:
                    logging.info(f"Generated {total_files} files (events: {events_count}, metrics: {metrics_count})")
                
                time.sleep(0.001)
        
        except KeyboardInterrupt:
            logging.info("Received interrupt signal")
        finally:
            logging.info(f"Shutting down. Total files generated: {events_count + metrics_count}")


def signal_handler(signum, frame):
    global shutdown_flag
    logging.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_flag = True


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic retail store performance data"
    )
    parser.add_argument("--events-dir", type=str, default="/sftp/retail/events",
                        help="Directory for transaction events (default: /sftp/retail/events)")
    parser.add_argument("--metrics-dir", type=str, default=None,
                        help="Directory for store metrics (local mode)")
    parser.add_argument("--metrics-gcs-bucket", type=str, default=None,
                        help="GCS bucket path for metrics (e.g., gs://bucket-name/metrics/)")
    parser.add_argument("--files-per-minute", type=int, default=1000,
                        help="Total files per minute (default: 1000)")
    parser.add_argument("--extended-schema", action="store_true",
                        help="Use extended schema with additional columns")
    parser.add_argument("--log-level", type=str, default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    
    args = parser.parse_args()
    
    if not args.metrics_gcs_bucket and not args.metrics_dir:
        args.metrics_dir = "/sftp/retail/metrics"
    
    if args.metrics_gcs_bucket and args.metrics_dir:
        logging.warning("Both GCS and local specified. Using GCS.")
        args.metrics_dir = None
    
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    generator = RetailDataGenerator(
        events_dir=args.events_dir,
        metrics_dir=args.metrics_dir,
        metrics_gcs_bucket=args.metrics_gcs_bucket,
        files_per_minute=args.files_per_minute,
        extended_schema=args.extended_schema
    )
    
    generator.run()


if __name__ == "__main__":
    main()

