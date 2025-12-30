#!/usr/bin/env python3
"""
Retail Store Performance Data Generator
Generates synthetic transaction events and operational metrics for retail analytics demo.

Supports two retail categories:
- Fast Food Chains with drive-through metrics
- Apparel Retail with fitting room and online pickup metrics

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
STORE_CATEGORIES = ["fast_food", "apparel"]

# Store types by category
FAST_FOOD_TYPES = ["drive_through", "dine_in", "express_counter", "food_court", "flagship_restaurant"]
APPAREL_TYPES = ["flagship", "mall", "outlet", "express"]

# Brands by category
FAST_FOOD_BRANDS = ["BurgerKing", "TimHortons", "QuickBite", "DriveThruExpress"]
APPAREL_BRANDS = ["Lululemon", "Arcteryx", "AthleticWear", "OutdoorGear"]

REGIONS = ["west-coast", "east-coast", "midwest", "southeast", "northeast", "southwest"]
STORE_COUNT = 100  # Total number of stores

# Apparel retail event types
APPAREL_EVENT_TYPES = {
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
    "FITTING_ROOM_WAIT": ["Customer waiting for fitting room - {wait_time}min", 4],
    "ONLINE_PICKUP_READY": ["Online order {order_id} ready for pickup", 6],
}

# Fast food event types
FAST_FOOD_EVENT_TYPES = {
    "TRANSACTION": ["Order completed ${amount:.2f} - Order #{order_num}", 6],
    "PAYMENT_DECLINED": ["Payment declined - {reason}", 4],
    "REFUND": ["Refund processed ${amount:.2f} - {reason}", 5],
    "ORDER_COMPLETE": ["Order {order_num} ready - {channel}", 6],
    "ORDER_DELAYED": ["Order {order_num} delayed - expected {delay}min", 4],
    "DRIVE_THROUGH_TIMEOUT": ["Drive-through lane {lane} timeout - {duration}sec", 3],
    "MOBILE_ORDER_READY": ["Mobile order {order_id} ready for pickup", 6],
    "INVENTORY_LOW": ["Ingredient {ingredient} below reorder point ({qty} remaining)", 4],
    "EQUIPMENT_FAILURE": ["Equipment failure - {equipment} at station {station}", 2],
    "WRONG_ORDER_REPORTED": ["Customer reported incorrect order - Order #{order_num}", 3],
    "FOOD_SAFETY_ALERT": ["Food safety alert - {issue} at station {station}", 2],
    "LOYALTY_SIGNUP": ["New loyalty member enrolled - {tier}", 6],
    "KIOSK_ERROR": ["Self-service kiosk {kiosk_id} error - {error_type}", 3],
}

# Apparel-specific metrics
APPAREL_METRICS = {
    "hourly_sales": {"oid": "1.3.6.1.4.1.1", "min": 500, "max": 25000, "anomaly_threshold": 22000},
    "transactions_per_hour": {"oid": "1.3.6.1.4.1.2", "min": 10, "max": 300, "anomaly_threshold": 280},
    "avg_basket_size": {"oid": "1.3.6.1.4.1.3", "min": 15, "max": 150, "anomaly_threshold": 130},
    "checkout_wait_time_sec": {"oid": "1.3.6.1.4.1.4", "min": 30, "max": 600, "anomaly_threshold": 300},
    "conversion_rate_pct": {"oid": "1.3.6.1.4.1.5", "min": 5, "max": 45, "anomaly_threshold": 40},
    "customer_traffic": {"oid": "1.3.6.1.4.1.6", "min": 50, "max": 2000, "anomaly_threshold": 1800},
    "return_rate_pct": {"oid": "1.3.6.1.4.1.7", "min": 0, "max": 15, "anomaly_threshold": 12},
    "staff_productivity": {"oid": "1.3.6.1.4.1.8", "min": 100, "max": 500, "anomaly_threshold": 450},
}

# Apparel extended metrics for schema evolution demo
APPAREL_EXTENDED_METRICS = {
    "fitting_room_usage_pct": {"oid": "1.3.6.1.4.1.9", "min": 10, "max": 85, "anomaly_threshold": 80},
    "online_pickup_rate_pct": {"oid": "1.3.6.1.4.1.10", "min": 5, "max": 40, "anomaly_threshold": 35},
}

# Fast food core metrics
FAST_FOOD_METRICS = {
    "hourly_sales": {"oid": "1.3.6.1.4.2.1", "min": 800, "max": 15000, "anomaly_threshold": 13500},
    "transactions_per_hour": {"oid": "1.3.6.1.4.2.2", "min": 20, "max": 250, "anomaly_threshold": 230},
    "avg_order_value": {"oid": "1.3.6.1.4.2.3", "min": 8, "max": 45, "anomaly_threshold": 40},
    "drive_through_wait_time_sec": {"oid": "1.3.6.1.4.2.4", "min": 90, "max": 480, "anomaly_threshold": 420},
    "drive_through_throughput_per_hour": {"oid": "1.3.6.1.4.2.5", "min": 15, "max": 120, "anomaly_threshold": 110},
    "drive_through_order_accuracy_pct": {"oid": "1.3.6.1.4.2.6", "min": 85, "max": 100, "anomaly_threshold": 88},
    "speaker_to_window_time_sec": {"oid": "1.3.6.1.4.2.7", "min": 45, "max": 300, "anomaly_threshold": 270},
    "kitchen_ticket_time_sec": {"oid": "1.3.6.1.4.2.8", "min": 120, "max": 600, "anomaly_threshold": 540},
    "customer_traffic": {"oid": "1.3.6.1.4.2.9", "min": 100, "max": 3000, "anomaly_threshold": 2800},
    "staff_productivity": {"oid": "1.3.6.1.4.2.10", "min": 80, "max": 350, "anomaly_threshold": 320},
}

# Fast food extended metrics for schema evolution demo
FAST_FOOD_EXTENDED_METRICS = {
    "peak_hour_service_time_sec": {"oid": "1.3.6.1.4.2.11", "min": 150, "max": 720, "anomaly_threshold": 650},
    "mobile_order_pickup_time_sec": {"oid": "1.3.6.1.4.2.12", "min": 60, "max": 240, "anomaly_threshold": 220},
    "dine_in_table_turnover_rate": {"oid": "1.3.6.1.4.2.13", "min": 1, "max": 8, "anomaly_threshold": 7},
    "food_waste_pct": {"oid": "1.3.6.1.4.2.14", "min": 2, "max": 25, "anomaly_threshold": 20},
}

# Global flag for graceful shutdown
shutdown_flag = False


@dataclass
class Store:
    """Represents a retail store"""
    store_id: str
    store_category: str  # fast_food or apparel
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
        extended_schema: bool = False,
        fast_food_ratio: float = 0.5
    ):
        self.events_dir = Path(events_dir)
        self.metrics_dir = Path(metrics_dir) if metrics_dir else None
        self.metrics_gcs_client = GCSClient(metrics_gcs_bucket) if metrics_gcs_bucket else None
        self.files_per_minute = files_per_minute
        self.extended_schema = extended_schema
        self.fast_food_ratio = fast_food_ratio
        self.stores = self._generate_stores()
        
        self.metrics_output_mode = "gcs" if metrics_gcs_bucket else "local"
        
        self.events_dir.mkdir(parents=True, exist_ok=True)
        if self.metrics_dir:
            self.metrics_dir.mkdir(parents=True, exist_ok=True)
        
        self.events_files_per_min = files_per_minute // 2
        self.metrics_files_per_min = files_per_minute // 2
        self.events_interval = 60.0 / self.events_files_per_min if self.events_files_per_min > 0 else 1.0
        self.metrics_interval = 60.0 / self.metrics_files_per_min if self.metrics_files_per_min > 0 else 1.0
        
        # Count stores by category
        fast_food_count = sum(1 for s in self.stores if s.store_category == "fast_food")
        apparel_count = sum(1 for s in self.stores if s.store_category == "apparel")
        
        logging.info(f"Initialized generator: {files_per_minute} files/min")
        logging.info(f"Events: {self.events_files_per_min} files/min -> local SFTP")
        logging.info(f"Metrics: {self.metrics_files_per_min} files/min -> {self.metrics_output_mode}")
        logging.info(f"Extended schema: {self.extended_schema}")
        logging.info(f"Fast food ratio: {self.fast_food_ratio:.2f}")
        logging.info(f"Generated {len(self.stores)} retail stores ({fast_food_count} fast food, {apparel_count} apparel)")
    
    def _generate_stores(self) -> List[Store]:
        """Generate a list of retail stores based on fast_food_ratio"""
        stores = []
        fast_food_count = int(STORE_COUNT * self.fast_food_ratio)
        apparel_count = STORE_COUNT - fast_food_count
        
        for i in range(STORE_COUNT):
            region = random.choice(REGIONS)
            district = f"district-{random.randint(1, 20)}"
            address = f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Cedar', 'Park', 'Market'])} {random.choice(['St', 'Ave', 'Blvd', 'Dr'])}"
            
            # Determine store category based on ratio
            if i < fast_food_count:
                store_category = "fast_food"
                store_type = random.choice(FAST_FOOD_TYPES)
                brand = random.choice(FAST_FOOD_BRANDS)
                store_id = f"store-{region[:4]}-ff-{i:03d}"
            else:
                store_category = "apparel"
                store_type = random.choice(APPAREL_TYPES)
                brand = random.choice(APPAREL_BRANDS)
                store_id = f"store-{region[:4]}-ap-{i:03d}"
            
            stores.append(Store(
                store_id=store_id,
                store_category=store_category,
                store_type=store_type,
                region=region,
                brand=brand,
                district=district,
                address=address
            ))
        
        return stores
    
    def generate_transaction_event(self, store: Store) -> str:
        """Generate RFC 5424 compliant transaction/event log based on store category"""
        facility = 16
        
        # Select event types based on store category
        event_types_pool = FAST_FOOD_EVENT_TYPES if store.store_category == "fast_food" else APPAREL_EVENT_TYPES
        event_type = random.choice(list(event_types_pool.keys()))
        message_template, default_severity = event_types_pool[event_type]
        
        if random.random() < 0.1:
            severity = random.choice([2, 3])
        else:
            severity = default_severity
        
        priority = (facility * 8) + severity
        version = 1
        
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        hostname = store.store_id
        
        # Structured data
        sd_id = "retail@32473"
        
        # Generate event-specific data based on category
        if store.store_category == "fast_food":
            sd, msg, app_name = self._generate_fast_food_event(event_type, message_template, store, sd_id)
        else:  # apparel
            sd, msg, app_name = self._generate_apparel_event(event_type, message_template, store, sd_id)
        
        procid = str(random.randint(1000, 9999))
        msgid = event_type
        
        return f"<{priority}>{version} {timestamp} {hostname} {app_name} {procid} {msgid} {sd} {msg}"
    
    def _generate_apparel_event(self, event_type: str, message_template: str, store: Store, sd_id: str) -> tuple:
        """Generate apparel-specific event data"""
        terminal = f"POS-{random.randint(1, 12):02d}"
        
        if event_type == "TRANSACTION":
            amount = random.uniform(15, 500)
            items = random.randint(1, 15)
            sd = f'[{sd_id} terminal="{terminal}" payment="{random.choice(["credit", "debit", "cash", "mobile"])}" items="{items}" storeCategory="apparel"]'
            msg = message_template.format(amount=amount, items=items)
            app_name = "POS"
        elif event_type == "PAYMENT_DECLINED":
            reason = random.choice(["insufficient_funds", "expired_card", "fraud_alert", "network_error"])
            sd = f'[{sd_id} terminal="{terminal}" reason="{reason}" storeCategory="apparel"]'
            msg = message_template.format(reason=reason)
            app_name = "PAYMENT"
        elif event_type == "REFUND":
            amount = random.uniform(20, 400)
            reason = random.choice(["defective", "wrong_size", "changed_mind", "price_match"])
            sd = f'[{sd_id} terminal="{terminal}" amount="{amount:.2f}" storeCategory="apparel"]'
            msg = message_template.format(amount=amount, reason=reason)
            app_name = "POS"
        elif event_type in ["INVENTORY_LOW", "STOCKOUT"]:
            sku = f"{random.randint(10000, 99999)}"
            qty = random.randint(0, 10) if event_type == "INVENTORY_LOW" else 0
            sd = f'[{sd_id} sku="{sku}" category="{random.choice(["tops", "bottoms", "footwear", "accessories"])}" storeCategory="apparel"]'
            msg = message_template.format(sku=sku, qty=qty)
            app_name = "INVENTORY"
        elif event_type == "THEFT_ALERT":
            sku = f"{random.randint(10000, 99999)}"
            sd = f'[{sd_id} sku="{sku}" exit="{random.choice(["main", "side", "emergency"])}" storeCategory="apparel"]'
            msg = message_template.format(sku=sku)
            app_name = "SECURITY"
        elif event_type == "POS_ERROR":
            error_type = random.choice(["printer_jam", "scanner_fail", "network_timeout", "cash_drawer_stuck"])
            sd = f'[{sd_id} terminal="{terminal}" error="{error_type}" storeCategory="apparel"]'
            msg = message_template.format(terminal=terminal, error_type=error_type)
            app_name = "POS"
        elif event_type == "FITTING_ROOM_WAIT":
            wait_time = random.randint(5, 30)
            sd = f'[{sd_id} waitTime="{wait_time}" customerCount="{random.randint(1, 8)}" storeCategory="apparel"]'
            msg = message_template.format(wait_time=wait_time)
            app_name = "FITTING_ROOM"
        elif event_type == "ONLINE_PICKUP_READY":
            order_id = f"WEB{random.randint(100000, 999999)}"
            sd = f'[{sd_id} orderId="{order_id}" pickupLocation="{random.choice(["front", "back", "curbside"])}" storeCategory="apparel"]'
            msg = message_template.format(order_id=order_id)
            app_name = "ONLINE_PICKUP"
        elif event_type == "CHECKOUT_TIMEOUT":
            sd = f'[{sd_id} terminal="{terminal}" storeCategory="apparel"]'
            msg = message_template.format(terminal=terminal)
            app_name = "POS"
        elif event_type == "PRICE_OVERRIDE":
            sku = f"{random.randint(10000, 99999)}"
            reason = random.choice(["manager_approval", "price_match", "promotion", "loyalty_discount"])
            sd = f'[{sd_id} terminal="{terminal}" sku="{sku}" storeCategory="apparel"]'
            msg = message_template.format(sku=sku, reason=reason)
            app_name = "POS"
        else:  # LOYALTY_SIGNUP
            tier = random.choice(["silver", "gold", "platinum"])
            sd = f'[{sd_id} terminal="{terminal}" tier="{tier}" storeCategory="apparel"]'
            msg = message_template.format(tier=tier)
            app_name = "LOYALTY"
        
        return sd, msg, app_name
    
    def _generate_fast_food_event(self, event_type: str, message_template: str, store: Store, sd_id: str) -> tuple:
        """Generate fast food-specific event data"""
        terminal = f"POS-{random.randint(1, 8):02d}"
        
        if event_type == "TRANSACTION":
            amount = random.uniform(8, 75)
            order_num = random.randint(100, 999)
            sd = f'[{sd_id} terminal="{terminal}" payment="{random.choice(["credit", "debit", "cash", "mobile"])}" orderNum="{order_num}" storeCategory="fast_food"]'
            msg = message_template.format(amount=amount, order_num=order_num)
            app_name = "POS"
        elif event_type == "PAYMENT_DECLINED":
            reason = random.choice(["insufficient_funds", "expired_card", "fraud_alert", "network_error"])
            sd = f'[{sd_id} terminal="{terminal}" reason="{reason}" storeCategory="fast_food"]'
            msg = message_template.format(reason=reason)
            app_name = "PAYMENT"
        elif event_type == "REFUND":
            amount = random.uniform(5, 50)
            reason = random.choice(["wrong_order", "food_quality", "wait_time", "missing_items"])
            sd = f'[{sd_id} terminal="{terminal}" amount="{amount:.2f}" storeCategory="fast_food"]'
            msg = message_template.format(amount=amount, reason=reason)
            app_name = "POS"
        elif event_type == "ORDER_COMPLETE":
            order_num = random.randint(100, 999)
            channel = random.choice(["drive_through", "counter", "kiosk", "mobile_app"])
            sd = f'[{sd_id} orderNum="{order_num}" channel="{channel}" storeCategory="fast_food"]'
            msg = message_template.format(order_num=order_num, channel=channel)
            app_name = "KITCHEN"
        elif event_type == "ORDER_DELAYED":
            order_num = random.randint(100, 999)
            delay = random.randint(5, 20)
            sd = f'[{sd_id} orderNum="{order_num}" delayMinutes="{delay}" storeCategory="fast_food"]'
            msg = message_template.format(order_num=order_num, delay=delay)
            app_name = "KITCHEN"
        elif event_type == "DRIVE_THROUGH_TIMEOUT":
            lane = random.randint(1, 2)
            duration = random.randint(300, 600)
            sd = f'[{sd_id} lane="{lane}" duration="{duration}" storeCategory="fast_food"]'
            msg = message_template.format(lane=lane, duration=duration)
            app_name = "DRIVE_THROUGH"
        elif event_type == "MOBILE_ORDER_READY":
            order_id = f"MOB{random.randint(100000, 999999)}"
            sd = f'[{sd_id} orderId="{order_id}" pickupSpot="{random.randint(1, 8)}" storeCategory="fast_food"]'
            msg = message_template.format(order_id=order_id)
            app_name = "MOBILE_ORDER"
        elif event_type == "INVENTORY_LOW":
            ingredient = random.choice(["beef_patties", "buns", "lettuce", "tomatoes", "cheese", "fries", "drinks", "cups"])
            qty = random.randint(5, 50)
            sd = f'[{sd_id} ingredient="{ingredient}" quantity="{qty}" storeCategory="fast_food"]'
            msg = message_template.format(ingredient=ingredient, qty=qty)
            app_name = "INVENTORY"
        elif event_type == "EQUIPMENT_FAILURE":
            equipment = random.choice(["fryer", "grill", "ice_machine", "drink_dispenser", "freezer", "oven"])
            station = random.choice(["prep", "cook", "beverage", "assembly"])
            sd = f'[{sd_id} equipment="{equipment}" station="{station}" storeCategory="fast_food"]'
            msg = message_template.format(equipment=equipment, station=station)
            app_name = "EQUIPMENT"
        elif event_type == "WRONG_ORDER_REPORTED":
            order_num = random.randint(100, 999)
            sd = f'[{sd_id} orderNum="{order_num}" issue="{random.choice(["missing_item", "wrong_item", "incorrect_customization"])}" storeCategory="fast_food"]'
            msg = message_template.format(order_num=order_num)
            app_name = "CUSTOMER_SERVICE"
        elif event_type == "FOOD_SAFETY_ALERT":
            issue = random.choice(["temp_violation", "expired_product", "contamination_risk", "cleaning_required"])
            station = random.choice(["prep", "cook", "storage", "assembly"])
            sd = f'[{sd_id} issue="{issue}" station="{station}" storeCategory="fast_food"]'
            msg = message_template.format(issue=issue, station=station)
            app_name = "FOOD_SAFETY"
        elif event_type == "KIOSK_ERROR":
            kiosk_id = f"KIOSK-{random.randint(1, 6):02d}"
            error_type = random.choice(["payment_reader_fail", "screen_unresponsive", "printer_jam", "network_error"])
            sd = f'[{sd_id} kioskId="{kiosk_id}" error="{error_type}" storeCategory="fast_food"]'
            msg = message_template.format(kiosk_id=kiosk_id, error_type=error_type)
            app_name = "KIOSK"
        else:  # LOYALTY_SIGNUP
            tier = random.choice(["basic", "premium", "vip"])
            sd = f'[{sd_id} terminal="{terminal}" tier="{tier}" storeCategory="fast_food"]'
            msg = message_template.format(tier=tier)
            app_name = "LOYALTY"
        
        return sd, msg, app_name
    
    def _generate_metric_value(self, metric_info: dict) -> float:
        """Generate a metric value with occasional anomalies"""
        if random.random() < 0.05:
            return random.uniform(metric_info["anomaly_threshold"], metric_info["max"])
        
        mid = (metric_info["min"] + metric_info["max"]) / 2
        std = (metric_info["max"] - metric_info["min"]) / 6
        value = random.gauss(mid, std)
        return max(metric_info["min"], min(metric_info["max"], value))
    
    def generate_store_metrics(self, store: Store) -> dict:
        """Generate operational metrics for a store based on category"""
        timestamp = datetime.now(timezone.utc).isoformat()
        
        # Select metrics based on store category
        if store.store_category == "fast_food":
            metrics_pool = FAST_FOOD_METRICS.copy()
            if self.extended_schema:
                metrics_pool.update(FAST_FOOD_EXTENDED_METRICS)
        else:  # apparel
            metrics_pool = APPAREL_METRICS.copy()
            if self.extended_schema:
                metrics_pool.update(APPAREL_EXTENDED_METRICS)
        
        metric_name = random.choice(list(metrics_pool.keys()))
        metric_info = metrics_pool[metric_name]
        
        value = self._generate_metric_value(metric_info)
        
        record = {
            "timestamp": timestamp,
            "store_id": store.store_id,
            "store_category": store.store_category,
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
            if store.store_category == "fast_food":
                record["operating_hours"] = random.choice(["6am-11pm", "6am-midnight", "24hr", "7am-10pm"])
                record["seating_capacity"] = random.choice([30, 50, 75, 100, 150])
                record["has_drive_through"] = store.store_type == "drive_through"
            else:  # apparel
                record["operating_hours"] = random.choice(["9am-9pm", "10am-8pm", "10am-9pm", "11am-7pm"])
                record["square_footage"] = random.choice([3000, 5000, 10000, 25000, 50000])
            record["last_remodel"] = (datetime.now(timezone.utc) - timedelta(days=random.randint(30, 1825))).strftime("%Y-%m-%d")
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
        description="Generate synthetic retail store performance data for fast food and apparel retail"
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
    parser.add_argument("--fast-food-ratio", type=float, default=0.5,
                        help="Ratio of fast food stores to total stores (0.0-1.0, default: 0.5)")
    parser.add_argument("--log-level", type=str, default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    
    args = parser.parse_args()
    
    # Validate fast-food-ratio
    if not 0.0 <= args.fast_food_ratio <= 1.0:
        parser.error("--fast-food-ratio must be between 0.0 and 1.0")
    
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
        extended_schema=args.extended_schema,
        fast_food_ratio=args.fast_food_ratio
    )
    
    generator.run()


if __name__ == "__main__":
    main()

