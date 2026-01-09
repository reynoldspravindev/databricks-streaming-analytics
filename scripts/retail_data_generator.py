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
from typing import List, Optional, Tuple, Dict

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

# Configuration
STORE_CATEGORIES = ["fast_food", "apparel"]

# Store types by category
FAST_FOOD_TYPES = ["drive_through", "dine_in", "express_counter", "food_court", "flagship_restaurant"]
APPAREL_TYPES = ["flagship", "mall", "outlet", "express"]

# Brands by category
FAST_FOOD_BRANDS = ["BurgerKing", "TimHortons", "Popeyes", "QSRExpress"]
APPAREL_BRANDS = ["Lululemon", "Arcteryx", "AthleticWear", "OutdoorGear"]

REGIONS = ["west-coast", "east-coast", "midwest", "southeast", "northeast", "southwest"]
STORE_COUNT = 2000  # Total number of stores

# Geographic coordinate ranges by region (for realistic store locations)
REGION_COORDINATES = {
    "west-coast": {
        "lat_range": (32.5, 49.0),
        "lon_range": (-124.5, -117.0),
        "major_cities": [
            (34.05, -118.24, 0.30),  # Los Angeles (30% of stores)
            (37.77, -122.41, 0.25),  # San Francisco (25%)
            (32.72, -117.16, 0.15),  # San Diego (15%)
            (45.52, -122.68, 0.10),  # Portland (10%)
            (47.61, -122.33, 0.10),  # Seattle (10%)
            (38.58, -121.49, 0.10),  # Sacramento/others (10%)
        ]
    },
    "east-coast": {
        "lat_range": (25.8, 47.5),
        "lon_range": (-80.5, -67.0),
        "major_cities": [
            (40.71, -74.01, 0.30),   # New York City (30%)
            (25.76, -80.19, 0.20),   # Miami (20%)
            (42.36, -71.06, 0.15),   # Boston (15%)
            (39.95, -75.17, 0.15),   # Philadelphia (15%)
            (38.90, -77.04, 0.10),   # Washington DC (10%)
            (33.75, -84.39, 0.10),   # Atlanta (10%)
        ]
    },
    "midwest": {
        "lat_range": (37.0, 49.0),
        "lon_range": (-104.0, -80.5),
        "major_cities": [
            (41.88, -87.63, 0.35),   # Chicago (35%)
            (39.74, -104.99, 0.15),  # Denver (15%)
            (39.10, -94.58, 0.12),   # Kansas City (12%)
            (44.98, -93.27, 0.12),   # Minneapolis (12%)
            (39.96, -83.00, 0.13),   # Columbus (13%)
            (42.33, -83.05, 0.13),   # Detroit (13%)
        ]
    },
    "southeast": {
        "lat_range": (24.5, 39.5),
        "lon_range": (-92.0, -75.5),
        "major_cities": [
            (33.75, -84.39, 0.30),   # Atlanta (30%)
            (35.23, -80.84, 0.15),   # Charlotte (15%)
            (36.17, -86.78, 0.12),   # Nashville (12%)
            (30.27, -81.88, 0.10),   # Jacksonville (10%)
            (29.76, -95.37, 0.13),   # Houston (13%)
            (32.78, -79.93, 0.10),   # Charleston (10%)
            (35.96, -83.92, 0.10),   # Knoxville (10%)
        ]
    },
    "northeast": {
        "lat_range": (40.0, 47.5),
        "lon_range": (-79.5, -67.0),
        "major_cities": [
            (40.71, -74.01, 0.30),   # NYC Metro (30%)
            (42.36, -71.06, 0.20),   # Boston (20%)
            (40.44, -79.99, 0.15),   # Pittsburgh (15%)
            (42.88, -78.88, 0.12),   # Buffalo (12%)
            (43.08, -70.76, 0.12),   # Portland ME (12%)
            (41.82, -71.42, 0.11),   # Providence (11%)
        ]
    },
    "southwest": {
        "lat_range": (25.8, 42.0),
        "lon_range": (-115.0, -94.5),
        "major_cities": [
            (29.76, -95.37, 0.25),   # Houston (25%)
            (29.42, -98.49, 0.15),   # San Antonio (15%)
            (32.78, -96.80, 0.18),   # Dallas (18%)
            (33.45, -112.07, 0.15),  # Phoenix (15%)
            (35.08, -106.65, 0.10),  # Albuquerque (10%)
            (30.27, -97.74, 0.10),   # Austin (10%)
            (36.11, -115.17, 0.07),  # Las Vegas (7%)
        ]
    }
}

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
    # Hardware-specific events (HME Drive-Thru Timer System)
    "LOOP_SENSOR_MALFUNCTION": ["Loop sensor {location} reporting intermittent - {issue}", 3],
    "KDS_SCREEN_DOWN": ["KDS display {screen_id} offline - manual backup mode", 2],
    "POS_NETWORK_TIMEOUT": ["POS unable to reach payment processor - {duration}sec delay", 3],
    "BUMP_BAR_STUCK": ["KDS bump bar {station} unresponsive - manual timer override", 3],
    "HME_SYSTEM_REBOOT": ["Drive-thru timer system restarted - data gap {duration}min", 2],
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

# Fast food core metrics (HME Drive-Thru Timer System)
FAST_FOOD_METRICS = {
    # Financial Metrics (POS Source)
    "hourly_sales": {
        "oid": "1.3.6.1.4.2.1",
        "min": 800,
        "max": 15000,
        "anomaly_threshold": 13500,
        "polarity": "positive",
        "source": "POS"
    },
    "transactions_per_hour": {
        "oid": "1.3.6.1.4.2.2",
        "min": 20,
        "max": 250,
        "anomaly_threshold": 230,
        "polarity": "positive",
        "source": "POS"
    },
    "avg_order_value": {
        "oid": "1.3.6.1.4.2.3",
        "min": 8,
        "max": 45,
        "anomaly_threshold": 40,
        "polarity": "positive",
        "source": "POS"
    },
    
    # Drive-Through Speed of Service KPIs (HME Timer System)
    "drive_through_total_experience_time_sec": {
        "oid": "1.3.6.1.4.2.4",
        "min": 210,
        "max": 420,
        "anomaly_threshold": 390,
        "polarity": "negative",
        "benchmark": 270,
        "source": "LOOP_INTEGRATED",
        "normalize_by_order": True,
        "description": "Loop A trigger (arrival at speaker) to Loop B departure (leaving window)"
    },
    "drive_through_menu_board_time_sec": {
        "oid": "1.3.6.1.4.2.5",
        "min": 15,
        "max": 90,
        "anomaly_threshold": 75,
        "polarity": "negative",
        "benchmark": 25,
        "source": "LOOP_A",
        "normalize_by_order": True,
        "description": "Loop A arrival to Loop A departure (ordering time)"
    },
    "drive_through_window_time_sec": {
        "oid": "1.3.6.1.4.2.6",
        "min": 30,
        "max": 180,
        "anomaly_threshold": 150,
        "polarity": "negative",
        "benchmark": 50,
        "source": "LOOP_B",
        "normalize_by_order": True,
        "description": "Loop B arrival to Loop B departure (window service time)"
    },
    "drive_through_kds_bump_time_sec": {
        "oid": "1.3.6.1.4.2.7",
        "min": 25,
        "max": 120,
        "anomaly_threshold": 100,
        "polarity": "negative",
        "benchmark": 40,
        "source": "KDS_BUMP",
        "normalize_by_order": True,
        "description": "KDS order display to bump bar press (kitchen production)"
    },
    "drive_through_cars_per_hour": {
        "oid": "1.3.6.1.4.2.8",
        "min": 20,
        "max": 150,
        "anomaly_threshold": 140,
        "polarity": "positive",
        "benchmark": 100,
        "source": "LOOP_INTEGRATED",
        "normalize_by_order": False,
        "description": "Count of cars completing drive-through in 60-min block"
    },
    "drive_through_order_accuracy_pct": {
        "oid": "1.3.6.1.4.2.9",
        "min": 85,
        "max": 100,
        "anomaly_threshold": 88,
        "polarity": "positive",
        "source": "POS_INTEGRATED",
        "normalize_by_order": False
    },
    
    # Additional operational metrics
    "customer_traffic": {
        "oid": "1.3.6.1.4.2.10",
        "min": 100,
        "max": 3000,
        "anomaly_threshold": 2800,
        "polarity": "positive",
        "source": "LOOP_A"
    },
    "staff_productivity": {
        "oid": "1.3.6.1.4.2.11",
        "min": 80,
        "max": 350,
        "anomaly_threshold": 320,
        "polarity": "positive",
        "source": "POS_INTEGRATED"
    },
    "peak_hour_service_time_sec": {
        "oid": "1.3.6.1.4.2.12",
        "min": 150,
        "max": 720,
        "anomaly_threshold": 650,
        "polarity": "negative",
        "source": "LOOP_INTEGRATED"
    },
    "mobile_order_pickup_time_sec": {
        "oid": "1.3.6.1.4.2.13",
        "min": 60,
        "max": 240,
        "anomaly_threshold": 220,
        "polarity": "negative",
        "source": "POS_INTEGRATED"
    },
}

# Fast food extended metrics for schema evolution demo
FAST_FOOD_EXTENDED_METRICS = {
    "dine_in_table_turnover_rate": {
        "oid": "1.3.6.1.4.2.14",
        "min": 1,
        "max": 8,
        "anomaly_threshold": 7,
        "polarity": "positive",
        "source": "POS_INTEGRATED"
    },
    "food_waste_pct": {
        "oid": "1.3.6.1.4.2.15",
        "min": 2,
        "max": 25,
        "anomaly_threshold": 20,
        "polarity": "negative",
        "source": "POS_INTEGRATED"
    }
}

# Event-to-Metric Impact Mappings (Fast Food Only)
FAST_FOOD_EVENT_IMPACTS = {
    # Hardware failures
    "LOOP_SENSOR_MALFUNCTION": {
        "drive_through_total_experience_time_sec": 0.40,
        "drive_through_cars_per_hour": -0.25,
    },
    "KDS_SCREEN_DOWN": {
        "drive_through_kds_bump_time_sec": 0.80,
        "drive_through_total_experience_time_sec": 0.60,
        "drive_through_order_accuracy_pct": -0.15,
    },
    "POS_NETWORK_TIMEOUT": {
        "drive_through_window_time_sec": 0.70,
        "drive_through_total_experience_time_sec": 0.50,
        "transactions_per_hour": -0.30,
    },
    "BUMP_BAR_STUCK": {
        "drive_through_kds_bump_time_sec": 0.50,
        "drive_through_window_time_sec": 0.30,
    },
    "HME_SYSTEM_REBOOT": {
        "drive_through_total_experience_time_sec": 0.90,
        "drive_through_cars_per_hour": -0.60,
    },
    
    # Operational failures
    "EQUIPMENT_FAILURE": {
        "drive_through_kds_bump_time_sec": 0.70,
        "drive_through_total_experience_time_sec": 0.60,
        "drive_through_window_time_sec": 0.40,
        "drive_through_cars_per_hour": -0.40,
    },
    "INVENTORY_LOW": {
        "drive_through_menu_board_time_sec": 0.40,
        "drive_through_kds_bump_time_sec": 0.30,
        "hourly_sales": -0.25,
        "customer_traffic": -0.10,
    },
    "FOOD_SAFETY_ALERT": {
        "hourly_sales": -0.40,
        "customer_traffic": -0.30,
        "drive_through_cars_per_hour": -0.35,
    },
    "WRONG_ORDER_REPORTED": {
        "drive_through_order_accuracy_pct": -0.10,
        "drive_through_window_time_sec": 0.25,
    },
    "DRIVE_THROUGH_TIMEOUT": {
        "drive_through_total_experience_time_sec": 0.35,
        "drive_through_window_time_sec": 0.40,
        "drive_through_cars_per_hour": -0.25,
    },
    "KIOSK_ERROR": {
        "transactions_per_hour": -0.15,
        "customer_traffic": -0.08,
    },
    "ORDER_DELAYED": {
        "drive_through_kds_bump_time_sec": 0.30,
        "drive_through_total_experience_time_sec": 0.25,
    },
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
    performance_tier: Optional[str] = None  # excellent, good, average, poor (fast_food only)
    latitude: Optional[float] = None  # Geographic latitude
    longitude: Optional[float] = None  # Geographic longitude


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
        
        # Track active events per store (fast food only)
        self.store_event_state: Dict[str, List[Tuple[str, datetime, int]]] = {}
        self.event_window_minutes = 15
        
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
        if HAS_NUMPY:
            logging.info("NumPy available: Enhanced realistic data generation enabled for fast food stores")
        else:
            logging.warning("NumPy not available: Using simple data generation for all stores")
    
    def _generate_coordinates(self, region: str) -> Tuple[float, float]:
        """
        Generate realistic lat/lon coordinates for a store in a region.
        Uses major city clustering (80% near cities, 20% distributed).
        """
        region_data = REGION_COORDINATES[region]
        
        # 80% of stores cluster near major cities
        if random.random() < 0.80:
            # Choose a major city based on weights
            cities = region_data["major_cities"]
            city_weights = [city[2] for city in cities]
            chosen_city = random.choices(cities, weights=city_weights)[0]
            
            city_lat, city_lon, _ = chosen_city
            
            # Add random offset around the city (normally distributed)
            # Standard deviation of ~0.15 degrees (~10 miles) for suburban spread
            lat = random.gauss(city_lat, 0.15)
            lon = random.gauss(city_lon, 0.15)
        else:
            # 20% distributed across the region (rural/small towns)
            lat = random.uniform(*region_data["lat_range"])
            lon = random.uniform(*region_data["lon_range"])
        
        # Clamp to region bounds
        lat = max(region_data["lat_range"][0], min(region_data["lat_range"][1], lat))
        lon = max(region_data["lon_range"][0], min(region_data["lon_range"][1], lon))
        
        # Round to 6 decimal places (~0.1 meter precision)
        return round(lat, 6), round(lon, 6)
    
    def _generate_stores(self) -> List[Store]:
        """Generate a list of retail stores based on fast_food_ratio"""
        stores = []
        fast_food_count = int(STORE_COUNT * self.fast_food_ratio)
        apparel_count = STORE_COUNT - fast_food_count
        
        for i in range(STORE_COUNT):
            region = random.choice(REGIONS)
            district = f"district-{random.randint(1, 20)}"
            address = f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Cedar', 'Park', 'Market'])} {random.choice(['St', 'Ave', 'Blvd', 'Dr'])}"
            
            # Generate geographic coordinates for the store
            latitude, longitude = self._generate_coordinates(region)
            
            # Determine store category based on ratio
            if i < fast_food_count:
                store_category = "fast_food"
                store_type = random.choice(FAST_FOOD_TYPES)
                brand = random.choice(FAST_FOOD_BRANDS)
                store_id = f"store-{region[:4]}-ff-{i:03d}"
                
                # Assign performance tier for fast food stores
                # Excellent: Taco Bell-level (4:15 OTD, ~100+ cars/hr)
                # Good: Above average (4:45 OTD)
                # Average: Industry median (5:15 OTD)
                # Poor: Burger King historical (~6:00 OTD)
                performance_tier = random.choices(
                    ["excellent", "good", "average", "poor"],
                    weights=[0.20, 0.40, 0.30, 0.10]
                )[0]
            else:
                store_category = "apparel"
                store_type = random.choice(APPAREL_TYPES)
                brand = random.choice(APPAREL_BRANDS)
                store_id = f"store-{region[:4]}-ap-{i:03d}"
                performance_tier = None  # No profile for apparel
            
            stores.append(Store(
                store_id=store_id,
                store_category=store_category,
                store_type=store_type,
                region=region,
                brand=brand,
                district=district,
                address=address,
                performance_tier=performance_tier,
                latitude=latitude,
                longitude=longitude
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
        elif event_type == "LOOP_SENSOR_MALFUNCTION":
            location = random.choice(["speaker_post", "pickup_window", "pre_menu"])
            issue = random.choice(["intermittent_trigger", "no_signal", "false_positives", "timing_drift"])
            sd = f'[{sd_id} location="{location}" issue="{issue}" storeCategory="fast_food"]'
            msg = message_template.format(location=location, issue=issue)
            app_name = "HME_SYSTEM"
        elif event_type == "KDS_SCREEN_DOWN":
            screen_id = f"KDS-{random.randint(1, 4):02d}"
            sd = f'[{sd_id} screenId="{screen_id}" storeCategory="fast_food"]'
            msg = message_template.format(screen_id=screen_id)
            app_name = "KDS_SYSTEM"
        elif event_type == "POS_NETWORK_TIMEOUT":
            duration = random.randint(5, 45)
            sd = f'[{sd_id} duration="{duration}" storeCategory="fast_food"]'
            msg = message_template.format(duration=duration)
            app_name = "POS_NETWORK"
        elif event_type == "BUMP_BAR_STUCK":
            station = random.choice(["grill", "fry", "assembly", "beverage"])
            sd = f'[{sd_id} station="{station}" storeCategory="fast_food"]'
            msg = message_template.format(station=station)
            app_name = "KDS_SYSTEM"
        elif event_type == "HME_SYSTEM_REBOOT":
            duration = random.randint(2, 10)
            sd = f'[{sd_id} duration="{duration}" storeCategory="fast_food"]'
            msg = message_template.format(duration=duration)
            app_name = "HME_SYSTEM"
        elif event_type == "LOYALTY_SIGNUP":
            tier = random.choice(["basic", "premium", "vip"])
            sd = f'[{sd_id} terminal="{terminal}" tier="{tier}" storeCategory="fast_food"]'
            msg = message_template.format(tier=tier)
            app_name = "LOYALTY"
        else:
            # Fallback for any unhandled event type
            sd = f'[{sd_id} terminal="{terminal}" storeCategory="fast_food"]'
            msg = message_template
            app_name = "SYSTEM"
        
        return sd, msg, app_name
    
    def _generate_metric_value(self, metric_info: dict, store: Store, hour: int) -> float:
        """Generate a metric value - routes to enhanced or simple generation"""
        # Check if this is fast food with enhanced generation enabled
        if store.store_category == "fast_food" and "polarity" in metric_info and HAS_NUMPY:
            return self._generate_realistic_value(metric_info, store, hour)
        else:
            # Use existing simple generation for apparel or if numpy not available
            if random.random() < 0.05:
                return random.uniform(metric_info["anomaly_threshold"], metric_info["max"])
            
            mid = (metric_info["min"] + metric_info["max"]) / 2
            std = (metric_info["max"] - metric_info["min"]) / 6
            value = random.gauss(mid, std)
            return max(metric_info["min"], min(metric_info["max"], value))
    
    def _calculate_order_complexity_factor(self, order_value: float, item_count: int) -> float:
        """
        Calculate timing adjustment factor based on order size.
        
        Industry Rule:
        - Small order ($8-15, 1-2 items): Baseline (1.0x)
        - Medium order ($15-30, 3-5 items): +20% time allowance (1.2x)
        - Large order ($30-50, 6-10 items): +50% time allowance (1.5x)
        - Mega order ($50+, 10+ items): +80% time allowance (1.8x)
        """
        if order_value < 15:
            return 1.0
        elif order_value < 30:
            return 1.2
        elif order_value < 50:
            return 1.5
        else:
            return 1.8
    
    def _get_time_multiplier(self, hour: int, metric_polarity: str, store_category: str) -> float:
        """Get time-of-day multiplier - only applies to fast_food"""
        if store_category != "fast_food":
            return 1.0
        
        # Drive-through peak patterns
        # Lunch Rush: 11 AM - 1 PM (worst performance)
        if hour in [11, 12]:
            return 1.5 if metric_polarity == "negative" else 1.4
        # Post-lunch: 1-2 PM
        elif hour == 13:
            return 1.3 if metric_polarity == "negative" else 1.2
        # Dinner Rush: 5 PM - 7 PM
        elif hour in [17, 18]:
            return 1.4 if metric_polarity == "negative" else 1.3
        # Post-dinner: 7-8 PM
        elif hour == 19:
            return 1.2 if metric_polarity == "negative" else 1.1
        # Off-peak: 2-4 PM, 8-10 PM
        elif hour in [14, 15, 16, 20, 21]:
            return 0.8 if metric_polarity == "negative" else 0.9
        # Late night/early morning (if 24hr)
        elif hour in [22, 23, 0, 1, 2, 3, 4, 5, 6]:
            return 0.6 if metric_polarity == "negative" else 0.7
        # Breakfast: 7-10 AM (moderate)
        elif hour in [7, 8, 9, 10]:
            return 1.1 if metric_polarity == "negative" else 1.05
        else:
            return 1.0
    
    def _generate_realistic_value(self, metric_info: dict, store: Store, hour: int) -> float:
        """Generate realistic value using beta distribution for fast food"""
        if not HAS_NUMPY:
            # Fallback to simple generation
            mid = (metric_info["min"] + metric_info["max"]) / 2
            std = (metric_info["max"] - metric_info["min"]) / 6
            value = random.gauss(mid, std)
            return max(metric_info["min"], min(metric_info["max"], value))
        
        polarity = metric_info.get("polarity", "positive")
        tier = store.performance_tier if store.performance_tier else "average"
        
        # Map tier to beta distribution parameters
        if polarity == "negative":  # Lower is better
            alpha_beta_map = {
                "excellent": (2, 8),   # Skew toward low values
                "good": (3, 6),
                "average": (5, 5),     # Symmetric
                "poor": (8, 2)         # Skew toward high values
            }
        else:  # Higher is better
            alpha_beta_map = {
                "excellent": (8, 2),   # Skew toward high values
                "good": (6, 3),
                "average": (5, 5),
                "poor": (2, 8)         # Skew toward low values
            }
        
        alpha, beta = alpha_beta_map[tier]
        
        # 5% chance of anomaly
        if random.random() < 0.05:
            percentile = np.random.beta(alpha/2, beta/2)  # More extreme distribution
        else:
            percentile = np.random.beta(alpha, beta)
        
        base_value = metric_info["min"] + percentile * (metric_info["max"] - metric_info["min"])
        
        # Apply time multiplier
        time_mult = self._get_time_multiplier(hour, polarity, store.store_category)
        
        if polarity == "negative":
            value = base_value * time_mult
        else:
            value = base_value * time_mult
        
        return max(metric_info["min"], min(metric_info["max"], value))
    
    def _track_event(self, store_id: str, event_type: str, severity: int, timestamp: datetime):
        """Track event for causality (fast food only)"""
        if store_id not in self.store_event_state:
            self.store_event_state[store_id] = []
        
        self.store_event_state[store_id].append((event_type, timestamp, severity))
        
        # Clean old events (> 15 minutes)
        cutoff_time = timestamp - timedelta(minutes=self.event_window_minutes)
        self.store_event_state[store_id] = [
            (et, ts, sev) for et, ts, sev in self.store_event_state[store_id]
            if ts > cutoff_time
        ]
    
    def _get_active_events(self, store_id: str, current_time: datetime) -> List[Tuple[str, datetime, int]]:
        """Get active events for a store"""
        if store_id not in self.store_event_state:
            return []
        
        cutoff_time = current_time - timedelta(minutes=self.event_window_minutes)
        return [
            (et, ts, sev) for et, ts, sev in self.store_event_state[store_id]
            if ts > cutoff_time
        ]
    
    def _apply_event_impacts(self, metric_name: str, base_value: float, store: Store, current_time: datetime) -> float:
        """Apply event impacts to metric value (fast food only)"""
        
        # Only apply to fast food stores
        if store.store_category != "fast_food":
            return base_value
        
        active_events = self._get_active_events(store.store_id, current_time)
        if not active_events:
            return base_value
        
        impact_factor = 1.0
        
        for event_type, event_time, severity in active_events:
            if event_type in FAST_FOOD_EVENT_IMPACTS:
                metric_impacts = FAST_FOOD_EVENT_IMPACTS[event_type]
                if metric_name in metric_impacts:
                    base_impact = metric_impacts[metric_name]
                    
                    # Severity multiplier (lower severity number = worse = higher impact)
                    # Severity 2 (critical) = 1.0x, Severity 6 (info) = 0.0x
                    severity_multiplier = max(0, (6 - severity) / 4)
                    
                    # Time decay (older events have less impact)
                    minutes_ago = (current_time - event_time).total_seconds() / 60
                    time_decay = max(0.3, 1.0 - (minutes_ago / self.event_window_minutes) * 0.7)
                    
                    adjusted_impact = base_impact * severity_multiplier * time_decay
                    impact_factor *= (1.0 + adjusted_impact)
        
        return base_value * impact_factor
    
    def _generate_drive_thru_timing(self, store: Store, order_value: float, item_count: int, hour: int) -> Dict[str, float]:
        """
        Generate correlated drive-through timing metrics reflecting HME sensor system.
        
        Timing Flow:
        1. Car hits Loop A (speaker) -> Menu Board Time starts
        2. Order sent to KDS -> KDS Bump Time starts
        3. Car hits Loop B (window) -> Window Time starts
        4. Car leaves Loop B -> Total Experience ends
        """
        
        # Get base values from realistic generation
        menu_board_base = self._generate_realistic_value(
            FAST_FOOD_METRICS["drive_through_menu_board_time_sec"],
            store, hour
        )
        kds_bump_base = self._generate_realistic_value(
            FAST_FOOD_METRICS["drive_through_kds_bump_time_sec"],
            store, hour
        )
        window_base = self._generate_realistic_value(
            FAST_FOOD_METRICS["drive_through_window_time_sec"],
            store, hour
        )
        
        # Apply order complexity normalization
        complexity_factor = self._calculate_order_complexity_factor(order_value, item_count)
        
        menu_board_time = menu_board_base * (1 + (complexity_factor - 1) * 0.5)  # 50% impact
        kds_bump_time = kds_bump_base * complexity_factor  # Full impact on kitchen
        window_time = window_base * (1 + (complexity_factor - 1) * 0.7)  # 70% impact on handoff
        
        # Transit time between stations (relatively constant)
        speaker_to_window_transit = random.uniform(30, 60)
        
        # Total experience calculation (realistic overlap)
        total_experience = menu_board_time + speaker_to_window_transit + window_time
        
        # If kitchen is slow, it extends total time (food not ready when car arrives at window)
        if kds_bump_time > (menu_board_time + speaker_to_window_transit):
            kitchen_delay = kds_bump_time - (menu_board_time + speaker_to_window_transit)
            total_experience += kitchen_delay
        
        # Calculate throughput (cars per hour) based on total experience time
        efficiency = random.uniform(0.60, 0.80)
        cars_per_hour = (3600 / total_experience) * efficiency if total_experience > 0 else 20
        
        return {
            "drive_through_menu_board_time_sec": round(menu_board_time, 1),
            "drive_through_kds_bump_time_sec": round(kds_bump_time, 1),
            "drive_through_window_time_sec": round(window_time, 1),
            "drive_through_total_experience_time_sec": round(total_experience, 1),
            "drive_through_cars_per_hour": round(cars_per_hour, 0),
        }
    
    def generate_store_metrics(self, store: Store) -> dict:
        """Generate operational metrics for a store based on category"""
        timestamp = datetime.now(timezone.utc)
        hour = timestamp.hour
        
        # Select metrics based on store category
        if store.store_category == "fast_food":
            # Generate order context (from POS integration)
            order_value = random.uniform(8, 75)
            item_count = max(1, int(order_value / random.uniform(6, 12)))
            
            # Decide which metric type to generate
            metric_choice = random.random()
            
            if metric_choice < 0.60 and HAS_NUMPY:  # 60% of time, generate full drive-thru timing
                # Generate correlated timing metrics
                timing_metrics = self._generate_drive_thru_timing(store, order_value, item_count, hour)
                
                # Pick one metric to report
                metric_name = random.choice(list(timing_metrics.keys()))
                value = timing_metrics[metric_name]
                metric_info = FAST_FOOD_METRICS[metric_name]
                
            else:  # 40% of time, generate other metrics (sales, traffic, etc.)
                metrics_pool = FAST_FOOD_METRICS.copy()
                if self.extended_schema:
                    metrics_pool.update(FAST_FOOD_EXTENDED_METRICS)
                
                # Exclude drive-through timing metrics from random selection
                non_timing_metrics = {k: v for k, v in metrics_pool.items() 
                                     if k not in ["drive_through_menu_board_time_sec",
                                                  "drive_through_kds_bump_time_sec",
                                                  "drive_through_window_time_sec",
                                                  "drive_through_total_experience_time_sec",
                                                  "drive_through_cars_per_hour"]}
                
                metric_name = random.choice(list(non_timing_metrics.keys()))
                metric_info = non_timing_metrics[metric_name]
                value = self._generate_metric_value(metric_info, store, hour)
            
            # Apply event impacts
            value = self._apply_event_impacts(metric_name, value, store, timestamp)
            
            # Build record with hardware source metadata
            record = {
                "timestamp": timestamp.isoformat(),
                "store_id": store.store_id,
                "store_category": store.store_category,
                "store_type": store.store_type,
                "region": store.region,
                "brand": store.brand,
                "district": store.district,
                "address": store.address,
                "latitude": store.latitude,
                "longitude": store.longitude,
                "metric_name": metric_name,
                "value": round(value, 2),
                "oid": metric_info["oid"],
                
                # Hardware integration metadata
                "data_source": "HME_TIMER_SYSTEM",
                "sensor_type": metric_info.get("source", "UNKNOWN"),
                "order_value": round(order_value, 2),
                "item_count": item_count,
                "normalized": metric_info.get("normalize_by_order", False),
            }
            
        else:  # apparel - existing simple generation
            metrics_pool = APPAREL_METRICS.copy()
            if self.extended_schema:
                metrics_pool.update(APPAREL_EXTENDED_METRICS)
            
            metric_name = random.choice(list(metrics_pool.keys()))
            metric_info = metrics_pool[metric_name]
            value = self._generate_metric_value(metric_info, store, hour)
            
            record = {
                "timestamp": timestamp.isoformat(),
                "store_id": store.store_id,
                "store_category": store.store_category,
                "store_type": store.store_type,
                "region": store.region,
                "brand": store.brand,
                "district": store.district,
                "address": store.address,
                "latitude": store.latitude,
                "longitude": store.longitude,
                "oid": metric_info["oid"],
                "metric_name": metric_name,
                "value": round(value, 2),
            }
        
        # Extended schema fields
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
        
        num_entries = random.randint(1000, 2500)
        current_time = datetime.now(timezone.utc)
        
        try:
            with open(filename, 'w') as f:
                for _ in range(num_entries):
                    store = random.choice(self.stores)
                    log_entry = self.generate_transaction_event(store)
                    f.write(log_entry + "\n")
                    
                    # Track fast food events for causality
                    if store.store_category == "fast_food":
                        # Parse event type and severity from log_entry
                        # Format: <priority>version timestamp hostname appname procid MSGID ...
                        try:
                            parts = log_entry.split()
                            if len(parts) >= 7:
                                msgid = parts[6]  # Event type
                                priority_str = parts[0].strip('<>')
                                priority = int(priority_str)
                                severity = priority % 8
                                self._track_event(store.store_id, msgid, severity, current_time)
                        except (ValueError, IndexError):
                            # Skip tracking if parsing fails
                            pass
            
            logging.debug(f"Created events file: {filename} ({num_entries} entries)")
        except Exception as e:
            logging.error(f"Error writing events file {filename}: {e}")
    
    def write_metrics_file(self, file_count: int):
        """Write a store metrics file"""
        timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")[:-3]
        
        num_entries = random.randint(2000, 2500)
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

