#!/usr/bin/env python3
"""
Telco IMS (IP Multimedia Subsystem) Synthetic Data Generator for AWS Databricks

Generates synthetic IMS data including:
- VoLTE Call Detail Records (CDRs)
- SIP Session metrics
- Subscriber session data
- IMS Core network metrics (P-CSCF, I-CSCF, S-CSCF)
- Media gateway metrics

Data is joinable with GCP gold tables via device_id, location, and device_key fields.

Usage (in Databricks notebook):
    from ims_data_generator import IMSDataGenerator
    
    # With default config
    generator = IMSDataGenerator()
    
    # With custom config
    config = IMSGeneratorConfig(
        device_count=200,
        num_subscribers=10000,
        anomaly_rate=0.10
    )
    generator = IMSDataGenerator(config=config)
    
    cdr_df = generator.generate_volte_cdrs_df(spark, num_records=10000)
    session_df = generator.generate_sip_sessions_df(spark, num_records=5000)
"""

import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field, asdict

# =============================================================================
# CONFIGURATION CLASS - Customize these values for your environment
# =============================================================================

@dataclass
class IMSGeneratorConfig:
    """
    Configuration for IMS data generation.
    
    Customize these values to match your GCP network performance data for joins.
    """
    
    # -------------------------------------------------------------------------
    # Network Device Configuration (must match GCP gold tables for joins)
    # -------------------------------------------------------------------------
    device_types: List[str] = field(default_factory=lambda: [
        "router", "switch", "firewall", "load_balancer"
    ])
    
    locations: List[str] = field(default_factory=lambda: [
        "datacenter-east", "datacenter-west", "datacenter-central", 
        "edge-north", "edge-south"
    ])
    
    vendors: List[str] = field(default_factory=lambda: [
        "Cisco", "Juniper", "Arista", "Palo Alto", "F5"
    ])
    
    device_count: int = 100  # Total number of network devices (must match GCP generator)
    
    # -------------------------------------------------------------------------
    # Subscriber Configuration
    # -------------------------------------------------------------------------
    num_subscribers: int = 5000  # Total subscriber pool size
    
    subscription_types: List[str] = field(default_factory=lambda: [
        "Prepaid", "Postpaid", "Enterprise", "MVNO"
    ])
    
    roaming_probability: float = 0.15  # 15% of subscribers are roaming
    
    # -------------------------------------------------------------------------
    # IMS Network Components
    # -------------------------------------------------------------------------
    ims_node_types: List[str] = field(default_factory=lambda: [
        "P-CSCF", "I-CSCF", "S-CSCF", "HSS", "MGCF", "MRF", "BGCF", "AS"
    ])
    
    # -------------------------------------------------------------------------
    # Call/Session Configuration
    # -------------------------------------------------------------------------
    codec_types: List[str] = field(default_factory=lambda: [
        "AMR-WB", "AMR-NB", "EVS", "G.711", "G.729", "OPUS"
    ])
    
    call_types: List[str] = field(default_factory=lambda: [
        "VoLTE", "VoWiFi", "ViLTE", "VoNR"  # Voice over LTE, WiFi, Video LTE, 5G NR
    ])
    
    qos_classes: List[str] = field(default_factory=lambda: [
        "QCI_1", "QCI_2", "QCI_3", "QCI_4", "QCI_5"  # 5QI classes
    ])
    
    rate_plans: List[str] = field(default_factory=lambda: [
        "Unlimited", "Metered", "Corporate", "Family"
    ])
    
    # -------------------------------------------------------------------------
    # Quality & Performance Rates
    # -------------------------------------------------------------------------
    call_answer_rate: float = 0.85  # 85% of calls are answered
    sip_success_rate: float = 0.95  # 95% SIP session success rate
    anomaly_rate: float = 0.05  # 5% anomaly rate for metrics
    
    video_call_rate: float = 0.10  # 10% of calls are video
    emergency_call_rate: float = 0.001  # 0.1% emergency calls
    international_call_rate: float = 0.05  # 5% international calls
    
    # -------------------------------------------------------------------------
    # Time Window Configuration (hours)
    # -------------------------------------------------------------------------
    cdr_lookback_hours: int = 72  # CDRs span last 3 days
    sip_session_lookback_hours: int = 48  # SIP sessions span last 2 days
    node_metrics_lookback_hours: int = 24  # Node metrics span last day
    subscriber_session_lookback_hours: int = 168  # Subscriber sessions span last 7 days
    
    # -------------------------------------------------------------------------
    # Quality Metrics Ranges
    # -------------------------------------------------------------------------
    mos_score_range: tuple = (3.2, 4.5)  # Mean Opinion Score range
    jitter_range_ms: tuple = (0, 50)  # Jitter in milliseconds
    packet_loss_range_pct: tuple = (0, 3)  # Packet loss percentage
    rtt_range_ms: tuple = (20, 200)  # Round-trip time in milliseconds
    
    # -------------------------------------------------------------------------
    # Device Manufacturer Configuration (for subscriber sessions)
    # -------------------------------------------------------------------------
    device_manufacturers: List[str] = field(default_factory=lambda: [
        "Apple", "Samsung", "Google", "OnePlus", "Xiaomi"
    ])
    
    device_models: List[str] = field(default_factory=lambda: [
        "iPhone 15 Pro", "Galaxy S24", "Pixel 8", "OnePlus 12", "14 Ultra"
    ])
    
    os_versions: List[str] = field(default_factory=lambda: [
        "iOS 17.4", "Android 14", "Android 13"
    ])
    
    access_technologies: List[str] = field(default_factory=lambda: [
        "5G-SA", "5G-NSA", "LTE", "LTE-A"
    ])
    
    # -------------------------------------------------------------------------
    # Data Source Identifier
    # -------------------------------------------------------------------------
    data_source: str = "aws_ims_generator"


# =============================================================================
# Default Configuration Instance
# =============================================================================

DEFAULT_CONFIG = IMSGeneratorConfig()

# =============================================================================
# Static Reference Data (SIP responses, termination causes)
# =============================================================================

# SIP Response codes (RFC 3261)
SIP_RESPONSES = {
    100: "Trying",
    180: "Ringing", 
    183: "Session Progress",
    200: "OK",
    302: "Moved Temporarily",
    400: "Bad Request",
    401: "Unauthorized",
    403: "Forbidden",
    404: "Not Found",
    408: "Request Timeout",
    480: "Temporarily Unavailable",
    486: "Busy Here",
    487: "Request Terminated",
    500: "Server Internal Error",
    503: "Service Unavailable",
    504: "Server Time-out"
}

# Call termination causes (Q.850)
TERMINATION_CAUSES = [
    "Normal", "User Busy", "No Answer", "Call Rejected", "Number Changed",
    "Network Congestion", "Bearer Capability Not Available", "Facility Rejected",
    "Normal Clearing", "Destination Out of Order"
]

# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class NetworkDevice:
    """Network device matching GCP schema"""
    device_id: str
    device_type: str
    location: str
    vendor: str
    model: str
    ip_address: str
    device_key: str
    region: str


@dataclass
class IMSSubscriber:
    """IMS subscriber profile"""
    imsi: str  # International Mobile Subscriber Identity
    msisdn: str  # Phone number
    impu: str  # IMS Public User Identity (SIP URI)
    impi: str  # IMS Private User Identity
    subscription_type: str
    home_location: str
    roaming_status: str


# =============================================================================
# IMS Data Generator
# =============================================================================

class IMSDataGenerator:
    """
    Generates synthetic IMS (IP Multimedia Subsystem) data for telco analytics.
    
    All generated data includes join keys compatible with GCP gold tables:
    - device_id
    - location
    - device_key
    - region
    
    Example:
        # Use default configuration
        generator = IMSDataGenerator()
        
        # Use custom configuration
        config = IMSGeneratorConfig(
            device_count=200,
            num_subscribers=10000,
            anomaly_rate=0.10,
            call_answer_rate=0.90
        )
        generator = IMSDataGenerator(config=config)
    """
    
    def __init__(self, config: Optional[IMSGeneratorConfig] = None, seed: Optional[int] = None):
        """
        Initialize the IMS data generator.
        
        Args:
            config: IMSGeneratorConfig object with customizable settings.
                    If None, uses DEFAULT_CONFIG.
            seed: Optional random seed for reproducibility
        """
        self.config = config or DEFAULT_CONFIG
        
        if seed:
            random.seed(seed)
        
        self.devices = self._generate_network_devices()
        self.ims_nodes = self._generate_ims_nodes()
        self.subscribers = self._generate_subscribers(num_subscribers=self.config.num_subscribers)
    
    def _derive_region(self, location: str) -> str:
        """Derive region from location (matches GCP gold table logic)"""
        if "east" in location:
            return "east"
        elif "west" in location:
            return "west"
        elif "central" in location:
            return "central"
        elif "north" in location:
            return "north"
        elif "south" in location:
            return "south"
        return "unknown"
    
    def _generate_network_devices(self) -> List[NetworkDevice]:
        """Generate network devices matching GCP schema"""
        devices = []
        models = {
            "Cisco": {"router": "ASR-9000", "switch": "Nexus-9300", "firewall": "ASA-5525", "load_balancer": "ACE-4710"},
            "Juniper": {"router": "MX960", "switch": "EX4650", "firewall": "SRX5800", "load_balancer": "LB-8"},
            "Arista": {"router": "7280R3", "switch": "7050X3", "firewall": "7280R3", "load_balancer": "7280R3"},
            "Palo Alto": {"router": "PA-5250", "switch": "PA-5250", "firewall": "PA-5250", "load_balancer": "PA-5250"},
            "F5": {"router": "BIG-IP-i15800", "switch": "BIG-IP-i15800", "firewall": "BIG-IP-i15800", "load_balancer": "BIG-IP-i15800"},
        }
        
        for i in range(self.config.device_count):
            device_type = self.config.device_types[i % len(self.config.device_types)]
            location = self.config.locations[i % len(self.config.locations)]
            vendor = self.config.vendors[i % len(self.config.vendors)]
            model = models.get(vendor, {}).get(device_type, f"{vendor}-{device_type}")
            device_id = f"{device_type}-{location}-{i:03d}"
            ip_address = f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
            device_key = f"{device_id}_{location}"
            region = self._derive_region(location)
            
            devices.append(NetworkDevice(
                device_id=device_id,
                device_type=device_type,
                location=location,
                vendor=vendor,
                model=model,
                ip_address=ip_address,
                device_key=device_key,
                region=region
            ))
        
        return devices
    
    def _generate_ims_nodes(self) -> List[Dict[str, Any]]:
        """Generate IMS core network nodes"""
        nodes = []
        for location in self.config.locations:
            for node_type in self.config.ims_node_types:
                node_id = f"ims-{node_type.lower()}-{location}-001"
                nodes.append({
                    "node_id": node_id,
                    "node_type": node_type,
                    "location": location,
                    "region": self._derive_region(location),
                    "ip_address": f"10.100.{random.randint(1, 254)}.{random.randint(1, 254)}",
                    "status": "active",
                    "capacity_pct": random.uniform(30, 85)
                })
        return nodes
    
    def _generate_subscribers(self, num_subscribers: int) -> List[IMSSubscriber]:
        """Generate IMS subscriber profiles"""
        subscribers = []
        home_weight = 1 - self.config.roaming_probability
        roaming_weight = self.config.roaming_probability
        
        for i in range(num_subscribers):
            imsi = f"310{random.randint(100, 999)}{random.randint(1000000000, 9999999999)}"
            msisdn = f"+1{random.randint(200, 999)}{random.randint(1000000, 9999999)}"
            home_location = random.choice(self.config.locations)
            
            subscribers.append(IMSSubscriber(
                imsi=imsi,
                msisdn=msisdn,
                impu=f"sip:{msisdn}@ims.telco.com",
                impi=f"{imsi}@ims.telco.com",
                subscription_type=random.choice(self.config.subscription_types),
                home_location=home_location,
                roaming_status=random.choices(["Home", "Roaming"], weights=[home_weight, roaming_weight])[0]
            ))
        
        return subscribers
    
    def _generate_timestamp(self, hours_back: int = 24) -> datetime:
        """Generate a random timestamp within the specified time window"""
        now = datetime.now(timezone.utc)
        random_seconds = random.randint(0, hours_back * 3600)
        return now - timedelta(seconds=random_seconds)
    
    # =========================================================================
    # VoLTE Call Detail Records (CDRs)
    # =========================================================================
    
    def generate_volte_cdr(self) -> Dict[str, Any]:
        """
        Generate a single VoLTE Call Detail Record.
        
        Returns a CDR with network device join keys for cross-cloud analytics.
        """
        caller = random.choice(self.subscribers)
        callee = random.choice(self.subscribers)
        
        # Ensure caller != callee
        while callee.msisdn == caller.msisdn:
            callee = random.choice(self.subscribers)
        
        # Select network devices involved in the call path
        originating_device = random.choice(self.devices)
        terminating_device = random.choice(self.devices)
        
        # Call timing (using config lookback hours)
        call_start = self._generate_timestamp(hours_back=self.config.cdr_lookback_hours)
        call_duration_seconds = random.choices(
            [random.randint(0, 10), random.randint(10, 120), random.randint(120, 1800), random.randint(1800, 7200)],
            weights=[0.15, 0.35, 0.40, 0.10]
        )[0]
        call_end = call_start + timedelta(seconds=call_duration_seconds)
        
        # Call success/failure (using config answer rate)
        is_answered = random.random() < self.config.call_answer_rate
        termination_cause = "Normal Clearing" if is_answered else random.choice(TERMINATION_CAUSES[1:])
        
        # QoS metrics (using config ranges)
        if is_answered and call_duration_seconds > 0:
            mos_score = round(random.uniform(*self.config.mos_score_range), 2)
            jitter_ms = round(random.uniform(*self.config.jitter_range_ms), 2)
            packet_loss_pct = round(random.uniform(*self.config.packet_loss_range_pct), 3)
            rtt_ms = round(random.uniform(*self.config.rtt_range_ms), 2)
        else:
            mos_score = None
            jitter_ms = None
            packet_loss_pct = None
            rtt_ms = None
        
        return {
            # Unique identifiers
            "cdr_id": str(uuid.uuid4()),
            "session_id": str(uuid.uuid4()),
            
            # Timestamps
            "call_start_time": call_start.isoformat(),
            "call_end_time": call_end.isoformat() if is_answered else None,
            "call_duration_seconds": call_duration_seconds if is_answered else 0,
            
            # Caller info
            "caller_imsi": caller.imsi,
            "caller_msisdn": caller.msisdn,
            "caller_impu": caller.impu,
            "caller_subscription_type": caller.subscription_type,
            "caller_home_location": caller.home_location,
            "caller_roaming_status": caller.roaming_status,
            
            # Callee info
            "callee_msisdn": callee.msisdn,
            "callee_impu": callee.impu,
            "callee_subscription_type": callee.subscription_type,
            "callee_home_location": callee.home_location,
            
            # Call details (using config values)
            "call_type": random.choice(self.config.call_types),
            "codec": random.choice(self.config.codec_types),
            "is_video": random.random() < self.config.video_call_rate,
            "is_emergency": random.random() < self.config.emergency_call_rate,
            "is_international": random.random() < self.config.international_call_rate,
            "is_answered": is_answered,
            "termination_cause": termination_cause,
            
            # QoS metrics
            "qos_class": random.choice(self.config.qos_classes),
            "mos_score": mos_score,
            "jitter_ms": jitter_ms,
            "packet_loss_pct": packet_loss_pct,
            "rtt_ms": rtt_ms,
            
            # Network path - JOIN KEYS for GCP gold tables
            "originating_device_id": originating_device.device_id,
            "originating_device_type": originating_device.device_type,
            "originating_location": originating_device.location,
            "originating_device_key": originating_device.device_key,
            "originating_region": originating_device.region,
            
            "terminating_device_id": terminating_device.device_id,
            "terminating_device_type": terminating_device.device_type,
            "terminating_location": terminating_device.location,
            "terminating_device_key": terminating_device.device_key,
            "terminating_region": terminating_device.region,
            
            # Billing
            "billable_duration_seconds": call_duration_seconds if is_answered else 0,
            "rate_plan": random.choice(self.config.rate_plans),
            
            # Metadata
            "record_timestamp": datetime.now(timezone.utc).isoformat(),
            "data_source": self.config.data_source
        }
    
    def generate_volte_cdrs(self, num_records: int = 1000) -> List[Dict[str, Any]]:
        """Generate multiple VoLTE CDRs"""
        return [self.generate_volte_cdr() for _ in range(num_records)]
    
    # =========================================================================
    # SIP Session Metrics
    # =========================================================================
    
    def generate_sip_session(self) -> Dict[str, Any]:
        """
        Generate a SIP session metric record.
        
        Captures SIP signaling performance metrics for IMS analysis.
        """
        subscriber = random.choice(self.subscribers)
        ims_node = random.choice(self.ims_nodes)
        device = random.choice(self.devices)
        
        session_start = self._generate_timestamp(hours_back=self.config.sip_session_lookback_hours)
        
        # SIP transaction metrics
        invite_response_time_ms = round(random.uniform(50, 500), 2)
        registration_time_ms = round(random.uniform(100, 2000), 2)
        
        # Success/failure scenarios (using config success rate)
        is_successful = random.random() < self.config.sip_success_rate
        final_response_code = 200 if is_successful else random.choice([400, 401, 403, 404, 408, 480, 486, 500, 503])
        
        return {
            # Identifiers
            "session_id": str(uuid.uuid4()),
            "call_id": f"{uuid.uuid4()}@{ims_node['ip_address']}",
            
            # Timestamps
            "session_start_time": session_start.isoformat(),
            "session_end_time": (session_start + timedelta(seconds=random.randint(1, 3600))).isoformat(),
            
            # Subscriber
            "subscriber_imsi": subscriber.imsi,
            "subscriber_msisdn": subscriber.msisdn,
            "subscriber_impu": subscriber.impu,
            
            # IMS Node info
            "ims_node_id": ims_node["node_id"],
            "ims_node_type": ims_node["node_type"],
            "ims_node_location": ims_node["location"],
            
            # Network device - JOIN KEYS for GCP
            "device_id": device.device_id,
            "device_type": device.device_type,
            "location": device.location,
            "device_key": device.device_key,
            "region": device.region,
            
            # SIP Signaling metrics
            "sip_method": random.choice(["INVITE", "REGISTER", "SUBSCRIBE", "OPTIONS", "MESSAGE", "NOTIFY"]),
            "final_response_code": final_response_code,
            "final_response_desc": SIP_RESPONSES.get(final_response_code, "Unknown"),
            "is_successful": is_successful,
            
            # Timing metrics
            "invite_response_time_ms": invite_response_time_ms,
            "registration_time_ms": registration_time_ms,
            "session_setup_time_ms": round(random.uniform(200, 3000), 2),
            
            # Retransmission metrics
            "invite_retransmissions": random.choices([0, 1, 2, 3], weights=[0.7, 0.2, 0.08, 0.02])[0],
            "total_retransmissions": random.randint(0, 5),
            
            # Authentication
            "auth_method": random.choice(["AKA", "Digest", "TLS-Client-Cert"]),
            "auth_success": is_successful,
            
            # Metadata
            "record_timestamp": datetime.now(timezone.utc).isoformat(),
            "data_source": self.config.data_source
        }
    
    def generate_sip_sessions(self, num_records: int = 1000) -> List[Dict[str, Any]]:
        """Generate multiple SIP session records"""
        return [self.generate_sip_session() for _ in range(num_records)]
    
    # =========================================================================
    # IMS Node Performance Metrics
    # =========================================================================
    
    def generate_ims_node_metrics(self) -> Dict[str, Any]:
        """
        Generate IMS core network node performance metrics.
        
        Includes P-CSCF, I-CSCF, S-CSCF, HSS metrics for capacity planning.
        """
        ims_node = random.choice(self.ims_nodes)
        device = random.choice([d for d in self.devices if d.location == ims_node["location"]])
        
        timestamp = self._generate_timestamp(hours_back=self.config.node_metrics_lookback_hours)
        
        # Node-specific metrics
        if ims_node["node_type"] == "P-CSCF":
            active_sessions = random.randint(1000, 50000)
            registrations_per_sec = random.uniform(10, 500)
        elif ims_node["node_type"] == "S-CSCF":
            active_sessions = random.randint(5000, 100000)
            registrations_per_sec = random.uniform(50, 1000)
        elif ims_node["node_type"] == "HSS":
            active_sessions = random.randint(10000, 500000)  # Subscriber records
            registrations_per_sec = random.uniform(100, 2000)
        else:
            active_sessions = random.randint(500, 20000)
            registrations_per_sec = random.uniform(5, 200)
        
        # Is there an anomaly? (using config rate)
        is_anomaly = random.random() < self.config.anomaly_rate
        
        return {
            # Identifiers
            "metric_id": str(uuid.uuid4()),
            "timestamp": timestamp.isoformat(),
            
            # IMS Node info
            "ims_node_id": ims_node["node_id"],
            "ims_node_type": ims_node["node_type"],
            "ims_node_location": ims_node["location"],
            "ims_node_region": ims_node["region"],
            
            # Network device - JOIN KEYS for GCP
            "device_id": device.device_id,
            "device_type": device.device_type,
            "location": device.location,
            "device_key": device.device_key,
            "region": device.region,
            
            # Capacity metrics
            "active_sessions": active_sessions,
            "max_sessions": active_sessions * 2,
            "session_utilization_pct": round((active_sessions / (active_sessions * 2)) * 100, 2),
            
            # Performance metrics
            "cpu_utilization_pct": round(random.uniform(20, 95) if is_anomaly else random.uniform(20, 70), 2),
            "memory_utilization_pct": round(random.uniform(30, 98) if is_anomaly else random.uniform(30, 75), 2),
            "disk_io_ops_per_sec": random.randint(100, 10000),
            
            # Throughput metrics
            "registrations_per_sec": round(registrations_per_sec, 2),
            "invites_per_sec": round(registrations_per_sec * 0.3, 2),
            "messages_per_sec": round(registrations_per_sec * 0.5, 2),
            
            # Latency metrics
            "avg_response_time_ms": round(random.uniform(50, 500) if is_anomaly else random.uniform(20, 150), 2),
            "p95_response_time_ms": round(random.uniform(200, 2000) if is_anomaly else random.uniform(50, 300), 2),
            "p99_response_time_ms": round(random.uniform(500, 5000) if is_anomaly else random.uniform(100, 500), 2),
            
            # Error metrics
            "error_rate_pct": round(random.uniform(1, 10) if is_anomaly else random.uniform(0, 1), 3),
            "failed_registrations": random.randint(10, 500) if is_anomaly else random.randint(0, 20),
            "timeout_count": random.randint(5, 100) if is_anomaly else random.randint(0, 10),
            
            # Health
            "is_anomaly": is_anomaly,
            "health_status": "Degraded" if is_anomaly else "Healthy",
            
            # Metadata
            "record_timestamp": datetime.now(timezone.utc).isoformat(),
            "data_source": self.config.data_source
        }
    
    def generate_ims_node_metrics_batch(self, num_records: int = 1000) -> List[Dict[str, Any]]:
        """Generate multiple IMS node metric records"""
        return [self.generate_ims_node_metrics() for _ in range(num_records)]
    
    # =========================================================================
    # Subscriber Session Analytics
    # =========================================================================
    
    def generate_subscriber_session(self) -> Dict[str, Any]:
        """
        Generate subscriber session analytics data.
        
        Tracks subscriber behavior and service usage patterns.
        """
        subscriber = random.choice(self.subscribers)
        device = random.choice(self.devices)
        
        session_start = self._generate_timestamp(hours_back=self.config.subscriber_session_lookback_hours)
        session_duration_minutes = random.choices(
            [random.randint(1, 5), random.randint(5, 30), random.randint(30, 120), random.randint(120, 480)],
            weights=[0.20, 0.35, 0.30, 0.15]
        )[0]
        
        # Service usage
        voice_minutes = round(random.uniform(0, session_duration_minutes * 0.5), 2)
        video_minutes = round(random.uniform(0, session_duration_minutes * 0.2), 2)
        data_mb = round(random.uniform(0, 500), 2)
        
        return {
            # Identifiers
            "session_id": str(uuid.uuid4()),
            "session_start_time": session_start.isoformat(),
            "session_end_time": (session_start + timedelta(minutes=session_duration_minutes)).isoformat(),
            "session_duration_minutes": session_duration_minutes,
            
            # Subscriber info
            "subscriber_imsi": subscriber.imsi,
            "subscriber_msisdn": subscriber.msisdn,
            "subscription_type": subscriber.subscription_type,
            "home_location": subscriber.home_location,
            "roaming_status": subscriber.roaming_status,
            
            # Network device - JOIN KEYS for GCP
            "device_id": device.device_id,
            "device_type": device.device_type,
            "location": device.location,
            "device_key": device.device_key,
            "region": device.region,
            
            # Service usage
            "voice_minutes_used": voice_minutes,
            "video_minutes_used": video_minutes,
            "data_mb_used": data_mb,
            "sms_count": random.randint(0, 50),
            
            # Call statistics
            "voice_calls_made": random.randint(0, 10),
            "voice_calls_received": random.randint(0, 10),
            "video_calls_made": random.randint(0, 3),
            "missed_calls": random.randint(0, 5),
            
            # Quality metrics (using config MOS range)
            "avg_call_mos_score": round(random.uniform(*self.config.mos_score_range), 2),
            "dropped_calls": random.randint(0, 2),
            "failed_call_setups": random.randint(0, 3),
            
            # Device info (using config values)
            "device_manufacturer": random.choice(self.config.device_manufacturers),
            "device_model": random.choice(self.config.device_models),
            "os_version": random.choice(self.config.os_versions),
            
            # Network access (using config values)
            "access_technology": random.choice(self.config.access_technologies),
            "cell_id": f"CELL-{device.location}-{random.randint(1000, 9999)}",
            "signal_strength_dbm": random.randint(-110, -60),
            
            # Metadata
            "record_timestamp": datetime.now(timezone.utc).isoformat(),
            "data_source": self.config.data_source
        }
    
    def generate_subscriber_sessions(self, num_records: int = 1000) -> List[Dict[str, Any]]:
        """Generate multiple subscriber session records"""
        return [self.generate_subscriber_session() for _ in range(num_records)]
    
    # =========================================================================
    # Spark DataFrame Generation Methods (for Databricks)
    # =========================================================================
    
    def generate_volte_cdrs_df(self, spark, num_records: int = 10000):
        """
        Generate VoLTE CDRs as a Spark DataFrame.
        
        Args:
            spark: SparkSession
            num_records: Number of records to generate
            
        Returns:
            Spark DataFrame with VoLTE CDR data
        """
        data = self.generate_volte_cdrs(num_records)
        return spark.createDataFrame(data)
    
    def generate_sip_sessions_df(self, spark, num_records: int = 5000):
        """
        Generate SIP sessions as a Spark DataFrame.
        
        Args:
            spark: SparkSession  
            num_records: Number of records to generate
            
        Returns:
            Spark DataFrame with SIP session data
        """
        data = self.generate_sip_sessions(num_records)
        return spark.createDataFrame(data)
    
    def generate_ims_node_metrics_df(self, spark, num_records: int = 5000):
        """
        Generate IMS node metrics as a Spark DataFrame.
        
        Args:
            spark: SparkSession
            num_records: Number of records to generate
            
        Returns:
            Spark DataFrame with IMS node metrics
        """
        data = self.generate_ims_node_metrics_batch(num_records)
        return spark.createDataFrame(data)
    
    def generate_subscriber_sessions_df(self, spark, num_records: int = 10000):
        """
        Generate subscriber sessions as a Spark DataFrame.
        
        Args:
            spark: SparkSession
            num_records: Number of records to generate
            
        Returns:
            Spark DataFrame with subscriber session data
        """
        data = self.generate_subscriber_sessions(num_records)
        return spark.createDataFrame(data)
    
    # =========================================================================
    # Unity Catalog Table Writing Methods
    # =========================================================================
    
    def write_to_unity_catalog(
        self,
        spark,
        catalog: str,
        schema: str,
        num_volte_cdrs: int = 100000,
        num_sip_sessions: int = 50000,
        num_node_metrics: int = 25000,
        num_subscriber_sessions: int = 75000,
        mode: str = "overwrite",
        enable_cdf: bool = True,
        optimize_tables: bool = True
    ) -> Dict[str, str]:
        """
        Generate and write all IMS data tables to Unity Catalog.
        
        Creates 4 Delta tables optimized for Delta Sharing:
        - ims_volte_cdrs: VoLTE Call Detail Records
        - ims_sip_sessions: SIP Session Metrics
        - ims_node_metrics: IMS Node Performance
        - ims_subscriber_sessions: Subscriber Analytics
        
        Args:
            spark: SparkSession
            catalog: Unity Catalog name (e.g., 'telco_ims_aws')
            schema: Schema/database name (e.g., 'ims_data')
            num_volte_cdrs: Number of VoLTE CDR records
            num_sip_sessions: Number of SIP session records
            num_node_metrics: Number of IMS node metric records
            num_subscriber_sessions: Number of subscriber session records
            mode: Write mode ('overwrite' or 'append')
            enable_cdf: Enable Change Data Feed for Delta Sharing
            optimize_tables: Run OPTIMIZE after writing
            
        Returns:
            Dict with table names and record counts
        """
        results = {}
        
        # Create catalog and schema if they don't exist
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
        spark.sql(f"USE CATALOG {catalog}")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        spark.sql(f"USE SCHEMA {schema}")
        
        print(f"Writing IMS data to {catalog}.{schema}")
        
        # Write VoLTE CDRs
        results["ims_volte_cdrs"] = self._write_volte_cdrs_table(
            spark, catalog, schema, num_volte_cdrs, mode, enable_cdf
        )
        
        # Write SIP Sessions
        results["ims_sip_sessions"] = self._write_sip_sessions_table(
            spark, catalog, schema, num_sip_sessions, mode, enable_cdf
        )
        
        # Write IMS Node Metrics
        results["ims_node_metrics"] = self._write_node_metrics_table(
            spark, catalog, schema, num_node_metrics, mode, enable_cdf
        )
        
        # Write Subscriber Sessions
        results["ims_subscriber_sessions"] = self._write_subscriber_sessions_table(
            spark, catalog, schema, num_subscriber_sessions, mode, enable_cdf
        )
        
        # Optimize tables if requested
        if optimize_tables:
            print("Optimizing tables...")
            for table_name in results.keys():
                full_table = f"{catalog}.{schema}.{table_name}"
                spark.sql(f"OPTIMIZE {full_table}")
                spark.sql(f"ANALYZE TABLE {full_table} COMPUTE STATISTICS FOR ALL COLUMNS")
            print("[OK] All tables optimized!")
        
        return results
    
    def _write_volte_cdrs_table(
        self,
        spark,
        catalog: str,
        schema: str,
        num_records: int,
        mode: str,
        enable_cdf: bool
    ) -> int:
        """Write VoLTE CDRs to Unity Catalog table."""
        table_name = f"{catalog}.{schema}.ims_volte_cdrs"
        print(f"Generating {num_records:,} VoLTE CDRs...")
        
        df = self.generate_volte_cdrs_df(spark, num_records)
        
        writer = df.write \
            .format("delta") \
            .mode(mode) \
            .option("overwriteSchema", "true") \
            .option("delta.enableChangeDataFeed", str(enable_cdf).lower()) \
            .partitionBy("originating_region", "call_type")
        
        writer.saveAsTable(table_name)
        
        # Add table comment
        spark.sql(f"""
            ALTER TABLE {table_name} 
            SET TBLPROPERTIES (
                'comment' = 'VoLTE Call Detail Records - joinable with GCP gold tables via device_id/location',
                'quality' = 'gold',
                'delta.enableChangeDataFeed' = '{str(enable_cdf).lower()}'
            )
        """)
        
        count = spark.table(table_name).count()
        print(f"[OK] Created {table_name} ({count:,} records)")
        return count
    
    def _write_sip_sessions_table(
        self,
        spark,
        catalog: str,
        schema: str,
        num_records: int,
        mode: str,
        enable_cdf: bool
    ) -> int:
        """Write SIP Sessions to Unity Catalog table."""
        table_name = f"{catalog}.{schema}.ims_sip_sessions"
        print(f"Generating {num_records:,} SIP sessions...")
        
        df = self.generate_sip_sessions_df(spark, num_records)
        
        writer = df.write \
            .format("delta") \
            .mode(mode) \
            .option("overwriteSchema", "true") \
            .option("delta.enableChangeDataFeed", str(enable_cdf).lower()) \
            .partitionBy("region", "ims_node_type")
        
        writer.saveAsTable(table_name)
        
        spark.sql(f"""
            ALTER TABLE {table_name} 
            SET TBLPROPERTIES (
                'comment' = 'SIP Session signaling metrics - joinable with GCP gold tables',
                'quality' = 'gold',
                'delta.enableChangeDataFeed' = '{str(enable_cdf).lower()}'
            )
        """)
        
        count = spark.table(table_name).count()
        print(f"[OK] Created {table_name} ({count:,} records)")
        return count
    
    def _write_node_metrics_table(
        self,
        spark,
        catalog: str,
        schema: str,
        num_records: int,
        mode: str,
        enable_cdf: bool
    ) -> int:
        """Write IMS Node Metrics to Unity Catalog table."""
        table_name = f"{catalog}.{schema}.ims_node_metrics"
        print(f"Generating {num_records:,} IMS node metrics...")
        
        df = self.generate_ims_node_metrics_df(spark, num_records)
        
        writer = df.write \
            .format("delta") \
            .mode(mode) \
            .option("overwriteSchema", "true") \
            .option("delta.enableChangeDataFeed", str(enable_cdf).lower()) \
            .partitionBy("ims_node_type", "region")
        
        writer.saveAsTable(table_name)
        
        spark.sql(f"""
            ALTER TABLE {table_name} 
            SET TBLPROPERTIES (
                'comment' = 'IMS Core network node performance metrics',
                'quality' = 'gold',
                'delta.enableChangeDataFeed' = '{str(enable_cdf).lower()}'
            )
        """)
        
        count = spark.table(table_name).count()
        print(f"[OK] Created {table_name} ({count:,} records)")
        return count
    
    def _write_subscriber_sessions_table(
        self,
        spark,
        catalog: str,
        schema: str,
        num_records: int,
        mode: str,
        enable_cdf: bool
    ) -> int:
        """Write Subscriber Sessions to Unity Catalog table."""
        table_name = f"{catalog}.{schema}.ims_subscriber_sessions"
        print(f"Generating {num_records:,} subscriber sessions...")
        
        df = self.generate_subscriber_sessions_df(spark, num_records)
        
        writer = df.write \
            .format("delta") \
            .mode(mode) \
            .option("overwriteSchema", "true") \
            .option("delta.enableChangeDataFeed", str(enable_cdf).lower()) \
            .partitionBy("region", "subscription_type")
        
        writer.saveAsTable(table_name)
        
        spark.sql(f"""
            ALTER TABLE {table_name} 
            SET TBLPROPERTIES (
                'comment' = 'Subscriber usage analytics - joinable with GCP gold tables',
                'quality' = 'gold',
                'delta.enableChangeDataFeed' = '{str(enable_cdf).lower()}'
            )
        """)
        
        count = spark.table(table_name).count()
        print(f"[OK] Created {table_name} ({count:,} records)")
        return count
    
    def setup_delta_share(
        self,
        spark,
        catalog: str,
        schema: str,
        share_name: str = "telco_ims_share",
        recipient_name: Optional[str] = None,
        recipient_sharing_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Set up Delta Sharing for IMS tables.
        
        Creates a share and optionally a recipient for cross-cloud data sharing.
        
        Args:
            spark: SparkSession
            catalog: Unity Catalog name
            schema: Schema name containing IMS tables
            share_name: Name for the Delta Share
            recipient_name: Optional recipient name
            recipient_sharing_id: Optional Databricks-to-Databricks sharing ID
            
        Returns:
            Dict with share details and activation info
        """
        result = {
            "share_name": share_name,
            "tables_shared": [],
            "recipient": None
        }
        
        # Create the share
        spark.sql(f"""
            CREATE SHARE IF NOT EXISTS {share_name}
            COMMENT 'Telco IMS data share for cross-cloud analytics with GCP'
        """)
        print(f"[OK] Created share: {share_name}")
        
        # Add tables to the share
        tables = [
            ("ims_volte_cdrs", "VoLTE Call Detail Records"),
            ("ims_sip_sessions", "SIP Session Metrics"),
            ("ims_node_metrics", "IMS Node Performance"),
            ("ims_subscriber_sessions", "Subscriber Analytics")
        ]
        
        for table_name, description in tables:
            full_table = f"{catalog}.{schema}.{table_name}"
            try:
                spark.sql(f"""
                    ALTER SHARE {share_name}
                    ADD TABLE {full_table}
                    COMMENT '{description}'
                """)
                result["tables_shared"].append(table_name)
                print(f"[OK] Added {table_name} to share")
            except Exception as e:
                if "already exists" in str(e).lower():
                    result["tables_shared"].append(table_name)
                    print(f"[INFO] {table_name} already in share")
                else:
                    print(f"[ERROR] Error adding {table_name}: {e}")
        
        # Create recipient if specified
        if recipient_name:
            try:
                if recipient_sharing_id:
                    # Databricks-to-Databricks sharing
                    spark.sql(f"""
                        CREATE RECIPIENT IF NOT EXISTS {recipient_name}
                        USING ID '{recipient_sharing_id}'
                        COMMENT 'GCP Databricks workspace - Databricks-to-Databricks sharing'
                    """)
                else:
                    # Open sharing (for demo/dev)
                    spark.sql(f"""
                        CREATE RECIPIENT IF NOT EXISTS {recipient_name}
                        COMMENT 'GCP Databricks workspace recipient'
                    """)
                
                # Grant access
                spark.sql(f"GRANT SELECT ON SHARE {share_name} TO RECIPIENT {recipient_name}")
                
                # Get recipient details
                recipient_info = spark.sql(f"DESCRIBE RECIPIENT {recipient_name}").collect()
                result["recipient"] = {
                    "name": recipient_name,
                    "info": [row.asDict() for row in recipient_info]
                }
                print(f"[OK] Created recipient: {recipient_name}")
                
            except Exception as e:
                print(f"[WARNING] Recipient creation note: {e}")
        
        return result


# =============================================================================
# Standalone execution for testing
# =============================================================================

if __name__ == "__main__":
    import json
    
    print("=" * 70)
    print("IMS Data Generator - Test Output")
    print("=" * 70)
    
    # Show default configuration
    print("\n--- Default Configuration ---")
    print(f"  Device Count: {DEFAULT_CONFIG.device_count}")
    print(f"  Subscribers: {DEFAULT_CONFIG.num_subscribers}")
    print(f"  Locations: {DEFAULT_CONFIG.locations}")
    print(f"  Call Answer Rate: {DEFAULT_CONFIG.call_answer_rate * 100}%")
    print(f"  Anomaly Rate: {DEFAULT_CONFIG.anomaly_rate * 100}%")
    print(f"  Data Source: {DEFAULT_CONFIG.data_source}")
    
    # Create generator with default config
    print("\n--- Using Default Configuration ---")
    generator = IMSDataGenerator(seed=42)
    
    # Generate sample records
    print("\n--- Sample VoLTE CDR ---")
    cdr = generator.generate_volte_cdr()
    print(json.dumps(cdr, indent=2, default=str))
    
    print("\n--- Sample SIP Session ---")
    sip = generator.generate_sip_session()
    print(json.dumps(sip, indent=2, default=str))
    
    print("\n--- Sample IMS Node Metrics ---")
    node_metrics = generator.generate_ims_node_metrics()
    print(json.dumps(node_metrics, indent=2, default=str))
    
    print("\n--- Sample Subscriber Session ---")
    sub_session = generator.generate_subscriber_session()
    print(json.dumps(sub_session, indent=2, default=str))
    
    # Example with custom configuration
    print("\n" + "=" * 70)
    print("Custom Configuration Example")
    print("=" * 70)
    
    custom_config = IMSGeneratorConfig(
        device_count=50,
        num_subscribers=1000,
        locations=["site-alpha", "site-beta", "site-gamma"],
        call_answer_rate=0.90,
        anomaly_rate=0.10,
        data_source="custom_ims_source"
    )
    
    print(f"\n--- Custom Configuration ---")
    print(f"  Device Count: {custom_config.device_count}")
    print(f"  Subscribers: {custom_config.num_subscribers}")
    print(f"  Locations: {custom_config.locations}")
    print(f"  Call Answer Rate: {custom_config.call_answer_rate * 100}%")
    print(f"  Anomaly Rate: {custom_config.anomaly_rate * 100}%")
    print(f"  Data Source: {custom_config.data_source}")
    
    custom_generator = IMSDataGenerator(config=custom_config, seed=123)
    custom_cdr = custom_generator.generate_volte_cdr()
    print(f"\n--- Custom Config CDR Sample ---")
    print(f"  Device ID: {custom_cdr['originating_device_id']}")
    print(f"  Location: {custom_cdr['originating_location']}")
    print(f"  Data Source: {custom_cdr['data_source']}")
    
    print("\n--- Join Keys for GCP Integration ---")
    print("The following fields can be used to join with GCP gold tables:")
    print("  - device_id (matches dim_devices.device_id)")
    print("  - location (matches dim_devices.location)")
    print("  - device_key (matches dim_devices.device_key)")
    print("  - region (matches dim_devices.region)")
    
    print("\n--- Configuration Options ---")
    print("Available IMSGeneratorConfig parameters:")
    for field_name, field_type in IMSGeneratorConfig.__annotations__.items():
        default_val = getattr(DEFAULT_CONFIG, field_name)
        if isinstance(default_val, list):
            print(f"  {field_name}: List (default: {len(default_val)} items)")
        else:
            print(f"  {field_name}: {type(default_val).__name__} = {default_val}")
    
    print("\n" + "=" * 70)

