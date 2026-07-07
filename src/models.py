from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass
class MQTTPacket:
    received_at: datetime
    topic: str
    packet_type: str
    sender: str
    from_num: Optional[int]
    to_num: Optional[int]
    channel: Optional[int]
    raw_json: str
    summary: str
    packet_id:  Optional[str] = None
    db_id:      Optional[int] = None
    source_tag: str = "mqtt_json"

    def to_display_row(self) -> list:
        if self.to_num == 4294967295:
            to_str = "BCAST"
        elif self.to_num is not None:
            to_str = f"!{self.to_num:08x}"
        else:
            to_str = ""
        return [
            self.received_at.strftime("%m-%d %H:%M:%S"),
            self.topic,
            self.packet_type,
            self.sender,
            f"!{self.from_num:08x}" if self.from_num is not None else "",
            to_str,
            str(self.channel) if self.channel is not None else "",
            self.summary,
        ]


@dataclass
class Node:
    node_id: str

    # Identity
    long_name: Optional[str] = None
    short_name: Optional[str] = None
    hardware: Optional[str] = None
    role: Optional[str] = None
    firmware_version: Optional[str] = None
    region: Optional[str] = None

    # Position
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude: Optional[float] = None
    location_name: Optional[str] = None
    position_precision: Optional[int] = None

    # Timestamps
    first_seen: Optional[datetime] = None
    last_heard: Optional[datetime] = None           # max of all sources
    last_mqtt_seen: Optional[datetime] = None
    last_reference_seen: Optional[datetime] = None
    last_position_seen: Optional[datetime] = None
    last_nodeinfo_seen: Optional[datetime] = None
    last_telemetry_seen: Optional[datetime] = None
    last_text_seen: Optional[datetime] = None
    last_map_seen: Optional[datetime] = None

    # Counters
    packet_count: int = 0
    position_count: int = 0
    telemetry_count: int = 0
    nodeinfo_count: int = 0
    message_count: int = 0

    # Source tracking
    sources_seen: List[str] = field(default_factory=list)
    last_source: Optional[str] = None
    last_packet_type: Optional[str] = None
    last_topic: Optional[str] = None

    # MQTT routing metadata — which roots and channels this node has been seen on
    seen_roots:    List[str] = field(default_factory=list)
    seen_channels: List[str] = field(default_factory=list)

    # Computed intelligence fields (set by app.py via intelligence module)
    status: str = "Unknown"
    is_local: Optional[bool] = None
    distance_miles: Optional[float] = None

    # ── Display helpers ───────────────────────────────────────────────────────

    def display_name(self) -> str:
        return self.long_name or self.short_name or self.node_id

    def source_label(self) -> str:
        # MAP requires a successfully decoded map report (timestamped).
        # Reference imports and other non-mqtt_json sources are NOT MAP.
        s        = set(self.sources_seen)
        has_mqtt = "mqtt_json" in s
        has_map  = self.last_map_seen is not None
        has_ref  = "meshmap_reference" in s and not has_mqtt and not has_map
        if has_mqtt and has_map:
            return "MQTT+MAP"
        if has_mqtt:
            return "MQTT"
        if has_map:
            return "MAP"
        if has_ref:
            return "REF"
        return "MEMORY"

    @property
    def position_source(self) -> Optional[str]:
        """Which source contributed the stored GPS position."""
        if self.latitude is None:
            return None
        if self.last_position_seen is not None:
            return "MQTT JSON"
        if self.last_map_seen is not None:
            return "MAP"
        return "unknown"

    def age_str(self) -> str:
        if self.last_heard is None:
            return ""
        delta = datetime.now() - self.last_heard
        s = int(delta.total_seconds())
        if s < 60:
            return f"{s}s ago"
        if s < 3600:
            return f"{s // 60}m ago"
        if s < 86400:
            return f"{s // 3600}h ago"
        return f"{s // 86400}d ago"

    def to_display_row(self) -> list:
        """13 columns for the Node List tab."""
        loc = self.location_name or (
            f"{self.latitude:.4f}, {self.longitude:.4f}"
            if self.latitude is not None else ""
        )
        dist = f"{self.distance_miles:.1f} mi" if self.distance_miles is not None else ""
        last_s = self.last_heard.strftime("%m-%d %H:%M") if self.last_heard else ""
        first_s = self.first_seen.strftime("%m-%d %H:%M") if self.first_seen else ""
        return [
            self.display_name(),
            self.node_id,
            self.status,
            self.source_label(),
            loc,
            dist,
            last_s,
            first_s,
            str(self.packet_count),
            str(self.position_count),
            str(self.telemetry_count),
            str(self.message_count),
            self.hardware or "",
        ]
