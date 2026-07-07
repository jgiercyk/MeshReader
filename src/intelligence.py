"""
Pure functions for node intelligence: status, distance, local-area classification.
No Qt, no DB — just computation on Node objects.
"""

import logging
from datetime import datetime, timedelta
from math import atan2, cos, radians, sin, sqrt
from typing import Optional

from models import Node

SOURCE_MQTT = "mqtt_json"
SOURCE_MAP = "meshmap_reference"
SOURCE_MANUAL = "manual_import"

STATUS_ACTIVE = "Active"
STATUS_RECENT = "Recent"
STATUS_STALE = "Stale"
STATUS_OLD = "Old"
STATUS_REFERENCE = "Reference Only"
STATUS_UNKNOWN = "Unknown"


def compute_status(
    node: Node,
    active_min: int = 15,
    recent_h: int = 8,
    old_d: int = 7,
) -> str:
    if node.last_mqtt_seen is None:
        if node.last_reference_seen is not None:
            return STATUS_REFERENCE
        return STATUS_UNKNOWN
    age = datetime.now() - node.last_mqtt_seen
    if age <= timedelta(minutes=active_min):
        return STATUS_ACTIVE
    if age <= timedelta(hours=recent_h):
        return STATUS_RECENT
    if age <= timedelta(days=old_d):
        return STATUS_STALE
    return STATUS_OLD


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 3958.8
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def node_has_valid_position(node: Node) -> bool:
    """Return True only when the node has a valid, plottable GPS position.

    Altitude alone is not a position.  A node without valid lat/lon must not
    be plotted on the map, must not have distance calculated, and must not
    retain a stale map marker.
    """
    lat, lon = node.latitude, node.longitude
    return (
        lat is not None
        and lon is not None
        and -90.0 <= lat <= 90.0
        and -180.0 <= lon <= 180.0
    )


def compute_distance(node: Node, home_lat: float, home_lon: float) -> Optional[float]:
    if not node_has_valid_position(node):
        # Log only when coordinates are present but out of valid range — that is a bug,
        # not the normal case of a node that simply has no GPS.
        if node.latitude is not None and node.longitude is not None:
            logging.error(
                "compute_distance: out-of-range coordinates on %s (lat=%s lon=%s)",
                node.node_id, node.latitude, node.longitude,
            )
        return None
    return haversine_miles(home_lat, home_lon, node.latitude, node.longitude)


def enrich_node(
    node: Node,
    home_lat: float,
    home_lon: float,
    radius_miles: float,
    active_min: int = 15,
    recent_h: int = 8,
    old_d: int = 7,
) -> Node:
    """Compute and set status / distance_miles / is_local on the node in-place."""
    node.status = compute_status(node, active_min, recent_h, old_d)
    node.distance_miles = compute_distance(node, home_lat, home_lon)
    node.is_local = (
        node.distance_miles is not None and node.distance_miles <= radius_miles
    )
    return node
