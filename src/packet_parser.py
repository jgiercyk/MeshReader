import json
import hashlib
from datetime import datetime
from typing import Optional, Dict, Any

from models import MQTTPacket

HARDWARE_NAMES: Dict[int, str] = {
    0: "UNSET", 1: "TLORA_V2", 2: "TLORA_V1", 3: "TLORA_V2_1_1P6",
    4: "TBEAM", 6: "HELTEC_V2_0", 7: "TBEAM_V0P7", 8: "T_ECHO",
    9: "TLORA_V1_3", 10: "RAK4631", 11: "HELTEC_V2_1", 12: "HELTEC_V1",
    13: "LILYGO_TBEAM_S3_CORE", 14: "RAK11200", 15: "NANO_G1",
    16: "TLORA_V2_1_1P8", 17: "TLORA_T3_S3", 18: "NANO_G1_EXPLORER",
    19: "NANO_G2_ULTRA", 20: "LORA_TYPE", 21: "WIPHONE", 22: "WIO_WM1110",
    23: "RAK2560", 24: "HELTEC_HRU_3601", 25: "STATION_G1", 26: "RAK11310",
    27: "SENSELORA_RP2040", 28: "SENSELORA_S3", 29: "CANARYONE",
    30: "RP2040_LORA", 31: "STATION_G2", 32: "LORA_RELAY_V1",
    33: "NRF52840DK", 34: "PPR", 36: "NRF52_UNKNOWN", 37: "PORTDUINO",
    38: "ANDROID_SIM", 39: "DIY_V1", 40: "NRF52840_PCA10059",
    42: "M5STACK", 43: "HELTEC_V3", 44: "HELTEC_WSL_V3",
    47: "RPI_PICO", 48: "HELTEC_WIRELESS_TRACKER",
    49: "HELTEC_WIRELESS_PAPER", 50: "T_DECK", 51: "T_WATCH_S3",
    53: "HELTEC_HT62", 56: "ESP32_DIY_1W", 58: "UNPHONE",
    60: "CDEBYTE_EORA_S3", 63: "RADIOMASTER_900_BANDIT_NANO",
    64: "HELTEC_CAPSULE_SENSOR_V3", 65: "HELTEC_VISION_MASTER_T190",
    66: "HELTEC_VISION_MASTER_E213", 67: "HELTEC_VISION_MASTER_E290",
    68: "HELTEC_MESH_NODE_T114", 69: "SENSECAP_INDICATOR",
    70: "TRACKER_T1000_E", 71: "RAK3172", 72: "WIO_E5",
    73: "RADIOMASTER_900_BANDIT", 75: "RP2040_FEATHER_RFM95",
    76: "M5STACK_COREBASIC", 77: "M5STACK_CORE2", 78: "RPI_PICO2",
    79: "M5STACK_CORES3", 80: "SEEED_XIAO_S3",
}


def _hw_name(hw: Any) -> str:
    if isinstance(hw, int):
        return HARDWARE_NAMES.get(hw, f"HW_{hw}")
    return str(hw) if hw else ""


def _lat_lon(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    v = float(raw)
    if v == 0.0:
        return None  # lat_i/lon_i == 0 means no GPS fix or position sharing disabled
    return v / 1e7 if abs(v) > 1000 else v


def normalize_node_id(value: Any) -> Optional[str]:
    """Return a consistent '!hex' key for a node ID.

    Handles: integer, '!bfea70e2', '0xbfea70e2', decimal uint32 string,
    bare 8-char hex string.  All forms → lowercase '!xxxxxxxx'.
    """
    if value is None:
        return None
    if isinstance(value, int):
        return f"!{value:08x}"
    text = str(value).strip().lower()
    if not text:
        return None
    if text.startswith("0x"):
        try:
            return f"!{int(text, 16):08x}"
        except ValueError:
            return text
    if text.startswith("!"):
        return text
    # Pure decimal node number (Meshtastic JSON "from"/"to" as decimal strings)
    if text.isdigit():
        try:
            num = int(text)
            if 0 <= num <= 0xFFFFFFFF:
                return f"!{num:08x}"
        except ValueError:
            pass
    # Bare hex string (no prefix) — treat as node ID if it looks hex
    if len(text) <= 8 and all(c in "0123456789abcdef" for c in text):
        try:
            return f"!{int(text, 16):08x}"
        except ValueError:
            pass
    return text


def node_id_to_int(value: Any) -> Optional[int]:
    """Parse a node ID to its integer representation.

    Handles: integer, '!bfea70e2', '0xbfea70e2', plain decimal string.
    Returns None instead of raising on unrecognised formats.
    """
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip().lower()
    try:
        if text.startswith("!"):
            return int(text[1:], 16)
        if text.startswith("0x"):
            return int(text, 16)
        return int(text)
    except ValueError:
        return None


def extract_map_node_id(topic: str) -> Optional[str]:
    """Extract node ID from a map-report topic.

    Expected form: msh/<region>/2/map/<node_id>
    Returns None for the base topic (no node ID segment) or unrecognised forms.
    """
    parts = topic.rstrip("/").split("/")
    try:
        idx = parts.index("map")
    except ValueError:
        return None
    if idx + 1 >= len(parts):
        return None
    candidate = parts[idx + 1].strip()
    if not candidate:
        return None
    return normalize_node_id(candidate)


def parse_packet(
    topic: str,
    payload_str: str,
    source_tag: str = "mqtt_json",
) -> Optional["MQTTPacket"]:
    """Parse a JSON-source MQTT payload into an MQTTPacket.

    Returns None if the payload is not valid JSON or not a JSON object.
    The caller is responsible for counting errors / ignored packets.
    This function must NOT be called for map_report or protobuf sources.
    """
    now = datetime.now()
    try:
        data = json.loads(payload_str)
    except (json.JSONDecodeError, ValueError):
        import logging as _log
        _log.debug(
            "JSON parse failed: source=%s topic=%s len=%d preview=%r",
            source_tag, topic, len(payload_str), payload_str[:48],
        )
        return None   # caller records as ignored/error

    if not isinstance(data, dict):
        return None

    packet_type = str(data.get("type", "unknown"))
    sender = normalize_node_id(data.get("sender")) or "unknown"
    from_num = node_id_to_int(data.get("from"))
    to_num   = node_id_to_int(data.get("to"))
    channel  = data.get("channel")
    packet_id = data.get("id")
    summary = _build_summary(packet_type, data)

    return MQTTPacket(
        received_at=now,
        topic=topic,
        packet_type=packet_type,
        sender=sender,
        from_num=from_num,
        to_num=to_num,
        channel=int(channel) if channel is not None else None,
        raw_json=payload_str,
        summary=summary,
        packet_id=str(packet_id) if packet_id is not None else None,
        source_tag=source_tag,
    )


def _build_summary(packet_type: str, data: Dict[str, Any]) -> str:
    payload = data.get("payload", {})
    try:
        if packet_type == "text":
            if isinstance(payload, str):
                return f"Text: {payload[:100]}"
            if isinstance(payload, dict):
                return f"Text: {payload.get('text', str(payload))[:100]}"
            return "Text message"

        if packet_type == "nodeinfo" and isinstance(payload, dict):
            ln = payload.get("longname", payload.get("long_name", ""))
            sn = payload.get("shortname", payload.get("short_name", ""))
            hw = _hw_name(payload.get("hw_model", payload.get("hardware")))
            result = f"NodeInfo: {ln} ({sn})"
            if hw:
                result += f" [{hw}]"
            return result

        if packet_type == "position" and isinstance(payload, dict):
            lat = _lat_lon(payload.get("latitude_i", payload.get("latitude")))
            lon = _lat_lon(payload.get("longitude_i", payload.get("longitude")))
            alt = payload.get("altitude", "")
            if lat is not None and lon is not None:
                s = f"Position: {lat:.5f}, {lon:.5f}"
                if alt:
                    s += f" alt={alt}m"
                return s
            return "Position (no coords)"

        if packet_type == "telemetry" and isinstance(payload, dict):
            # Merge nested sub-objects and flat fields; nested takes precedence
            _sub_keys = {"device_metrics", "environment_metrics", "power_metrics"}
            dm = payload.get("device_metrics") or {}
            em = payload.get("environment_metrics") or {}
            flat = {k: v for k, v in payload.items() if k not in _sub_keys}
            m = {**flat, **em, **dm}
            parts = []
            if "battery_level" in m:
                parts.append(f"bat={m['battery_level']}%")
            if "voltage" in m:
                parts.append(f"V={m['voltage']:.2f}v")
            if "channel_utilization" in m:
                parts.append(f"chUtil={m['channel_utilization']:.1f}%")
            if "air_util_tx" in m:
                parts.append(f"airTx={m['air_util_tx']:.1f}%")
            if "temperature" in m:
                parts.append(f"temp={m['temperature']:.1f}°C")
            if "relative_humidity" in m:
                parts.append(f"hum={m['relative_humidity']:.0f}%")
            if "barometric_pressure" in m:
                parts.append(f"pres={m['barometric_pressure']:.0f}hPa")
            return "Telemetry: " + ", ".join(parts) if parts else "Telemetry"

        if packet_type == "neighborinfo" and isinstance(payload, dict):
            n = len(payload.get("neighbors", []))
            return f"NeighborInfo: {n} neighbor(s)"

        if packet_type == "mapreport" and isinstance(payload, dict):
            ln = payload.get("long_name", payload.get("longname", ""))
            return f"MapReport: {ln}" if ln else "MapReport"

        if packet_type == "paxcounter" and isinstance(payload, dict):
            return f"PaxCounter: BLE={payload.get('ble', 0)} WiFi={payload.get('wifi', 0)}"

        return packet_type

    except Exception:
        return packet_type


def extract_node_updates(packet: "MQTTPacket") -> Optional[Dict[str, Any]]:
    """Return dict of node fields to update, or None if nothing useful."""
    if packet.packet_type not in ("nodeinfo", "position", "mapreport"):
        return None

    try:
        data = json.loads(packet.raw_json)
    except (json.JSONDecodeError, ValueError):
        return None

    if not isinstance(data, dict):
        return None

    payload = data.get("payload", {})
    if not isinstance(payload, dict):
        return None

    # 'from' identifies the node that originated the data.  Use it when present;
    # fall back to sender only for injected/gateway-only packets where from is absent.
    node_id = normalize_node_id(packet.from_num) if packet.from_num is not None else packet.sender
    updates: Dict[str, Any] = {"node_id": node_id}

    try:
        if packet.packet_type == "nodeinfo":
            ln = payload.get("longname", payload.get("long_name"))
            sn = payload.get("shortname", payload.get("short_name"))
            hw = payload.get("hw_model", payload.get("hardware"))
            if ln:
                updates["long_name"] = ln
            if sn:
                updates["short_name"] = sn
            if hw is not None:
                updates["hardware"] = _hw_name(hw)

        elif packet.packet_type == "position":
            lat = _lat_lon(payload.get("latitude_i", payload.get("latitude")))
            lon = _lat_lon(payload.get("longitude_i", payload.get("longitude")))
            alt = payload.get("altitude")
            prec = payload.get("precision_bits", payload.get("position_precision"))
            if lat is not None:
                updates["latitude"] = lat
            if lon is not None:
                updates["longitude"] = lon
            if alt is not None:
                updates["altitude"] = float(alt)
            if prec is not None:
                updates["position_precision"] = int(prec)

        elif packet.packet_type == "mapreport":
            ln = payload.get("long_name", payload.get("longname"))
            sn = payload.get("short_name", payload.get("shortname"))
            lat = _lat_lon(payload.get("latitude_i", payload.get("latitude")))
            lon = _lat_lon(payload.get("longitude_i", payload.get("longitude")))
            alt = payload.get("altitude")
            hw = payload.get("hw_model")
            if ln:
                updates["long_name"] = ln
            if sn:
                updates["short_name"] = sn
            if lat is not None:
                updates["latitude"] = lat
            if lon is not None:
                updates["longitude"] = lon
            if alt is not None:
                updates["altitude"] = float(alt)
            if hw is not None:
                updates["hardware"] = _hw_name(hw)

    except Exception:
        pass

    return updates if len(updates) > 1 else None


def compute_packet_hash(topic: str, payload: str, ts: datetime) -> str:
    bucket = ts.strftime("%Y%m%d%H%M")
    raw = f"{topic}|{payload}|{bucket}"
    return hashlib.sha256(raw.encode()).hexdigest()[:20]
