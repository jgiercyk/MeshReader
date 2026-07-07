"""
Minimal pure-Python protobuf decoder + AES-CTR decryptor for Meshtastic
map-report MQTT payloads.

Actual Meshtastic MeshPacket wire layout (current firmware):
  ServiceEnvelope {
    packet    (field 1): MeshPacket {
      from      (field 1): fixed32   ← sender node number → "!xxxxxxxx"
      to        (field 2): fixed32   ← destination
      channel   (field 3): uint32    ← channel index
      decoded   (field 4): Data      ← present when unencrypted
        portnum   (field 1): uint32  ← MAP_APP = 73
        payload   (field 2): bytes   ← MapReport proto bytes
      encrypted (field 5): bytes     ← present when AES-CTR encrypted
      id        (field 6): fixed32   ← packet ID (needed for AES-CTR nonce)
    }
    channel_id (field 2): string  e.g. "LongFast"
    gateway_id (field 3): string  e.g. "!75f1824c"
  }

  from/to/id use fixed32 wire_type=5.
  Lat/lon in MapReport use sfixed32 wire_type=5.
  All other integers use varint wire_type=0.

Decryption (only when field 5 is present, never when field 4 is present):
  Algorithm : AES-256-CTR
  Key       : channel PSK zero-padded to 32 bytes
  Nonce     : pack('<Q', packet_id) + pack('<Q', from_node)  [16 bytes LE]
"""
import base64
import logging
import struct
from typing import Any, Dict, List, Optional, Tuple

# ── Crypto availability ───────────────────────────────────────────────────────

try:
    from Crypto.Cipher import AES as _AES
    from Crypto.Util import Counter as _Counter
    _CRYPTO_AVAILABLE = True
    logging.debug("MAP_CRYPTO: pycryptodome available")
except ImportError:
    _CRYPTO_AVAILABLE = False
    logging.debug("MAP_CRYPTO: no crypto library — encrypted packets cannot be decrypted")

# ── Default channel keys ──────────────────────────────────────────────────────

# Meshtastic default public channel key ("AQ==" = 0x01), zero-padded to 32 bytes
# for AES-256. This is the well-known key for the default LongFast/public channel.
_DEFAULT_PSK_B64 = "AQ=="
_DEFAULT_KEY     = base64.b64decode(_DEFAULT_PSK_B64).ljust(32, b'\x00')

# Map of channel_id → AES key bytes.
# Seeded with the default public channels; can be extended at runtime via
# set_channel_key() from app config.
_CHANNEL_KEYS: Dict[str, bytes] = {}


def set_channel_key(channel_id: str, psk_b64: str) -> None:
    """Register a channel PSK for decryption. Call at startup from config."""
    try:
        key = base64.b64decode(psk_b64).ljust(32, b'\x00')
        _CHANNEL_KEYS[channel_id] = key
        logging.debug("MAP_CRYPTO: registered key for channel %r", channel_id)
    except Exception as exc:
        logging.warning("MAP_CRYPTO: bad PSK for channel %r: %s", channel_id, exc)


# ── Meshtastic port number ────────────────────────────────────────────────────

MAP_APP_PORTNUM = 73

# ── Wire-level parser ─────────────────────────────────────────────────────────

def _read_varint(data: bytes, pos: int) -> Tuple[int, int]:
    result = shift = 0
    while pos < len(data):
        b = data[pos]; pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, pos
        shift += 7
    return result, pos


def _parse_fields(data: bytes) -> Dict[int, List[Tuple[int, Any]]]:
    """
    Shallow-parse one protobuf layer into {field_num: [(wire_type, value)]}.
    Stops gracefully on any parse error.

    wire_type 0 → int
    wire_type 1 → bytes (8)
    wire_type 2 → bytes
    wire_type 5 → bytes (4)
    """
    fields: Dict[int, List[Tuple[int, Any]]] = {}
    pos, n = 0, len(data)
    while pos < n:
        try:
            tag, pos = _read_varint(data, pos)
        except Exception:
            break
        if tag == 0:
            break
        fnum  = tag >> 3
        wtype = tag & 7
        try:
            if wtype == 0:
                val, pos = _read_varint(data, pos)
            elif wtype == 1:
                if pos + 8 > n: break
                val = data[pos:pos+8]; pos += 8
            elif wtype == 2:
                length, pos = _read_varint(data, pos)
                if pos + length > n: break
                val = data[pos:pos+length]; pos += length
            elif wtype == 5:
                if pos + 4 > n: break
                val = data[pos:pos+4]; pos += 4
            else:
                break
        except Exception:
            break
        fields.setdefault(fnum, []).append((wtype, val))
    return fields


def _str_f(fields: dict, num: int) -> Optional[str]:
    for wt, val in fields.get(num, []):
        if wt == 2:
            try:
                return val.decode("utf-8")
            except Exception:
                return val.decode("latin-1", errors="replace")
    return None


def _int_f(fields: dict, num: int) -> Optional[int]:
    """Return first integer for field_num. Handles varint (wt=0) and fixed32 (wt=5)."""
    for wt, val in fields.get(num, []):
        if wt == 0:
            return val
        if wt == 5:
            return struct.unpack('<I', val)[0]
    return None


def _bytes_f(fields: dict, num: int) -> Optional[bytes]:
    for wt, val in fields.get(num, []):
        if wt == 2:
            return val
    return None


def _i32(n: int) -> int:
    """Reinterpret a protobuf uint32 varint as signed int32 (for lat/lon)."""
    n &= 0xFFFFFFFF
    return n - (1 << 32) if n >= (1 << 31) else n


# ── Proto field numbers ───────────────────────────────────────────────────────

# ServiceEnvelope
_SE_PACKET    = 1
_SE_CHANNEL   = 2
_SE_GATEWAY   = 3

# MeshPacket — actual Meshtastic wire layout (firmware 2.x)
# "from" is a Python keyword; we access via raw field number, no naming conflict.
_MP_FROM      = 1   # fixed32 sender node_num
_MP_TO        = 2   # fixed32 destination
_MP_CHANNEL   = 3   # uint32 channel index
_MP_DECODED   = 4   # Data (unencrypted)
_MP_ENCRYPTED = 5   # bytes (AES-CTR ciphertext)
_MP_ID        = 6   # fixed32 packet ID — needed for AES-CTR nonce

# Human-readable names for debug output
_MP_FIELD_NAMES = {
    _MP_FROM:      "from",
    _MP_TO:        "to",
    _MP_CHANNEL:   "channel",
    _MP_DECODED:   "decoded",
    _MP_ENCRYPTED: "encrypted",
    _MP_ID:        "id",
}

# Data
_D_PORTNUM    = 1
_D_PAYLOAD    = 2

# MapReport — matches Meshtastic mqtt.proto firmware 2.5+
# NOTE: field 8 is has_default_channel (bool), NOT latitude_i.
# latitude_i is field 9, longitude_i is field 10.
_MR_LONG_NAME  = 1
_MR_SHORT_NAME = 2
_MR_ROLE       = 3   # Config.DeviceConfig.Role enum
_MR_HW_MODEL   = 4
_MR_FIRMWARE   = 5
_MR_REGION     = 6
_MR_MODEM_PRE  = 7
_MR_HAS_DEF_CH = 8   # bool — has_default_channel (NOT a coord field)
_MR_LATITUDE_I = 9   # sfixed32 degrees×1e7
_MR_LONGITUDE_I= 10  # sfixed32 degrees×1e7
_MR_ALTITUDE   = 11  # int32 meters (varint; apply _i32 for sign)
_MR_PRECISION  = 12
_MR_NUM_NODES  = 13
_MR_HAS_OPTED  = 14  # has_opted_report_location

_MR_FIELD_NAMES = {
    _MR_LONG_NAME:  "long_name",
    _MR_SHORT_NAME: "short_name",
    _MR_ROLE:       "role",
    _MR_HW_MODEL:   "hw_model",
    _MR_FIRMWARE:   "firmware_version",
    _MR_REGION:     "region",
    _MR_MODEM_PRE:  "modem_preset",
    _MR_HAS_DEF_CH: "has_default_channel",
    _MR_LATITUDE_I: "latitude_i",
    _MR_LONGITUDE_I:"longitude_i",
    _MR_ALTITUDE:   "altitude",
    _MR_PRECISION:  "position_precision",
    _MR_NUM_NODES:  "num_online_local_nodes",
    _MR_HAS_OPTED:  "has_opted_report_location",
}


# ── Decryption ────────────────────────────────────────────────────────────────

def _get_channel_key(channel_id: Optional[str]) -> Optional[bytes]:
    """Return AES key for channel_id, or None if channel is unknown/private."""
    if channel_id and channel_id in _CHANNEL_KEYS:
        return _CHANNEL_KEYS[channel_id]
    # Default public channels
    if channel_id in (None, "", "LongFast", "2", "0"):
        return _DEFAULT_KEY
    return None


def _try_decrypt(
    enc_bytes:  bytes,
    channel_id: Optional[str],
    from_node:  int,
    packet_id:  int,
) -> Optional[bytes]:
    """
    Attempt AES-256-CTR decryption of enc_bytes.
    Returns decrypted Data bytes (validated as parseable protobuf) or None.

    Nonce (16 bytes, little-endian):
      bytes  0-7 : packet_id as uint64 LE
      bytes 8-15 : from_node as uint64 LE
    """
    if not _CRYPTO_AVAILABLE:
        return None

    key = _get_channel_key(channel_id)
    if key is None:
        return None

    try:
        nonce     = struct.pack('<Q', packet_id & 0xFFFFFFFF) + \
                    struct.pack('<Q', from_node & 0xFFFFFFFF)
        nonce_int = int.from_bytes(nonce, 'big')
        ctr       = _Counter.new(128, initial_value=nonce_int)
        cipher    = _AES.new(key, _AES.MODE_CTR, counter=ctr)
        decrypted = cipher.decrypt(enc_bytes)

        # Validate: parse as Data; accept if portnum field is in plausible range
        test = _parse_fields(decrypted)
        portnum = _int_f(test, _D_PORTNUM)
        if portnum is not None and (portnum <= 0 or portnum > 512):
            return None
        if not test:
            return None
        return decrypted
    except Exception as exc:
        logging.debug("MAP_DECRYPT: exception: %s", exc)
        return None


# ── Public API ────────────────────────────────────────────────────────────────

def decode_map_payload(
    payload_bytes: bytes,
) -> Tuple[Optional[dict], dict]:
    """
    Decode a Meshtastic map-report MQTT payload.

    Returns (result, debug_info):
      result     — node field dict on success, None on failure
      debug_info — always populated for caller logging

    Node ID comes from MeshPacket.from (→ "!xxxxxxxx") or gateway_id.
    MapReport itself carries no node identity.

    Result dict keys:
      node_id, long_name, short_name, hw_model_int, firmware_version,
      region, latitude, longitude, altitude, position_precision
    """
    dbg: dict = {
        "payload_len":           len(payload_bytes),
        "payload_hex":           payload_bytes[:16].hex(),
        "gateway_id":            None,
        "channel_id":            None,
        "from_num":              None,
        "from_id":               None,
        "to_num":                None,
        "packet_id":             None,
        "portnum":               None,
        "mp_fields":             [],
        "mp_field_names":        {},
        "has_decoded":           False,
        "has_encrypted":         False,
        "enc_len":               None,
        "decoded_fields":        {},
        "decoded_payload_len":   0,
        "decoded_payload_hex":   "",
        "mr_fields":             {},
        "long_name":             None,
        "short_name":            None,
        "lat_i_raw":             None,
        "lon_i_raw":             None,
        "lat_deg":               None,
        "lon_deg":               None,
        "alt_signed":            None,
        "pos_valid":             False,
        "pos_reject_reason":     None,
        "alt_valid":             False,
        "decrypted_with":        None,
        "crypto_available":      _CRYPTO_AVAILABLE,
        "fail":                  None,
    }

    if not payload_bytes:
        dbg["fail"] = "empty_payload"
        return None, dbg

    # ── Stage 1: ServiceEnvelope ──────────────────────────────────────────────
    se_fields  = _parse_fields(payload_bytes)
    mesh_bytes = _bytes_f(se_fields, _SE_PACKET)
    gateway_id = _str_f(se_fields, _SE_GATEWAY)
    channel_id = _str_f(se_fields, _SE_CHANNEL)
    dbg["gateway_id"] = gateway_id
    dbg["channel_id"] = channel_id

    if not mesh_bytes:
        logging.debug("MAP: no SE.packet field — trying raw MapReport  hex=%s",
                      dbg["payload_hex"])
        result = _decode_map_report_bytes(payload_bytes, node_id=None, dbg=dbg)
        if result is None:
            dbg["fail"] = "no_se_and_raw_mr_failed"
        return result, dbg

    # ── Stage 2: MeshPacket ───────────────────────────────────────────────────
    mp_fields    = _parse_fields(mesh_bytes)
    # from and to are fixed32 (wire_type=5); _int_f handles both varint and fixed32
    from_num     = _int_f(mp_fields, _MP_FROM)   # None or 0 → not encoded
    to_num       = _int_f(mp_fields, _MP_TO)
    packet_id    = _int_f(mp_fields, _MP_ID) or 0
    data_bytes   = _bytes_f(mp_fields, _MP_DECODED)     # field 4 — present when unencrypted
    enc_bytes    = _bytes_f(mp_fields, _MP_ENCRYPTED)   # field 5 — present when encrypted

    mp_field_names = {
        n: _MP_FIELD_NAMES.get(n, f"#{n}")
        for n in sorted(mp_fields.keys())
    }

    dbg["from_num"]  = from_num
    dbg["from_id"]   = f"!{from_num:08x}" if from_num else None
    dbg["to_num"]    = to_num
    dbg["packet_id"] = packet_id
    dbg["mp_fields"] = sorted(mp_fields.keys())
    dbg["mp_field_names"] = mp_field_names
    dbg["has_decoded"]    = data_bytes is not None
    dbg["has_encrypted"]  = enc_bytes is not None
    dbg["enc_len"]   = len(enc_bytes) if enc_bytes else None

    # Derive reliable node_id: prefer from_num, fall back to gateway_id
    node_id = (f"!{from_num:08x}" if from_num else None) or gateway_id

    logging.debug(
        "MAP_MP: from=%s (field1)  to=0x%x  packet_id=%s  node_id=%s  "
        "WhichOneof=%s  fields=%s",
        dbg["from_id"],
        to_num or 0,
        packet_id,
        node_id,
        ("decoded" if data_bytes else "encrypted" if enc_bytes else "none"),
        mp_field_names,
    )

    # ── Stage 3: use decoded directly, or decrypt if only encrypted field present
    # Field 4 (decoded) present → use it directly; NEVER attempt decryption.
    # Field 5 (encrypted) present, field 4 absent → attempt AES-CTR decryption.
    if data_bytes:
        # Decoded payload present — no decryption needed or wanted
        pass

    elif enc_bytes:
        gateway_node = 0
        if gateway_id and gateway_id.startswith("!") and len(gateway_id) > 1:
            try:
                gateway_node = int(gateway_id[1:], 16)
            except ValueError:
                pass

        candidates = list(dict.fromkeys([from_num or 0, gateway_node, 0]))
        for candidate_from in candidates:
            result = _try_decrypt(enc_bytes, channel_id, candidate_from, packet_id)
            if result is not None:
                data_bytes = result
                dbg["decrypted_with"] = candidate_from
                logging.debug(
                    "MAP_DECRYPT: success  channel=%s  from_node=%s  packet_id=%s",
                    channel_id, candidate_from, packet_id,
                )
                break

        if not data_bytes:
            if not _CRYPTO_AVAILABLE:
                dbg["fail"] = "encrypted_no_crypto_lib"
            elif _get_channel_key(channel_id) is None:
                dbg["fail"] = "encrypted_unknown_channel"
            else:
                dbg["fail"] = "encrypted"
            return None, dbg

    else:
        dbg["fail"] = "no_decoded_or_encrypted_field"
        return None, dbg

    # ── Stage 4: Data message ─────────────────────────────────────────────────
    d_fields  = _parse_fields(data_bytes)
    portnum   = _int_f(d_fields, _D_PORTNUM)
    map_bytes = _bytes_f(d_fields, _D_PAYLOAD)
    dbg["portnum"]           = portnum
    dbg["decoded_fields"]    = {n: f"#{n}" for n in sorted(d_fields.keys())}
    dbg["decoded_payload_len"] = len(map_bytes) if map_bytes else 0
    dbg["decoded_payload_hex"] = map_bytes[:32].hex() if map_bytes else ""

    logging.debug(
        "MAP_DATA: portnum=%s  payload_len=%s  payload_hex=%s  data_fields=%s",
        portnum,
        dbg["decoded_payload_len"],
        dbg["decoded_payload_hex"],
        dbg["decoded_fields"],
    )

    # For map topics, accept missing portnum (0/default) — treat as MapReport
    if portnum is not None and portnum != MAP_APP_PORTNUM:
        dbg["fail"] = f"wrong_portnum_{portnum}"
        logging.debug("MAP: portnum mismatch %d != %d", portnum, MAP_APP_PORTNUM)
        return None, dbg

    if not map_bytes:
        dbg["fail"] = "empty_data_payload"
        return None, dbg

    # ── Stage 5: MapReport ────────────────────────────────────────────────────
    result = _decode_map_report_bytes(map_bytes, node_id=node_id, dbg=dbg)

    if result is None:
        dbg["fail"] = dbg.get("fail") or "map_report_no_usable_fields"
        logging.debug(
            "MAP_MR: FAILED  node_id=%s  long_name=%s  short_name=%s  "
            "lat_i_raw=%s  lon_i_raw=%s  pos_reject=%s",
            node_id, dbg.get("long_name"), dbg.get("short_name"),
            dbg.get("lat_i_raw"), dbg.get("lon_i_raw"),
            dbg.get("pos_reject_reason"),
        )
    else:
        logging.debug(
            "MAP_MR: OK  node_id=%s  name=%r/%r  lat=%.6f  lon=%.6f  alt=%s",
            result.get("node_id"),
            result.get("long_name"), result.get("short_name"),
            result.get("latitude") or 0.0,
            result.get("longitude") or 0.0,
            result.get("altitude"),
        )

    return result, dbg


# ── Internal: MapReport field decoder ────────────────────────────────────────

def _decode_map_report_bytes(
    data: bytes,
    node_id: Optional[str],
    dbg: Optional[dict] = None,
) -> Optional[dict]:
    fields = _parse_fields(data)
    if not fields:
        return None

    mr_field_names = {n: _MR_FIELD_NAMES.get(n, f"#{n}") for n in sorted(fields.keys())}

    long_name  = _str_f(fields, _MR_LONG_NAME)
    short_name = _str_f(fields, _MR_SHORT_NAME)
    role       = _int_f(fields, _MR_ROLE)
    hw_model   = _int_f(fields, _MR_HW_MODEL)
    firmware   = _str_f(fields, _MR_FIRMWARE)
    region     = _int_f(fields, _MR_REGION)
    lat_i_raw  = _int_f(fields, _MR_LATITUDE_I)
    lon_i_raw  = _int_f(fields, _MR_LONGITUDE_I)
    alt_raw    = _int_f(fields, _MR_ALTITUDE)
    precision  = _int_f(fields, _MR_PRECISION)

    # Convert raw unsigned ints to signed degrees×1e7 (sfixed32 / int32)
    lat_signed = _i32(lat_i_raw) if lat_i_raw is not None else None
    lon_signed = _i32(lon_i_raw) if lon_i_raw is not None else None
    alt_signed = _i32(alt_raw)   if alt_raw   is not None else None

    lat_deg = lat_signed / 1e7 if lat_signed is not None else None
    lon_deg = lon_signed / 1e7 if lon_signed is not None else None

    # Position sanity check: both fields must be present, non-zero, and in range.
    # lat_i=0 / lon_i=0 means GPS not acquired — treat as absent.
    pos_valid = (
        lat_deg is not None and lon_deg is not None
        and lat_deg != 0.0 and lon_deg != 0.0
        and -90.0  <= lat_deg <=  90.0
        and -180.0 <= lon_deg <= 180.0
    )
    # Altitude sanity: apply _i32 sign fix then range-check
    alt_valid = alt_signed is not None and -500 <= alt_signed <= 10_000

    # Build a human-readable rejection reason list for logging
    pos_reasons: list = []
    if not pos_valid:
        if lat_i_raw is None:
            pos_reasons.append("missing_latitude_i")
        elif lat_deg == 0.0:
            pos_reasons.append("zero_default_latitude")
        elif lat_deg is not None and not (-90.0 <= lat_deg <= 90.0):
            pos_reasons.append("latitude_out_of_range")
        if lon_i_raw is None:
            pos_reasons.append("missing_longitude_i")
        elif lon_deg == 0.0:
            pos_reasons.append("zero_default_longitude")
        elif lon_deg is not None and not (-180.0 <= lon_deg <= 180.0):
            pos_reasons.append("longitude_out_of_range")
    if alt_raw is not None and not alt_valid:
        pos_reasons.append(f"invalid_altitude({alt_signed})")
    pos_reject_reason = ", ".join(pos_reasons) if pos_reasons else None

    if dbg is not None:
        dbg["mr_fields"]         = mr_field_names
        dbg["long_name"]         = long_name
        dbg["short_name"]        = short_name
        dbg["lat_i_raw"]         = lat_i_raw
        dbg["lon_i_raw"]         = lon_i_raw
        dbg["lat_deg"]           = lat_deg
        dbg["lon_deg"]           = lon_deg
        dbg["alt_signed"]        = alt_signed
        dbg["pos_valid"]         = pos_valid
        dbg["pos_reject_reason"] = pos_reject_reason
        dbg["alt_valid"]         = alt_valid

    # Require at least a node name or valid position to be useful
    if not (long_name or short_name or pos_valid):
        return None

    result: dict = {}
    if node_id:
        result["node_id"] = node_id
    if long_name:
        result["long_name"] = long_name
    if short_name:
        result["short_name"] = short_name
    if role is not None:
        result["role"] = str(role)
    if hw_model is not None:
        result["hw_model_int"] = hw_model
    if firmware:
        result["firmware_version"] = firmware
    if region is not None:
        result["region"] = str(region)
    # Only include position if it passed the sanity check
    if pos_valid:
        result["latitude"]  = lat_deg
        result["longitude"] = lon_deg
        if alt_valid:
            result["altitude"] = float(alt_signed)
    if precision is not None:
        result["position_precision"] = precision

    return result or None
