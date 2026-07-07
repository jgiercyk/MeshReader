"""
Import node records from external reference sources (MeshMap JSON, CSV, etc.)
Returns a list of Node objects with source set to meshmap_reference.
Field names are tried in multiple variants to handle different export formats.
"""

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

from models import Node
from packet_parser import normalize_node_id

SOURCE_NAME = "meshmap_reference"


def import_file(path: str) -> Tuple[List[Node], int, List[str]]:
    """Return (nodes, success_count, error_messages)."""
    p = Path(path)
    suffix = p.suffix.lower()
    try:
        if suffix == ".csv":
            return _import_csv(p)
        elif suffix in (".json", ".jsonl"):
            return _import_json(p)
        else:
            return [], 0, [f"Unsupported file type: {suffix} — use .json, .jsonl, or .csv"]
    except Exception as exc:
        return [], 0, [f"Import failed: {exc}"]


# ── Format-specific parsers ───────────────────────────────────────────────────

def _import_json(path: Path) -> Tuple[List[Node], int, List[str]]:
    with open(path, encoding="utf-8") as f:
        text = f.read().strip()

    # Try: JSONL, JSON array, JSON object-map, wrapped {"nodes": [...]}
    raw_list: list = []
    try:
        if text.startswith("{") and "\n" in text:
            raw_list = [json.loads(line) for line in text.splitlines() if line.strip()]
        else:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                raw_list = parsed
            elif isinstance(parsed, dict):
                raw_list = parsed.get("nodes", list(parsed.values()))
    except json.JSONDecodeError as exc:
        return [], 0, [f"JSON parse error: {exc}"]

    nodes, errors = _parse_records(raw_list)
    return nodes, len(nodes), errors


def _import_csv(path: Path) -> Tuple[List[Node], int, List[str]]:
    with open(path, newline="", encoding="utf-8") as f:
        raw_list = list(csv.DictReader(f))
    nodes, errors = _parse_records(raw_list)
    return nodes, len(nodes), errors


def _parse_records(raw_list: list) -> Tuple[List[Node], List[str]]:
    nodes, errors = [], []
    for rec in raw_list:
        if not isinstance(rec, dict):
            continue
        node = _parse_record(rec)
        if node:
            nodes.append(node)
        else:
            errors.append(f"Skipped (no valid node_id): {str(rec)[:80]}")
    return nodes, errors


# ── Single-record parser ──────────────────────────────────────────────────────

def _g(rec: dict, *keys, default=None):
    """Try multiple key spellings; return first non-blank match."""
    for k in keys:
        v = rec.get(k)
        if v is not None and str(v).strip() not in ("", "null", "None", "0"):
            return v
    return default


def _to_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
        return None if f == 0.0 else f
    except (ValueError, TypeError):
        return None


def _to_dt(v) -> Optional[datetime]:
    if v is None:
        return None
    # Unix epoch integer (MeshMap exports)
    try:
        return datetime.fromtimestamp(int(v))
    except (ValueError, TypeError, OSError):
        pass
    # ISO 8601 string (Command Post node reference exports)
    try:
        s = str(v).strip().rstrip("Z")
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None


def _parse_record(rec: dict) -> Optional[Node]:
    raw_id = _g(rec, "id", "node_id", "nodeId", "Id", "nodeNum")
    if not raw_id:
        return None
    node_id = normalize_node_id(raw_id)
    if not node_id:
        return None

    last_seen = _to_dt(_g(rec, "lastHeard", "last_heard", "lastSeen", "last_seen", "time"))
    now = datetime.now()

    lat = _to_float(_g(rec, "latitude", "lat"))
    lon = _to_float(_g(rec, "longitude", "lon", "lng"))
    alt = _to_float(_g(rec, "altitude", "alt"))

    hw = _g(rec, "hwModel", "hw_model", "hardware_model", "hardware")

    return Node(
        node_id=node_id,
        long_name=_g(rec, "longName", "long_name", "longname", "name"),
        short_name=_g(rec, "shortName", "short_name", "shortname"),
        hardware=str(hw) if hw else None,
        role=_g(rec, "role"),
        firmware_version=_g(rec, "firmwareVersion", "firmware_version"),
        region=_g(rec, "region"),
        latitude=lat,
        longitude=lon,
        altitude=alt,
        location_name=_g(rec, "location_name", "locationName"),
        first_seen=last_seen or now,
        last_heard=last_seen,
        last_reference_seen=last_seen or now,
        sources_seen=[SOURCE_NAME],
        last_source=SOURCE_NAME,
        status="Reference Only",
    )
