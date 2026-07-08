#!/usr/bin/env python3
"""
merge_node_references.py — Mesh Command Post node reference merger
──────────────────────────────────────────────────────────────────
Locates two reference-export CSVs in inputs/, merges them (newer is primary,
older fills missing nodes and blank fields), writes the merged CSV + a report,
then imports the result into the live app database.

Usage
    python merge_node_references.py              # full run
    python merge_node_references.py --dry-run   # merge + report only; skip DB import

The app must be closed before running this script.
"""

import argparse
import csv
import json
import re
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).parent
INPUTS_DIR   = PROJECT_ROOT / "inputs"
BACKUPS_DIR  = PROJECT_ROOT / "backups"
APP_DB       = Path.home() / ".mesh_command_post" / "history.db"
APP_BACKUPS  = Path.home() / ".mesh_command_post" / "backups"

NOW     = datetime.now()
NOW_STR = NOW.strftime("%Y%m%d_%H%M%S")
NOW_ISO = NOW.isoformat()

# ── Output column order (matches app export format) ───────────────────────────

CSV_COLUMNS = [
    "node_id", "long_name", "short_name", "display_name",
    "latitude", "longitude", "altitude", "location_name",
    "position_source", "position_precision",
    "first_seen", "last_seen", "last_position_seen", "last_mqtt_seen",
    "last_map_seen", "last_telemetry_seen", "last_message_seen",
    "sources_seen", "packet_count", "position_count", "telemetry_count",
    "message_count", "last_packet_type", "last_topic",
    "hardware_model", "role", "firmware_version", "region",
    "modem_preset", "channel_name", "status", "is_local",
    "distance_from_home_miles",
]

# ── Node ID normalization ─────────────────────────────────────────────────────

def normalize_node_id(raw) -> str | None:
    """Return canonical lowercase !xxxxxxxx form, or None if invalid."""
    if not raw:
        return None
    s = str(raw).strip().lower()
    hex_part = s.lstrip("!")
    if re.fullmatch(r"[0-9a-f]{8}", hex_part):
        return f"!{hex_part}"
    # Pure decimal uint32
    if re.fullmatch(r"\d+", s):
        n = int(s)
        if 0 <= n <= 0xFFFFFFFF:
            return f"!{n:08x}"
    return None


# ── Position validation ───────────────────────────────────────────────────────

def is_valid_position(lat_str, lon_str) -> bool:
    """Return True only for plottable, non-artifact coordinates."""
    try:
        lat = float(lat_str) if lat_str not in (None, "") else None
        lon = float(lon_str) if lon_str not in (None, "") else None
    except (ValueError, TypeError):
        return False
    if lat is None or lon is None:
        return False
    # Null island / Africa conga-line artifact (old wrong-field-number decode)
    if abs(lat) < 0.01:
        return False
    return -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0


# ── CSV discovery and timestamp parsing ──────────────────────────────────────

_TS_RE = re.compile(r"(\d{8})_(\d{4})")

def _parse_filename_ts(path: Path) -> datetime | None:
    """Extract export timestamp from filename like mesh_node_reference_YYYYMMDD_HHMM*.csv."""
    m = _TS_RE.search(path.stem)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M")
    except ValueError:
        return None


def find_export_files() -> list[Path]:
    """Return all node reference CSV files in INPUTS_DIR sorted oldest-first."""
    files = list(INPUTS_DIR.glob("mesh_node_reference_*.csv"))
    # Exclude already-merged output files
    files = [f for f in files if "MERGED" not in f.name]
    if not files:
        return []

    def sort_key(p: Path):
        ts = _parse_filename_ts(p)
        return ts if ts else datetime.fromtimestamp(p.stat().st_mtime)

    return sorted(files, key=sort_key)


# ── CSV reader ────────────────────────────────────────────────────────────────

def read_csv(path: Path) -> dict[str, dict]:
    """Return {node_id: row_dict} from a node-reference CSV."""
    result: dict[str, dict] = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            nid = normalize_node_id(row.get("node_id"))
            if nid:
                result[nid] = row
    return result


# ── Source-list helpers ───────────────────────────────────────────────────────

def parse_sources(v: str | None) -> list[str]:
    if not v:
        return []
    try:
        parsed = json.loads(v)
        if isinstance(parsed, list):
            return [str(s) for s in parsed if s]
    except (json.JSONDecodeError, TypeError):
        pass
    # Fallback: comma-separated
    return [s.strip() for s in str(v).split(",") if s.strip()]


def merge_sources(s1: list[str], s2: list[str], extra: list[str] | None = None) -> str:
    seen: list[str] = []
    for s in s1 + s2 + (extra or []):
        if s and s not in seen:
            seen.append(s)
    return json.dumps(seen)


# ── Field helpers ─────────────────────────────────────────────────────────────

def _blank(v) -> bool:
    return v in (None, "", "None", "null", "nan")


def _newer_or_fill(newer_val, older_val):
    """Return newer if not blank, else older."""
    return newer_val if not _blank(newer_val) else older_val


# ── Core merge ────────────────────────────────────────────────────────────────

def merge_records(newer_row: dict, older_row: dict) -> dict:
    """Merge an older row into a newer row.  Newer wins on counts/timestamps;
    identity fields fill from older when blank; position follows validity rules."""
    merged = dict(newer_row)  # start from newer

    # --- Identity fields: newer wins unless blank, then fill from older ---
    for fld in ("long_name", "short_name", "display_name",
                 "hardware_model", "firmware_version", "role",
                 "region", "modem_preset", "channel_name"):
        if _blank(merged.get(fld)):
            merged[fld] = older_row.get(fld, "")

    # --- Sources: union ---
    merged["sources_seen"] = merge_sources(
        parse_sources(newer_row.get("sources_seen")),
        parse_sources(older_row.get("sources_seen")),
    )

    # --- Position: prefer newer valid; restore older valid if newer has none ---
    newer_has_pos = is_valid_position(newer_row.get("latitude"), newer_row.get("longitude"))
    older_has_pos = is_valid_position(older_row.get("latitude"), older_row.get("longitude"))

    if newer_has_pos:
        pass  # keep newer position (already in merged)
    elif older_has_pos:
        merged["latitude"]         = older_row.get("latitude", "")
        merged["longitude"]        = older_row.get("longitude", "")
        merged["altitude"]         = older_row.get("altitude", "")
        merged["location_name"]    = older_row.get("location_name", "")
        merged["position_source"]  = older_row.get("position_source", "")
        merged["position_precision"] = older_row.get("position_precision", "")
        merged["last_position_seen"] = older_row.get("last_position_seen", "")
    else:
        # Neither has valid position — clear position fields to avoid bad data
        merged["latitude"] = merged["longitude"] = merged["altitude"] = ""
        merged["location_name"] = merged["position_source"] = ""
        merged["position_precision"] = merged["last_position_seen"] = ""

    # --- Always clear distance (will be recalculated by app on import) ---
    merged["distance_from_home_miles"] = ""
    merged["is_local"] = ""

    return merged


def _add_missing(older_row: dict) -> dict:
    """Build a merged row from an older-only record (missing from newer)."""
    row = dict(older_row)
    row["node_id"] = normalize_node_id(row.get("node_id")) or row.get("node_id", "")

    # Validate position; clear if bad
    if not is_valid_position(row.get("latitude"), row.get("longitude")):
        row["latitude"] = row["longitude"] = row["altitude"] = ""
        row["location_name"] = row["position_source"] = ""
        row["position_precision"] = row["last_position_seen"] = ""

    # Mark as recovered
    existing_sources = parse_sources(row.get("sources_seen"))
    row["sources_seen"] = merge_sources(existing_sources, [], ["IMPORTED_RECOVERY"])
    row["status"] = "Reference Only"
    row["distance_from_home_miles"] = ""
    row["is_local"] = ""
    return row


# ── Merge orchestration ──────────────────────────────────────────────────────

def merge(older: dict[str, dict], newer: dict[str, dict]) -> tuple[list[dict], dict]:
    """Return (merged_rows_list, stats_dict)."""
    stats = {
        "from_newer_only": 0,
        "from_older_only": 0,
        "merged_both":     0,
        "pos_restored":    0,
        "pos_cleared":     0,
        "identity_filled": 0,
        "bad_pos_rejected": 0,
    }

    merged_rows: list[dict] = []

    # Pass 1: all nodes from newer (primary)
    for nid, newer_row in newer.items():
        if nid in older:
            # Present in both — merge
            merged = merge_records(newer_row, older[nid])
            stats["merged_both"] += 1

            newer_had_pos = is_valid_position(newer_row.get("latitude"), newer_row.get("longitude"))
            older_had_pos = is_valid_position(older[nid].get("latitude"), older[nid].get("longitude"))

            if not newer_had_pos and older_had_pos:
                stats["pos_restored"] += 1
            if not newer_had_pos and not older_had_pos:
                had_coords = not _blank(newer_row.get("latitude"))
                if had_coords:
                    stats["bad_pos_rejected"] += 1

            # Count identity fills
            for fld in ("long_name", "short_name", "hardware_model",
                        "firmware_version", "role", "region"):
                newer_blank = _blank(newer_row.get(fld))
                older_has   = not _blank(older[nid].get(fld))
                if newer_blank and older_has:
                    stats["identity_filled"] += 1
        else:
            # Newer only
            if not is_valid_position(newer_row.get("latitude"), newer_row.get("longitude")):
                has_coords = not _blank(newer_row.get("latitude"))
                if has_coords:
                    stats["bad_pos_rejected"] += 1
            row = dict(newer_row)
            row["node_id"] = nid
            row["distance_from_home_miles"] = ""
            row["is_local"] = ""
            merged = row
            stats["from_newer_only"] += 1

        merged_rows.append(merged)

    # Pass 2: nodes only in older — restore as reference
    for nid, older_row in older.items():
        if nid not in newer:
            merged_rows.append(_add_missing(older_row))
            if not is_valid_position(older_row.get("latitude"), older_row.get("longitude")):
                has_coords = not _blank(older_row.get("latitude"))
                if has_coords:
                    stats["bad_pos_rejected"] += 1
            stats["from_older_only"] += 1

    return merged_rows, stats


# ── Output writers ────────────────────────────────────────────────────────────

def write_merged_csv(rows: list[dict], path: Path) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow({col: row.get(col, "") for col in CSV_COLUMNS})


def write_report(
    report_path: Path,
    older_path: Path, older_count: int,
    newer_path: Path, newer_count: int,
    merged_count: int,
    stats: dict,
    merged_csv_path: Path,
    backup_path: Path | None,
    db_added: int, db_updated: int,
    dry_run: bool,
    warnings: list[str],
) -> None:
    lines = [
        "Mesh Command Post — Node Reference Merge Report",
        f"Generated : {NOW.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "── Input files ──────────────────────────────────────────────────────",
        f"  Older : {older_path.name}  ({older_count} nodes)",
        f"  Newer : {newer_path.name}  ({newer_count} nodes)",
        "",
        "── Merge results ────────────────────────────────────────────────────",
        f"  Merged node count           : {merged_count}",
        f"  Nodes from newer only       : {stats['from_newer_only']}",
        f"  Nodes from older only       : {stats['from_older_only']}  ← recovered",
        f"  Nodes present in both       : {stats['merged_both']}",
        f"  Positions restored from old : {stats['pos_restored']}",
        f"  Identity fields filled      : {stats['identity_filled']}",
        f"  Bad positions rejected      : {stats['bad_pos_rejected']}",
        f"  Distance fields cleared     : {merged_count}  (recalculated by app)",
        f"  Node records DELETED        : 0  ← no records are ever deleted",
        "",
        "── Output files ─────────────────────────────────────────────────────",
        f"  Merged CSV : {merged_csv_path}",
        f"  Report     : {report_path}",
        "",
        "── Database ─────────────────────────────────────────────────────────",
    ]

    if backup_path:
        lines.append(f"  Backup     : {backup_path}")
    else:
        lines.append("  Backup     : (none — fresh DB or backup skipped)")

    if dry_run:
        lines.append("  Import     : SKIPPED (--dry-run mode)")
    else:
        lines.append(f"  Nodes added to DB    : {db_added}")
        lines.append(f"  Nodes updated in DB  : {db_updated}")
        lines.append( "  Nodes deleted from DB: 0")

    if warnings:
        lines += ["", "── Warnings ─────────────────────────────────────────────────────"]
        for w in warnings:
            lines.append(f"  ! {w}")

    lines += [
        "",
        "── Acceptance notes ─────────────────────────────────────────────────",
        "  - The current database was not rolled back.",
        "  - Newer node data was preserved; older filled missing records only.",
        "  - Distance is NOT imported — app recalculates from home coordinates.",
        "  - Nodes without valid lat/lon will not appear on the map.",
        "  - Identity-only MAP nodes remain in the Nodes tab without markers.",
    ]

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


# ── DB backup ─────────────────────────────────────────────────────────────────

def backup_db(db_path: Path, backup_dir: Path) -> Path | None:
    """Backup db_path to backup_dir using SQLite's native backup API."""
    if not db_path.exists():
        print(f"  DB not found at {db_path} — skipping backup.")
        return None

    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_name = f"node_database_before_merge_{NOW_STR}.db"
    backup_path = backup_dir / backup_name

    try:
        src  = sqlite3.connect(str(db_path))
        dst  = sqlite3.connect(str(backup_path))
        src.backup(dst)
        dst.close()
        src.close()
        print(f"  DB backed up ->{backup_path}")
        return backup_path
    except Exception as exc:
        print(f"  ERROR: DB backup failed: {exc}", file=sys.stderr)
        return None


# ── DB import ─────────────────────────────────────────────────────────────────

def _str_or_none(row: dict, key: str):
    v = row.get(key, "")
    return v if not _blank(v) else None


def _float_or_none(row: dict, key: str):
    v = row.get(key, "")
    if _blank(v):
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _int_or_zero(row: dict, key: str) -> int:
    v = row.get(key, "")
    try:
        return int(float(v)) if not _blank(v) else 0
    except (ValueError, TypeError):
        return 0


def import_to_db(merged_rows: list[dict], db_path: Path) -> tuple[int, int]:
    """Merge-import merged nodes into the app DB.  Returns (added, updated)."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Verify nodes table exists
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='nodes'"
    ).fetchone()
    if not exists:
        conn.close()
        raise RuntimeError("nodes table not found in DB — is this the right database?")

    added = updated = 0

    for row in merged_rows:
        node_id = row.get("node_id")
        if not node_id:
            continue

        lat  = _float_or_none(row, "latitude")
        lon  = _float_or_none(row, "longitude")
        # Validate before writing to DB
        if lat is not None and lon is not None:
            if not is_valid_position(row.get("latitude"), row.get("longitude")):
                lat = lon = None

        existing = conn.execute(
            "SELECT * FROM nodes WHERE node_id=?", (node_id,)
        ).fetchone()

        sources = parse_sources(row.get("sources_seen"))
        if "meshmap_reference" not in sources:
            sources.append("meshmap_reference")
        sources_json = json.dumps(sources)

        last_seen = _str_or_none(row, "last_seen")
        first_seen = _str_or_none(row, "first_seen") or NOW_ISO

        if existing is None:
            # Insert new node
            conn.execute(
                """INSERT INTO nodes (
                    node_id, long_name, short_name, last_heard, packet_count,
                    last_packet_type, latitude, longitude, altitude, hardware,
                    first_seen, last_reference_seen, sources_seen, last_source,
                    role, firmware_version, region, status
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'Reference Only')""",
                (
                    node_id,
                    _str_or_none(row, "long_name"),
                    _str_or_none(row, "short_name"),
                    last_seen or NOW_ISO,
                    _int_or_zero(row, "packet_count"),
                    _str_or_none(row, "last_packet_type") or "reference_import",
                    lat, lon,
                    _float_or_none(row, "altitude"),
                    _str_or_none(row, "hardware_model"),
                    first_seen,
                    NOW_ISO,
                    sources_json,
                    "meshmap_reference",
                    _str_or_none(row, "role"),
                    _str_or_none(row, "firmware_version"),
                    _str_or_none(row, "region"),
                ),
            )
            added += 1

        else:
            # Merge — fill blanks only, never overwrite live observations
            set_parts = ["last_reference_seen=?", "sources_seen=?"]
            params: list = [NOW_ISO, sources_json]

            # Update last_heard if reference is more recent
            ex_heard = existing["last_heard"]
            if last_seen and (ex_heard is None or last_seen > ex_heard):
                set_parts.append("last_heard=?")
                params.append(last_seen)

            # Fill blank identity fields
            for db_col, csv_col in [
                ("long_name",       "long_name"),
                ("short_name",      "short_name"),
                ("hardware",        "hardware_model"),
                ("role",            "role"),
                ("firmware_version","firmware_version"),
                ("region",          "region"),
            ]:
                v = _str_or_none(row, csv_col)
                if v and not existing[db_col]:
                    set_parts.append(f"{db_col}=?")
                    params.append(v)

            # Fill blank position only if DB has none AND merged has valid coords
            if existing["latitude"] is None and lat is not None and lon is not None:
                set_parts.extend(["latitude=?", "longitude=?"])
                params.extend([lat, lon])
                alt = _float_or_none(row, "altitude")
                if alt is not None:
                    set_parts.append("altitude=?")
                    params.append(alt)

            # Set first_seen if missing
            if not existing["first_seen"]:
                set_parts.append("first_seen=?")
                params.append(first_seen)

            params.append(node_id)
            conn.execute(
                f"UPDATE nodes SET {', '.join(set_parts)} WHERE node_id=?", params
            )
            updated += 1

    conn.commit()
    conn.close()
    return added, updated


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Create merged CSV and report but skip DB import.",
    )
    args = parser.parse_args()

    warnings: list[str] = []

    # ── 1. Locate input files ─────────────────────────────────────────────
    print(f"\nScanning {INPUTS_DIR} for export files...")
    export_files = find_export_files()

    if len(export_files) < 2:
        print(f"ERROR: need at least 2 CSV files in {INPUTS_DIR}; found {len(export_files)}.",
              file=sys.stderr)
        sys.exit(1)

    older_path, newer_path = export_files[-2], export_files[-1]
    print(f"  Older : {older_path.name}")
    print(f"  Newer : {newer_path.name}")

    # ── 2. Read CSVs ──────────────────────────────────────────────────────
    print("\nReading files...")
    older_nodes = read_csv(older_path)
    newer_nodes = read_csv(newer_path)
    print(f"  Older: {len(older_nodes)} nodes")
    print(f"  Newer: {len(newer_nodes)} nodes")

    # ── 3. Merge ──────────────────────────────────────────────────────────
    print("\nMerging...")
    merged_rows, stats = merge(older_nodes, newer_nodes)
    print(f"  Merged: {len(merged_rows)} nodes total")
    print(f"  Recovered from older: {stats['from_older_only']}")
    print(f"  Positions restored  : {stats['pos_restored']}")
    print(f"  Bad positions skipped: {stats['bad_pos_rejected']}")

    if len(merged_rows) < len(newer_nodes):
        warnings.append(
            f"Merged count ({len(merged_rows)}) < newer count ({len(newer_nodes)}) — "
            "unexpected; check for duplicate node IDs in newer file."
        )

    # ── 4. Write merged CSV ───────────────────────────────────────────────
    merged_csv_name = f"mesh_node_reference_MERGED_{NOW.strftime('%Y%m%d_%H%M')}.csv"
    merged_csv_path = INPUTS_DIR / merged_csv_name
    print(f"\nWriting merged CSV ->{merged_csv_path.name}...")
    write_merged_csv(merged_rows, merged_csv_path)

    # ── 5. Backup DB ──────────────────────────────────────────────────────
    print("\nBacking up app database...")
    BACKUPS_DIR.mkdir(parents=True, exist_ok=True)
    backup_path = backup_db(APP_DB, BACKUPS_DIR)

    if not backup_path and APP_DB.exists() and not args.dry_run:
        print("ERROR: backup failed — aborting DB import for safety.", file=sys.stderr)
        sys.exit(1)

    # ── 6. Import to DB ───────────────────────────────────────────────────
    db_added = db_updated = 0

    if args.dry_run:
        print("\nDry-run mode: skipping DB import.")
    elif not APP_DB.exists():
        print(f"\nWarning: app DB not found at {APP_DB} — skipping import.")
        warnings.append(f"App DB not found at {APP_DB}; merged CSV ready to import manually via app.")
    else:
        print(f"\nImporting into {APP_DB}...")
        try:
            db_added, db_updated = import_to_db(merged_rows, APP_DB)
            print(f"  Added  : {db_added} new nodes")
            print(f"  Updated: {db_updated} existing nodes (blanks filled)")
            print(f"  Deleted: 0 node records")
        except Exception as exc:
            print(f"ERROR during DB import: {exc}", file=sys.stderr)
            warnings.append(f"DB import error: {exc}")

    # ── 7. Write report ───────────────────────────────────────────────────
    report_name = f"mesh_node_reference_MERGE_REPORT_{NOW.strftime('%Y%m%d_%H%M')}.txt"
    report_path = INPUTS_DIR / report_name
    write_report(
        report_path,
        older_path, len(older_nodes),
        newer_path, len(newer_nodes),
        len(merged_rows),
        stats,
        merged_csv_path,
        backup_path,
        db_added, db_updated,
        args.dry_run,
        warnings,
    )
    print(f"\nReport written ->{report_path.name}")

    # ── Done ──────────────────────────────────────────────────────────────
    print("\nDone.")
    if warnings:
        print(f"  {len(warnings)} warning(s) — see report for details.")
    if not args.dry_run and APP_DB.exists():
        print(f"  Launch the app to verify the {db_added} restored + {db_updated} updated nodes.")


if __name__ == "__main__":
    main()
