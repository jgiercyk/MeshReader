import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from models import MQTTPacket, Node
from root_classifier import classify_root, activity_label

PACKET_RETAIN_HOURS    = 48
PACKET_RETAIN_MAX_ROWS = 15_000

# ── Schema ────────────────────────────────────────────────────────────────────

_CREATE_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS packets (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    packet_id     TEXT,
    received_at   TEXT NOT NULL,
    topic         TEXT NOT NULL,
    packet_type   TEXT NOT NULL,
    sender        TEXT NOT NULL,
    from_num      INTEGER,
    to_num        INTEGER,
    channel       INTEGER,
    raw_json      TEXT NOT NULL,
    summary       TEXT,
    dedup_hash    TEXT
);

CREATE INDEX IF NOT EXISTS idx_pk_received ON packets(received_at DESC);
CREATE INDEX IF NOT EXISTS idx_pk_id       ON packets(packet_id);
CREATE INDEX IF NOT EXISTS idx_pk_hash     ON packets(dedup_hash);
CREATE INDEX IF NOT EXISTS idx_pk_sender   ON packets(sender);
CREATE INDEX IF NOT EXISTS idx_pk_type     ON packets(packet_type);

CREATE TABLE IF NOT EXISTS nodes (
    node_id               TEXT PRIMARY KEY,
    long_name             TEXT,
    short_name            TEXT,
    last_heard            TEXT,
    packet_count          INTEGER DEFAULT 0,
    last_packet_type      TEXT,
    latitude              REAL,
    longitude             REAL,
    altitude              REAL,
    hardware              TEXT,
    location_name         TEXT
);

CREATE INDEX IF NOT EXISTS idx_nodes_heard ON nodes(last_heard DESC);

CREATE TABLE IF NOT EXISTS geocode_cache (
    lat_key       REAL NOT NULL,
    lon_key       REAL NOT NULL,
    location_name TEXT NOT NULL,
    cached_at     TEXT NOT NULL,
    PRIMARY KEY (lat_key, lon_key)
);

CREATE TABLE IF NOT EXISTS watchlist (
    node_id              TEXT PRIMARY KEY,
    label                TEXT,
    notes                TEXT,
    alert_when_seen      INTEGER DEFAULT 1,
    alert_when_position  INTEGER DEFAULT 0,
    alert_when_message   INTEGER DEFAULT 0,
    created_at           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mqtt_roots (
    root_topic          TEXT PRIMARY KEY,
    enabled             INTEGER NOT NULL DEFAULT 0,
    auto_connect        INTEGER NOT NULL DEFAULT 0,
    manual              INTEGER NOT NULL DEFAULT 0,
    first_seen          TEXT,
    last_seen           TEXT,
    packet_count_total  INTEGER NOT NULL DEFAULT 0,
    packet_count_recent INTEGER NOT NULL DEFAULT 0,
    packets_per_minute  REAL    NOT NULL DEFAULT 0.0,
    channels_seen       TEXT    NOT NULL DEFAULT '[]',
    country             TEXT,
    state_code          TEXT,
    region              TEXT,
    root_type           TEXT    NOT NULL DEFAULT 'unknown',
    last_discovery_run  TEXT,
    last_connected      TEXT,
    notes               TEXT
);

CREATE INDEX IF NOT EXISTS idx_mqtt_roots_auto_connect ON mqtt_roots(auto_connect);
CREATE INDEX IF NOT EXISTS idx_mqtt_roots_last_seen    ON mqtt_roots(last_seen DESC);

CREATE TABLE IF NOT EXISTS app_migrations (
    migration_id TEXT PRIMARY KEY,
    applied_at   TEXT NOT NULL
);
"""

# ── Idempotent schema migrations ──────────────────────────────────────────────

_MIGRATIONS = [
    "ALTER TABLE nodes ADD COLUMN location_name TEXT",
    "ALTER TABLE nodes ADD COLUMN first_seen TEXT",
    "ALTER TABLE nodes ADD COLUMN last_mqtt_seen TEXT",
    "ALTER TABLE nodes ADD COLUMN last_reference_seen TEXT",
    "ALTER TABLE nodes ADD COLUMN last_position_seen TEXT",
    "ALTER TABLE nodes ADD COLUMN last_nodeinfo_seen TEXT",
    "ALTER TABLE nodes ADD COLUMN last_telemetry_seen TEXT",
    "ALTER TABLE nodes ADD COLUMN last_text_seen TEXT",
    "ALTER TABLE nodes ADD COLUMN sources_seen TEXT DEFAULT '[]'",
    "ALTER TABLE nodes ADD COLUMN last_source TEXT",
    "ALTER TABLE nodes ADD COLUMN last_topic TEXT",
    "ALTER TABLE nodes ADD COLUMN position_count INTEGER DEFAULT 0",
    "ALTER TABLE nodes ADD COLUMN telemetry_count INTEGER DEFAULT 0",
    "ALTER TABLE nodes ADD COLUMN nodeinfo_count INTEGER DEFAULT 0",
    "ALTER TABLE nodes ADD COLUMN message_count INTEGER DEFAULT 0",
    "ALTER TABLE nodes ADD COLUMN role TEXT",
    "ALTER TABLE nodes ADD COLUMN firmware_version TEXT",
    "ALTER TABLE nodes ADD COLUMN region TEXT",
    "ALTER TABLE nodes ADD COLUMN status TEXT DEFAULT 'Unknown'",
    "ALTER TABLE nodes ADD COLUMN is_local INTEGER",
    "ALTER TABLE nodes ADD COLUMN distance_miles REAL",
    "ALTER TABLE nodes ADD COLUMN last_map_seen TEXT",
    "ALTER TABLE nodes ADD COLUMN position_precision INTEGER",
    "ALTER TABLE nodes ADD COLUMN seen_roots TEXT DEFAULT '[]'",
    "ALTER TABLE nodes ADD COLUMN seen_channels TEXT DEFAULT '[]'",
    "ALTER TABLE mqtt_roots ADD COLUMN staged INTEGER NOT NULL DEFAULT 0",
]


class Storage:
    def __init__(
        self,
        db_path: Path,
        retain_hours: int = PACKET_RETAIN_HOURS,
        retain_max_rows: int = PACKET_RETAIN_MAX_ROWS,
    ):
        self.db_path = db_path
        self._retain_hours    = retain_hours
        self._retain_max_rows = retain_max_rows
        self._last_cleanup:   Optional[datetime] = None
        # Counts of what startup cleanup did; read by app.py to log to the UI.
        # nodes_deleted is always 0 — cleanup routines modify fields, never delete records.
        self.startup_stats: Dict[str, int] = {
            "positions_cleared": 0,
            "altitudes_cleared": 0,
            "source_tags_cleaned": 0,
            "invalid_packets_removed": 0,
            "nodes_deleted": 0,
            "packets_deleted_by_age": 0,
            "packets_deleted_by_cap": 0,
        }
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout=5000")  # wait up to 5 s before raising OperationalError
        self._conn.executescript(_CREATE_SQL)
        self._backup_database()           # safe copy before any data changes
        self._run_migrations()
        # Clear any (0.0, 0.0) coordinates — these come from GPS-not-acquired packets
        # where the firmware sent latitude_i=0, longitude_i=0.
        cur = self._conn.execute(
            "UPDATE nodes SET latitude=NULL, longitude=NULL "
            "WHERE latitude IS NOT NULL AND longitude IS NOT NULL "
            "AND ABS(latitude) < 1e-6 AND ABS(longitude) < 1e-6"
        )
        if cur.rowcount:
            logging.info("DB: cleared %d null-island (0,0) coordinates", cur.rowcount)
        self._migrate_clean_bad_map_positions()
        self._migrate_clean_map_sources()
        self._migrate_clear_orphan_altitude()
        # Purge any INVALID rows left by earlier builds that called json.loads() on
        # binary map payloads and stored the failure as packet_type='INVALID'.
        cur = self._conn.execute("DELETE FROM packets WHERE packet_type='INVALID'")
        self.startup_stats["invalid_packets_removed"] = cur.rowcount
        result = self._cleanup_old_packets()
        self.startup_stats["packets_deleted_by_age"] = result["deleted_by_age"]
        self.startup_stats["packets_deleted_by_cap"] = result["deleted_by_cap"]
        self._conn.commit()
        self.checkpoint_wal()

    def _backup_database(self) -> None:
        """Create a rolling timestamped backup of the DB before startup migrations.

        Uses SQLite's native backup API, which is WAL-safe and works while the
        connection is open.  Keeps the 5 most recent backups and silently skips
        fresh (empty) databases where there is nothing to protect.
        """
        try:
            node_count = self._conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
        except Exception:
            return  # nodes table doesn't exist yet — fresh DB, nothing to back up

        if node_count == 0:
            return  # empty DB, no data worth backing up

        backup_dir = self.db_path.parent / "backups"
        try:
            backup_dir.mkdir(exist_ok=True)
        except Exception as exc:
            logging.warning("DB: could not create backup directory: %s", exc)
            return

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = backup_dir / f"history_{ts}.db"
        try:
            # Checkpoint WAL into main DB first so the backup is self-contained
            try:
                self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except Exception:
                pass
            bconn = sqlite3.connect(str(backup_path))
            self._conn.backup(bconn)
            bconn.close()
            logging.info("DB: backup created → %s (%d nodes)", backup_path.name, node_count)
        except Exception as exc:
            logging.warning("DB: backup failed: %s", exc)
            return

        # Prune; keep the 5 most recent backups
        try:
            backups = sorted(
                backup_dir.glob("history_*.db"),
                key=lambda p: p.stat().st_mtime,
            )
            for old in backups[:-5]:
                try:
                    old.unlink(missing_ok=True)
                    logging.info("DB: pruned old backup: %s", old.name)
                except Exception as prune_exc:
                    logging.warning("DB: could not prune backup %s: %s", old.name, prune_exc)
        except Exception:
            pass

    def _run_migrations(self) -> None:
        for sql in _MIGRATIONS:
            try:
                self._conn.execute(sql)
            except sqlite3.OperationalError:
                pass  # column already exists
        self._run_data_migrations()

    def _run_data_migrations(self) -> None:
        """One-time data corrections tracked by app_migrations table."""
        now = datetime.now().isoformat()

        def _ran(mid: str) -> bool:
            return bool(self._conn.execute(
                "SELECT 1 FROM app_migrations WHERE migration_id=?", (mid,)
            ).fetchone())

        def _mark(mid: str) -> None:
            self._conn.execute(
                "INSERT OR IGNORE INTO app_migrations VALUES (?,?)", (mid, now)
            )

        # msh/US was seeded as auto_connect=1 by earlier defaults.
        # The national root generates very high traffic and should not be
        # subscribed automatically. Correct it once; user can re-enable via Root Manager.
        if not _ran("disable_msh_us_auto_connect_2026_07"):
            self._conn.execute(
                "UPDATE mqtt_roots SET auto_connect=0, enabled=0 WHERE root_topic='msh/US'"
            )
            _mark("disable_msh_us_auto_connect_2026_07")

        # Remove roots that contain the protocol-level segment '/2' as a path component.
        # Valid roots are always msh/{country}[/{state}[/{region}]] — they never end
        # in '/2' or contain '/2/' (that segment belongs in derived topic strings).
        # These bad entries come from the old depth-based root extractor.
        if not _ran("purge_invalid_protocol_level_roots_2026_07"):
            deleted = self._conn.execute(
                "DELETE FROM mqtt_roots WHERE root_topic LIKE '%/2' OR root_topic LIKE '%/2/%'"
            ).rowcount
            if deleted:
                logging.info("Purged %d invalid MQTT root(s) containing /2 segment", deleted)
            _mark("purge_invalid_protocol_level_roots_2026_07")

        self._conn.commit()

    def _migrate_clean_bad_map_positions(self) -> None:
        """Clear MAP-derived positions that were corrupted by the old wrong field mapping.

        Old decoder had latitude_i=field8 (actually has_default_channel=0 or 1), so
        nodes got stored with latitude≈0 and longitude=the actual latitude value.
        Signature: ABS(latitude) < 0.01, no MQTT JSON position packet ever arrived.
        """
        rows = self._conn.execute(
            """SELECT node_id, latitude, longitude, altitude
               FROM nodes
               WHERE last_map_seen IS NOT NULL
                 AND last_position_seen IS NULL
                 AND latitude IS NOT NULL
                 AND ABS(latitude) < 0.01"""
        ).fetchall()
        if not rows:
            return
        for row in rows:
            self._conn.execute(
                """UPDATE nodes
                   SET latitude=NULL, longitude=NULL, altitude=NULL, location_name=NULL
                   WHERE node_id=?""",
                (row["node_id"],),
            )
        self.startup_stats["positions_cleared"] += len(rows)
        self._conn.commit()
        logging.info(
            "DB: cleared %d bad MAP positions (latitude≈0 artifact from old field mapping)",
            len(rows),
        )

    def _migrate_clean_map_sources(self) -> None:
        """Remove 'mqtt_map' from sources_seen for nodes that have no last_map_seen.

        Nodes tagged mqtt_map without a timestamp were written by an older build
        that decoded incorrectly (garbage node IDs) and never set last_map_seen.
        Keeping them inflates the MAP count and confuses source_label().
        """
        rows = self._conn.execute(
            "SELECT node_id, sources_seen FROM nodes WHERE last_map_seen IS NULL"
        ).fetchall()
        changed = 0
        for row in rows:
            try:
                sources = json.loads(row["sources_seen"] or "[]")
            except (ValueError, TypeError):
                sources = []
            if "mqtt_map" in sources:
                clean = [s for s in sources if s != "mqtt_map"]
                self._conn.execute(
                    "UPDATE nodes SET sources_seen=? WHERE node_id=?",
                    (json.dumps(clean), row["node_id"])
                )
                changed += 1
        if changed:
            self.startup_stats["source_tags_cleaned"] += changed
            self._conn.commit()
            logging.info("DB: removed stale mqtt_map tag from %d nodes (no last_map_seen)", changed)

    def _migrate_clear_orphan_altitude(self) -> None:
        """Clear altitude stored without a valid lat/lon.

        MapReport decoding can store altitude even when the position is rejected.
        Altitude without coordinates is meaningless for display/map purposes and
        can mislead the UI into showing a height with no position.
        """
        cur = self._conn.execute(
            """UPDATE nodes SET altitude=NULL
               WHERE altitude IS NOT NULL
                 AND (latitude IS NULL OR longitude IS NULL)"""
        )
        if cur.rowcount:
            self.startup_stats["altitudes_cleared"] += cur.rowcount
            self._conn.commit()
            logging.info("DB: cleared orphan altitude from %d nodes (no lat/lon)", cur.rowcount)

    # ── Retention and maintenance ─────────────────────────────────────────────

    def _cleanup_old_packets(self) -> Dict[str, int]:
        """Delete packets older than _retain_hours, then enforce the row cap.

        Returns counts of rows removed by each criterion.  The caller is
        responsible for the final commit() — this method commits only when it
        has work to do so intermediate states are atomic.
        """
        deleted_by_age = 0
        deleted_by_cap = 0

        cur = self._conn.execute(
            "DELETE FROM packets WHERE received_at < datetime('now', ?)",
            (f"-{self._retain_hours} hours",),
        )
        deleted_by_age = cur.rowcount

        count = self._conn.execute("SELECT COUNT(*) FROM packets").fetchone()[0]
        if count > self._retain_max_rows:
            excess = count - self._retain_max_rows
            cur = self._conn.execute(
                "DELETE FROM packets WHERE id IN "
                "(SELECT id FROM packets ORDER BY received_at ASC LIMIT ?)",
                (excess,),
            )
            deleted_by_cap = cur.rowcount

        if deleted_by_age or deleted_by_cap:
            self._conn.commit()
            logging.info(
                "DB cleanup: deleted %d packets by age (>%dh), %d by row cap (>%d)",
                deleted_by_age, self._retain_hours, deleted_by_cap, self._retain_max_rows,
            )

        self._last_cleanup = datetime.now()
        return {"deleted_by_age": deleted_by_age, "deleted_by_cap": deleted_by_cap}

    def cleanup_packets(self) -> Dict[str, int]:
        """Public entry point for scheduled cleanup; checkpoints WAL after deletion."""
        result = self._cleanup_old_packets()
        self.checkpoint_wal()
        return result

    def checkpoint_wal(self) -> None:
        """Merge WAL pages into the main DB file (TRUNCATE mode)."""
        try:
            self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            pass

    def get_db_size_info(self) -> Dict[str, float]:
        """Return sizes in MB for .db, -wal, and -shm files."""
        def _mb(path: Path) -> float:
            try:
                return path.stat().st_size / 1_048_576
            except OSError:
                return 0.0

        db_mb  = _mb(self.db_path)
        wal_mb = _mb(Path(str(self.db_path) + "-wal"))
        shm_mb = _mb(Path(str(self.db_path) + "-shm"))
        return {
            "db_mb":    db_mb,
            "wal_mb":   wal_mb,
            "shm_mb":   shm_mb,
            "total_mb": db_mb + wal_mb + shm_mb,
        }

    def get_packet_stats(self) -> Dict[str, Any]:
        """Return packet table diagnostics and current retention settings."""
        row = self._conn.execute(
            "SELECT COUNT(*) as total, MIN(received_at) as oldest, MAX(received_at) as newest "
            "FROM packets"
        ).fetchone()
        return {
            "packet_count":    row["total"],
            "oldest_packet":   row["oldest"],
            "newest_packet":   row["newest"],
            "retain_hours":    self._retain_hours,
            "retain_max_rows": self._retain_max_rows,
            "last_cleanup":    self._last_cleanup.isoformat() if self._last_cleanup else None,
        }

    # ── Packet helpers ────────────────────────────────────────────────────────

    def is_duplicate(self, packet: MQTTPacket, dedup_hash: str) -> bool:
        if packet.packet_id:
            row = self._conn.execute(
                "SELECT 1 FROM packets WHERE packet_id=? LIMIT 1", (packet.packet_id,)
            ).fetchone()
            if row:
                return True
        row = self._conn.execute(
            "SELECT 1 FROM packets WHERE dedup_hash=? LIMIT 1", (dedup_hash,)
        ).fetchone()
        return row is not None

    def store_packet(self, packet: MQTTPacket, dedup_hash: str) -> Optional[int]:
        """Insert a packet row.  Caller must call commit() when ready."""
        try:
            cur = self._conn.execute(
                """INSERT INTO packets
                   (packet_id, received_at, topic, packet_type, sender,
                    from_num, to_num, channel, raw_json, summary, dedup_hash)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    packet.packet_id,
                    packet.received_at.isoformat(),
                    packet.topic, packet.packet_type, packet.sender,
                    packet.from_num, packet.to_num, packet.channel,
                    packet.raw_json, packet.summary, dedup_hash,
                ),
            )
            return cur.lastrowid
        except sqlite3.Error:
            return None

    def commit(self) -> None:
        """Explicit commit — call once after a batch of store/upsert operations."""
        self._conn.commit()

    def get_recent_packets(self, limit: int = 200) -> List[MQTTPacket]:
        rows = self._conn.execute(
            "SELECT * FROM packets ORDER BY received_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [self._row_to_packet(r) for r in rows]

    # ── Node helpers — MQTT source ────────────────────────────────────────────

    def upsert_node_from_mqtt(
        self,
        updates: Dict[str, Any],
        packet_type: str,
        topic: str = "",
        source: str = "mqtt_json",
        mqtt_root: Optional[str] = None,
        mqtt_channel: Optional[str] = None,
    ) -> Node:
        """Upsert a node from a live MQTT packet observation.

        `updates` must include 'node_id'.  All other fields are optional.
        """
        node_id = updates["node_id"]
        now = datetime.now()
        now_str = now.isoformat()

        existing = self._conn.execute(
            "SELECT * FROM nodes WHERE node_id=?", (node_id,)
        ).fetchone()

        # Build merged sources list
        if existing:
            try:
                sources = json.loads(existing["sources_seen"] or "[]")
            except (ValueError, TypeError):
                sources = []
        else:
            sources = []
        if source not in sources:
            sources.append(source)
        sources_json = json.dumps(sources)

        # Type-specific timestamp and counter field
        type_ts_field = {
            "position":   "last_position_seen",
            "nodeinfo":   "last_nodeinfo_seen",
            "telemetry":  "last_telemetry_seen",
            "text":       "last_text_seen",
            "mapreport":  "last_nodeinfo_seen",
        }.get(packet_type)

        type_cnt_field = {
            "position":  "position_count",
            "nodeinfo":  "nodeinfo_count",
            "telemetry": "telemetry_count",
            "text":      "message_count",
            "mapreport": "nodeinfo_count",
        }.get(packet_type)

        if existing is None:
            init_roots = json.dumps([mqtt_root] if mqtt_root else [])
            init_chans = json.dumps([mqtt_channel] if mqtt_channel else [])
            # INSERT new node
            self._conn.execute(
                """INSERT INTO nodes (
                    node_id, long_name, short_name, last_heard, packet_count,
                    last_packet_type, latitude, longitude, altitude, hardware,
                    first_seen, last_mqtt_seen, sources_seen, last_source, last_topic,
                    role, firmware_version, region, status, position_precision,
                    seen_roots, seen_channels
                ) VALUES (?,?,?,?,1,?,?,?,?,?,?,?,?,?,?,?,?,?,'Unknown',?,?,?)""",
                (
                    node_id,
                    updates.get("long_name"), updates.get("short_name"),
                    now_str, packet_type,
                    updates.get("latitude"), updates.get("longitude"),
                    updates.get("altitude"), updates.get("hardware"),
                    now_str, now_str, sources_json, source, topic or None,
                    updates.get("role"), updates.get("firmware_version"),
                    updates.get("region"),
                    updates.get("position_precision"),
                    init_roots, init_chans,
                ),
            )
            if type_ts_field:
                self._conn.execute(
                    f"UPDATE nodes SET {type_ts_field}=? WHERE node_id=?",
                    (now_str, node_id),
                )
            if type_cnt_field:
                self._conn.execute(
                    f"UPDATE nodes SET {type_cnt_field}=1 WHERE node_id=?",
                    (node_id,),
                )
            if source == "mqtt_map":
                self._conn.execute(
                    "UPDATE nodes SET last_map_seen=? WHERE node_id=?",
                    (now_str, node_id),
                )
        else:
            # UPDATE existing node
            set_parts = [
                "last_heard=?",
                "last_mqtt_seen=?",
                "packet_count=packet_count+1",
                "last_packet_type=?",
                "sources_seen=?",
                "last_source=?",
            ]
            params: list = [now_str, now_str, packet_type, sources_json, source]

            if source == "mqtt_map":
                set_parts.append("last_map_seen=?")
                params.append(now_str)

            if topic:
                set_parts.append("last_topic=?")
                params.append(topic)

            # Only overwrite identity/position fields if the new value is present
            for fld in ("long_name", "short_name", "hardware", "role",
                        "firmware_version", "region"):
                if updates.get(fld) is not None:
                    set_parts.append(f"{fld}=?")
                    params.append(updates[fld])

            # MapReport cannot overwrite a position that came from an MQTT JSON
            # position packet (last_position_seen set).  Identity fields still update.
            _map_pos_protected = (
                source == "mqtt_map"
                and existing["last_position_seen"] is not None
            )
            for fld in ("latitude", "longitude", "altitude", "position_precision"):
                if updates.get(fld) is not None:
                    if _map_pos_protected:
                        continue
                    set_parts.append(f"{fld}=?")
                    params.append(updates[fld])
                    if fld == "latitude":
                        # Position updated — reset location_name so geocoder re-runs
                        # only if coords actually changed
                        old_lat = existing["latitude"]
                        if old_lat != updates[fld]:
                            set_parts.append("location_name=NULL")

            if type_ts_field:
                set_parts.append(f"{type_ts_field}=?")
                params.append(now_str)
            if type_cnt_field:
                set_parts.append(f"{type_cnt_field}={type_cnt_field}+1")

            # Set first_seen if missing
            if not existing["first_seen"]:
                set_parts.append("first_seen=?")
                params.append(now_str)

            # Merge seen_roots
            if mqtt_root:
                try:
                    roots_seen = json.loads(existing["seen_roots"] or "[]")
                except (ValueError, TypeError):
                    roots_seen = []
                if mqtt_root not in roots_seen:
                    roots_seen.append(mqtt_root)
                    set_parts.append("seen_roots=?")
                    params.append(json.dumps(roots_seen))

            # Merge seen_channels
            if mqtt_channel:
                try:
                    chans_seen = json.loads(existing["seen_channels"] or "[]")
                except (ValueError, TypeError):
                    chans_seen = []
                if mqtt_channel not in chans_seen:
                    chans_seen.append(mqtt_channel)
                    set_parts.append("seen_channels=?")
                    params.append(json.dumps(chans_seen))

            params.append(node_id)
            self._conn.execute(
                f"UPDATE nodes SET {', '.join(set_parts)} WHERE node_id=?", params
            )

        # Caller must call commit() — no implicit commit here.
        return self.get_node(node_id)  # type: ignore[return-value]

    # ── Node helpers — reference source ───────────────────────────────────────

    def upsert_reference_node(self, ref_node: Node) -> Node:
        """Import a reference node.  Does not overwrite live MQTT data."""
        node_id = ref_node.node_id
        now_str = datetime.now().isoformat()
        ref_ts = (ref_node.last_reference_seen or datetime.now()).isoformat()
        source = "meshmap_reference"

        existing = self._conn.execute(
            "SELECT * FROM nodes WHERE node_id=?", (node_id,)
        ).fetchone()

        if existing is None:
            # Brand new node from reference
            sources = [source]
            self._conn.execute(
                """INSERT INTO nodes (
                    node_id, long_name, short_name, last_heard, packet_count,
                    last_packet_type, latitude, longitude, altitude, hardware,
                    first_seen, last_reference_seen, sources_seen, last_source,
                    role, status
                ) VALUES (?,?,?,?,0,'reference_import',?,?,?,?,?,?,?,?,'','Reference Only')""",
                (
                    node_id,
                    ref_node.long_name, ref_node.short_name,
                    ref_ts,
                    ref_node.latitude, ref_node.longitude, ref_node.altitude,
                    ref_node.hardware,
                    (ref_node.first_seen or datetime.now()).isoformat(),
                    ref_ts,
                    json.dumps([source]),
                    source,
                ),
            )
        else:
            # Merge: update reference timestamp and sources; fill blanks only
            try:
                sources = json.loads(existing["sources_seen"] or "[]")
            except (ValueError, TypeError):
                sources = []
            if source not in sources:
                sources.append(source)

            set_parts = ["last_reference_seen=?", "sources_seen=?"]
            params: list = [ref_ts, json.dumps(sources)]

            # Update last_heard if reference is more recent
            ex_heard = existing["last_heard"]
            if ex_heard is None or ref_ts > ex_heard:
                set_parts.append("last_heard=?")
                params.append(ref_ts)

            # Fill blank identity fields
            for fld, val in [
                ("long_name",  ref_node.long_name),
                ("short_name", ref_node.short_name),
                ("hardware",   ref_node.hardware),
                ("role",       ref_node.role),
            ]:
                if val and not existing[fld]:
                    set_parts.append(f"{fld}=?")
                    params.append(val)

            # Fill blank position only if MQTT has never provided one
            if ref_node.latitude is not None and existing["latitude"] is None:
                set_parts.extend(["latitude=?", "longitude=?"])
                params.extend([ref_node.latitude, ref_node.longitude])
                if ref_node.altitude is not None:
                    set_parts.append("altitude=?")
                    params.append(ref_node.altitude)

            # Set first_seen if missing
            if not existing["first_seen"]:
                set_parts.append("first_seen=?")
                params.append((ref_node.first_seen or datetime.now()).isoformat())

            params.append(node_id)
            self._conn.execute(
                f"UPDATE nodes SET {', '.join(set_parts)} WHERE node_id=?", params
            )

        self._conn.commit()
        return self.get_node(node_id)  # type: ignore[return-value]

    # ── Computed fields ───────────────────────────────────────────────────────

    def update_node_computed(
        self,
        node_id: str,
        status: str,
        is_local: Optional[bool],
        distance_miles: Optional[float],
    ) -> None:
        """Update computed intelligence fields.  Caller must call commit()."""
        self._conn.execute(
            "UPDATE nodes SET status=?, is_local=?, distance_miles=? WHERE node_id=?",
            (status, int(is_local) if is_local is not None else None, distance_miles, node_id),
        )

    # ── Location / geocode ────────────────────────────────────────────────────

    def set_node_location(self, node_id: str, location_name: str) -> Optional[Node]:
        self._conn.execute(
            "UPDATE nodes SET location_name=? WHERE node_id=?", (location_name, node_id)
        )
        self._conn.commit()
        return self.get_node(node_id)

    def get_geocode(self, lat_key: float, lon_key: float) -> Optional[str]:
        row = self._conn.execute(
            "SELECT location_name FROM geocode_cache WHERE lat_key=? AND lon_key=?",
            (lat_key, lon_key),
        ).fetchone()
        return row["location_name"] if row else None

    def set_geocode(self, lat_key: float, lon_key: float, location_name: str) -> None:
        self._conn.execute(
            """INSERT INTO geocode_cache (lat_key, lon_key, location_name, cached_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(lat_key, lon_key) DO UPDATE SET
                   location_name=excluded.location_name,
                   cached_at=excluded.cached_at""",
            (lat_key, lon_key, location_name, datetime.now().isoformat()),
        )
        self._conn.commit()

    # ── Node queries ──────────────────────────────────────────────────────────

    def get_node(self, node_id: str) -> Optional[Node]:
        row = self._conn.execute(
            "SELECT * FROM nodes WHERE node_id=?", (node_id,)
        ).fetchone()
        return self._row_to_node(row) if row else None

    def get_all_nodes(self) -> List[Node]:
        rows = self._conn.execute(
            "SELECT * FROM nodes ORDER BY last_heard DESC"
        ).fetchall()
        return [self._row_to_node(r) for r in rows]

    # ── Watchlist ─────────────────────────────────────────────────────────────

    def get_watchlist(self) -> List[dict]:
        rows = self._conn.execute("SELECT * FROM watchlist ORDER BY created_at").fetchall()
        return [dict(r) for r in rows]

    def add_watchlist(self, node_id: str, label: str = "", notes: str = "") -> None:
        self._conn.execute(
            """INSERT INTO watchlist (node_id, label, notes, created_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(node_id) DO UPDATE SET label=excluded.label, notes=excluded.notes""",
            (node_id, label, notes, datetime.now().isoformat()),
        )
        self._conn.commit()

    def remove_watchlist(self, node_id: str) -> None:
        self._conn.execute("DELETE FROM watchlist WHERE node_id=?", (node_id,))
        self._conn.commit()

    def check_watchlist(self, node_id: str) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM watchlist WHERE node_id=?", (node_id,)
        ).fetchone()
        return dict(row) if row else None

    # ── Export ────────────────────────────────────────────────────────────────

    def export_jsonl(self, path: str) -> None:
        rows = self._conn.execute(
            "SELECT raw_json FROM packets ORDER BY received_at ASC"
        ).fetchall()
        with open(path, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(r["raw_json"].strip() + "\n")

    def export_nodes_csv(self, path: str) -> None:
        nodes = self.get_all_nodes()
        import csv
        headers = [
            "node_id", "display_name", "long_name", "short_name", "hardware",
            "status", "sources", "location", "latitude", "longitude", "altitude",
            "distance_miles", "is_local",
            "first_seen", "last_seen", "last_mqtt_seen", "last_reference_seen",
            "packet_count", "position_count", "telemetry_count", "message_count",
        ]
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(headers)
            for n in nodes:
                w.writerow([
                    n.node_id, n.display_name(), n.long_name or "", n.short_name or "",
                    n.hardware or "", n.status, n.source_label(),
                    n.location_name or "", n.latitude or "", n.longitude or "",
                    n.altitude or "", n.distance_miles or "", n.is_local or "",
                    n.first_seen.isoformat() if n.first_seen else "",
                    n.last_heard.isoformat() if n.last_heard else "",
                    n.last_mqtt_seen.isoformat() if n.last_mqtt_seen else "",
                    n.last_reference_seen.isoformat() if n.last_reference_seen else "",
                    n.packet_count, n.position_count, n.telemetry_count, n.message_count,
                ])

    def export_nodes_json(self, path: str) -> None:
        import json as _json
        nodes = self.get_all_nodes()
        out = []
        for n in nodes:
            out.append({
                "node_id": n.node_id,
                "display_name": n.display_name(),
                "long_name": n.long_name,
                "short_name": n.short_name,
                "hardware": n.hardware,
                "status": n.status,
                "sources": n.sources_seen,
                "source_label": n.source_label(),
                "location_name": n.location_name,
                "latitude": n.latitude,
                "longitude": n.longitude,
                "altitude": n.altitude,
                "distance_miles": n.distance_miles,
                "is_local": n.is_local,
                "first_seen": n.first_seen.isoformat() if n.first_seen else None,
                "last_seen": n.last_heard.isoformat() if n.last_heard else None,
                "last_mqtt_seen": n.last_mqtt_seen.isoformat() if n.last_mqtt_seen else None,
                "last_reference_seen": n.last_reference_seen.isoformat() if n.last_reference_seen else None,
                "packet_count": n.packet_count,
                "position_count": n.position_count,
                "telemetry_count": n.telemetry_count,
                "message_count": n.message_count,
            })
        with open(path, "w", encoding="utf-8") as f:
            _json.dump(out, f, indent=2)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _row_to_packet(self, row: sqlite3.Row) -> MQTTPacket:
        return MQTTPacket(
            received_at=datetime.fromisoformat(row["received_at"]),
            topic=row["topic"],
            packet_type=row["packet_type"],
            sender=row["sender"],
            from_num=row["from_num"],
            to_num=row["to_num"],
            channel=row["channel"],
            raw_json=row["raw_json"],
            summary=row["summary"] or "",
            packet_id=row["packet_id"],
            db_id=row["id"],
        )

    def _row_to_node(self, row: sqlite3.Row) -> Node:
        keys = row.keys()

        def _ts(col: str) -> Optional[datetime]:
            v = row[col] if col in keys else None
            return datetime.fromisoformat(v) if v else None

        def _str(col: str) -> Optional[str]:
            return row[col] if col in keys else None

        def _int(col: str, default: int = 0) -> int:
            v = row[col] if col in keys else None
            return int(v) if v is not None else default

        sources_raw = row["sources_seen"] if "sources_seen" in keys else "[]"
        try:
            sources = json.loads(sources_raw or "[]")
        except (ValueError, TypeError):
            sources = []

        try:
            seen_roots = json.loads(row["seen_roots"] if "seen_roots" in keys else "[]" or "[]")
        except (ValueError, TypeError):
            seen_roots = []

        try:
            seen_channels = json.loads(row["seen_channels"] if "seen_channels" in keys else "[]" or "[]")
        except (ValueError, TypeError):
            seen_channels = []

        return Node(
            node_id=row["node_id"],
            long_name=_str("long_name"),
            short_name=_str("short_name"),
            hardware=_str("hardware"),
            role=_str("role"),
            firmware_version=_str("firmware_version"),
            region=_str("region"),
            latitude=row["latitude"] if "latitude" in keys else None,
            longitude=row["longitude"] if "longitude" in keys else None,
            altitude=row["altitude"] if "altitude" in keys else None,
            location_name=_str("location_name"),
            position_precision=row["position_precision"] if "position_precision" in keys else None,
            first_seen=_ts("first_seen"),
            last_heard=_ts("last_heard"),
            last_mqtt_seen=_ts("last_mqtt_seen"),
            last_reference_seen=_ts("last_reference_seen"),
            last_position_seen=_ts("last_position_seen"),
            last_nodeinfo_seen=_ts("last_nodeinfo_seen"),
            last_telemetry_seen=_ts("last_telemetry_seen"),
            last_text_seen=_ts("last_text_seen"),
            last_map_seen=_ts("last_map_seen"),
            packet_count=_int("packet_count"),
            position_count=_int("position_count"),
            telemetry_count=_int("telemetry_count"),
            nodeinfo_count=_int("nodeinfo_count"),
            message_count=_int("message_count"),
            sources_seen=sources,
            seen_roots=seen_roots,
            seen_channels=seen_channels,
            last_source=_str("last_source"),
            last_packet_type=_str("last_packet_type"),
            last_topic=_str("last_topic"),
            status=_str("status") or "Unknown",
            is_local=bool(row["is_local"]) if ("is_local" in keys and row["is_local"] is not None) else None,
            distance_miles=row["distance_miles"] if "distance_miles" in keys else None,
        )

    # ── MQTT root table ───────────────────────────────────────────────────────

    def ensure_default_roots(self, defaults: List[dict]) -> None:
        """Insert default roots if absent; correct auto_connect on non-manual rows.

        Each entry: {root_topic, enabled, auto_connect, notes}.
        - New rows: inserted with manual=0.
        - Existing rows where manual=0: auto_connect is corrected to the desired value
          so that changes to defaults (e.g. disabling msh/US) propagate to existing DBs.
        - Existing rows where manual=1: left completely untouched — user owns them.
        """
        now = datetime.now().isoformat()
        for d in defaults:
            root        = d["root_topic"]
            desired_ac  = int(d.get("auto_connect", 1))
            desired_en  = int(d.get("enabled", 1))

            existing = self._conn.execute(
                "SELECT manual FROM mqtt_roots WHERE root_topic=?", (root,)
            ).fetchone()

            if existing is None:
                cls = classify_root(root)
                self._conn.execute(
                    """INSERT OR IGNORE INTO mqtt_roots (
                        root_topic, enabled, auto_connect, manual, first_seen,
                        country, state_code, region, root_type, notes
                    ) VALUES (?,?,?,0,?,?,?,?,?,?)""",
                    (
                        root, desired_en, desired_ac, now,
                        cls["country"], cls["state_code"],
                        cls["region"],  cls["root_type"],
                        d.get("notes", ""),
                    ),
                )
            elif not existing["manual"]:
                # Propagate default changes to DB-seeded (non-manual) rows
                self._conn.execute(
                    """UPDATE mqtt_roots
                       SET auto_connect=?, enabled=?
                       WHERE root_topic=? AND manual=0""",
                    (desired_ac, desired_en, root),
                )
        self._conn.commit()

    def upsert_mqtt_root(
        self,
        root_topic: str,
        packet_count_delta: int = 0,
        channels: Optional[Set[str]] = None,
        is_discovery: bool = False,
        discovery_duration_seconds: int = 60,
    ) -> None:
        """Update or insert a discovered/active MQTT root.

        Called by the discovery collector and by the live packet handler.
        Does NOT change enabled or auto_connect — those are user-controlled.
        """
        # Reject roots that embed the protocol-level channel segment '/2'.
        # Valid roots are msh/{country}[/{state}[/{region}]] — never containing '/2'.
        if root_topic.endswith("/2") or "/2/" in root_topic:
            logging.warning("upsert_mqtt_root: rejected invalid root %r", root_topic)
            return
        now = datetime.now().isoformat()
        existing = self._conn.execute(
            "SELECT * FROM mqtt_roots WHERE root_topic=?", (root_topic,)
        ).fetchone()

        new_channels: set
        if channels:
            new_channels = set(channels)
        else:
            new_channels = set()

        if existing is None:
            cls = classify_root(root_topic)
            ppm = (packet_count_delta / discovery_duration_seconds * 60.0
                   if is_discovery and discovery_duration_seconds else 0.0)
            self._conn.execute(
                """INSERT INTO mqtt_roots (
                    root_topic, first_seen, last_seen,
                    packet_count_total, packet_count_recent, packets_per_minute,
                    channels_seen, country, state_code, region, root_type,
                    last_discovery_run
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    root_topic, now, now,
                    packet_count_delta,
                    packet_count_delta if is_discovery else 0,
                    ppm,
                    json.dumps(sorted(new_channels)),
                    cls["country"], cls["state_code"],
                    cls["region"],  cls["root_type"],
                    now if is_discovery else None,
                ),
            )
        else:
            try:
                old_ch = set(json.loads(existing["channels_seen"] or "[]"))
            except (ValueError, TypeError):
                old_ch = set()
            merged = sorted(old_ch | new_channels)

            ppm = (packet_count_delta / discovery_duration_seconds * 60.0
                   if is_discovery and discovery_duration_seconds
                   else float(existing["packets_per_minute"] or 0))

            parts = ["last_seen=?", "packet_count_total=packet_count_total+?",
                     "channels_seen=?"]
            params: list = [now, packet_count_delta, json.dumps(merged)]

            if is_discovery:
                parts += ["packet_count_recent=?", "packets_per_minute=?",
                          "last_discovery_run=?"]
                params += [packet_count_delta, ppm, now]

            params.append(root_topic)
            self._conn.execute(
                f"UPDATE mqtt_roots SET {', '.join(parts)} WHERE root_topic=?",
                params,
            )
        self._conn.commit()

    def get_all_mqtt_roots(self) -> List[dict]:
        rows = self._conn.execute(
            "SELECT * FROM mqtt_roots ORDER BY packet_count_total DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_auto_connect_roots(self) -> List[str]:
        rows = self._conn.execute(
            "SELECT root_topic FROM mqtt_roots WHERE auto_connect=1 ORDER BY root_topic"
        ).fetchall()
        return [r["root_topic"] for r in rows]

    def set_root_enabled(self, root_topic: str, enabled: bool) -> None:
        self._conn.execute(
            "UPDATE mqtt_roots SET enabled=? WHERE root_topic=?",
            (int(enabled), root_topic),
        )
        self._conn.commit()

    def set_root_auto_connect(self, root_topic: str, auto_connect: bool) -> None:
        self._conn.execute(
            "UPDATE mqtt_roots SET auto_connect=? WHERE root_topic=?",
            (int(auto_connect), root_topic),
        )
        self._conn.commit()

    def set_root_notes(self, root_topic: str, notes: str) -> None:
        self._conn.execute(
            "UPDATE mqtt_roots SET notes=? WHERE root_topic=?",
            (notes, root_topic),
        )
        self._conn.commit()

    def update_root_last_connected(self, root_topic: str) -> None:
        self._conn.execute(
            "UPDATE mqtt_roots SET last_connected=? WHERE root_topic=?",
            (datetime.now().isoformat(), root_topic),
        )
        self._conn.commit()

    def add_manual_root(
        self,
        root_topic: str,
        enabled: bool = True,
        auto_connect: bool = True,
        notes: str = "",
    ) -> None:
        """Add a user-entered root.  Updates existing row if already known."""
        now = datetime.now().isoformat()
        existing = self._conn.execute(
            "SELECT root_topic FROM mqtt_roots WHERE root_topic=?", (root_topic,)
        ).fetchone()
        if existing:
            self._conn.execute(
                """UPDATE mqtt_roots
                   SET enabled=?, auto_connect=?, manual=1, notes=?
                   WHERE root_topic=?""",
                (int(enabled), int(auto_connect), notes, root_topic),
            )
        else:
            cls = classify_root(root_topic)
            self._conn.execute(
                """INSERT INTO mqtt_roots (
                    root_topic, enabled, auto_connect, manual, first_seen,
                    country, state_code, region, root_type, notes
                ) VALUES (?,?,?,1,?,?,?,?,?,?)""",
                (
                    root_topic, int(enabled), int(auto_connect), now,
                    cls["country"], cls["state_code"],
                    cls["region"],  cls["root_type"],
                    notes,
                ),
            )
        self._conn.commit()

    def delete_mqtt_root(self, root_topic: str) -> None:
        self._conn.execute(
            "DELETE FROM mqtt_roots WHERE root_topic=?", (root_topic,)
        )
        self._conn.commit()

    def set_root_staged(self, root_topic: str, staged: bool) -> None:
        self._conn.execute(
            "UPDATE mqtt_roots SET staged=? WHERE root_topic=?",
            (int(staged), root_topic),
        )
        self._conn.commit()

    def get_staged_roots(self) -> List[str]:
        rows = self._conn.execute(
            "SELECT root_topic FROM mqtt_roots WHERE staged=1 ORDER BY root_topic"
        ).fetchall()
        return [r["root_topic"] for r in rows]

    def close(self) -> None:
        self._conn.close()


# ── Standalone export helpers (no DB needed) ──────────────────────────────────

def export_node_reference(nodes: "List[Node]", filter_label: str) -> str:
    """Serialize a list of Node objects as a mesh_command_post_node_reference JSON string.

    filter_label is stored in the envelope (e.g. 'all_known', 'visible_window',
    'active_recent', 'gps_only').  I/O is handled by the caller.
    """
    import json as _json
    from datetime import timezone

    def _dt(dt: "Optional[datetime]") -> "Optional[str]":
        return dt.isoformat() if dt is not None else None

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    node_records = []
    for n in nodes:
        node_records.append({
            "node_id":                 n.node_id,
            "long_name":               n.long_name,
            "short_name":              n.short_name,
            "display_name":            n.display_name(),
            "latitude":                n.latitude,
            "longitude":               n.longitude,
            "altitude":                n.altitude,
            "location_name":           n.location_name,
            "position_source":         n.position_source,
            "position_precision":      n.position_precision,
            "first_seen":              _dt(n.first_seen),
            "last_seen":               _dt(n.last_heard),
            "last_position_seen":      _dt(n.last_position_seen),
            "last_mqtt_seen":          _dt(n.last_mqtt_seen),
            "last_map_seen":           _dt(n.last_map_seen),
            "last_telemetry_seen":     _dt(n.last_telemetry_seen),
            "last_message_seen":       _dt(n.last_text_seen),
            "sources_seen":            n.sources_seen,
            "packet_count":            n.packet_count,
            "position_count":          n.position_count,
            "telemetry_count":         n.telemetry_count,
            "message_count":           n.message_count,
            "last_packet_type":        n.last_packet_type,
            "last_topic":              n.last_topic,
            "hardware_model":          n.hardware,
            "role":                    n.role,
            "firmware_version":        n.firmware_version,
            "region":                  n.region,
            "modem_preset":            None,   # not tracked in Node model
            "channel_name":            None,   # not tracked in Node model
            "status":                  n.status,
            "is_local":                n.is_local,
            "distance_from_home_miles": n.distance_miles,
        })

    doc = {
        "export_type":      "mesh_command_post_node_reference",
        "app_version":      "1.0.0",
        "exported_at":      now_utc,
        "visibility_filter": filter_label,
        "node_count":       len(node_records),
        "nodes":            node_records,
    }
    return _json.dumps(doc, indent=2)


def export_node_reference_csv(nodes: "List[Node]") -> str:
    """Serialize a list of Node objects as a CSV string with DictWriter.

    Returns the full CSV text (including header); I/O is handled by the caller.
    """
    import csv as _csv
    import io as _io

    fieldnames = [
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

    def _dt(dt: "Optional[datetime]") -> str:
        return dt.isoformat() if dt is not None else ""

    buf = _io.StringIO()
    writer = _csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    for n in nodes:
        writer.writerow({
            "node_id":                  n.node_id,
            "long_name":                n.long_name or "",
            "short_name":               n.short_name or "",
            "display_name":             n.display_name(),
            "latitude":                 "" if n.latitude  is None else n.latitude,
            "longitude":                "" if n.longitude is None else n.longitude,
            "altitude":                 "" if n.altitude  is None else n.altitude,
            "location_name":            n.location_name or "",
            "position_source":          n.position_source or "",
            "position_precision":       "" if n.position_precision is None else n.position_precision,
            "first_seen":               _dt(n.first_seen),
            "last_seen":                _dt(n.last_heard),
            "last_position_seen":       _dt(n.last_position_seen),
            "last_mqtt_seen":           _dt(n.last_mqtt_seen),
            "last_map_seen":            _dt(n.last_map_seen),
            "last_telemetry_seen":      _dt(n.last_telemetry_seen),
            "last_message_seen":        _dt(n.last_text_seen),
            "sources_seen":             json.dumps(n.sources_seen),
            "packet_count":             n.packet_count,
            "position_count":           n.position_count,
            "telemetry_count":          n.telemetry_count,
            "message_count":            n.message_count,
            "last_packet_type":         n.last_packet_type or "",
            "last_topic":               n.last_topic or "",
            "hardware_model":           n.hardware or "",
            "role":                     n.role or "",
            "firmware_version":         n.firmware_version or "",
            "region":                   n.region or "",
            "modem_preset":             "",
            "channel_name":             "",
            "status":                   n.status,
            "is_local":                 "" if n.is_local is None else n.is_local,
            "distance_from_home_miles": "" if n.distance_miles is None else n.distance_miles,
        })
    return buf.getvalue()
