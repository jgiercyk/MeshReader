# Mesh Command Post — Project Context

## What This App Is

Read-only Meshtastic MQTT monitor. Subscribes to live Meshtastic MQTT feeds,
displays packets, tracks nodes, and renders a Leaflet/OSM map with GPS positions.
**This app NEVER transmits to the mesh — it is strictly receive-only.**

## Stack

| Component | Version |
|-----------|---------|
| Python    | 3.14    |
| PySide6   | 6.11    |
| paho-mqtt | 2.x     |
| SQLite    | (stdlib) |
| PyInstaller | 6.21 |
| pycryptodome | optional — enables AES-CTR decryption of Map Reports |

## How to Run

**From source:**
```powershell
cd src
python main.py
```

**Pre-built exe:**
```
dist\MeshCommandPost\MeshCommandPost.exe
```

**Rebuild after code changes:**
```powershell
python -m PyInstaller --noconfirm MeshCommandPost.spec
```

## Project Layout

```
src/
  main.py               # Entry point — QApplication setup, high-DPI, Fusion style
  app.py                # App(QObject) — signal wiring, packet routing, UI flush timer
  models.py             # MQTTPacket and Node dataclasses + display helpers
  packet_parser.py      # parse_packet(), extract_node_updates(), compute_packet_hash()
  storage.py            # Storage(SQLite) — packets + nodes, dedup, migration, export
  map_decoder.py        # Minimal protobuf parser + AES-CTR decryptor for Map Reports
  intelligence.py       # Pure functions: node status, haversine distance, enrich_node()
  production_mqtt.py    # ProductionMqttClient — the ONE authoritative MQTT connection
  source_manager.py     # SourceConfig, SourceManager — stats scaffold + config storage
  subscription_registry.py  # SubscriptionRegistry — live sub tracking; DIRECT/ROOT_DERIVED/MAP_TYPE
  discovery_client.py   # DiscoveryClient — isolated one-shot root discovery (loop_forever on daemon thread)
  topic_probe_client.py # TopicProbeClient — isolated diagnostic probe (loop_start design)
  root_classifier.py    # classify_root() / activity_label() — root metadata helpers
  config_manager.py     # ConfigManager — JSON settings with typed property accessors
  geocoder.py           # Background Nominatim reverse geocoder, SQLite-cached, 1 req/sec
  reference_importer.py # Import node records from JSON/CSV (MeshMap exports, etc.)
  registry.py           # NodeRegistry — in-memory node cache, display helpers, stats
  ui/
    main_window.py      # MainWindow — tab container, visibility bar, event log, export
    packet_feed.py      # Packets tab — incremental insert, filter bar, JSON detail pane
    node_list.py        # Nodes tab — dirty-flag upsert, filter buttons, context menu
    map_view.py         # Map tab — Leaflet in QWebEngineView, single-load + JS updates
    source_panel.py     # Source panel — per-source MQTT status table, Edit dialog
    root_manager.py     # Root Manager dialog — two-pane discovered/staged/active roots
    topic_probe_dialog.py # Topic Probe and Live Subs dialogs
    message_view.py     # Messages tab — text packets only
    telemetry_view.py   # Telemetry tab — device + environment metrics
```

## Data Storage

| Item | Path |
|------|------|
| SQLite DB | `~/.mesh_command_post/history.db` |
| App config / settings | `~/.mesh_command_post/intelligence_config.json` |
| Debug log | `~/.mesh_command_post/debug.log` |

History loaded on startup: last 300 packets, all nodes.

## Source Types

| Source | Decoder | Default | Notes |
|--------|---------|---------|-------|
| MQTT JSON | `json` | Enabled | Primary feed; `msh/US/2/json/#` |
| MQTT Map Reports | `map_report` | Disabled | Binary protobuf; `msh/US/2/map/#` |
| MQTT Raw (Advanced) | `protobuf` | Disabled | All `msh/US/2/#` traffic — high volume, overlaps other sources |

Raw/Advanced source requires explicit confirmation to enable and is for short
diagnostic sessions only.

## Safe Baseline Mode

`SAFE_MODE_MQTT = True` is a module-level constant in `app.py`. The flag controls:

- **Production path is always fixed** to `msh/US/2/json/#` and `msh/US/2/map/#`. These are never changed by Root Manager, discovery, or probe — in any mode.
- **Root Manager is fully functional** for browsing, discovery, and staging. Roots move through four states: Discovered → Staged → Auto-connect → Active. In Safe Mode, staging writes to DB but never activates a live MQTT subscription automatically.
- **Discovery is always allowed.** `start_discovery()` always runs; discovered roots are saved to the DB as Discovered/Staged state only — never auto-activated.
- The mode banner in the source panel and Root Manager reflects the current state.

When `SAFE_MODE_MQTT = False` (Normal Mode), Root Manager Add/Remove buttons call `subscribe_root()`/`unsubscribe_root()` — the `SourceManager` worker path.

**The production path (`msh/US/2/json/#`, `msh/US/2/map/#`) MUST NOT be changed by any Root Manager action, discovery result, or database load.**

## Key Architecture Decisions

- **`ProductionMqttClient` is the ONE production MQTT connection.** It lives in
  `production_mqtt.py`, uses `loop_start()` + Event-based wait (same pattern as
  `TopicProbeClient`), subscribes to exactly `msh/US/2/json/#` and `msh/US/2/map/#`,
  and auto-reconnects with exponential backoff. It emits Qt signals from the paho
  daemon thread; Qt auto-queues them to the main thread.
- **`SourceManager` workers do NOT run.** They are kept as stats scaffolding only —
  `record_received()`, `record_decoded()`, `record_ignored()`, `set_connected()` update
  their counters so the source panel displays correct packet counts and status.
  Never call `connect_all()`, `reset_source()`, or `start()` on source workers.
- **All DB writes on the main Qt thread.** MQTT worker threads only emit signals;
  they never call SQLite directly.
- **MQTT thread → signal → main thread.** `ProductionMqttClient` emits
  `packet_received`; `App._on_prod_packet` routes by topic to the correct decoder.
- **Topic routing in `_on_prod_packet`:** `/2/json/` → `_handle_json_packet`,
  `/2/map/` → `_handle_map_packet`. Source tag (mqtt_json / mqtt_map) determines
  which stats counters increment.
- **Timer-based batch UI flush (750 ms).** `App._flush_ui` drains `_pending_packets`
  and `_dirty_node_ids` once per tick — decouples high MQTT arrival rate from display.
- **Dedup by packet_id first, hash fallback.** SHA-256 of `topic|payload|minute-bucket`
  prevents duplicate rows when the same packet arrives on multiple overlapping topics.
- **Dirty-flag node table.** `NodeListWidget.upsert_node()` marks rows for in-place
  cell update vs. full rebuild; `flush_table()` does the minimal work each tick.
- **Map: single HTML load + runJavaScript.** The Leaflet page loads once; all marker
  updates go through `page().runJavaScript()`. Zoom/pan state is never lost.
- **`--onedir` PyInstaller** (not `--onefile`) — required because `QtWebEngineProcess.exe`
  must be a real file on disk. Always use `MeshCommandPost.spec` to rebuild.
- **Geocoder on daemon thread, rate-limited 1 req/sec** (Nominatim ToS). Results arrive
  as Qt signals on the main thread. Cache keyed to 2 decimal places (~1 km).
- **MAP position protection.** If a node already has a position from a JSON position
  packet (`last_position_seen` set), Map Reports cannot overwrite it.

## MapReport Decoder Flow

```
MQTT bytes (latin-1 decoded for lossless round-trip)
  └─ ServiceEnvelope (protobuf)
       ├─ field 1 (bytes)  → MeshPacket
       │    ├─ field 1 (fixed32) → from_node  → "!xxxxxxxx" node ID
       │    ├─ field 4 (bytes)   → Data (unencrypted)
       │    │    ├─ field 1 (varint) → portnum  (must be 73 = MAP_APP)
       │    │    └─ field 2 (bytes)  → MapReport payload
       │    ├─ field 5 (bytes)   → encrypted bytes (AES-CTR, if field 4 absent)
       │    └─ field 6 (fixed32) → packet_id (needed for AES nonce)
       ├─ field 2 (string) → channel_id  (e.g. "LongFast")
       └─ field 3 (string) → gateway_id  (e.g. "!75f1824c")

MapReport fields:
  1  long_name            9  latitude_i   (sfixed32, degrees×1e7)
  2  short_name          10  longitude_i  (sfixed32, degrees×1e7)
  3  role                11  altitude     (int32, meters)
  4  hw_model            12  position_precision
  5  firmware_version    13  num_online_local_nodes
  6  region              14  has_opted_report_location
  7  modem_preset
  8  has_default_channel  ← bool, NOT a coordinate
```

**CRITICAL:** Field 8 is `has_default_channel` (bool), not a coordinate.
`latitude_i` = field **9**, `longitude_i` = field **10**.

Node ID comes from `MeshPacket.from` (field 1) — the MapReport message itself
carries no node identity. Gateway ID is used as fallback if `from` is absent.

AES-CTR nonce: `pack('<Q', packet_id) + pack('<Q', from_node)` (16 bytes LE).

## Known Working Features

- Live Meshtastic JSON packet monitoring with filter bar and JSON detail pane
- Node list with status, distance, GPS, source label, and per-packet-type counters
- Leaflet/OSM map with live marker updates (zoom/pan preserved)
- Reverse geocoding via Nominatim (SQLite-cached, rate-limited)
- Map Report binary protobuf decoding (AES-CTR decryption with default LongFast key)
- Visibility window (1h – 30d or all-time) for nodes and map markers
- Reference node import (MeshMap JSON/CSV exports)
- Node watchlist with seen/position/message alerts
- CSV and JSON export (both packet feed and node list)
- Multi-source MQTT with per-source stats and edit dialog
- Session-persistent settings (sources, visibility window, home coordinates)
- Startup history load (last 300 packets, all nodes)
- DB schema migrations (idempotent ALTER TABLE + data cleanup on startup)

## Known Limitations

- **No app icon.** PyInstaller uses the default Windows icon. Add `--icon app.ico`
  to `MeshCommandPost.spec` when an icon is ready.
- **Map requires internet.** Leaflet JS/CSS and OSM tiles load from CDN. Offline
  equals blank map; the rest of the app still works.
- **No general protobuf decode** for `msh/REGION/2/e/#` encrypted topics. The
  "MQTT Raw (Advanced)" source receives these but cannot decode them.
- **Packet display cap.** Feed holds 1000 packets in memory, shows up to 500
  after filtering. Fine for v1.
- **pycryptodome optional.** Without it, encrypted Map Reports cannot be decrypted.
  Install via `pip install pycryptodome`.
- **`SubscriptionRegistry.record_packet()` is never called.** The method exists but
  `_on_prod_packet` in `app.py` updates `source_manager.record_received()` instead of
  the registry. Packet counts in Root Manager's right pane will therefore always show 0.
  Not a crash risk, but the display is misleading. Fix by calling `sub_registry.record_packet(topic)`
  from `_on_prod_packet` when the change is explicitly requested.
- **`SourceManager.unsubscribe_topics()` does not exist.** Normal Mode "Remove Direct
  Subscription" in `root_manager._act_remove()` calls `self._app.source_manager.unsubscribe_topics()`
  which will NameError. This path is never reached in Safe Baseline Mode. Fix before
  enabling Normal Mode by routing to `unsubscribe_root()` instead.

## DO NOT BREAK — Critical Lessons Learned

1. **Never enable "MQTT Raw (Advanced)" by default.** It subscribes to all regional
   traffic and floods the packet feed.
2. **Never feed raw binary payloads into the JSON parser.** Map Report and protobuf
   payloads must go through `map_decoder.decode_map_payload()` via the `map_report`
   decoder path, never through `json.loads()`.
3. **Never store malformed decode attempts in the `packets` table.** Old builds that
   called `json.loads()` on binary data stored rows with `packet_type='INVALID'`; the
   DB migration on startup deletes any such rows.
4. **Never rebuild the full node table on every packet.** Use the dirty-flag pattern:
   `upsert_node()` marks a node dirty; `flush_table()` does in-place cell updates
   unless the row count has changed.
5. **Never overwrite a good MQTT JSON position with an unvalidated MapReport coord.**
   `storage.upsert_node_from_mqtt()` skips latitude/longitude updates from `mqtt_map`
   source when `last_position_seen` is already set.
6. **Never mark a node as MAP unless a MapReport was successfully decoded.** The
   `last_map_seen` timestamp is the authoritative indicator; `sources_seen` containing
   `mqtt_map` is insufficient on its own.
7. **MeshPacket wire layout (firmware 2.x):**
   - field 1 = `from` (fixed32) — sender node number
   - field 4 = `decoded` Data (unencrypted)
   - field 5 = `encrypted` bytes (AES-CTR ciphertext)
8. **MapReport field numbering (firmware 2.5+):**
   - field 8 = `has_default_channel` (bool) — NOT a coordinate
   - field 9 = `latitude_i` (sfixed32)
   - field 10 = `longitude_i` (sfixed32)
9. **Packet retention and node memory are separate concerns.** Packets are stored in
   the `packets` table with a row limit enforced at display time, not at write time.
   Node records in `nodes` persist indefinitely and are enriched with computed fields.
10. **`--onedir` only.** Never use `--onefile` — it breaks `QtWebEngineProcess.exe`.
11. **Production MQTT: use `ProductionMqttClient`, not `SourceManager` workers.**
    The `SourceManager` worker lifecycle (`loop_forever`, `join(4s)`) proved unreliable
    for production. `ProductionMqttClient` uses `loop_start()` + Event wait (same
    design as the proven `TopicProbeClient`). Never restart source_manager workers
    as the primary connection mechanism.
12. **Do NOT call `QTimer.singleShot(0, fn)` per MQTT packet.** This floods the Qt
    event queue and freezes countdown timers. The paho daemon thread emits Qt signals
    directly; Qt auto-queues them — no manual marshaling needed per message.
13. **Python `threading.Lock` is NOT re-entrant.** Calling `snapshot()` (which acquires
    `_lock`) inside `with self._lock:` deadlocks permanently. Call `snapshot()` only
    after `loop_stop()` when no paho callbacks can fire.
14. **`_load_sources()` must NOT load DB roots at startup.** Loading roots from the
    `mqtt_roots` SQLite table caused stale SC subscriptions to persist across restarts.
    `db_roots = []` is hardcoded; roots are managed only via Root Manager UI.
15. **Topic Probe is read-only diagnostic only.** The broker, credentials, and topics
    are confirmed working: `msh/US/2/json/#` and `msh/US/2/map/#`. Probe results can
    inform config, but the probe itself never feeds production packet processing.
16. **Root Manager staging does NOT activate live subscriptions in Safe Mode.** The
    four root states (Discovered, Staged, Auto-connect, Active) are explicit. Staging
    writes `staged=1` to the `mqtt_roots` DB table only. Never auto-promote Staged →
    Active without an explicit user action and Normal Mode enabled.
17. **`mqtt_client.py` has been deleted.** It was dead code — an old `MQTTClient` using
    `loop_forever()` that was never imported. Do not recreate it. If MQTT connection
    logic is needed, extend `ProductionMqttClient` or create a properly isolated client.

## No Cowboy Programming Rules

These are permanent engineering discipline requirements for this project. Follow them in every change.

1. **One problem at a time.** Fix the stated issue. Do not clean up adjacent code while fixing a bug, and do not refactor while adding a feature.
2. **Preserve the known-good baseline.** The baseline is: `msh/US/2/json/#` and `msh/US/2/map/#` connected, MQTT Raw disabled, `SAFE_MODE_MQTT = True`. Never break it.
3. **No hidden behavior.** No hidden subscriptions, no background discovery, no silent reconnect loops, no silent DB writes from diagnostics, no broad MQTT subscriptions unless explicitly requested.
4. **Small, testable changes.** Each change should be a single concern. Git-commit after each working change.
5. **Explain before risky changes.** Architecture changes, threading model changes, MQTT lifecycle changes, SQLite schema changes, Root Manager behavior changes — explain the plan and get agreement first.
6. **No speculative rewrites.** Do not redesign something that works just because you have a "better idea." Only rewrite when there is a confirmed problem.
7. **Diagnostics must be isolated.** Diagnostic clients get their own MQTT client, never piggyback on production. They do not write to the `packets`, `nodes`, or `map` tables. They clean up when done.
8. **One source of truth.** MQTT subscriptions are tracked by `SubscriptionRegistry`. UI renders the actual subscription state — never a cached assumption. `_load_sources()` always sets `db_roots = []`.
9. **Resource use matters.** Monitor CPU, memory, SQLite locking, DB/WAL growth, UI update rate, MQTT traffic, reconnect timer accumulation, stale threads.
10. **If uncertain, stop and report.** Add logging, write a minimal diagnostic, reproduce the issue, report findings, then propose the smallest fix. Do not guess.

### Required End-of-Change Checklist

Before marking any task complete, report:

- What exact issue was fixed
- Files changed (with line numbers for key changes)
- Whether the safe MQTT baseline still works (`msh/US/2/json/#`, `msh/US/2/map/#`, Raw disabled)
- Whether Root Manager / discovery / probe were touched
- Whether any dead code was removed
- Whether any resource risk remains
- What was tested
- What was NOT tested
- Whether CLAUDE.md was updated

## Build Checklist

After any code change:

1. `cd C:\Users\jim\MQTTReader\mesh-command-post`
2. `python -m PyInstaller --noconfirm MeshCommandPost.spec`
3. Verify the build completes without errors.
4. Launch `dist\MeshCommandPost\MeshCommandPost.exe`.
5. Check the map tab loads (Leaflet tiles appear).
6. Confirm a source connects and packets arrive in the feed.
7. Verify no errors in `~/.mesh_command_post/debug.log`.

## Future Backlog (do not build yet)

- Protobuf decode for `msh/REGION/2/e/#` general encrypted topics
- Private broker / custom channel PSK configuration UI
- MQTT replay from JSONL file
- KML export
- Inno Setup installer wrapper
- Offline tile caching
