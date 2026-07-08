# SQLite Audit & Retention Plan — Mesh Command Post
*Generated 2026-07-04*

---

## Database Files and Sizes

| File | Size |
|------|------|
| `~/.mesh_command_post/history.db` | 34.29 MB |
| `history.db-wal` | 4.26 MB (not yet checkpointed) |
| `history.db-shm` | 0.03 MB |
| **Live total** | **38.59 MB** |
| Backup: `history_20260701_091957.db` | 16.23 MB |
| Backup: `history_20260701_102140.db` | 16.74 MB |
| Backup: `history_20260701_103101.db` | 16.76 MB |
| Backup: `history_20260701_232023.db` | 23.70 MB |
| Backup: `history_20260703_101650.db` | 31.77 MB |
| **Backups total** | **105.2 MB** |
| `debug.log` | 12.75 MB |
| **Grand total on disk** | **~156 MB** |

The WAL file is not checkpointed because the app was stopped without a clean shutdown.
`PRAGMA wal_checkpoint(PASSIVE)` at next startup (or after cleanup) would merge it into
the main file and shrink it.

---

## Tables — Row Counts and Purpose

| Table | Rows | Purpose | Verdict |
|-------|------|---------|---------|
| `packets` | **52,693** | Full raw JSON packet log with history | **DANGEROUS** — no expiration |
| `nodes` | 1,303 | One row per known node, current state | **SAFE** |
| `geocode_cache` | 1,503 | Reverse geocode results keyed by lat/lon | **SAFE** |
| `watchlist` | 0 | User-defined alert list | **SAFE** |
| `sqlite_sequence` | 1 | SQLite internal autoincrement tracker | (internal) |

---

## Packets Table — Detailed Breakdown

**By type:**

| packet_type | Rows | Total raw_json |
|-------------|------|----------------|
| `position` | 27,595 | 7.88 MB |
| `telemetry` | 10,490 | 2.91 MB |
| `nodeinfo` | 5,805 | 1.47 MB |
| `""` (blank) | 5,418 | 0.79 MB |
| `text` | 2,743 | 0.92 MB |
| `traceroute` | 310 | 0.12 MB |
| `sendtext` | 230 | 0.04 MB |
| `neighborinfo` | 101 | 0.03 MB |
| `unknown` | 1 | ~0 |
| **Total** | **52,693** | **14.16 MB raw_json** |

**Date range:** 2026-06-29 to 2026-07-03 — approximately 4 days of data.

**Blank `packet_type` rows (5,418):** Legitimate Meshtastic network packets where the
firmware emits `"type": ""` for encrypted or unrecognized portnum packets. Not a bug in
this app; network noise being faithfully stored.

**`sendtext` packets (230):** Broadcast text messages received on the MQTT feed where
the sending node's identity wasn't resolvable. Not outgoing transmissions from this app.
Read-only integrity confirmed.

**`unknown` node (28,274 `packet_count`):** The aggregate counter on the single `nodes`
row for node_id=`"unknown"` — not 28k separate rows. All packets where sender
normalization failed roll up here. One node record; harmless.

**Dedup effectiveness:** 69 possible duplicates out of 52,426 rows with a packet_id
(0.13%). Dedup is working correctly.

---

## Growth Projection

| Metric | Value |
|--------|-------|
| Packets ingested in last 24h | ~4,100 |
| Projected 7-day ingestion | ~28,700 packets |
| Projected 30-day ingestion | ~123,000 packets |
| Raw JSON per packet (avg) | ~275 bytes |
| Raw JSON growth/day | ~1.1 MB |
| DB with SQLite overhead (~2.5x) | ~2.7 MB/day |
| **Projected monthly DB growth** | **~80 MB/month** |

At this rate, the DB will reach ~1 GB within 12–13 months. Backups multiply this
(5 copies kept). With the debug.log growing unchecked, total disk usage could plausibly
reach 2–3 GB within a year.

---

## Specific Answers

| Question | Answer |
|----------|--------|
| Raw packet history stored? | **Yes** — all packets, indefinitely, in `packets.raw_json` |
| Expiration policy? | **None** — no DELETE, no age cutoff, no row cap |
| Cleanup on startup? | Only for `INVALID` rows and migration data fixes |
| Scheduled cleanup? | **No** QTimer-driven periodic cleanup |
| Duplicate packets? | Rarely (69/52k, 0.13%) — dedup works well |
| Malformed messages retained? | Blank-type rows yes; `INVALID` rows deleted on startup |
| Telemetry: every-event vs. latest? | Every event in `packets`; `nodes` stores only latest timestamps + counts (correct) |
| WAL checkpointing? | **Not explicit** — WAL grows until OS merges it |
| DB size reporting? | **Not present** |

---

## Table Verdicts

| Table | Verdict | Reason |
|-------|---------|--------|
| `nodes` | **SAFE** | Bounded by unique nodes; latest-state-only |
| `geocode_cache` | **SAFE** | Unique (lat,lon) PK enforces natural bound |
| `watchlist` | **SAFE** | User-controlled; currently empty |
| `packets` | **DANGEROUS** | Grows without bound; primary disk risk |

---

## Recommended Storage Model

### `nodes` — no changes needed
The schema is clean: one row per node, latest-state-only, with per-type counters.
This is exactly right.

### `geocode_cache` — no changes needed
UPSERT semantics with unique (lat,lon) key self-limits growth. Tiny and stable.

### `packets` — add retention limits
Proposed defaults:
- Maximum age: **7 days**
- Maximum row count: **15,000 rows**
- Whichever limit triggers first wins
- Startup cleanup + scheduled cleanup every 30 minutes while app runs
- `PRAGMA wal_checkpoint(PASSIVE)` after each cleanup

### `debug.log` — out of scope here, but worth noting
At 12.75 MB after 4 days, it will grow unboundedly. Consider switching
`logging.basicConfig` to a `RotatingFileHandler` (e.g. 5 MB × 3 rotations).

---

## Implementation Plan

### Files to modify
- `src/storage.py`
- `src/app.py`

### Changes to `src/storage.py`

1. **Add retention constants** at module level:
   ```python
   PACKET_RETAIN_DAYS = 7
   PACKET_RETAIN_MAX_ROWS = 15_000
   ```

2. **Add `_cleanup_old_packets()` method** to `Storage`:
   - DELETE packets older than `PACKET_RETAIN_DAYS` days
   - If row count still exceeds `PACKET_RETAIN_MAX_ROWS`, DELETE oldest rows down to limit
   - Return dict with `{"deleted_by_age": N, "deleted_by_cap": N}`

3. **Add `checkpoint_wal()` method** to `Storage`:
   - Run `PRAGMA wal_checkpoint(PASSIVE)`

4. **Add `get_db_size_mb()` method** to `Storage`:
   - Return combined size of `.db` + `.db-wal` + `.db-shm` in MB

5. **Call `_cleanup_old_packets()` in `Storage.__init__`** after existing migrations,
   store result in `startup_stats`.

### Changes to `src/app.py`

6. **Log packet count + DB size** in the startup summary (already logs node count).

7. **Add a 30-minute periodic cleanup QTimer** in `App.__init__`:
   - Calls `self.storage.cleanup_packets()` (a public wrapper)
   - Calls `self.storage.checkpoint_wal()`
   - Logs result to `self.window.log()` only if rows were deleted

8. **Log cleanup stats** from `startup_stats` alongside position/altitude/invalid counts.

### What is NOT changing
- `nodes` table — untouched
- `geocode_cache` table — untouched
- `watchlist` table — untouched
- Dedup logic — unchanged (recent 24h window covers all real-time needs)
- Backup rotation — unchanged (still keeps 5 most recent)
- No `VACUUM` — explicitly excluded

---

## No-Touch Constraints

- Do not remove useful node state
- Do not break node discovery, map display, node tab, or node_id → location_name propagation
- Do not run VACUUM automatically
- Do not delete data without approval
- Raw MQTT payloads remain stored (just aged out after 7 days)
