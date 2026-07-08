import json
import logging
import logging.handlers
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

from production_mqtt import JSON_TOPIC, MAP_TOPIC, ProductionMqttClient

# ── Safe Baseline Mode flag ────────────────────────────────────────────────────
# When True: production MQTT is fixed to the two proven feeds (JSON + Map).
# Root Manager remains fully functional for browsing, discovery, and staging.
# Staged roots do NOT become active until the user explicitly enables Normal Mode.
# Set to False only after the baseline has been stable for an extended period.
SAFE_MODE_MQTT = True

from PySide6.QtCore import QObject, QTimer, Signal, Slot

_log_file    = Path.home() / ".mesh_command_post" / "debug.log"
_log_handler = logging.handlers.RotatingFileHandler(
    str(_log_file),
    maxBytes=5 * 1024 * 1024,
    backupCount=3,
    encoding="utf-8",
)
_log_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger().addHandler(_log_handler)

import intelligence
import reference_importer
from config_manager import ConfigManager
from geocoder import ReverseGeocoder
from map_decoder import decode_map_payload
from models import MQTTPacket
from packet_parser import HARDWARE_NAMES, compute_packet_hash, extract_node_updates, normalize_node_id, parse_packet
from registry import NodeRegistry
from discovery_client import DiscoveryClient
from source_manager import (
    DEFAULT_SOURCES, SOURCE_MQTT_JSON, SOURCE_MQTT_MAP,
    SourceConfig, SourceManager,
    extract_root, extract_channel,
)
from storage import Storage
from subscription_registry import SubscriptionRegistry, ROOT_DERIVED
from ui.main_window import MainWindow

APP_DIR       = Path.home() / ".mesh_command_post"
CONFIG_FILE   = APP_DIR / "intelligence_config.json"
SETTINGS_FILE = APP_DIR / "settings.json"
DB_FILE       = APP_DIR / "history.db"

_UI_FLUSH_MS          = 750    # UI refresh interval
_SLOW_PACKET_MS       = 30     # warn if a single packet takes longer (ms)
_SLOW_FLUSH_MS        = 200    # warn if a full UI flush takes longer (ms)
_SLOW_SECTION_MS      = 100    # warn if any one flush section takes longer (ms)
_MAP_LOG_THROTTLE_SEC = 60     # min seconds between repeated map-ignored logs per topic
_MAP_UI_LOG_MAX       = 3      # max map-ignored events shown in the UI event log
_MAP_DEBUG_PKTS       = 5      # first N map packets get a full field dump in the event log
_FLUSH_PACKET_CAP     = 80     # max packets sent to the UI per tick — backpressure limit
_FLUSH_QUEUE_LIMIT    = 400    # drop oldest packets when queue exceeds this depth


class App(QObject):
    # Emitted when root discovery scan completes; payload maps root → stats dict
    discovery_result   = Signal(dict)
    # Discovery lifecycle signals — UI uses these for countdown / button state
    discovery_started  = Signal(str, int)       # topic, duration_sec
    discovery_tick     = Signal(int, int, int)  # remaining_sec, roots_found, packets_seen
    discovery_finished = Signal(int, int)       # roots_found, packets_seen
    discovery_stopped  = Signal()
    # Emitted when test root subscriptions are added (True) or all removed (False)
    test_mode_changed  = Signal(bool)

    def __init__(self):
        super().__init__()
        APP_DIR.mkdir(parents=True, exist_ok=True)
        self.safe_mode = SAFE_MODE_MQTT

        self.config   = ConfigManager(CONFIG_FILE)
        self.storage  = Storage(
            DB_FILE,
            retain_hours=self.config.packet_retain_hours,
            retain_max_rows=self.config.packet_retain_max_rows,
        )
        self.registry = NodeRegistry()

        # Seed only msh/US/SC as an auto-connect root.
        # msh/US is intentionally NOT seeded — it generates national-scale traffic
        # and must be explicitly enabled by the user in the Root Manager.
        # msh/US/2/json/# (uppercase US) is the direct subscription on the JSON source.
        # SC root known but NOT auto-connected — msh/US/2/json/# already covers all US traffic.
        # auto_connect=0 propagates to existing installations via ensure_default_roots.
        self.storage.ensure_default_roots([
            {"root_topic": "msh/US/SC", "enabled": 0, "auto_connect": 0,
             "notes": "South Carolina state root (not active — msh/US/2/json/# covers this)"},
        ])

        sources = self._load_sources()
        self.source_manager = SourceManager(sources, parent=self)

        self.geocoder = ReverseGeocoder(self.storage, parent=self)
        self.window   = MainWindow(self)

        # Throttle dict for map-ignored debug log: topic → last_log_monotonic
        self._map_log_times:        Dict[str, float] = {}
        self._map_ui_log_count:     int              = 0
        self._map_summary_logged:   bool             = False
        self._map_debug_count:      int              = 0   # UI field-dump counter
        self._map_encrypted_noted:  bool             = False  # one-time encrypted note
        # Position outcome counters (per session)
        self._map_pos_accepted:     int              = 0
        self._map_pos_rejected:     int              = 0   # had coords but invalid
        self._map_identity_only:    int              = 0   # no position fields at all

        # ── batch-flush state ──────────────────────────────────────────────
        self._pending_packets: List[MQTTPacket] = []
        self._dirty_node_ids:  Set[str]          = set()

        # ── MQTT diagnostic tracking ───────────────────────────────────────
        # Log the first 20 unique topics received to debug.log so we can
        # confirm exactly what traffic is arriving.
        self._topics_seen: Set[str] = set()

        # Subscription registry — single source of truth for active subscriptions
        self.sub_registry = SubscriptionRegistry()
        self.source_manager.topic_subscribed.connect(self._on_topic_subscribed)
        self.source_manager.topic_unsubscribed.connect(self._on_topic_unsubscribed)

        # ── Production MQTT client ─────────────────────────────────────────────
        # Bypasses the source_manager worker lifecycle entirely.
        # One connection, two subscriptions: JSON + Map. Auto-reconnects.
        json_cfg = self.source_manager.get_config(SOURCE_MQTT_JSON)
        self._prod_mqtt = ProductionMqttClient(
            broker   = json_cfg.broker   if json_cfg else "mqtt.meshtastic.org",
            port     = json_cfg.port     if json_cfg else 1883,
            username = json_cfg.username if json_cfg else "",
            password = json_cfg.password if json_cfg else "",
            parent   = self,
        )
        self._prod_mqtt.packet_received.connect(self._on_prod_packet)
        self._prod_mqtt.connected_event.connect(self._on_prod_connected)
        self._prod_mqtt.disconnected_event.connect(self._on_prod_disconnected)
        self._prod_mqtt.status_changed.connect(self._on_prod_status)
        self._prod_mqtt.log_message.connect(self.window.log)

        # Force JSON + Map configs enabled with baseline topics so the source
        # panel shows them as enabled from the start.
        for tag, topic in [
            (SOURCE_MQTT_JSON, JSON_TOPIC),
            (SOURCE_MQTT_MAP,  MAP_TOPIC),
        ]:
            cfg = self.source_manager.get_config(tag)
            if cfg:
                cfg.enabled    = True
                cfg.topic      = topic
                cfg.mqtt_roots = []

        # Discovery client — isolated, never mixes with production traffic
        self._disc_client: Optional[DiscoveryClient] = None
        # Authoritative discovery state — read by both source panel and root manager
        self.disc_running:      bool            = False
        self.disc_started_at:   Optional[float] = None
        self.disc_duration_sec: int             = 0
        self.disc_topic:        str             = ""
        self.disc_roots_found:  int             = 0
        self.disc_packets_seen: int             = 0
        self._disc_was_stopped: bool            = False
        # Test root subscriptions — added on top of the two baseline feeds.
        # Never populated at startup; only via test_subscribe_root().
        self._test_roots: List[str] = []
        # Single countdown timer — 1 s interval, Qt main thread only
        self._disc_countdown_timer = QTimer(self)
        self._disc_countdown_timer.setInterval(1000)
        self._disc_countdown_timer.timeout.connect(self._disc_tick)

        # UI refresh timer — decouples MQTT rate from display rate
        self._ui_timer = QTimer(self)
        self._ui_timer.setInterval(_UI_FLUSH_MS)
        self._ui_timer.timeout.connect(self._flush_ui)
        self._ui_timer.start()

        # Scheduled packet cleanup — runs every 30 minutes while the app is open
        self._cleanup_timer = QTimer(self)
        self._cleanup_timer.setInterval(30 * 60 * 1000)
        self._cleanup_timer.timeout.connect(self._run_periodic_cleanup)
        self._cleanup_timer.start()

        # Periodic root discovery — disabled for baseline stability.
        # Enable manually via "Discover Now" in the source panel.
        self._discovery_timer = None

        # ── signal wiring ──────────────────────────────────────────────────
        self.source_manager.packet_received.connect(self._on_packet)
        self.source_manager.source_status_changed.connect(self._on_source_status)
        self.geocoder.location_ready.connect(self._on_location_ready)

        # ── load history ───────────────────────────────────────────────────
        try:
            packets = self.storage.get_recent_packets(300)
            nodes   = self.storage.get_all_nodes()
            for node in nodes:
                intelligence.enrich_node(
                    node,
                    self.config.home_lat, self.config.home_lon,
                    self.config.local_radius,
                    self.config.active_minutes, self.config.recent_hours,
                    self.config.old_days,
                )
            self.registry.load_many(nodes)

            self.window.set_visibility_hours(self.config.visibility_hours)
            self.window.load_history(packets, nodes)

            # Startup summary — always shown so the user can confirm what ran
            stats = self.storage.startup_stats
            pos      = stats.get("positions_cleared", 0)
            alt      = stats.get("altitudes_cleared", 0)
            inv      = stats.get("invalid_packets_removed", 0)
            pkt_age  = stats.get("packets_deleted_by_age", 0)
            pkt_cap  = stats.get("packets_deleted_by_cap", 0)
            self.window.log(
                f"Loaded {len(nodes)} node(s), {len(packets)} packet(s)."
            )
            cleanup_parts = []
            if pos:     cleanup_parts.append(f"cleared bad position data on {pos} node(s)")
            if alt:     cleanup_parts.append(f"cleared orphan altitude on {alt} node(s)")
            if inv:     cleanup_parts.append(f"removed {inv} invalid packet row(s)")
            if pkt_age: cleanup_parts.append(f"removed {pkt_age} packet(s) older than {self.config.packet_retain_hours}h")
            if pkt_cap: cleanup_parts.append(f"removed {pkt_cap} packet(s) by row cap ({self.config.packet_retain_max_rows:,})")
            if cleanup_parts:
                self.window.log(
                    "DB cleanup: " + "; ".join(cleanup_parts) + ". Node records deleted: 0."
                )
            else:
                self.window.log("DB cleanup: nothing to clean. Node records deleted: 0.")

            sz  = self.storage.get_db_size_info()
            pst = self.storage.get_packet_stats()
            oldest = (pst["oldest_packet"] or "")[:10] or "—"
            newest = (pst["newest_packet"] or "")[:10] or "—"
            self.window.log(
                f"DB: {sz['db_mb']:.1f}MB + WAL {sz['wal_mb']:.1f}MB = {sz['total_mb']:.1f}MB  |  "
                f"{pst['packet_count']:,} packets  |  "
                f"oldest {oldest}  newest {newest}  |  "
                f"retain {pst['retain_hours']}h / {pst['retain_max_rows']:,} rows"
            )

            # Log every enabled source with its actual subscription topics
            for src in sources:
                if src.enabled:
                    topics = src.effective_topics()
                    self.window.log(
                        f"Source [{src.source_tag}]: {src.broker}:{src.port}"
                        + (f"  roots={src.mqtt_roots}" if src.mqtt_roots else f"  topic={src.topic!r}")
                    )
                    for t in topics:
                        self.window.log(f"  Subscribe: {t}")
                    logging.info(
                        "MQTT source %s: broker=%s:%d  effective_topics=%s",
                        src.source_tag, src.broker, src.port, topics,
                    )

            for node in nodes:
                if node.latitude is not None and not node.location_name:
                    self.geocoder.request(node.node_id, node.latitude, node.longitude)
        except Exception as exc:
            self.window.log(f"History load failed: {exc}")

    def show(self) -> None:
        self.window.show()
        # Start the production MQTT client after the window is painted.
        QTimer.singleShot(200, self._prod_mqtt.start)

    # ── Source config ─────────────────────────────────────────────────────────

    def save_sources(self, sources: List[SourceConfig]) -> None:
        self.config.set("sources", [s.to_dict() for s in sources])

    def apply_root_changes(self) -> None:
        """Rebuild active MQTT subscriptions from the auto_connect roots in the DB."""
        roots = self.storage.get_auto_connect_roots()
        json_cfg = self.source_manager.get_config(SOURCE_MQTT_JSON)
        if json_cfg:
            new_cfg = SourceConfig(
                name=json_cfg.name, source_tag=json_cfg.source_tag,
                enabled=json_cfg.enabled, broker=json_cfg.broker,
                port=json_cfg.port, tls=json_cfg.tls,
                username=json_cfg.username, password=json_cfg.password,
                topic=json_cfg.topic, decoder=json_cfg.decoder,
                description=json_cfg.description,
                mqtt_roots=roots,
            )
            self.source_manager.update_config(new_cfg)
            all_cfgs = self.source_manager.all_configs()
            self.config.set("sources", [c.to_dict() for c in all_cfgs])
        if roots:
            self.window.log(
                f"Active roots ({len(roots)}): {', '.join(roots)}"
            )
            for root in roots:
                self.storage.update_root_last_connected(root)
        else:
            self.window.log("No auto-connect roots configured — not subscribing.")

    def save_roots(self, roots: List[str]) -> None:
        """Legacy shim: add each root to DB then apply."""
        for r in roots:
            self.storage.add_manual_root(r, enabled=True, auto_connect=True)
        self.apply_root_changes()

    # ── subscription registry callbacks ──────────────────────────────────────

    @Slot(str, str, str, str)
    def _on_topic_subscribed(self, tag: str, topic: str, sub_type: str,
                             parent_root: str) -> None:
        self.sub_registry.register(topic, tag, sub_type, parent_root or None)
        self.window.log(f"Subscribe: {topic}  [{sub_type}]")

    @Slot(str, str)
    def _on_topic_unsubscribed(self, tag: str, topic: str) -> None:
        self.sub_registry.unregister(topic)
        self.window.log(f"Unsubscribed: {topic}")

    # ── runtime root subscribe / unsubscribe ──────────────────────────────────

    def subscribe_root(self, root: str) -> bool:
        """Subscribe the JSON source to a root immediately.

        Updates DB (enabled=1, last_connected) and in-memory config.
        Returns False if the source is not currently connected.
        """
        ok = self.source_manager.subscribe_root(root)
        if ok:
            self.storage.set_root_enabled(root, True)
            self.storage.update_root_last_connected(root)
            self._save_sources_config()
        return ok

    def unsubscribe_root(self, root: str) -> None:
        """Unsubscribe the JSON source from a root immediately.

        Updates DB (enabled=0) and in-memory config.
        The root remains in mqtt_roots — use Forget to delete it.
        """
        self.source_manager.unsubscribe_root(root)
        self.storage.set_root_enabled(root, False)
        self._save_sources_config()

    def _save_sources_config(self) -> None:
        all_cfgs = self.source_manager.all_configs()
        self.config.set("sources", [c.to_dict() for c in all_cfgs])

    # ── ProductionMqttClient slots ────────────────────────────────────────────

    @Slot(str, str)
    def _on_prod_packet(self, topic: str, payload: str) -> None:
        """Route a raw ProductionMqttClient message to the correct decoder."""
        if "/2/json/" in topic:
            tag = SOURCE_MQTT_JSON
        elif "/2/map/" in topic:
            tag = SOURCE_MQTT_MAP
        else:
            logging.debug("Production MQTT: unroutable topic: %s", topic)
            return
        self.source_manager.record_received(tag)
        if tag == SOURCE_MQTT_JSON:
            self._handle_json_packet(tag, topic, payload)
        else:
            self._handle_map_packet(tag, topic, payload)

    @Slot(str)
    def _on_prod_connected(self, client_id: str) -> None:
        """ProductionMqttClient successfully connected and subscribed."""
        now = datetime.now()
        for tag in (SOURCE_MQTT_JSON, SOURCE_MQTT_MAP):
            self.source_manager.set_connected(tag, True, now)
        # Register the two baseline subscriptions as the authoritative live subs
        self.sub_registry.clear_all()
        from subscription_registry import DIRECT, MAP_TYPE
        self.sub_registry.register(JSON_TOPIC, SOURCE_MQTT_JSON, DIRECT)
        self.sub_registry.register(MAP_TOPIC,  SOURCE_MQTT_MAP,  MAP_TYPE)
        # Re-register any test root subscriptions (paho already re-subscribed them)
        for root in self._test_roots:
            derived = f"{root}/2/json/#"
            self.sub_registry.register(derived, SOURCE_MQTT_JSON, ROOT_DERIVED, root)
            logging.info("Production MQTT: re-registered test root  %s", derived)
        # Refresh the live subs label in the source panel
        sp = getattr(self.window, "_source_panel", None)
        if sp:
            sp._refresh_subs_label()
        logging.info("Production MQTT: connected  id=%s  subs registered", client_id)

    @Slot(int)
    def _on_prod_disconnected(self, rc: int) -> None:
        """ProductionMqttClient disconnected from broker."""
        for tag in (SOURCE_MQTT_JSON, SOURCE_MQTT_MAP):
            self.source_manager.set_connected(tag, False)
        self.sub_registry.unregister(JSON_TOPIC)
        self.sub_registry.unregister(MAP_TOPIC)
        sp = getattr(self.window, "_source_panel", None)
        if sp:
            sp._refresh_subs_label()
        logging.info("Production MQTT: disconnected  rc=%d", rc)

    @Slot(str)
    def _on_prod_status(self, text: str) -> None:
        """Forward ProductionMqttClient status to both source panel rows."""
        self.source_manager.source_status_changed.emit(SOURCE_MQTT_JSON, text)
        self.source_manager.source_status_changed.emit(SOURCE_MQTT_MAP,  text)

    # ── Controlled test-root subscriptions ────────────────────────────────────
    # These add a single derived topic (root/2/json/#) on top of the two
    # baseline feeds without touching SAFE_MODE_MQTT or the source_manager workers.

    def test_subscribe_root(self, root: str) -> bool:
        """Subscribe to one additional root on the production connection for testing.

        Derives the topic as '{root}/2/json/#' and adds it to the live paho
        connection.  Registers in sub_registry so Root Manager shows it as Active.
        Safe to call while connected or before connection (queued for next connect).
        Returns True if subscription was sent to the broker.
        """
        if root in self._test_roots:
            self.window.log(f"Test root already active: {root}")
            return True
        derived = f"{root}/2/json/#"
        sent = self._prod_mqtt.subscribe_extra(derived)
        self.sub_registry.register(derived, SOURCE_MQTT_JSON, ROOT_DERIVED, root)
        self._test_roots.append(root)
        self.window.log(
            f"Test subscribe: {derived}"
            + ("  ✓ sent to broker" if sent else "  (queued — will send on connect)")
        )
        logging.info(
            "test_subscribe_root: root=%s  derived=%s  connected=%s  sent=%s",
            root, derived, self._prod_mqtt.connected, sent,
        )
        sp = getattr(self.window, "_source_panel", None)
        if sp:
            sp._refresh_subs_label()
        self.test_mode_changed.emit(True)
        return sent

    def test_unsubscribe_root(self, root: str) -> None:
        """Remove a test root subscription from the production connection."""
        derived = f"{root}/2/json/#"
        self._prod_mqtt.unsubscribe_extra(derived)
        self.sub_registry.unregister(derived)
        if root in self._test_roots:
            self._test_roots.remove(root)
        self.window.log(f"Test unsubscribed: {derived}")
        logging.info("test_unsubscribe_root: root=%s  derived=%s", root, derived)
        sp = getattr(self.window, "_source_panel", None)
        if sp:
            sp._refresh_subs_label()
        if not self._test_roots:
            self.test_mode_changed.emit(False)

    def rollback_to_safe_baseline(self) -> None:
        """Remove all test root subscriptions and return to Safe Baseline Mode."""
        if not self._test_roots:
            self.window.log("Already at Safe Baseline — no test roots to remove.")
            return
        for root in list(self._test_roots):
            self.test_unsubscribe_root(root)
        self.window.log(
            "Rolled back to Safe Baseline Mode.  "
            "Live subscriptions: msh/US/2/json/#  msh/US/2/map/#"
        )

    # ── discovery ─────────────────────────────────────────────────────────────

    def start_discovery(self, duration_sec: int = 60) -> bool:
        """Launch an isolated DiscoveryClient to scan for active MQTT roots.

        Discovery traffic is 100% separate from the normal packet pipeline.
        Discovery packets never enter the packets table, node registry, or map.
        In Safe Baseline Mode, discovered roots are saved to the DB but remain
        in Discovered/Staged state — they are never promoted to Active automatically.

        Returns False (without starting) if discovery is already running.
        """
        if self.disc_running:
            logging.info("Discovery: already running — ignoring duplicate start request")
            return False

        if self._disc_client and self._disc_client.running:
            self._disc_client.stop()

        json_cfg = self.source_manager.get_config(SOURCE_MQTT_JSON)
        if json_cfg is None:
            self.window.log("Root discovery: no JSON source configured.")
            return False

        self._disc_client = DiscoveryClient(
            broker=json_cfg.broker,
            port=json_cfg.port,
            username=json_cfg.username,
            password=json_cfg.password,
            on_root_seen=lambda root, ch: None,
            on_log=self._on_disc_log,
            on_done=self._on_disc_done,
        )

        self.disc_running      = True
        self.disc_started_at   = time.monotonic()
        self.disc_duration_sec = duration_sec
        self.disc_topic        = DiscoveryClient.DISCOVERY_TOPIC
        self.disc_roots_found  = 0
        self.disc_packets_seen = 0
        self._disc_was_stopped = False

        self._disc_client.start(duration_sec)
        self._disc_countdown_timer.start()

        logging.info(
            "Discovery started: topic=%s, duration=%d", self.disc_topic, duration_sec
        )
        self.window.log(
            f"Root discovery started — {DiscoveryClient.DISCOVERY_TOPIC} for {duration_sec}s "
            "(isolated connection; production data unaffected)"
        )
        self.discovery_started.emit(self.disc_topic, duration_sec)
        return True

    def stop_discovery(self) -> None:
        """Cancel discovery early — cleanup completes asynchronously in _on_disc_done."""
        if not self.disc_running:
            return
        logging.info("Discovery: stop requested")
        self._disc_was_stopped = True
        self._disc_countdown_timer.stop()
        if self._disc_client:
            self._disc_client.stop()
        # _on_disc_done() will be called from the discovery thread and finish cleanup

    @Slot()
    def _disc_tick(self) -> None:
        """Fires every 1 s while discovery is running; recalculates remaining from wall clock."""
        if not self.disc_running or self.disc_started_at is None:
            self._disc_countdown_timer.stop()
            return

        remaining = max(
            0, self.disc_duration_sec - int(time.monotonic() - self.disc_started_at)
        )

        if self._disc_client:
            try:
                roots, packets = self._disc_client.get_counts()
                self.disc_roots_found  = roots
                self.disc_packets_seen = packets
            except Exception:
                pass

        logging.debug(
            "Discovery tick: remaining=%d, roots=%d, packets=%d",
            remaining, self.disc_roots_found, self.disc_packets_seen,
        )
        self.discovery_tick.emit(remaining, self.disc_roots_found, self.disc_packets_seen)

        if remaining == 0:
            self._disc_countdown_timer.stop()

    def _on_disc_log(self, msg: str) -> None:
        """Marshal discovery log message from the worker thread to the main thread."""
        QTimer.singleShot(0, lambda: self.window.log(msg))

    def _on_disc_done(self, roots: dict) -> None:
        """Called from the discovery thread on every exit path (timeout or stop)."""
        def _work():
            try:
                self._disc_countdown_timer.stop()

                was_stopped        = self._disc_was_stopped
                self._disc_was_stopped = False
                n_roots   = len(roots)
                n_packets = self.disc_packets_seen

                for root, data in roots.items():
                    try:
                        self.storage.upsert_mqtt_root(
                            root,
                            packet_count_delta=data.get("packet_count", 0),
                            channels=data.get("channels", set()),
                            is_discovery=True,
                            discovery_duration_seconds=self.disc_duration_sec,
                        )
                    except Exception as exc:
                        logging.warning("upsert_mqtt_root %s: %s", root, exc)

                all_rows = self.storage.get_all_mqtt_roots()
                result   = {row["root_topic"]: dict(row) for row in all_rows}

                self.disc_running = False

                if was_stopped:
                    self.window.log(
                        f"Discovery stopped. {n_roots} root(s) found, "
                        f"{n_packets} packet(s) seen."
                    )
                    logging.info(
                        "Discovery stopped: roots=%d, packets=%d", n_roots, n_packets
                    )
                    self.discovery_stopped.emit()
                else:
                    if n_roots:
                        self.window.log(
                            f"Discovery complete: {n_roots} root(s) found — "
                            + ", ".join(sorted(roots.keys()))
                        )
                    else:
                        self.window.log("Discovery complete: no roots found.")
                    logging.info(
                        "Discovery complete: roots=%d, packets=%d", n_roots, n_packets
                    )
                    self.discovery_finished.emit(n_roots, n_packets)

                logging.info("Discovery cleanup complete")
                self.discovery_result.emit(result)

            except Exception as exc:
                logging.error("Discovery _on_disc_done error: %s", exc)
                self.disc_running      = False
                self._disc_was_stopped = False
                self.discovery_stopped.emit()

        QTimer.singleShot(0, _work)

    @Slot()
    def _auto_discover(self) -> None:
        if not self.disc_running:
            self.start_discovery(self.config.discovery_duration_seconds)

    # ── Reference import ──────────────────────────────────────────────────────

    def import_reference(self, path: str) -> None:
        nodes, count, errors = reference_importer.import_file(path)
        for error in errors:
            self.window.log(f"Import warning: {error}")

        imported = 0
        for ref_node in nodes:
            try:
                node = self.storage.upsert_reference_node(ref_node)
                if node:
                    self._enrich_and_update(node)
                    imported += 1
            except Exception as exc:
                self.window.log(f"Import error for {ref_node.node_id}: {exc}")

        self.window.log(
            f"Reference import: {imported} nodes imported from {Path(path).name}"
        )
        self.window.refresh_stats(self.registry.get_stats())

    # ── Map jump ──────────────────────────────────────────────────────────────

    def jump_to_node(self, node_id: str) -> bool:
        """Switch to the Map tab and pan to node_id.

        Normalises node_id (prepend '!', lowercase hex).
        Logs a friendly message if the node has no known position.
        Returns True on success, False if the node cannot be located.
        """
        # Normalise
        node_id = node_id.strip()
        if node_id.startswith("!"):
            node_id = "!" + node_id[1:].lower()
        else:
            node_id = "!" + node_id.lower()

        node = self.registry.get(node_id)
        if node is None:
            self.window.log(f"No known location for {node_id} — cannot jump to map.")
            return False

        lat, lon = node.latitude, node.longitude
        if (lat is None or lon is None
                or not (-90 <= lat <= 90)
                or not (-180 <= lon <= 180)):
            self.window.log(f"No known location for {node_id} — cannot jump to map.")
            return False

        self.window.switch_to_map_tab()
        self.window.map_view.jump_to(lat, lon, node_id)
        return True

    # ── Hot path: MQTT packet received ───────────────────────────────────────

    @Slot(str, str, str)
    def _on_packet(self, source_tag: str, topic: str, payload: str) -> None:
        """Route incoming MQTT message to the correct decoder.

        When a source uses mqtt_roots, the decoder is determined per-message
        from the topic path (/2/json/ → json, /2/e/ → map_report/ServiceEnvelope).
        Legacy single-topic sources use the source's configured decoder.
        """
        cfg = self.source_manager.get_config(source_tag)

        if cfg and cfg.mqtt_roots:
            # Multi-root source: route by topic path segment
            if "/2/e/" in topic or topic.endswith("/2/e"):
                self._handle_map_packet(source_tag, topic, payload)
            else:
                self._handle_json_packet(source_tag, topic, payload)
            return

        decoder = cfg.decoder if cfg else "json"
        if decoder == "map_report":
            self._handle_map_packet(source_tag, topic, payload)
        elif decoder == "protobuf":
            self._handle_protobuf_packet(source_tag, topic, payload)
        else:
            self._handle_json_packet(source_tag, topic, payload)

    # ── JSON decoder path ─────────────────────────────────────────────────────

    def _handle_json_packet(
        self, source_tag: str, topic: str, payload: str
    ) -> None:
        t0 = time.monotonic()
        try:
            # Log the first 20 unique topics to debug.log to confirm what traffic arrives
            if topic not in self._topics_seen:
                self._topics_seen.add(topic)
                n = len(self._topics_seen)
                if n <= 20:
                    logging.info("MQTT topic #%d: %s", n, topic)

            # Extract MQTT routing metadata for stats and node tracking
            mqtt_root    = extract_root(topic)
            mqtt_channel = extract_channel(topic)

            packet = parse_packet(topic, payload, source_tag=source_tag)
            if packet is None:
                self.source_manager.record_ignored(source_tag, "json_decode_failed")
                return

            dedup_hash = compute_packet_hash(topic, payload, packet.received_at)
            if not self.storage.is_duplicate(packet, dedup_hash):
                db_id = self.storage.store_packet(packet, dedup_hash)
                packet.db_id = db_id

            updates = extract_node_updates(packet)
            if updates is None:
                # For types where extract_node_updates returns None (telemetry, text,
                # traceroute, sendtext, etc.), use from_num as the node identity when
                # available — some packet types omit "sender" but always carry "from".
                from_id = normalize_node_id(packet.from_num) if packet.from_num is not None else None
                updates = {"node_id": from_id or packet.sender}
            node = self.storage.upsert_node_from_mqtt(
                updates, packet.packet_type, packet.topic, source=source_tag,
                mqtt_root=mqtt_root, mqtt_channel=mqtt_channel,
            )

            if node:
                intelligence.enrich_node(
                    node,
                    self.config.home_lat, self.config.home_lon,
                    self.config.local_radius,
                    self.config.active_minutes, self.config.recent_hours,
                    self.config.old_days,
                )
                self.storage.update_node_computed(
                    node.node_id, node.status, node.is_local, node.distance_miles
                )
                self.registry.update(node)
                if node.packet_count == 1:
                    self.window.log(
                        f"New node: {node.display_name()} ({node.node_id}) via {source_tag}"
                    )
                self._check_watchlist(node, packet.packet_type)

                if node.latitude is not None and not node.location_name:
                    self.geocoder.request(node.node_id, node.latitude, node.longitude)

                self._dirty_node_ids.add(node.node_id)

            # Log text messages to the event log so they're visible during
            # the forced-transmit diagnostic test.
            if packet.packet_type in ("text", "sendtext"):
                try:
                    raw = json.loads(payload)
                    p = raw.get("payload", "")
                    text = (p if isinstance(p, str) else p.get("text", str(p))) if p else ""
                    sender_id = normalize_node_id(packet.from_num) if packet.from_num is not None else packet.sender
                    name = (node.display_name() if node else sender_id) or sender_id
                    self.window.log(
                        f"TEXT from {name} ({sender_id})"
                        + (f": {text[:120]}" if text else " [no text]")
                        + f"  topic={topic}"
                    )
                except Exception:
                    pass

            # Do NOT commit here — deferred to _flush_ui every 750ms.
            # Batching commits eliminates per-packet disk sync which was freezing the UI.
            self._pending_packets.append(packet)
            self.source_manager.record_decoded(source_tag)

        except Exception as exc:
            tb = traceback.format_exc()
            logging.error("JSON packet error\ntopic=%s\n%s", topic, tb)
            self.source_manager.record_error(source_tag, str(exc))
            self.window.on_error(
                f"Packet processing error: {exc} (topic={topic}, see debug.log)"
            )
        finally:
            elapsed_ms = (time.monotonic() - t0) * 1000
            if elapsed_ms > _SLOW_PACKET_MS:
                logging.warning("SLOW json packet: %.1fms  topic=%s", elapsed_ms, topic)

    # ── Map report decoder path ───────────────────────────────────────────────

    def _handle_map_packet(
        self, source_tag: str, topic: str, payload: str
    ) -> None:
        """Decode a map-report MQTT payload via minimal protobuf parser.

        Payload was decoded from bytes using latin-1 in the source worker, so
        round-tripping via encode("latin-1") gives back the exact original bytes.
        No MQTTPacket is created; no packet DB row is written.

        Node ID is taken from MeshPacket.from (or gateway_id fallback) — NOT from
        inside the MapReport message, which carries no node identity.
        """
        t0 = time.monotonic()
        try:
            payload_bytes = payload.encode("latin-1")
            decoded, dbg  = decode_map_payload(payload_bytes)

            # ── Debug field dump for the first N map packets ──────────────────
            if self._map_debug_count < _MAP_DEBUG_PKTS:
                self._map_debug_count += 1
                lat_deg = dbg.get("lat_deg")
                lon_deg = dbg.get("lon_deg")
                lat_f = f"{lat_deg:.6f}" if lat_deg is not None else "—"
                lon_f = f"{lon_deg:.6f}" if lon_deg is not None else "—"
                self.window.log(
                    f"MAP_DBG #{self._map_debug_count}: "
                    f"gw={dbg.get('gateway_id')}  ch={dbg.get('channel_id')}\n"
                    f"  MeshPacket fields: {dbg.get('mp_field_names', dbg.get('mp_fields'))}\n"
                    f"  WhichOneof: {'decoded' if dbg.get('has_decoded') else 'encrypted' if dbg.get('has_encrypted') else 'none'}\n"
                    f"  from=field1={dbg.get('from_id')}  to=0x{dbg.get('to_num') or 0:08x}  "
                    f"pkt_id={dbg.get('packet_id')}\n"
                    f"  decoded_present={dbg.get('has_decoded')}  "
                    f"encrypted_present={dbg.get('has_encrypted')}  enc_len={dbg.get('enc_len')}\n"
                    f"  decoded_payload_len={dbg.get('decoded_payload_len')}  "
                    f"decoded_payload_hex={dbg.get('decoded_payload_hex')}\n"
                    f"  portnum={dbg.get('portnum')}  "
                    f"MapReport fields: {dbg.get('mr_fields')}\n"
                    f"  name={dbg.get('long_name')!r}/{dbg.get('short_name')!r}\n"
                    f"  lat_i_raw={dbg.get('lat_i_raw')}  lon_i_raw={dbg.get('lon_i_raw')}  "
                    f"lat={lat_f}  lon={lon_f}  alt_signed={dbg.get('alt_signed')}\n"
                    f"  pos_valid={dbg.get('pos_valid')}  "
                    f"pos_reject={dbg.get('pos_reject_reason')}  "
                    f"alt_valid={dbg.get('alt_valid')}\n"
                    f"  MapReport_ok={'yes' if decoded else 'no'}  "
                    f"decrypted_with={dbg.get('decrypted_with')}  "
                    f"fail={dbg.get('fail')}"
                )

            if decoded is None:
                reason = dbg.get("fail") or "decode_failed"
                self._throttled_map_log(topic, payload, reason)

                # Encrypted packets: suppress per-packet event-log spam.
                # Show a one-time note, then let the _flush_ui summary take over.
                if reason in ("encrypted", "encrypted_no_crypto_lib",
                              "encrypted_unknown_channel"):
                    if not self._map_encrypted_noted:
                        self._map_encrypted_noted = True
                        if reason == "encrypted_no_crypto_lib":
                            self.window.log(
                                "Map Reports: packets are encrypted; "
                                "pycryptodome not found — decryption unavailable."
                            )
                        elif reason == "encrypted_unknown_channel":
                            ch = dbg.get("channel_id") or "unknown"
                            self.window.log(
                                f"Map Reports: packets encrypted on channel {ch!r}; "
                                "no key configured for this channel."
                            )
                        else:
                            self.window.log(
                                "Map Reports: packets are encrypted (AES-CTR). "
                                "Attempting default LongFast key — "
                                "check debug.log for decryption results."
                            )
                    self.source_manager.record_ignored(source_tag, reason)
                    return

                # Other failures — show up to _MAP_UI_LOG_MAX times
                if self._map_ui_log_count < _MAP_UI_LOG_MAX:
                    self._map_ui_log_count += 1
                    self.window.log(
                        f"Map [{source_tag}] ignored: {reason} — "
                        f"topic={topic!r}  len={len(payload)}"
                    )
                    if self._map_ui_log_count == _MAP_UI_LOG_MAX:
                        self.window.log(
                            "Map Reports: further ignore messages suppressed "
                            "(check debug.log for detail)."
                        )
                self.source_manager.record_ignored(source_tag, reason)
                return

            # Track position outcome for session summary
            if dbg.get("pos_valid"):
                self._map_pos_accepted += 1
            elif dbg.get("lat_i_raw") is not None:
                # Position fields were present but failed sanity check
                self._map_pos_rejected += 1
            else:
                # No position fields in MapReport at all — identity-only packet
                self._map_identity_only += 1

            # Convert hw_model integer to hardware name string
            hw_int = decoded.pop("hw_model_int", None)
            if hw_int is not None:
                decoded["hardware"] = HARDWARE_NAMES.get(hw_int, f"HW_{hw_int}")

            node_id = decoded.get("node_id")
            if not node_id:
                # Should not happen after the decoder fix, but guard anyway
                logging.warning(
                    "MAP decoded but no node_id: topic=%s  dbg=%s", topic, dbg
                )
                self.source_manager.record_ignored(source_tag, "decoded_but_no_node_id")
                return

            node = self.storage.upsert_node_from_mqtt(
                decoded, packet_type="mapreport", topic=topic, source=source_tag
            )

            if node:
                intelligence.enrich_node(
                    node,
                    self.config.home_lat, self.config.home_lon,
                    self.config.local_radius,
                    self.config.active_minutes, self.config.recent_hours,
                    self.config.old_days,
                )
                self.storage.update_node_computed(
                    node.node_id, node.status, node.is_local, node.distance_miles
                )
                self.registry.update(node)
                if node.packet_count == 1:
                    self.window.log(
                        f"New node (map): {node.display_name()} ({node.node_id})"
                    )
                if node.latitude is not None and not node.location_name:
                    self.geocoder.request(node.node_id, node.latitude, node.longitude)
                self._dirty_node_ids.add(node.node_id)

            self.source_manager.record_decoded(source_tag)

        except Exception as exc:
            tb = traceback.format_exc()
            logging.error("Map packet error\ntopic=%s\n%s", topic, tb)
            self.source_manager.record_error(source_tag, str(exc))
        finally:
            elapsed_ms = (time.monotonic() - t0) * 1000
            if elapsed_ms > _SLOW_PACKET_MS:
                logging.warning("SLOW map packet: %.1fms  topic=%s", elapsed_ms, topic)

    def _throttled_map_log(self, topic: str, payload: str, reason: str) -> None:
        """Log a map-ignored event to debug.log at most once per topic per throttle window."""
        now = time.monotonic()
        if now - self._map_log_times.get(topic, 0) < _MAP_LOG_THROTTLE_SEC:
            return
        self._map_log_times[topic] = now
        preview_repr = repr(payload[:32])
        preview_hex  = payload[:16].encode("latin-1").hex()
        logging.debug(
            "MAP ignored: reason=%s  decoder=map_report  topic=%s  "
            "payload_len=%d  hex=%s  repr=%s",
            reason, topic, len(payload), preview_hex, preview_repr,
        )

    # ── Protobuf / raw decoder path ───────────────────────────────────────────

    def _handle_protobuf_packet(
        self, source_tag: str, topic: str, payload: str
    ) -> None:
        self.source_manager.record_ignored(source_tag, "protobuf_not_implemented")
        logging.debug(
            "PROTOBUF ignored (not implemented): source=%s topic=%s len=%d",
            source_tag, topic, len(payload),
        )

    # ── UI flush (timer, every 750 ms) ───────────────────────────────────────

    @Slot()
    def _flush_ui(self) -> None:
        t0 = time.monotonic()
        n_packets = len(self._pending_packets)
        n_nodes   = len(self._dirty_node_ids)

        # Section 0 — commit any pending DB writes from _on_packet
        # (per-packet commits were removed to eliminate per-packet disk sync)
        t_commit_start = time.monotonic()
        try:
            self.storage.commit()
        except Exception as exc:
            logging.error("_flush_ui commit: %s", exc)
        t_commit = (time.monotonic() - t_commit_start) * 1000
        if t_commit > 100:
            logging.warning("SLOW DB commit in _flush_ui: %.1fms", t_commit)

        # Section 1 — buffer new packets into feed/messages/telemetry
        t1 = time.monotonic()
        if self._pending_packets:
            # Backpressure: if queue is very deep, drop oldest to keep UI responsive.
            if len(self._pending_packets) > _FLUSH_QUEUE_LIMIT:
                dropped = len(self._pending_packets) - _FLUSH_QUEUE_LIMIT
                self._pending_packets = self._pending_packets[dropped:]
                logging.warning(
                    "UI backpressure: dropped %d oldest packets from queue "
                    "(queue was %d; limit %d)",
                    dropped, dropped + _FLUSH_QUEUE_LIMIT, _FLUSH_QUEUE_LIMIT,
                )
            # Cap packets per tick so the UI thread never blocks for long.
            if len(self._pending_packets) <= _FLUSH_PACKET_CAP:
                packets, self._pending_packets = self._pending_packets, []
            else:
                packets = self._pending_packets[:_FLUSH_PACKET_CAP]
                self._pending_packets = self._pending_packets[_FLUSH_PACKET_CAP:]
            self.window.on_packets_buffered(packets)
        t_packets = (time.monotonic() - t1) * 1000

        # Section 2 — push dirty nodes to all tabs
        t2 = time.monotonic()
        if self._dirty_node_ids:
            ids, self._dirty_node_ids = self._dirty_node_ids, set()
            nodes = [self.registry.get(nid) for nid in ids]
            nodes = [n for n in nodes if n is not None]
            if nodes:
                self.window.on_nodes_updated(nodes)
        t_nodes = (time.monotonic() - t2) * 1000

        # Section 3 — tick display flush + status bar
        t3 = time.monotonic()
        self.window.flush_display_tick()
        t_tick = (time.monotonic() - t3) * 1000

        # One-time map summary (fires once enough packets have been processed)
        if not self._map_summary_logged:
            map_stats = self.source_manager.get_stats(SOURCE_MQTT_MAP)
            rcvd = map_stats.get("packet_count", 0)
            dec  = map_stats.get("decoded_count", 0)
            ign  = map_stats.get("ignored_count", 0)
            total_pos = self._map_pos_accepted + self._map_pos_rejected + self._map_identity_only
            if rcvd >= 10 and total_pos + ign >= 10:
                reasons = map_stats.get("ignore_reasons", {})
                if reasons:
                    top_r, top_c = max(reasons.items(), key=lambda x: x[1])
                    reason_str = f"  Ign top: {top_r} ({top_c}×)."
                else:
                    reason_str = ""
                self.window.log(
                    f"Map Reports: {rcvd} rcvd  {dec} decoded  {ign} ignored.{reason_str}\n"
                    f"  Position: {self._map_pos_accepted} accepted  "
                    f"{self._map_pos_rejected} rejected  "
                    f"{self._map_identity_only} identity-only (no GPS)"
                )
                self._map_summary_logged = True

        elapsed_ms = (time.monotonic() - t0) * 1000
        if elapsed_ms > _SLOW_FLUSH_MS:
            logging.warning(
                "SLOW _flush_ui: total=%.1fms  [packets=%.1fms  nodes=%.1fms  "
                "tick=%.1fms]  n_packets=%d  n_nodes=%d",
                elapsed_ms, t_packets, t_nodes, t_tick, n_packets, n_nodes,
            )
        elif t_packets > _SLOW_SECTION_MS:
            logging.warning("SLOW packet-buffer section: %.1fms", t_packets)
        elif t_nodes > _SLOW_SECTION_MS:
            logging.warning("SLOW node-update section: %.1fms", t_nodes)
        elif t_tick > _SLOW_SECTION_MS:
            logging.warning("SLOW display-tick section: %.1fms", t_tick)

    # ── Scheduled maintenance ─────────────────────────────────────────────────

    @Slot()
    def _run_periodic_cleanup(self) -> None:
        result = self.storage.cleanup_packets()
        deleted = result["deleted_by_age"] + result["deleted_by_cap"]
        sz = self.storage.get_db_size_info()
        if deleted:
            self.window.log(
                f"DB cleanup: removed {result['deleted_by_age']} packet(s) by age, "
                f"{result['deleted_by_cap']} by row cap.  "
                f"DB: {sz['db_mb']:.1f}MB + WAL {sz['wal_mb']:.1f}MB"
            )
        else:
            logging.debug(
                "DB cleanup (scheduled): nothing to remove.  DB: %.1fMB + WAL %.1fMB",
                sz["db_mb"], sz["wal_mb"],
            )

    # ── Other signal handlers ─────────────────────────────────────────────────

    @Slot(str, str)
    def _on_source_status(self, tag: str, text: str) -> None:
        self.window.on_status(f"[{tag}] {text}")

    @Slot(str, str)
    def _on_location_ready(self, node_id: str, location_name: str) -> None:
        try:
            node = self.storage.set_node_location(node_id, location_name)
            if node:
                self._enrich_and_update(node)
        except Exception as exc:
            self.window.on_error(f"Geocode update error: {exc}")

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _enrich_and_update(self, node) -> None:
        intelligence.enrich_node(
            node,
            self.config.home_lat, self.config.home_lon,
            self.config.local_radius,
            self.config.active_minutes, self.config.recent_hours,
            self.config.old_days,
        )
        self.storage.update_node_computed(
            node.node_id, node.status, node.is_local, node.distance_miles
        )
        self.storage.commit()
        self.registry.update(node)
        self._dirty_node_ids.add(node.node_id)

    def _check_watchlist(self, node, packet_type: str) -> None:
        entry = self.storage.check_watchlist(node.node_id)
        if not entry:
            return
        label = entry.get("label") or node.node_id
        if entry.get("alert_when_seen"):
            self.window.log(f"WATCHLIST: {label} ({node.node_id}) seen — {packet_type}")
        if entry.get("alert_when_position") and packet_type == "position":
            self.window.log(f"WATCHLIST: {label} ({node.node_id}) position updated")
        if entry.get("alert_when_message") and packet_type == "text":
            self.window.log(f"WATCHLIST: {label} ({node.node_id}) sent a text message")

    def _load_sources(self) -> list:
        # Safe Mode: do not load root subscriptions from DB at startup.
        # _safe_mode_connect() enforces the baseline immediately after the window shows.
        # Root-derived subscriptions will remain disabled until the baseline is stable.
        db_roots: list = []

        raw = self.config.get("sources")
        if raw and isinstance(raw, list):
            sources = [SourceConfig.from_dict(d) for d in raw]
            for src in sources:
                if src.source_tag == SOURCE_MQTT_JSON:
                    src.mqtt_roots = db_roots   # clear any saved roots
                    # Migrate stale lowercase topic — broker requires uppercase US
                    if src.topic in ("msh/us/2/json/#", ""):
                        src.topic = "msh/US/2/json/#"
            self.config.set("sources", [s.to_dict() for s in sources])
            return sources

        old_settings = {}
        if SETTINGS_FILE.exists():
            try:
                with open(SETTINGS_FILE, encoding="utf-8") as f:
                    old_settings = json.load(f)
            except Exception:
                pass

        if old_settings:
            migrated = SourceConfig(
                name="MQTT JSON",
                source_tag=SOURCE_MQTT_JSON,
                enabled=True,
                broker=old_settings.get("broker", "mqtt.meshtastic.org"),
                port=int(old_settings.get("port", 1883)),
                tls=bool(old_settings.get("tls", False)),
                username=old_settings.get("username", "meshdev"),
                password=old_settings.get("password", "large4cats"),
                topic=old_settings.get("topic", "msh/US/2/json/#"),
                decoder="json",
                description="Primary Meshtastic JSON feed (migrated from settings)",
                mqtt_roots=db_roots,
            )
            sources = [migrated, *DEFAULT_SOURCES[1:]]
        else:
            sources = list(DEFAULT_SOURCES)
            for src in sources:
                if src.source_tag == SOURCE_MQTT_JSON:
                    src.mqtt_roots = db_roots

        self.config.set("sources", [s.to_dict() for s in sources])
        return sources


