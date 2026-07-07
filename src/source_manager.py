import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Set

import paho.mqtt.client as mqtt_lib
from PySide6.QtCore import QObject, Signal

# ── paho compatibility shim ────────────────────────────────────────────────────


def _make_client(client_id: str) -> mqtt_lib.Client:
    try:
        return mqtt_lib.Client(
            callback_api_version=mqtt_lib.CallbackAPIVersion.VERSION1,
            client_id=client_id,
        )
    except (AttributeError, TypeError):
        return mqtt_lib.Client(client_id=client_id)


# ── well-known source tag constants ───────────────────────────────────────────

SOURCE_MQTT_JSON = "mqtt_json"
SOURCE_MQTT_MAP  = "mqtt_map"
SOURCE_MQTT_RAW  = "mqtt_raw"


# ── helpers ───────────────────────────────────────────────────────────────────

def _extract_channel(topic: str) -> Optional[str]:
    """Extract channel name from an MQTT topic.

    Topic forms:
      msh/US/2/json/LongFast/!nodeid        → LongFast
      msh/US/SC/2/json/scmesh/!nodeid       → scmesh
      msh/US/SC/2/e/LongFast/!nodeid        → LongFast
    Finds the component after the first 'json', 'e', or 'map' segment.
    """
    parts = topic.split("/")
    for i, p in enumerate(parts):
        if p in ("json", "e", "map") and i + 1 < len(parts):
            return parts[i + 1]
    return None


def extract_root(topic: str) -> Optional[str]:
    """Extract MQTT root from a topic — everything before /2/json/, /2/e/, or /2/map/.

    Examples:
      msh/US/2/json/LongFast/!abc    → msh/US
      msh/US/SC/2/e/scmesh/!abc     → msh/US/SC
      msh/US/SC/2/json/scmesh/!abc  → msh/US/SC
    """
    for marker in ("/2/json/", "/2/e/", "/2/map/"):
        idx = topic.find(marker)
        if idx >= 0:
            return topic[:idx]
    return None


def extract_channel(topic: str) -> Optional[str]:
    """Public wrapper for _extract_channel."""
    return _extract_channel(topic)


# ── SourceConfig ──────────────────────────────────────────────────────────────

@dataclass
class SourceConfig:
    name:        str
    source_tag:  str
    enabled:     bool
    broker:      str
    port:        int
    tls:         bool
    username:    str
    password:    str
    topic:       str           # legacy single-topic (used when mqtt_roots is empty)
    decoder:     str           # "json", "map_report", "protobuf"
    description: str       = ""
    mqtt_roots:  List[str] = field(default_factory=list)

    # ── subscription topic derivation ─────────────────────────────────────────

    def effective_topics(self) -> List[str]:
        """Return actual MQTT subscription topic strings.

        Always includes self.topic (direct subscription) if set — case-preserved.
        If mqtt_roots is non-empty, also adds two derived subscriptions per root:
          {root}/2/json/#   — decoded JSON packets
          {root}/2/e/#      — encrypted/raw ServiceEnvelope packets
        Duplicate strings are silently dropped.
        """
        seen: set = set()
        topics: List[str] = []

        def _add(t: str) -> None:
            if t and t not in seen:
                seen.add(t)
                topics.append(t)

        _add(self.topic)  # direct subscription — always first, case-preserved
        for root in (self.mqtt_roots or []):
            r = root.rstrip("/")
            _add(f"{r}/2/json/#")
            # /2/e/# is NOT derived from roots for the JSON source.
            # Protobuf/encrypted traffic is handled only by the Raw Advanced source
            # via its own direct topic (msh/US/2/#), and only when that source is enabled.
        return topics

    # ── serialisation ─────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        d = {
            "name":        self.name,
            "source_tag":  self.source_tag,
            "enabled":     self.enabled,
            "broker":      self.broker,
            "port":        self.port,
            "tls":         self.tls,
            "username":    self.username,
            "password":    self.password,
            "topic":       self.topic,
            "decoder":     self.decoder,
            "description": self.description,
        }
        if self.mqtt_roots:
            d["mqtt_roots"] = self.mqtt_roots
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "SourceConfig":
        return cls(
            name=d.get("name", ""),
            source_tag=d.get("source_tag", ""),
            enabled=bool(d.get("enabled", False)),
            broker=d.get("broker", "mqtt.meshtastic.org"),
            port=int(d.get("port", 1883)),
            tls=bool(d.get("tls", False)),
            username=d.get("username", ""),
            password=d.get("password", ""),
            topic=d.get("topic", ""),
            decoder=d.get("decoder", "json"),
            description=d.get("description", ""),
            mqtt_roots=d.get("mqtt_roots", []),
        )


DEFAULT_SOURCES: List[SourceConfig] = [
    SourceConfig(
        name="MQTT JSON",
        source_tag=SOURCE_MQTT_JSON,
        enabled=True,
        broker="mqtt.meshtastic.org",
        port=1883,
        tls=False,
        username="meshdev",
        password="large4cats",
        topic="msh/US/2/json/#",           # broker uses uppercase US — case-sensitive MQTT
        decoder="json",
        mqtt_roots=[],                     # DB provides roots at startup
        description="Primary Meshtastic JSON feed",
    ),
    SourceConfig(
        name="MQTT Map Reports",
        source_tag=SOURCE_MQTT_MAP,
        enabled=False,
        broker="mqtt.meshtastic.org",
        port=1883,
        tls=False,
        username="meshdev",
        password="large4cats",
        topic="msh/US/2/map/#",
        decoder="map_report",
        description="Map report packets — JSON decode attempted first",
    ),
    SourceConfig(
        name="MQTT Raw (Advanced)",
        source_tag=SOURCE_MQTT_RAW,
        enabled=False,
        broker="mqtt.meshtastic.org",
        port=1883,
        tls=False,
        username="meshdev",
        password="large4cats",
        topic="msh/US/2/#",
        decoder="protobuf",
        description="All region/2 traffic — overlaps other sources when enabled",
    ),
]


# ── Per-source worker thread ───────────────────────────────────────────────────

class _SourceWorker:
    """One MQTT source on a daemon thread with exponential-backoff reconnect."""

    def __init__(
        self,
        tag: str,
        cfg: SourceConfig,
        on_packet,      # (topic: str, payload: str) -> None
        on_status,      # (status_text: str) -> None
        on_subscribed=None,    # (topic, sub_type, parent_root) -> None
        on_unsubscribed=None,  # (topic,) -> None
    ):
        self._tag            = tag
        self._cfg            = cfg
        self._on_packet      = on_packet
        self._on_status      = on_status
        self._on_subscribed  = on_subscribed  or (lambda t, st, pr: None)
        self._on_unsubscribed = on_unsubscribed or (lambda t: None)
        self._stop           = threading.Event()
        self._client: Optional[mqtt_lib.Client] = None
        self._thread: Optional[threading.Thread] = None
        # Stable client ID reused across reconnects — unique per source per process
        short = tag.replace("mqtt_", "")
        self._client_id = f"mcp_{short}_{os.getpid()}"

        # Runtime stats — scalar writes are GIL-safe; no extra lock needed
        self.connected:       bool               = False
        self.connected_since: Optional[datetime] = None
        self.last_packet:     Optional[datetime] = None
        self.packet_count:    int                = 0   # raw MQTT messages received
        self.decoded_count:   int                = 0   # successfully processed
        self.ignored_count:   int                = 0   # received but not stored
        self.ignore_reasons:  Dict[str, int]     = {}  # reason → count
        self.error_count:     int                = 0
        self.last_error:      str                = ""

        # Per-root and per-channel packet counts
        self.root_counts:    Dict[str, int] = {}
        self.channel_counts: Dict[str, int] = {}

        # Currently-subscribed topic filters (authoritative — mirrors the broker state)
        self._active_topics: Set[str] = set()

        # Diagnostic broad subscription tracking (legacy path — prefer DiscoveryClient)
        self._diag_patterns: Set[str] = set()

        # Set by _cb_disconnect so the worker thread (blocked in _connect_once) wakes up.
        # Must exist before any thread is started.
        self._disc_event = threading.Event()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name=f"mqtt-{self._tag}", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        """Signal the worker thread to stop. Worker thread handles cleanup."""
        self._stop.set()
        self._disc_event.set()   # wake the thread immediately if it's waiting
        def _watchdog():
            if self._thread and self._thread.is_alive():
                logging.warning("[%s] stop() watchdog: still alive after 5s", self._tag)
        t = threading.Timer(5.0, _watchdog)
        t.daemon = True
        t.start()

    def force_stop(self) -> None:
        """Signal the worker thread to stop immediately. Non-blocking."""
        self._stop.set()
        self._disc_event.set()

    def reset(self) -> None:
        """Force-stop, wait for thread exit, clear state, restart fresh."""
        self._on_status("Resetting…")
        self.force_stop()
        if self._thread:
            self._thread.join(timeout=6.0)
        # Drain subscription registry
        for t in list(self._active_topics):
            self._on_unsubscribed(t)
        self._active_topics.clear()
        self._diag_patterns.clear()
        self.connected       = False
        self.connected_since = None
        self._client         = None
        # Fresh events so start() can proceed
        self._stop       = threading.Event()
        self._disc_event = threading.Event()
        self.start()

    def update_config(self, cfg: SourceConfig) -> None:
        self._cfg = cfg

    def get_stats(self) -> dict:
        return {
            "connected":        self.connected,
            "connected_since":  self.connected_since,
            "last_packet":      self.last_packet,
            "packet_count":     self.packet_count,
            "decoded_count":    self.decoded_count,
            "ignored_count":    self.ignored_count,
            "ignore_reasons":   dict(self.ignore_reasons),
            "error_count":      self.error_count,
            "last_error":       self.last_error,
            "root_counts":      dict(self.root_counts),
            "channel_counts":   dict(self.channel_counts),
            "effective_topics": self._cfg.effective_topics(),
            "active_topics":    set(self._active_topics),  # ACTUAL subscribed topics
        }

    def subscribe_topics(self, topics: List[str], parent_root: Optional[str] = None) -> bool:
        """Dynamically subscribe to additional topic filters without restarting.

        Returns False if not currently connected.  Notifies the subscription registry
        via the on_subscribed callback.
        """
        client = self._client
        if client is None or not self.connected:
            return False
        for t in topics:
            if t in self._active_topics:
                continue  # already subscribed
            client.subscribe(t)
            logging.info("[%s] mqtt.subscribe (dynamic): %r", self._tag, t)
            self._active_topics.add(t)
            sub_type = "root-derived" if parent_root else "direct"
            self._on_subscribed(t, sub_type, parent_root)
        return True

    def unsubscribe_topics(self, topics: List[str]) -> bool:
        """Dynamically unsubscribe from topic filters.

        Always updates internal state; also calls unsubscribe on the broker client
        if connected.  Returns True even when disconnected (state is still cleared).
        """
        client = self._client
        for t in topics:
            self._active_topics.discard(t)
            self._on_unsubscribed(t)
            if client:
                try:
                    client.unsubscribe(t)
                except Exception:
                    pass
        return True

    def start_diagnostic_broad(self, pattern: str, duration_sec: int = 60) -> bool:
        """Temporarily subscribe to a broad wildcard for traffic discovery.

        Returns False if not connected.  Automatically unsubscribes after
        duration_sec seconds.  Safe to call from any thread.
        """
        client = self._client
        if client is None or not self.connected:
            return False
        if pattern in self._diag_patterns:
            return True  # already active
        self._diag_patterns.add(pattern)
        client.subscribe(pattern)
        t = threading.Timer(duration_sec, self._stop_diag_broad, args=(pattern,))
        t.daemon = True
        t.start()
        return True

    def _stop_diag_broad(self, pattern: str) -> None:
        client = self._client
        if client:
            try:
                client.unsubscribe(pattern)
            except Exception:
                pass
        self._diag_patterns.discard(pattern)

    # ── background thread ─────────────────────────────────────────────────────

    def _run(self) -> None:
        delay = 1
        while not self._stop.is_set():
            self._disc_event.clear()
            try:
                self._connect_once()
                delay = 1
            except Exception as exc:
                if not self._stop.is_set():
                    self.error_count += 1
                    self.last_error = str(exc)
                    self._on_status(f"Error: {str(exc)[:60]}")

            if self._stop.is_set():
                break
            self._on_status(f"Reconnecting in {delay}s…")
            self._stop.wait(timeout=delay)
            delay = min(delay * 2, 60)

    def _connect_once(self) -> None:
        """Connect, subscribe, and block until disconnected or stop() is called.

        Uses loop_start() so the paho network loop runs on its own daemon thread.
        This makes stop()/force_stop() non-blocking: they just set events, and this
        method wakes up, cleans up, and returns — no blocking disconnect from outside.
        """
        cfg    = self._cfg
        client = _make_client(self._client_id)
        self._client = client

        if cfg.username:
            client.username_pw_set(cfg.username, cfg.password)
        if cfg.tls:
            client.tls_set()

        client.on_connect    = self._cb_connect
        client.on_disconnect = self._cb_disconnect
        client.on_message    = self._cb_message

        self._on_status(f"Connecting to {cfg.broker}:{cfg.port}…")
        client.loop_start()      # paho network loop on a daemon thread
        client.connect(cfg.broker, cfg.port, keepalive=60)

        # Block this thread until stop() is called or the broker disconnects us.
        while not self._stop.is_set() and not self._disc_event.is_set():
            self._stop.wait(timeout=5.0)

        # Cleanup on this thread — never on the caller's thread
        self._client = None
        try:
            client.disconnect()
        except Exception:
            pass
        try:
            client.loop_stop()   # waits for paho daemon thread to exit
        except Exception:
            pass

    # ── paho callbacks (run on MQTT network thread) ───────────────────────────

    # CONNACK return codes (used in on_connect)
    _RC_MSGS = {
        1: "Wrong protocol version",
        2: "Bad client ID",
        3: "Broker unavailable",
        4: "Bad credentials",
        5: "Not authorized",
    }

    # paho internal error codes (used in on_disconnect)
    _DISC_RC_MSGS = {
        0:  "clean disconnect",
        1:  "out of memory",
        2:  "protocol error",
        3:  "invalid arguments",
        4:  "no connection",
        5:  "connection refused",
        6:  "not found / session lost / keepalive timeout",
        7:  "connection lost (network)",
        8:  "TLS error",
        9:  "payload too large",
        10: "not supported",
        11: "auth error",
        12: "ACL denied",
        13: "unknown error",
        14: "OS errno",
        15: "send queue full",
        16: "keepalive timeout",
    }

    def _cb_connect(self, client, userdata, flags, rc) -> None:
        if rc == 0:
            cfg    = self._cfg
            topics = cfg.effective_topics()
            for t in topics:
                client.subscribe(t)
                logging.info("[%s] mqtt.subscribe: %r", self._tag, t)
                self._active_topics.add(t)
                # Classify this subscription for the registry
                if t == cfg.topic:
                    sub_type    = "map" if "/map/" in t or t.endswith("/map/#") else "direct"
                    parent_root = None
                elif cfg.decoder == "protobuf":
                    sub_type    = "raw"
                    parent_root = None
                else:
                    sub_type    = "root-derived"
                    # parent_root = everything before /2/json/# or /2/e/#
                    parent_root = t.split("/2/json/#")[0] if "/2/json/#" in t else \
                                  t.split("/2/e/#")[0] if "/2/e/#" in t else None
                self._on_subscribed(t, sub_type, parent_root)
            self.connected       = True
            self.connected_since = datetime.now()
            logging.info("[%s] MQTT connected: broker=%s:%d client_id=%s",
                         self._tag, cfg.broker, cfg.port, self._client_id)
            self._on_status(
                f"Connected — {len(topics)} subscription(s)  [id: {self._client_id}]"
            )
        else:
            msg = self._RC_MSGS.get(rc, f"rc={rc}")
            self.connected = False
            self.error_count += 1
            self.last_error = f"Connect failed: {msg}"
            self._on_status(f"Connect failed: {msg}")
            client.disconnect()

    def _cb_disconnect(self, client, userdata, rc) -> None:
        self.connected       = False
        self.connected_since = None
        self._diag_patterns.clear()
        # Clear tracked topics and notify the registry
        for t in list(self._active_topics):
            self._on_unsubscribed(t)
        self._active_topics.clear()
        reason = self._DISC_RC_MSGS.get(rc, f"unknown rc={rc}")
        logging.warning("[%s] MQTT disconnected: %s (rc=%d) client_id=%s",
                        self._tag, reason, rc, self._client_id)
        if rc == 0 and self._stop.is_set():
            self._on_status("Disconnected")
        elif rc != 0 and not self._stop.is_set():
            self._on_status(f"Lost connection: {reason} (rc={rc})")
        # Wake the worker thread so it can clean up and (if not stopped) reconnect
        self._disc_event.set()

    def _cb_message(self, client, userdata, msg) -> None:
        try:
            topic = msg.topic

            # e and map topics carry binary ServiceEnvelope — must use latin-1
            is_binary = (
                "/2/e/" in topic
                or topic.endswith("/2/e")
                or "/2/map/" in topic
                or topic.endswith("/2/map")
                or self._cfg.decoder in ("map_report", "protobuf")
            )
            payload = msg.payload.decode(
                "latin-1" if is_binary else "utf-8", errors="replace"
            )
        except Exception as exc:
            self.error_count += 1
            self.last_error = f"Decode error: {exc}"
            return

        self.packet_count += 1
        self.last_packet = datetime.now()

        # Per-root count: find which configured root this topic belongs to
        for root in self._cfg.mqtt_roots:
            r = root.rstrip("/")
            if topic.startswith(r + "/"):
                self.root_counts[root] = self.root_counts.get(root, 0) + 1
                break

        # Per-channel count
        ch = _extract_channel(topic)
        if ch:
            self.channel_counts[ch] = self.channel_counts.get(ch, 0) + 1

        self._on_packet(msg.topic, payload)


# ── SourceManager ─────────────────────────────────────────────────────────────

class SourceManager(QObject):
    """Owns multiple MQTT sources; routes packets tagged with source_tag."""

    packet_received       = Signal(str, str, str)        # source_tag, topic, payload
    source_status_changed = Signal(str, str)             # source_tag, status_text
    topic_subscribed      = Signal(str, str, str, str)   # source_tag, topic, sub_type, parent_root
    topic_unsubscribed    = Signal(str, str)             # source_tag, topic

    def __init__(self, configs: List[SourceConfig], parent=None):
        super().__init__(parent)
        self._configs: Dict[str, SourceConfig] = {c.source_tag: c for c in configs}
        self._workers: Dict[str, _SourceWorker] = {}
        for cfg in configs:
            self._make_worker(cfg)

    # ── public API ────────────────────────────────────────────────────────────

    def connect_all(self) -> None:
        for tag, cfg in self._configs.items():
            if cfg.enabled:
                self._workers[tag].start()

    def disconnect_all(self) -> None:
        for w in self._workers.values():
            w.stop()

    def connect_source(self, tag: str) -> None:
        w = self._workers.get(tag)
        if w:
            w.start()

    def disconnect_source(self, tag: str) -> None:
        w = self._workers.get(tag)
        if w:
            w.stop()

    def reset_source(self, tag: str) -> None:
        """Force-stop and restart a single source with its current config."""
        w = self._workers.get(tag)
        if w and self._configs.get(tag, None) and self._configs[tag].enabled:
            w.reset()

    def restart_all(self) -> None:
        """Force-stop and restart every enabled source."""
        for tag, cfg in self._configs.items():
            w = self._workers.get(tag)
            if w and cfg.enabled:
                w.reset()

    def get_config(self, tag: str) -> Optional[SourceConfig]:
        return self._configs.get(tag)

    def all_configs(self) -> List[SourceConfig]:
        return list(self._configs.values())

    def get_stats(self, tag: str) -> dict:
        w = self._workers.get(tag)
        return w.get_stats() if w else {}

    def set_connected(
        self,
        tag:       str,
        connected: bool,
        since:     Optional[datetime] = None,
    ) -> None:
        """Update connection state for a source without starting its worker thread.

        Called by ProductionMqttClient to feed connected/disconnected state into
        the source panel display (which reads worker.connected every 2s).
        """
        w = self._workers.get(tag)
        if w:
            w.connected       = connected
            w.connected_since = since if connected else None

    def record_received(self, tag: str) -> None:
        """Increment the raw MQTT receive counter for a source.

        Call once per message before routing to the decoder.  Mirrors what
        _SourceWorker._cb_message does when workers run normally.
        """
        w = self._workers.get(tag)
        if w:
            w.packet_count += 1
            w.last_packet   = datetime.now()

    def record_decoded(self, tag: str) -> None:
        """Call after a packet from this source was successfully processed."""
        w = self._workers.get(tag)
        if w:
            w.decoded_count += 1

    def record_ignored(self, tag: str, reason: str = "unknown") -> None:
        """Call when a packet was received but not stored (unrecognised, filtered)."""
        w = self._workers.get(tag)
        if w:
            w.ignored_count += 1
            w.ignore_reasons[reason] = w.ignore_reasons.get(reason, 0) + 1

    def record_error(self, tag: str, msg: str) -> None:
        """Call when packet processing raised an unexpected exception."""
        w = self._workers.get(tag)
        if w:
            w.error_count += 1
            w.last_error = msg

    def update_config(self, new_cfg: SourceConfig) -> None:
        """Replace a source's config; reconnects the worker if it was running."""
        tag     = new_cfg.source_tag
        old     = self._workers.get(tag)
        was_alive = bool(old and old._thread and old._thread.is_alive())
        if old:
            old.stop()
        self._configs[tag] = new_cfg
        new_worker = self._make_worker(new_cfg)
        if was_alive and new_cfg.enabled:
            new_worker.start()

    def subscribe_root(
        self,
        root: str,
        source_tag: str = SOURCE_MQTT_JSON,
    ) -> bool:
        """Dynamically subscribe to a root's derived JSON topic without restarting.

        Updates the in-memory SourceConfig.mqtt_roots list so effective_topics()
        stays consistent.  Returns False if the source is not currently connected.
        """
        root = root.rstrip("/")
        derived = [f"{root}/2/json/#"]
        w = self._workers.get(source_tag)
        if not w:
            return False
        ok = w.subscribe_topics(derived, root)
        if ok:
            cfg = self._configs.get(source_tag)
            if cfg and root not in cfg.mqtt_roots:
                cfg.mqtt_roots = cfg.mqtt_roots + [root]
        return ok

    def unsubscribe_root(
        self,
        root: str,
        source_tag: str = SOURCE_MQTT_JSON,
    ) -> bool:
        """Dynamically unsubscribe from a root's derived topics.

        Always succeeds (updates tracking even if disconnected).
        """
        root = root.rstrip("/")
        derived = [f"{root}/2/json/#"]
        w = self._workers.get(source_tag)
        if w:
            w.unsubscribe_topics(derived)
        cfg = self._configs.get(source_tag)
        if cfg and root in cfg.mqtt_roots:
            cfg.mqtt_roots = [r for r in cfg.mqtt_roots if r != root]
        return True

    def active_roots(self, source_tag: str = SOURCE_MQTT_JSON) -> Set[str]:
        """Return parent roots of currently-subscribed root-derived topics."""
        w = self._workers.get(source_tag)
        if not w:
            return set()
        roots: Set[str] = set()
        for t in w._active_topics:
            if "/2/json/#" in t:
                roots.add(t.split("/2/json/#")[0])
            elif "/2/e/#" in t:
                roots.add(t.split("/2/e/#")[0])
        # Subtract the direct topic root (if topic ends in /2/json/#)
        cfg = self._configs.get(source_tag)
        if cfg and cfg.topic:
            direct_root = extract_root(cfg.topic.replace("#", "dummy"))
            roots.discard(direct_root)
        return roots

    def start_diagnostic_broad(
        self,
        tag: str,
        pattern: str = "msh/us/#",
        duration_sec: int = 60,
    ) -> bool:
        """Subscribe to a broad wildcard on the given source for diagnostic discovery.

        Automatically unsubscribes after duration_sec seconds.
        Returns True on success, False if the source is not connected.
        """
        w = self._workers.get(tag)
        return w.start_diagnostic_broad(pattern, duration_sec) if w else False

    # ── internal ──────────────────────────────────────────────────────────────

    def _make_worker(self, cfg: SourceConfig) -> _SourceWorker:
        tag = cfg.source_tag

        def on_packet(topic: str, payload: str) -> None:
            self.packet_received.emit(tag, topic, payload)

        def on_status(text: str) -> None:
            self.source_status_changed.emit(tag, text)

        def on_subscribed(topic: str, sub_type: str, parent_root) -> None:
            self.topic_subscribed.emit(tag, topic, sub_type, parent_root or "")

        def on_unsubscribed(topic: str) -> None:
            self.topic_unsubscribed.emit(tag, topic)

        w = _SourceWorker(tag, cfg, on_packet, on_status, on_subscribed, on_unsubscribed)
        self._workers[tag] = w
        return w
