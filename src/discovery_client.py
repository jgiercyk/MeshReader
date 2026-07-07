"""Isolated MQTT client for root discovery.

Discovery traffic is 100% separate from the normal packet pipeline:
  - Discovery packets are NEVER inserted into the packets table.
  - Discovery packets NEVER update nodes, the map, or the decoded feed.
  - The only output is a dict of discovered roots → stats, delivered via on_done().

Lifecycle:
  start(duration_sec)  →  connects, subscribes, collects roots
  [timer fires]        →  unsubscribes, disconnects, calls on_done(roots)
  stop()               →  cancels early, same cleanup
"""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime
from threading import Lock
from typing import Callable, Dict, Optional

import paho.mqtt.client as mqtt_lib

from source_manager import _make_client, extract_root, extract_channel


class DiscoveryClient:
    """One-shot discovery MQTT connection.

    Connects to the broker with a dedicated client ID, subscribes to the
    discovery topic for `duration_sec` seconds, collects root statistics,
    then disconnects cleanly and delivers the result via on_done().
    """

    # Broad enough to see all US-region roots; still more targeted than msh/#
    DISCOVERY_TOPIC = "msh/US/2/json/#"

    def __init__(
        self,
        broker:       str,
        port:         int,
        username:     str = "",
        password:     str = "",
        on_root_seen: Optional[Callable[[str, str], None]] = None,
        on_log:       Optional[Callable[[str], None]] = None,
        on_done:      Optional[Callable[[dict], None]] = None,
    ) -> None:
        self._broker   = broker
        self._port     = port
        self._username = username
        self._password = password
        # Callbacks — all optional; None → noop / log to debug
        self._on_root_seen = on_root_seen or (lambda root, ch: None)
        self._on_log  = on_log or (lambda m: logging.info("Discovery: %s", m))
        self._on_done = on_done or (lambda d: None)

        self._client: Optional[mqtt_lib.Client] = None
        self._stop   = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._timer:  Optional[threading.Timer]  = None

        self._lock  = Lock()
        self._roots: Dict[str, dict] = {}   # root → stats dict
        self._client_id = f"mcp_disc_{os.getpid()}"

    @property
    def running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    # ── public API ────────────────────────────────────────────────────────────

    def start(self, duration_sec: int) -> None:
        """Begin a discovery run.  Cancels any prior run first."""
        if self.running:
            self.stop()
        self._stop.clear()
        self._roots = {}
        self._thread = threading.Thread(
            target=self._run, args=(duration_sec,),
            name="mqtt-discovery", daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Cancel discovery immediately — safe to call from any thread."""
        self._stop.set()
        if self._timer:
            self._timer.cancel()
            self._timer = None
        self._kill_client(reason="cancelled")

    # ── internals ─────────────────────────────────────────────────────────────

    def _kill_client(self, reason: str = "") -> None:
        client = self._client
        if client is None:
            return
        self._client = None
        try:
            client.unsubscribe(self.DISCOVERY_TOPIC)
            self._on_log(f"Discovery unsubscribed: {self.DISCOVERY_TOPIC}"
                         + (f" ({reason})" if reason else ""))
        except Exception:
            pass
        try:
            client.disconnect()
        except Exception:
            pass

    def _run(self, duration_sec: int) -> None:
        try:
            client = _make_client(self._client_id)
            self._client = client

            if self._username:
                client.username_pw_set(self._username, self._password)

            client.on_connect    = self._cb_connect
            client.on_disconnect = self._cb_disconnect
            client.on_message    = self._cb_message

            # Automatically stop after duration
            self._timer = threading.Timer(duration_sec, self._timeout)
            self._timer.daemon = True
            self._timer.start()

            self._on_log(
                f"Discovery connecting to {self._broker}:{self._port} "
                f"(window: {duration_sec}s)…"
            )
            client.connect(self._broker, self._port, keepalive=60)
            client.loop_forever()

        except Exception as exc:
            logging.error("DiscoveryClient._run: %s", exc)
            self._on_log(f"Discovery error: {exc}")
        finally:
            # Deliver result on every exit path
            with self._lock:
                roots = dict(self._roots)
            self._client = None
            self._on_done(roots)

    def _timeout(self) -> None:
        """Called by threading.Timer when the discovery window expires."""
        self._stop.set()
        self._kill_client(reason="window expired")

    # ── paho callbacks ────────────────────────────────────────────────────────

    def _cb_connect(self, client, userdata, flags, rc) -> None:
        if rc == 0:
            logging.info("DiscoveryClient connected: client_id=%s", self._client_id)
            self._on_log(
                f"Discovery connected (id: {self._client_id}); "
                f"subscribed: {self.DISCOVERY_TOPIC}"
            )
            client.subscribe(self.DISCOVERY_TOPIC)
        else:
            self._on_log(f"Discovery connect failed: rc={rc}")
            self._stop.set()
            try:
                client.disconnect()
            except Exception:
                pass

    def _cb_disconnect(self, client, userdata, rc) -> None:
        logging.debug("DiscoveryClient disconnected rc=%d", rc)

    def _cb_message(self, client, userdata, msg) -> None:
        if self._stop.is_set():
            return
        try:
            root    = extract_root(msg.topic)
            if not root:
                return
            channel = extract_channel(msg.topic) or ""
            now     = datetime.now().isoformat()
            with self._lock:
                if root not in self._roots:
                    self._roots[root] = {
                        "first_seen":   now,
                        "last_seen":    now,
                        "packet_count": 0,
                        "channels":     set(),
                    }
                d = self._roots[root]
                d["last_seen"]    = now
                d["packet_count"] += 1
                if channel:
                    d["channels"].add(channel)
            # Notify caller for real-time progress (e.g., update a discovery counter)
            self._on_root_seen(root, channel)
        except Exception as exc:
            logging.debug("DiscoveryClient._cb_message: %s", exc)
