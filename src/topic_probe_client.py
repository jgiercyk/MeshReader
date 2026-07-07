"""Isolated MQTT client for probing specific topic filters.

Probe packets are NEVER inserted into the packets table, node registry, or map.
Results are delivered via on_done() (final summary dict, from the worker thread).
The on_done callback must marshal to the Qt main thread via QTimer.singleShot(0, ...).

Design:
  - paho loop_start() puts the network loop on its own daemon thread.
  - The probe worker thread sleeps in _stop.wait(duration_sec).
  - stop() is fully non-blocking — just sets the _stop event.
  - All cleanup (disconnect, loop_stop) happens on the worker thread.
  - snapshot() is never called while holding self._lock, avoiding self-deadlock.
"""
from __future__ import annotations

import logging
import os
import threading
from datetime import datetime
from threading import Lock
from typing import Callable, Dict, List, Optional

import paho.mqtt.client as mqtt_lib

from source_manager import _make_client, extract_channel


class TopicProbeClient:
    """One-shot probe: subscribes to a fixed topic list, collects stats, delivers results."""

    def __init__(
        self,
        broker: str,
        port: int,
        username: str = "",
        password: str = "",
        topics: Optional[List[str]] = None,
        on_packet_seen: Optional[Callable[[str, str], None]] = None,
        on_log: Optional[Callable[[str], None]] = None,
        on_done: Optional[Callable[[dict], None]] = None,
    ) -> None:
        self._broker         = broker
        self._port           = port
        self._username       = username
        self._password       = password
        self._topics         = list(topics or [])
        self._on_packet_seen = on_packet_seen or (lambda t, ch: None)
        self._on_log         = on_log or (lambda m: logging.info("Probe: %s", m))
        self._on_done        = on_done or (lambda d: None)

        self._client: Optional[mqtt_lib.Client] = None
        self._stop   = threading.Event()
        self._thread: Optional[threading.Thread] = None

        self._lock    = Lock()
        self._results: Dict[str, dict] = {}
        self._client_id = f"mcp_probe_{os.getpid()}"

    @property
    def running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    # ── public API ────────────────────────────────────────────────────────────

    def start(self, duration_sec: int) -> None:
        if self.running:
            self.stop()
        self._stop.clear()
        self._results = {}
        self._thread = threading.Thread(
            target=self._run, args=(duration_sec,),
            name="mqtt-probe", daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Non-blocking. Signals the worker thread; cleanup runs on the worker thread."""
        self._stop.set()

    def snapshot(self) -> Dict[str, dict]:
        """Thread-safe snapshot of current per-topic stats. Never call while holding self._lock."""
        with self._lock:
            out = {}
            for k, v in self._results.items():
                d = dict(v)
                d["channels"] = set(d["channels"])
                out[k] = d
            return out

    # ── worker thread ──────────────────────────────────────────────────────────

    def _run(self, duration_sec: int) -> None:
        client = None
        try:
            client = _make_client(self._client_id)
            self._client = client

            if self._username:
                client.username_pw_set(self._username, self._password)

            client.on_connect    = self._cb_connect
            client.on_disconnect = self._cb_disconnect
            client.on_message    = self._cb_message

            self._on_log(
                f"Topic probe connecting {self._broker}:{self._port} "
                f"({duration_sec}s window)…"
            )
            # loop_start() — paho network loop runs on its own daemon thread.
            # This lets _stop.wait() below be the only thing blocking this thread.
            client.loop_start()
            client.connect(self._broker, self._port, keepalive=60)

            # Block until stop() is called (user-cancel or dialog close)
            # or the duration elapses (normal completion).
            self._stop.wait(timeout=float(duration_sec))
            logging.info("Probe: wait complete  stopped=%s", self._stop.is_set())

        except Exception as exc:
            logging.error("TopicProbeClient._run: %s", exc)
            self._on_log(f"Probe error: {exc}")
        finally:
            # All cleanup runs here on the worker thread — never on the UI thread.
            self._client = None
            if client is not None:
                reason = "cancelled" if self._stop.is_set() else "window expired"
                self._on_log(f"Probe stopped ({reason})")
                try:
                    client.disconnect()   # queue DISCONNECT packet
                except Exception:
                    pass
                try:
                    client.loop_stop()    # wait for paho network thread to flush and exit
                except Exception:
                    pass

            # snapshot() acquires self._lock — only safe after loop_stop() ensures
            # no more _cb_message callbacks can fire and hold the lock.
            results = self.snapshot()
            self._on_done(results)

    # ── paho callbacks (run on paho network daemon thread) ────────────────────

    def _cb_connect(self, client, userdata, flags, rc) -> None:
        if rc == 0:
            logging.info("Probe: connected  client_id=%s", self._client_id)
            self._on_log(f"Probe connected (id: {self._client_id})")
            for t in self._topics:
                client.subscribe(t)
                self._on_log(f"Probe subscribed: {t}")
        else:
            self._on_log(f"Probe connect failed: rc={rc}")
            self._stop.set()

    def _cb_disconnect(self, client, userdata, rc) -> None:
        logging.debug("Probe: disconnected rc=%d", rc)

    def _cb_message(self, client, userdata, msg) -> None:
        if self._stop.is_set():
            return
        try:
            topic   = msg.topic
            channel = extract_channel(topic) or ""
            now     = datetime.now().isoformat()

            matched = topic
            for pf in self._topics:
                if _topic_matches(topic, pf):
                    matched = pf
                    break

            with self._lock:
                if matched not in self._results:
                    self._results[matched] = {
                        "packet_count": 0,
                        "channels":     set(),
                        "first_seen":   now,
                        "last_seen":    now,
                    }
                d = self._results[matched]
                d["packet_count"] += 1
                d["last_seen"]     = now
                if channel:
                    d["channels"].add(channel)

            self._on_packet_seen(matched, channel)
        except Exception as exc:
            logging.debug("TopicProbeClient._cb_message: %s", exc)


# ── MQTT wildcard matching ─────────────────────────────────────────────────────

def _topic_matches(topic: str, pattern: str) -> bool:
    if "#" not in pattern and "+" not in pattern:
        return topic == pattern
    tp = topic.split("/")
    pp = pattern.split("/")
    i = 0
    for seg in pp:
        if seg == "#":
            return True
        if i >= len(tp):
            return False
        if seg != "+" and seg != tp[i]:
            return False
        i += 1
    return i == len(tp)
