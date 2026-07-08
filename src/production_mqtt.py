"""Unified production MQTT client — one connection, JSON + Map subscriptions.

Design is identical to the proven TopicProbeClient pattern:
  loop_start() + Event.wait() + non-blocking stop()

Connects to mqtt.meshtastic.org and subscribes to exactly two topics:
  msh/US/2/json/#   (routed to JSON decoder)
  msh/US/2/map/#    (routed to Map decoder)

Never publishes. Never subscribes to SC, roots, raw, or protobuf topics.
Reconnects automatically with exponential backoff. 10-second connect timeout.
"""
from __future__ import annotations

import logging
import os
import random
import threading
import traceback
from typing import List, Optional

import paho.mqtt.client as mqtt_lib
from PySide6.QtCore import QObject, Signal

from source_manager import _make_client   # paho VERSION1 compatibility shim

JSON_TOPIC = "msh/US/2/json/#"
MAP_TOPIC  = "msh/US/2/map/#"

_MAX_BACKOFF_SEC = 60


class ProductionMqttClient(QObject):
    """One MQTT connection, two subscriptions (JSON + Map).

    Qt signals are emitted from paho's daemon thread.  Qt auto-queues them to
    the main thread via the default AutoConnection, so no QTimer.singleShot
    marshaling is needed for any signal.

    Signals
    -------
    packet_received(topic, payload)
        Raw message; payload is latin-1 decoded for lossless binary round-trip.
    connected_event(client_id)
        Fires once per successful on_connect + subscribe sequence.
    disconnected_event(rc)
        Fires on every broker disconnect.
    status_changed(text)
        Human-readable status for source panel: "Connected", "Connecting...",
        "Error: ...", "Reconnecting in Xs...", "Disconnected".
    log_message(text)
        Event log lines — connect to window.log.
    """

    packet_received    = Signal(str, str)   # (topic, payload)
    connected_event    = Signal(str)        # (client_id)
    disconnected_event = Signal(int)        # (rc)
    status_changed     = Signal(str)        # display text
    log_message        = Signal(str)        # event log

    def __init__(
        self,
        broker:   str,
        port:     int,
        username: str = "",
        password: str = "",
        parent=None,
    ) -> None:
        super().__init__(parent)
        self._broker   = broker
        self._port     = port
        self._username = username
        self._password = password

        self._thread:  Optional[threading.Thread] = None
        self._stop     = threading.Event()
        self._disc_evt = threading.Event()
        self._conn_evt = threading.Event()   # set in _cb_connect (success or fail)
        self.connected  = False
        self._client_id = self._fresh_id()
        # Live paho client — set while _connect_once() is running, None otherwise.
        # subscribe_extra / unsubscribe_extra use this for dynamic subscriptions.
        self._client: Optional[mqtt_lib.Client] = None
        # Extra subscriptions beyond JSON_TOPIC / MAP_TOPIC; re-applied every reconnect.
        self._extra_subs: List[str] = []

    # ── public API ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._disc_evt.clear()
        self._thread = threading.Thread(
            target=self._run, name="prod-mqtt", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        """Non-blocking. Signals worker thread; cleanup happens on that thread."""
        self._stop.set()
        self._disc_evt.set()

    def restart(self) -> None:
        """Stop current connection and reconnect with a fresh client ID."""
        self.stop()
        if self._thread:
            self._thread.join(timeout=6.0)
        self._client_id = self._fresh_id()
        self._stop     = threading.Event()
        self._disc_evt = threading.Event()
        self._conn_evt = threading.Event()
        self.start()

    @staticmethod
    def _fresh_id() -> str:
        return f"mesh_command_post_{os.getpid()}_{random.randint(10000, 99999)}"

    # ── worker thread ───────────────────────────────────────────────────────────

    def _run(self) -> None:
        backoff = 1
        while not self._stop.is_set():
            self._disc_evt.clear()
            self._conn_evt.clear()
            try:
                self._connect_once()
                backoff = 1   # successful session → reset backoff
            except Exception:
                if not self._stop.is_set():
                    logging.error(
                        "ProductionMqttClient error:\n%s", traceback.format_exc()
                    )
                    self.status_changed.emit("Error: see debug.log")
                    self.log_message.emit("MQTT error — see debug.log")

            if self._stop.is_set():
                break

            self.status_changed.emit(f"Reconnecting in {backoff}s…")
            logging.info("Production MQTT: reconnecting in %ds", backoff)
            self._stop.wait(timeout=backoff)
            backoff = min(backoff * 2, _MAX_BACKOFF_SEC)

        self.connected = False
        self.status_changed.emit("Disconnected")
        logging.info("Production MQTT: worker thread exiting")

    def subscribe_extra(self, topic: str) -> bool:
        """Dynamically subscribe to one additional topic on the live connection.

        Thread-safe (paho subscribe is thread-safe).  If not currently connected,
        the topic is queued and sent on the next successful reconnect.
        Returns True if subscription was sent to the broker, False if only queued.
        """
        if topic in self._extra_subs:
            return self.connected
        self._extra_subs.append(topic)
        client = self._client
        if client is not None and self.connected:
            try:
                res, mid = client.subscribe(topic)
                logging.info(
                    "Production MQTT: subscribe_extra  %s  res=%d  mid=%d", topic, res, mid
                )
                self.log_message.emit(f"  subscribed (test): {topic}  (res={res} mid={mid})")
                return True
            except Exception as exc:
                logging.error("subscribe_extra(%r): %s", topic, exc)
        return False

    def unsubscribe_extra(self, topic: str) -> None:
        """Remove a dynamic subscription and unsubscribe from the broker if connected."""
        if topic in self._extra_subs:
            self._extra_subs.remove(topic)
        client = self._client
        if client is not None:
            try:
                client.unsubscribe(topic)
                logging.info("Production MQTT: unsubscribe_extra  %s", topic)
                self.log_message.emit(f"  unsubscribed (test): {topic}")
            except Exception as exc:
                logging.error("unsubscribe_extra(%r): %s", topic, exc)

    # ── worker thread ───────────────────────────────────────────────────────────

    def _connect_once(self) -> None:
        client = _make_client(self._client_id)
        self._client = client

        if self._username:
            client.username_pw_set(self._username, self._password)

        client.on_connect    = self._cb_connect
        client.on_disconnect = self._cb_disconnect
        client.on_message    = self._cb_message

        logging.info(
            "Production MQTT: connecting  %s:%d  id=%s",
            self._broker, self._port, self._client_id,
        )
        self.status_changed.emit(f"Connecting to {self._broker}:{self._port}…")
        self.log_message.emit(
            f"Production MQTT connecting  {self._broker}:{self._port}"
            f"  id={self._client_id}"
        )

        client.loop_start()
        client.connect(self._broker, self._port, keepalive=60)

        # 10-second connect timeout — wakes worker if broker never sends CONNACK
        def _timeout_watchdog() -> None:
            if not self._conn_evt.is_set() and not self._stop.is_set():
                logging.error(
                    "Production MQTT: no CONNACK within 10s  %s:%d  id=%s",
                    self._broker, self._port, self._client_id,
                )
                self.status_changed.emit(
                    "Error: connect timeout (10s — broker not responding)"
                )
                self.log_message.emit(
                    f"MQTT timeout: no CONNACK within 10s  "
                    f"{self._broker}:{self._port}  id={self._client_id}"
                )
                self._disc_evt.set()

        wdog = threading.Timer(10.0, _timeout_watchdog)
        wdog.daemon = True
        wdog.start()

        # Block until stop() is called or the broker disconnects (or timeout)
        while not self._stop.is_set() and not self._disc_evt.is_set():
            self._stop.wait(timeout=5.0)

        wdog.cancel()

        # All cleanup on the worker thread — never on the caller's thread
        self.connected = False
        self._client   = None   # prevent main-thread subscribe_extra calls on dead client
        try:
            client.disconnect()
        except Exception:
            pass
        try:
            client.loop_stop()
        except Exception:
            pass

    # ── paho callbacks (paho daemon thread) ─────────────────────────────────────

    def _cb_connect(self, client, userdata, flags, rc) -> None:
        self._conn_evt.set()   # cancel the 10s watchdog
        if rc == 0:
            self.connected = True
            logging.info(
                "Production MQTT: connected  %s:%d  id=%s",
                self._broker, self._port, self._client_id,
            )
            self.log_message.emit(
                f"Production MQTT connected  {self._broker}:{self._port}"
                f"  id={self._client_id}"
            )
            for topic in (JSON_TOPIC, MAP_TOPIC):
                res, mid = client.subscribe(topic)
                logging.info(
                    "Production MQTT: subscribe  %s  res=%d  mid=%d",
                    topic, res, mid,
                )
                self.log_message.emit(f"  subscribed: {topic}  (res={res} mid={mid})")

            for topic in self._extra_subs:
                try:
                    res, mid = client.subscribe(topic)
                    logging.info(
                        "Production MQTT: subscribe_extra(reconnect)  %s  res=%d  mid=%d",
                        topic, res, mid,
                    )
                    self.log_message.emit(
                        f"  subscribed (test/reconnect): {topic}  (res={res} mid={mid})"
                    )
                except Exception as exc:
                    logging.error("subscribe_extra reconnect %r: %s", topic, exc)

            self.status_changed.emit("Connected")
            self.connected_event.emit(self._client_id)
        else:
            rc_names = {
                1: "wrong protocol", 2: "bad client ID",
                3: "broker unavailable", 4: "bad credentials", 5: "not authorized",
            }
            reason = rc_names.get(rc, f"rc={rc}")
            logging.error(
                "Production MQTT: connect failed  %s  id=%s", reason, self._client_id
            )
            self.status_changed.emit(f"Error: connect failed ({reason})")
            self.log_message.emit(f"MQTT connect failed: {reason}")
            self._disc_evt.set()

    def _cb_disconnect(self, client, userdata, rc) -> None:
        self.connected = False
        rc_names = {
            0: "clean", 1: "protocol", 4: "bad credentials",
            5: "not authorized", 7: "connection lost",
        }
        reason = rc_names.get(rc, f"rc={rc}")
        logging.warning(
            "Production MQTT: disconnected  reason=%s  rc=%d  id=%s",
            reason, rc, self._client_id,
        )
        if self._stop.is_set():
            self.log_message.emit("Production MQTT: clean stop")
        else:
            self.log_message.emit(
                f"Production MQTT disconnected: {reason}  rc={rc}"
            )
            self.status_changed.emit(f"Disconnected ({reason})")
        self.disconnected_event.emit(rc)
        self._disc_evt.set()

    def _cb_message(self, client, userdata, msg) -> None:
        try:
            # latin-1 for lossless binary round-trip (Map Reports are binary protobuf)
            payload = msg.payload.decode("latin-1")
            self.packet_received.emit(msg.topic, payload)
        except Exception:
            logging.debug(
                "ProductionMqttClient._cb_message: %s", traceback.format_exc()
            )
