import threading
import time
from typing import Optional

import paho.mqtt.client as mqtt
from PySide6.QtCore import QObject, Signal


def _make_client(client_id: str) -> mqtt.Client:
    try:
        return mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION1,
            client_id=client_id,
        )
    except (AttributeError, TypeError):
        return mqtt.Client(client_id=client_id)


class MQTTClient(QObject):
    packet_received = Signal(str, str)
    connected = Signal()
    disconnected = Signal()
    error_occurred = Signal(str)
    status_changed = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._client: Optional[mqtt.Client] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._config: dict = {}

    # ── Public API ──────────────────────────────────────────────────────────

    def connect_to_broker(self, config: dict) -> None:
        if self._thread and self._thread.is_alive():
            self.disconnect_from_broker()
            self._thread.join(timeout=3)

        self._config = config
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="mqtt-worker"
        )
        self._thread.start()

    def disconnect_from_broker(self) -> None:
        self._stop_event.set()
        client = self._client
        if client:
            try:
                client.disconnect()
            except Exception:
                pass
        self.status_changed.emit("Disconnected")
        self.disconnected.emit()

    @property
    def is_connected(self) -> bool:
        c = self._client
        return bool(c and c.is_connected())

    # ── Background thread ───────────────────────────────────────────────────

    def _run_loop(self) -> None:
        delay = 1
        while not self._stop_event.is_set():
            try:
                self._connect_once()
                delay = 1
            except Exception as exc:
                if not self._stop_event.is_set():
                    self.error_occurred.emit(f"MQTT: {exc}")

            if self._stop_event.is_set():
                break

            self.status_changed.emit(f"Reconnecting in {delay}s…")
            self._stop_event.wait(timeout=delay)
            delay = min(delay * 2, 60)

    def _connect_once(self) -> None:
        cfg = self._config
        broker = cfg.get("broker", "mqtt.meshtastic.org")
        port = int(cfg.get("port", 1883))
        username = cfg.get("username", "")
        password = cfg.get("password", "")
        tls = cfg.get("tls", False)
        topic = cfg.get("topic", "msh/US/2/json/#")

        client_id = f"mesh-cp-{int(time.time()) % 100000}"
        client = _make_client(client_id)
        self._client = client

        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message

        if username:
            client.username_pw_set(username, password)
        if tls:
            client.tls_set()

        self.status_changed.emit(f"Connecting to {broker}:{port}…")
        client.connect(broker, port, keepalive=60)
        client.loop_forever()

    # ── paho callbacks (run on MQTT network thread) ──────────────────────────

    def _on_connect(self, client, userdata, flags, rc) -> None:
        RC_MSGS = {
            0: None,
            1: "Wrong protocol version",
            2: "Bad client ID",
            3: "Broker unavailable",
            4: "Bad credentials",
            5: "Not authorized",
        }
        msg = RC_MSGS.get(rc, f"rc={rc}")
        if rc == 0:
            topic = self._config.get("topic", "msh/US/2/json/#")
            client.subscribe(topic)
            self.status_changed.emit(f"Connected — subscribed to {topic}")
            self.connected.emit()
        else:
            self.error_occurred.emit(f"Connect failed: {msg}")
            self.status_changed.emit(f"Connection failed: {msg}")
            client.disconnect()

    def _on_disconnect(self, client, userdata, rc) -> None:
        self._client = None
        if rc != 0 and not self._stop_event.is_set():
            self.status_changed.emit("Lost connection")
            self.disconnected.emit()

    def _on_message(self, client, userdata, msg) -> None:
        try:
            topic = msg.topic
            payload = msg.payload.decode("utf-8", errors="replace")
            self.packet_received.emit(topic, payload)
        except Exception as exc:
            self.error_occurred.emit(f"Message decode error: {exc}")
