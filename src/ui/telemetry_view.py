import json

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView, QHeaderView, QTableWidget, QTableWidgetItem,
    QVBoxLayout, QWidget,
)

from models import MQTTPacket

_SORT_ROLE = Qt.UserRole + 1


class _SortItem(QTableWidgetItem):
    """QTableWidgetItem that sorts numerically when _SORT_ROLE data is set."""
    def __lt__(self, other: "QTableWidgetItem") -> bool:
        a = self.data(_SORT_ROLE)
        b = other.data(_SORT_ROLE)
        if a is not None and b is not None:
            try:
                return float(a) < float(b)
            except (TypeError, ValueError):
                pass
        return self.text() < other.text()


COLUMNS = [
    "Date/Time", "Sender",
    "Bat%", "Volts", "ChUtil%", "AirTx%", "Uptime",
    "Temp°C", "Hum%", "Pres(hPa)",
    "Other",
]
_MAX_ROWS = 1000
_KNOWN_METRIC_KEYS = {"device_metrics", "environment_metrics", "power_metrics"}
_ALL_KNOWN_FIELDS = {
    "battery_level", "voltage", "channel_utilization", "air_util_tx", "uptime_seconds",
    "temperature", "relative_humidity", "barometric_pressure",
}


def extract_metrics(payload: dict) -> tuple:
    """Return (merged_flat_dict, extras_dict).

    Handles both nested Meshtastic format (fields under device_metrics /
    environment_metrics) and flat format (fields directly in payload).
    Nested sub-object values take precedence over flat duplicates.
    """
    dm = payload.get("device_metrics") or {}
    em = payload.get("environment_metrics") or {}
    pm = payload.get("power_metrics") or {}
    flat = {k: v for k, v in payload.items() if k not in _KNOWN_METRIC_KEYS}
    merged = {**flat, **pm, **em, **dm}
    extras = {k: v for k, v in merged.items() if k not in _ALL_KNOWN_FIELDS}
    return merged, extras


def fmt_uptime(seconds) -> str:
    try:
        s = int(seconds)
    except (TypeError, ValueError):
        return str(seconds)
    if s < 120:
        return f"{s}s"
    m = s // 60
    if m < 120:
        h_part, m_part = divmod(m, 60)
        if h_part == 0:
            return f"{m}m {s % 60}s"
        return f"{h_part}h {m_part}m"
    h = m // 60
    if h < 48:
        return f"{h}h {m % 60}m"
    d, rem_h = divmod(h, 24)
    return f"{d}d {rem_h}h"


class TelemetryViewWidget(QWidget):
    def __init__(self, registry=None, app=None, parent=None):
        super().__init__(parent)
        self._registry = registry
        self._app = app
        self._setup_ui()

    def _setup_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(4, 4, 4, 4)

        self._table = QTableWidget()
        self._table.setColumnCount(len(COLUMNS))
        self._table.setHorizontalHeaderLabels(COLUMNS)
        self._table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._table.setAlternatingRowColors(True)
        self._table.verticalHeader().setVisible(False)
        self._table.verticalHeader().setDefaultSectionSize(20)
        hdr = self._table.horizontalHeader()
        hdr.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(1, QHeaderView.Interactive)
        self._table.setColumnWidth(1, 260)
        for i in range(2, len(COLUMNS) - 1):
            hdr.setSectionResizeMode(i, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(len(COLUMNS) - 1, QHeaderView.Stretch)
        self._table.cellDoubleClicked.connect(self._on_cell_double_clicked)
        self._table.setSortingEnabled(True)
        self._table.horizontalHeader().setSortIndicator(0, Qt.DescendingOrder)
        root.addWidget(self._table)

    # ── Public API ──────────────────────────────────────────────────────────

    def add_packet(self, packet: MQTTPacket) -> None:
        if packet.packet_type != "telemetry":
            return

        try:
            data = json.loads(packet.raw_json)
            payload = data.get("payload", {})
        except Exception:
            payload = {}

        if not isinstance(payload, dict):
            payload = {}

        merged, extras = extract_metrics(payload)

        def _f(key: str, fmt: str = "{}") -> str:
            val = merged.get(key)
            if val is None:
                return ""
            try:
                return fmt.format(val)
            except Exception:
                return str(val)

        sender_disp = (
            self._registry.node_display(packet.sender)
            if self._registry else packet.sender
        )

        uptime_str = fmt_uptime(merged["uptime_seconds"]) if "uptime_seconds" in merged else ""

        row_data = [
            packet.received_at.strftime("%m-%d %H:%M:%S"),
            sender_disp,
            _f("battery_level", "{:.0f}"),
            _f("voltage", "{:.3f}"),
            _f("channel_utilization", "{:.2f}"),
            _f("air_util_tx", "{:.2f}"),
            uptime_str,
            _f("temperature", "{:.1f}"),
            _f("relative_humidity", "{:.1f}"),
            _f("barometric_pressure", "{:.1f}"),
            json.dumps(extras)[:200] if extras else "",
        ]

        # Numeric sort values parallel to row_data columns (col 0 = timestamp epoch)
        _numeric = {
            0: packet.received_at.timestamp(),
            2: merged.get("battery_level"),
            3: merged.get("voltage"),
            4: merged.get("channel_utilization"),
            5: merged.get("air_util_tx"),
            7: merged.get("temperature"),
            8: merged.get("relative_humidity"),
            9: merged.get("barometric_pressure"),
        }

        self._table.setSortingEnabled(False)
        self._table.insertRow(0)
        for col, val in enumerate(row_data):
            item = _SortItem(val)
            if col == 0:
                item.setData(Qt.UserRole, packet)
            sv = _numeric.get(col)
            if sv is not None:
                try:
                    item.setData(_SORT_ROLE, float(sv))
                except (TypeError, ValueError):
                    pass
            self._table.setItem(0, col, item)
        if self._table.rowCount() > _MAX_ROWS:
            self._table.removeRow(self._table.rowCount() - 1)
        self._table.setSortingEnabled(True)

    def refresh_nodes(self, node_ids: set) -> None:
        """One-pass repaint of Sender cells for a set of dirty node IDs."""
        if not self._registry or not node_ids:
            return
        reg = self._registry
        for row in range(self._table.rowCount()):
            item0 = self._table.item(row, 0)
            if item0 is None:
                continue
            packet: MQTTPacket = item0.data(Qt.UserRole)
            if packet is None:
                continue
            if packet.sender in node_ids:
                item = self._table.item(row, 1)
                if item:
                    item.setText(reg.node_display(packet.sender))

    def refresh_node(self, node_id: str) -> None:
        self.refresh_nodes({node_id})

    # ── Slots ───────────────────────────────────────────────────────────────

    def _on_cell_double_clicked(self, row: int, col: int) -> None:
        """Double-click — jump to the sender's location on the Map tab."""
        if self._app is None:
            return
        item = self._table.item(row, 0)
        if item is None:
            return
        packet: MQTTPacket = item.data(Qt.UserRole)
        if packet is None:
            return
        self._app.jump_to_node(packet.sender)
