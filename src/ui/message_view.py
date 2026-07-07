import json

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView, QHeaderView, QTableWidget, QTableWidgetItem,
    QVBoxLayout, QWidget,
)

from models import MQTTPacket

_SORT_ROLE = Qt.UserRole + 1


class _SortItem(QTableWidgetItem):
    def __lt__(self, other: "QTableWidgetItem") -> bool:
        a = self.data(_SORT_ROLE)
        b = other.data(_SORT_ROLE)
        if a is not None and b is not None:
            try:
                return float(a) < float(b)
            except (TypeError, ValueError):
                pass
        return self.text() < other.text()


COLUMNS = ["Date/Time", "Sender", "Destination", "Text", "Topic"]
_MAX_ROWS = 1000


class MessageViewWidget(QWidget):
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
        hdr.setSectionResizeMode(2, QHeaderView.Interactive)
        hdr.setSectionResizeMode(3, QHeaderView.Stretch)
        hdr.setSectionResizeMode(4, QHeaderView.Interactive)
        self._table.setColumnWidth(1, 240)
        self._table.setColumnWidth(2, 180)
        self._table.setColumnWidth(4, 200)
        self._table.cellDoubleClicked.connect(self._on_cell_double_clicked)
        self._table.setSortingEnabled(True)
        self._table.horizontalHeader().setSortIndicator(0, Qt.DescendingOrder)
        root.addWidget(self._table)

    # ── Public API ──────────────────────────────────────────────────────────

    def add_packet(self, packet: MQTTPacket) -> None:
        if packet.packet_type != "text":
            return

        try:
            data = json.loads(packet.raw_json)
            payload = data.get("payload", "")
            if isinstance(payload, dict):
                text = str(payload.get("text", json.dumps(payload)))
            else:
                text = str(payload) if payload is not None else ""
        except Exception:
            text = "[parse error]"

        sender_disp = (
            self._registry.node_display(packet.sender)
            if self._registry else packet.sender
        )
        dest_disp = self._dest_display(packet.to_num)

        row_data = [
            packet.received_at.strftime("%m-%d %H:%M:%S"),
            sender_disp,
            dest_disp,
            text,
            packet.topic,
        ]

        self._table.setSortingEnabled(False)
        self._table.insertRow(0)
        for col, val in enumerate(row_data):
            if col == 0:
                item = _SortItem(val)
                item.setData(Qt.UserRole, packet)
                item.setData(_SORT_ROLE, packet.received_at.timestamp())
            else:
                item = QTableWidgetItem(val)
            self._table.setItem(0, col, item)
        if self._table.rowCount() > _MAX_ROWS:
            self._table.removeRow(self._table.rowCount() - 1)
        self._table.setSortingEnabled(True)

    def refresh_nodes(self, node_ids: set) -> None:
        """One-pass repaint of Sender/Destination cells for a set of dirty node IDs."""
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

            if packet.to_num is not None and packet.to_num != 4294967295:
                tid = f"!{packet.to_num:08x}"
                if tid in node_ids:
                    item = self._table.item(row, 2)
                    if item:
                        item.setText(reg.node_display_int(packet.to_num))

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

    # ── Internal ────────────────────────────────────────────────────────────

    def _dest_display(self, to_num) -> str:
        if to_num is None or to_num == 4294967295:
            return "BROADCAST"
        if self._registry:
            return self._registry.node_display_int(to_num)
        return f"!{to_num:08x}"
