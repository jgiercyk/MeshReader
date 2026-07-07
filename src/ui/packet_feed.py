import csv
import json
from datetime import datetime
from typing import List, Optional, Set

from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QAbstractItemView, QApplication, QCheckBox, QComboBox, QFileDialog,
    QHBoxLayout, QHeaderView, QLabel, QLineEdit, QMessageBox, QPushButton,
    QSplitter, QTableWidget, QTableWidgetItem, QTextEdit, QVBoxLayout,
    QWidget,
)

from models import MQTTPacket

_SORT_ROLE = Qt.UserRole + 1   # numeric sort key for timestamp; Qt.UserRole is the packet ref


class _SortItem(QTableWidgetItem):
    """QTableWidgetItem that sorts numerically on _SORT_ROLE when set."""
    def __lt__(self, other: "QTableWidgetItem") -> bool:
        a = self.data(_SORT_ROLE)
        b = other.data(_SORT_ROLE)
        if a is not None and b is not None:
            try:
                return float(a) < float(b)
            except (TypeError, ValueError):
                pass
        return self.text() < other.text()


COLUMNS = ["Date/Time", "Topic", "Type", "Sender", "From", "To", "Ch", "Summary"]
_RAW_COLS   = [0, 1, 2, 6, 7]
_SENDER_COL = 3
_FROM_COL   = 4
_TO_COL     = 5
_MAX_PACKETS  = 1000   # internal buffer
_MAX_DISPLAY  = 500    # visible rows cap


class PacketFeedWidget(QWidget):
    def __init__(self, registry=None, app=None, parent=None):
        super().__init__(parent)
        self._packets: List[MQTTPacket] = []
        self._pending_display: List[MQTTPacket] = []   # buffered since last flush
        self._needs_full_refresh: bool = False
        self._paused = False
        self._registry = registry
        self._app = app
        self._setup_ui()

    # ── Setup ───────────────────────────────────────────────────────────────

    def _setup_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(4, 4, 4, 4)
        root.setSpacing(4)
        root.addLayout(self._build_filter_bar())

        splitter = QSplitter(Qt.Vertical)
        splitter.addWidget(self._build_table())
        splitter.addWidget(self._build_detail())
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 1)
        root.addWidget(splitter)

    def _build_filter_bar(self) -> QHBoxLayout:
        bar = QHBoxLayout()

        bar.addWidget(QLabel("Type:"))
        self._type_cb = QComboBox()
        self._type_cb.setFixedWidth(120)
        self._type_cb.addItem("All")
        self._type_cb.currentTextChanged.connect(self._on_filter_changed)
        bar.addWidget(self._type_cb)

        bar.addWidget(QLabel("Sender:"))
        self._sender_le = QLineEdit()
        self._sender_le.setPlaceholderText("filter…")
        self._sender_le.setFixedWidth(110)
        self._sender_le.textChanged.connect(self._on_filter_changed)
        bar.addWidget(self._sender_le)

        bar.addWidget(QLabel("Search:"))
        self._text_le = QLineEdit()
        self._text_le.setPlaceholderText("summary / topic…")
        self._text_le.setFixedWidth(130)
        self._text_le.textChanged.connect(self._on_filter_changed)
        bar.addWidget(self._text_le)

        self._pos_cb = QCheckBox("Has position")
        self._pos_cb.stateChanged.connect(self._on_filter_changed)
        bar.addWidget(self._pos_cb)

        self._txt_cb = QCheckBox("Has text")
        self._txt_cb.stateChanged.connect(self._on_filter_changed)
        bar.addWidget(self._txt_cb)

        self._live_cb = QCheckBox("Live only")
        self._live_cb.setToolTip(
            "Show only packets received since the current MQTT connection was established.\n"
            "Hides historical packets loaded from the database at startup."
        )
        self._live_cb.stateChanged.connect(self._on_filter_changed)
        bar.addWidget(self._live_cb)

        bar.addStretch()
        self._count_lbl = QLabel("0 packets")
        bar.addWidget(self._count_lbl)

        self._pause_btn = QPushButton("⏸ Pause")
        self._pause_btn.setFixedWidth(85)
        self._pause_btn.clicked.connect(self._toggle_pause)
        bar.addWidget(self._pause_btn)

        clr_btn = QPushButton("Clear Window")
        clr_btn.setFixedWidth(95)
        clr_btn.setToolTip(
            "Clear the visible packet table.\n"
            "Does NOT delete from the database — new packets continue arriving."
        )
        clr_btn.clicked.connect(self.clear_feed)
        bar.addWidget(clr_btn)

        exp_csv = QPushButton("Export CSV")
        exp_csv.clicked.connect(self._export_csv)
        bar.addWidget(exp_csv)

        exp_jsonl = QPushButton("Export JSONL")
        exp_jsonl.clicked.connect(self._export_jsonl)
        bar.addWidget(exp_jsonl)

        return bar

    def _build_table(self) -> QTableWidget:
        t = QTableWidget()
        t.setColumnCount(len(COLUMNS))
        t.setHorizontalHeaderLabels(COLUMNS)
        t.setSelectionBehavior(QAbstractItemView.SelectRows)
        t.setEditTriggers(QAbstractItemView.NoEditTriggers)
        t.setAlternatingRowColors(True)
        t.verticalHeader().setVisible(False)
        t.verticalHeader().setDefaultSectionSize(20)
        hdr = t.horizontalHeader()
        hdr.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(1, QHeaderView.Interactive)
        hdr.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(3, QHeaderView.Interactive)
        hdr.setSectionResizeMode(4, QHeaderView.Interactive)
        hdr.setSectionResizeMode(5, QHeaderView.Interactive)
        hdr.setSectionResizeMode(6, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(7, QHeaderView.Stretch)
        t.setColumnWidth(1, 200)
        t.setColumnWidth(3, 220)
        t.setColumnWidth(4, 220)
        t.setColumnWidth(5, 140)
        t.currentItemChanged.connect(self._on_item_changed)
        t.cellDoubleClicked.connect(self._on_cell_double_clicked)
        t.setSortingEnabled(True)
        t.horizontalHeader().setSortIndicator(0, Qt.DescendingOrder)  # default: newest first
        self._table = t
        return t

    def _build_detail(self) -> QWidget:
        w = QWidget()
        lay = QVBoxLayout(w)
        lay.setContentsMargins(0, 2, 0, 0)

        hdr = QHBoxLayout()
        self._detail_hdr = QLabel("Packet JSON:")
        hdr.addWidget(self._detail_hdr)
        hdr.addStretch()
        copy_btn = QPushButton("Copy JSON")
        copy_btn.setFixedHeight(22)
        copy_btn.clicked.connect(self._copy_json)
        hdr.addWidget(copy_btn)
        lay.addLayout(hdr)

        self._detail = QTextEdit()
        self._detail.setReadOnly(True)
        self._detail.setFont(QFont("Consolas", 9))
        self._detail.setMaximumHeight(220)
        lay.addWidget(self._detail)
        return w

    # ── Public API ──────────────────────────────────────────────────────────

    def add_packet(self, packet: MQTTPacket) -> None:
        """Buffer packet — call flush_display() to update the table."""
        self._packets.insert(0, packet)
        if len(self._packets) > _MAX_PACKETS:
            self._packets.pop()

        ptype = packet.packet_type
        if self._type_cb.findText(ptype) < 0:
            self._type_cb.addItem(ptype)

        self._pending_display.append(packet)

    def flush_display(self) -> None:
        """Push pending packets to the table — called from the app timer once per tick.

        Fast path (no filters): inserts N new rows at top — O(N) not O(all_rows).
        Slow path (filters active): full rebuild — only triggered by user filter changes.
        """
        if self._paused:
            return

        if self._needs_full_refresh:
            self._needs_full_refresh = False
            self._pending_display.clear()
            self._refresh_display()
            return

        if not self._pending_display:
            return

        if self._has_filters():
            # Filters active — full rebuild so only matching rows show
            self._pending_display.clear()
            self._refresh_display()
            return

        # ── Incremental fast path: insert new rows at top ──────────────────
        new_pkts, self._pending_display = self._pending_display, []
        reg = self._registry
        self._table.setSortingEnabled(False)
        self._table.setUpdatesEnabled(False)
        for packet in new_pkts:
            if self._table.rowCount() >= _MAX_DISPLAY:
                self._table.removeRow(self._table.rowCount() - 1)
            self._table.insertRow(0)
            raw = packet.to_display_row()
            for col, val in enumerate(raw):
                if reg:
                    if col == _SENDER_COL:
                        val = reg.node_display(packet.sender)
                    elif col == _FROM_COL:
                        val = reg.node_display_int(packet.from_num)
                    elif col == _TO_COL:
                        val = reg.node_display_int(packet.to_num)
                if col == 0:
                    item = _SortItem(val)
                    item.setData(_SORT_ROLE, packet.received_at.timestamp())
                else:
                    item = QTableWidgetItem(val)
                item.setData(Qt.UserRole, packet)
                self._table.setItem(0, col, item)
        self._table.setUpdatesEnabled(True)
        self._table.setSortingEnabled(True)
        # Count label: show live count when connected
        total = len(self._packets)
        connected_since = None
        if self._app:
            try:
                from source_manager import SOURCE_MQTT_JSON
                stats = self._app.source_manager.get_stats(SOURCE_MQTT_JSON)
                connected_since = stats.get("connected_since")
            except Exception:
                pass
        if connected_since:
            live = sum(1 for p in self._packets if p.received_at >= connected_since)
            self._count_lbl.setText(f"{total} packets  ({live} live)")
        else:
            self._count_lbl.setText(f"{total} packets")

    def clear_feed(self) -> None:
        self._packets.clear()
        self._pending_display.clear()
        self._table.setRowCount(0)
        self._detail.clear()

    def refresh_nodes(self, node_ids: Set[str]) -> None:
        """One-pass update of Sender/From/To cells for a set of dirty node IDs."""
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
                s = self._table.item(row, _SENDER_COL)
                if s:
                    s.setText(reg.node_display(packet.sender))

            if packet.from_num is not None:
                fid = f"!{packet.from_num:08x}"
                if fid in node_ids:
                    f = self._table.item(row, _FROM_COL)
                    if f:
                        f.setText(reg.node_display_int(packet.from_num))

            if packet.to_num is not None and packet.to_num != 4294967295:
                tid = f"!{packet.to_num:08x}"
                if tid in node_ids:
                    t = self._table.item(row, _TO_COL)
                    if t:
                        t.setText(reg.node_display_int(packet.to_num))

            # Also refresh detail header if this row is selected
            current = self._table.currentItem()
            if (current is not None and current.row() == row
                    and packet.sender in node_ids):
                self._detail_hdr.setText(
                    f"Sender: {reg.node_display(packet.sender)}"
                )

    # ── kept for compatibility (single-node refresh) ──────────────────────
    def refresh_node(self, node_id: str) -> None:
        self.refresh_nodes({node_id})

    # ── Internal ────────────────────────────────────────────────────────────

    def _has_filters(self) -> bool:
        return (
            self._type_cb.currentText() != "All"
            or bool(self._sender_le.text())
            or bool(self._text_le.text())
            or self._pos_cb.isChecked()
            or self._txt_cb.isChecked()
            or self._live_cb.isChecked()
        )

    def _on_filter_changed(self) -> None:
        """User changed a filter — clear pending queue and rebuild immediately."""
        self._pending_display.clear()
        if not self._paused:
            self._refresh_display()

    def _refresh_display(self) -> None:
        """Full rebuild from self._packets — O(min(n_packets, 500))."""
        type_f    = self._type_cb.currentText()
        sender_f  = self._sender_le.text().lower()
        text_f    = self._text_le.text().lower()
        want_pos  = self._pos_cb.isChecked()
        want_txt  = self._txt_cb.isChecked()
        live_only = self._live_cb.isChecked()

        # Determine connected_since for live-only filter
        connected_since: Optional[datetime] = None
        if live_only and self._app:
            try:
                from source_manager import SOURCE_MQTT_JSON
                stats = self._app.source_manager.get_stats(SOURCE_MQTT_JSON)
                connected_since = stats.get("connected_since")
            except Exception:
                pass

        filtered: List[MQTTPacket] = []
        live_count = 0
        for p in self._packets:
            if type_f != "All" and p.packet_type != type_f:
                continue
            if sender_f and sender_f not in p.sender.lower():
                continue
            if text_f and text_f not in p.summary.lower() and text_f not in p.topic.lower():
                continue
            if want_pos and p.packet_type != "position":
                continue
            if want_txt and p.packet_type != "text":
                continue
            is_live = connected_since is None or p.received_at >= connected_since
            if live_only and not is_live:
                continue
            if is_live:
                live_count += 1
            filtered.append(p)
            if len(filtered) >= _MAX_DISPLAY:
                break

        self._populate_table(filtered)
        total = len(self._packets)
        if connected_since and not live_only:
            # Compute live count across full (unfiltered) packet list
            live_total = sum(1 for p in self._packets if p.received_at >= connected_since)
            self._count_lbl.setText(f"{total} packets  ({live_total} live)")
        elif live_only:
            self._count_lbl.setText(f"{len(filtered)} live packets")
        else:
            self._count_lbl.setText(f"{total} packets")

    def _populate_table(self, packets: List[MQTTPacket]) -> None:
        reg = self._registry
        self._table.setSortingEnabled(False)
        self._table.setUpdatesEnabled(False)
        self._table.setRowCount(len(packets))
        for row, packet in enumerate(packets):
            raw = packet.to_display_row()
            for col, val in enumerate(raw):
                if reg:
                    if col == _SENDER_COL:
                        val = reg.node_display(packet.sender)
                    elif col == _FROM_COL:
                        val = reg.node_display_int(packet.from_num)
                    elif col == _TO_COL:
                        val = reg.node_display_int(packet.to_num)
                if col == 0:
                    item = _SortItem(val)
                    item.setData(_SORT_ROLE, packet.received_at.timestamp())
                else:
                    item = QTableWidgetItem(val)
                item.setData(Qt.UserRole, packet)
                self._table.setItem(row, col, item)
        self._table.setUpdatesEnabled(True)
        self._table.setSortingEnabled(True)

    # ── Slots ───────────────────────────────────────────────────────────────

    def _on_cell_double_clicked(self, row: int, col: int) -> None:
        """Double-click — jump to the originating node's location on the Map tab."""
        if self._app is None:
            return
        item = self._table.item(row, 0)
        if item is None:
            return
        packet: MQTTPacket = item.data(Qt.UserRole)
        if packet is None:
            return
        # Prefer 'from' (the packet originator) over 'sender' (the MQTT publisher).
        # Fall back to sender, then destination.
        from_id = f"!{packet.from_num:08x}" if packet.from_num is not None else None
        if from_id and self._app.jump_to_node(from_id):
            return
        if packet.sender and packet.sender != from_id:
            if self._app.jump_to_node(packet.sender):
                return
        if packet.to_num is not None and packet.to_num != 4294967295:
            self._app.jump_to_node(f"!{packet.to_num:08x}")

    def _on_item_changed(self, current, previous) -> None:
        if current is None:
            return
        item = self._table.item(current.row(), 0)
        if item is None:
            return
        packet: MQTTPacket = item.data(Qt.UserRole)
        if not packet:
            return

        try:
            pretty = json.dumps(json.loads(packet.raw_json), indent=2)
        except Exception:
            pretty = packet.raw_json

        if self._registry:
            sender_disp = self._registry.node_display(packet.sender)
        else:
            sender_disp = packet.sender
        self._detail_hdr.setText(f"Sender: {sender_disp}")
        self._detail.setPlainText(pretty)

    def _copy_json(self) -> None:
        text = self._detail.toPlainText()
        if text:
            QApplication.clipboard().setText(text)

    def _toggle_pause(self) -> None:
        self._paused = not self._paused
        self._pause_btn.setText("▶ Resume" if self._paused else "⏸ Pause")
        if not self._paused:
            # Full rebuild on resume to show everything accumulated while paused
            self._pending_display.clear()
            self._refresh_display()

    def _export_csv(self) -> None:
        path, _ = QFileDialog.getSaveFileName(
            self, "Export Packets", "packets.csv", "CSV (*.csv)"
        )
        if not path:
            return
        try:
            reg = self._registry
            headers = ["timestamp_utc"] + COLUMNS + ["Sender Location"]
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(headers)
                for p in self._packets:
                    row = [p.received_at.isoformat()] + p.to_display_row()
                    row.append(reg.location_str(p.sender) if reg else "")
                    w.writerow(row)
        except Exception as exc:
            QMessageBox.warning(self, "Export Error", str(exc))

    def _export_jsonl(self) -> None:
        path, _ = QFileDialog.getSaveFileName(
            self, "Export JSONL", "packets.jsonl", "JSONL (*.jsonl);;All Files (*)"
        )
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8") as f:
                for p in reversed(self._packets):
                    f.write(p.raw_json.strip() + "\n")
        except Exception as exc:
            QMessageBox.warning(self, "Export Error", str(exc))
