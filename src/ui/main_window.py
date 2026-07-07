from datetime import datetime
from typing import List, Optional

from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QComboBox, QFileDialog, QFrame, QHBoxLayout, QLabel, QMainWindow,
    QMessageBox, QPushButton, QTabWidget, QTextEdit, QVBoxLayout, QWidget,
)

from models import MQTTPacket, Node
from ui.packet_feed import PacketFeedWidget
from ui.node_list import NodeListWidget
from ui.map_view import MapViewWidget
from ui.message_view import MessageViewWidget
from ui.telemetry_view import TelemetryViewWidget
from ui.source_panel import SourcePanel

# ── Visibility presets (label, hours — None = all known) ─────────────────────
_VISIBILITY_PRESETS = [
    ("Last 1 hour",   1),
    ("Last 6 hours",  6),
    ("Last 12 hours", 12),
    ("Last 24 hours", 24),
    ("Last 48 hours", 48),
    ("Last 72 hours", 72),
    ("Last 7 days",   168),
    ("Last 30 days",  720),
    ("All known",     None),
]
_DEFAULT_VISIBILITY_IDX = 6   # "Last 7 days"


class MainWindow(QMainWindow):
    def __init__(self, app):
        super().__init__()
        self._app = app
        self._packet_times: List[datetime] = []
        self._total_packets: int = 0
        self._start_time = datetime.now()
        self.setWindowTitle("Mesh Command Post")
        self.setMinimumSize(1200, 780)
        self._build_ui()

    def _build_ui(self):
        root_widget = QWidget()
        self.setCentralWidget(root_widget)
        root = QVBoxLayout(root_widget)
        root.setContentsMargins(6, 6, 6, 4)
        root.setSpacing(4)

        # Source manager panel
        self._source_panel = SourcePanel(self._app.source_manager, self._app)
        self._source_panel.configs_changed.connect(
            lambda: self._app.save_sources(self._app.source_manager.all_configs())
        )
        root.addWidget(self._source_panel)

        # Visibility window toolbar
        root.addLayout(self._build_visibility_bar())

        # Tabs
        reg = self._app.registry
        self._tabs = QTabWidget()

        self._feed = PacketFeedWidget(registry=reg, app=self._app)
        self._tabs.addTab(self._feed, "📡 Packets")

        self._nodes = NodeListWidget(app=self._app)
        self._nodes.import_reference_requested.connect(self._app.import_reference)
        self._nodes.watchlist_add_requested.connect(self._on_watchlist_add)
        self._nodes.watchlist_remove_requested.connect(self._on_watchlist_remove)
        self._tabs.addTab(self._nodes, "🗂 Nodes")

        self._map = MapViewWidget()
        self.map_view = self._map   # public alias for app.jump_to_node
        self._tabs.addTab(self._map, "🗺 Map")

        self._msgs = MessageViewWidget(registry=reg, app=self._app)
        self._tabs.addTab(self._msgs, "💬 Messages")

        self._telem = TelemetryViewWidget(registry=reg, app=self._app)
        self._tabs.addTab(self._telem, "📊 Telemetry")

        root.addWidget(self._tabs, stretch=1)

        # Event log
        log_frame = QFrame()
        log_frame.setFrameStyle(QFrame.StyledPanel | QFrame.Sunken)
        log_lay = QVBoxLayout(log_frame)
        log_lay.setContentsMargins(4, 2, 4, 2)
        log_lay.setSpacing(2)

        log_hdr = QHBoxLayout()
        log_hdr.addWidget(QLabel("Event log:"))
        log_hdr.addStretch()

        exp_btn = QPushButton("Export DB JSONL")
        exp_btn.setFixedHeight(20)
        exp_btn.clicked.connect(self._export_db_jsonl)
        log_hdr.addWidget(exp_btn)

        clear_btn = QPushButton("Clear")
        clear_btn.setFixedHeight(20)
        clear_btn.clicked.connect(lambda: self._log.clear())
        log_hdr.addWidget(clear_btn)
        log_lay.addLayout(log_hdr)

        self._log = QTextEdit()
        self._log.setReadOnly(True)
        self._log.setMaximumHeight(80)
        self._log.setFont(QFont("Consolas", 8))
        self._log.document().setMaximumBlockCount(300)  # auto-trims oldest lines
        log_lay.addWidget(self._log)
        root.addWidget(log_frame)

    def _build_visibility_bar(self) -> QHBoxLayout:
        bar = QHBoxLayout()
        bar.setSpacing(8)

        bar.addWidget(QLabel("Visibility window:"))
        self._vis_combo = QComboBox()
        self._vis_combo.setFixedWidth(130)
        for label, hours in _VISIBILITY_PRESETS:
            self._vis_combo.addItem(label, userData=hours)
        self._vis_combo.setCurrentIndex(_DEFAULT_VISIBILITY_IDX)
        self._vis_combo.currentIndexChanged.connect(self._on_visibility_changed)
        bar.addWidget(self._vis_combo)

        bar.addWidget(QLabel("  "))
        self._vis_stats_lbl = QLabel("Known: —  Visible: —  Hidden: —  GPS: —/—")
        self._vis_stats_lbl.setStyleSheet("font-size: 11px; color: #555;")
        bar.addWidget(self._vis_stats_lbl)

        bar.addStretch()
        return bar

    # ── Visibility ────────────────────────────────────────────────────────────

    def set_visibility_hours(self, hours: Optional[int]) -> None:
        """Set the visibility window without triggering config save (for init)."""
        # Find the matching preset index
        for i, (_, h) in enumerate(_VISIBILITY_PRESETS):
            if h == hours:
                self._vis_combo.blockSignals(True)
                self._vis_combo.setCurrentIndex(i)
                self._vis_combo.blockSignals(False)
                break
        self._nodes.set_visibility_hours(hours)
        self._map.set_visibility_hours(hours)
        self._update_vis_stats()

    def _on_visibility_changed(self, idx: int) -> None:
        hours = self._vis_combo.itemData(idx)
        self._nodes.set_visibility_hours(hours)
        self._map.set_visibility_hours(hours)
        self._update_vis_stats()
        self._app.config.set("visibility_hours", hours)

    def _update_vis_stats(self) -> None:
        counts = self._nodes.get_counts()
        self._vis_stats_lbl.setText(
            f"Known: {counts['total']}  "
            f"Visible: {counts['visible']}  "
            f"Hidden: {counts['hidden']}  "
            f"GPS: {counts['gps_visible']}/{counts['gps_total']}"
        )

    # ── Map tab ───────────────────────────────────────────────────────────────

    def switch_to_map_tab(self) -> None:
        """Bring the Map tab to the front."""
        self._tabs.setCurrentWidget(self._map)

    # ── Batch handlers (called by App._flush_ui every 750 ms) ─────────────────

    def on_packets_buffered(self, packets: List[MQTTPacket]) -> None:
        """Buffer new packets into feed/messages/telemetry — no display update yet."""
        self._total_packets += len(packets)
        for p in packets:
            self._feed.add_packet(p)
            self._msgs.add_packet(p)
            self._telem.add_packet(p)
            self._packet_times.append(p.received_at)

    def on_nodes_updated(self, nodes: List[Node]) -> None:
        """Update all tabs for dirty nodes, then flush the node table once."""
        node_ids = {node.node_id for node in nodes}
        for node in nodes:
            self._nodes.upsert_node(node)
            self._map.upsert_node(node)
        self._feed.refresh_nodes(node_ids)    # one table pass
        self._msgs.refresh_nodes(node_ids)    # one table pass
        self._telem.refresh_nodes(node_ids)   # one table pass
        self._nodes.flush_table()
        self._update_vis_stats()

    def flush_display_tick(self) -> None:
        """Final display push for this timer tick — called once after all buffering."""
        self._feed.flush_display()   # incremental insert or full rebuild

        # Prune packet_times to last 60 seconds
        if self._packet_times:
            now = datetime.now()
            cutoff = now.timestamp() - 60
            self._packet_times = [
                t for t in self._packet_times if t.timestamp() > cutoff
            ]

        recent = len(self._packet_times)

        elapsed = datetime.now() - self._start_time
        total_s = int(elapsed.total_seconds())
        h, rem = divmod(total_s, 3600)
        m = rem // 60
        uptime_str = f"{h}h {m:02d}m" if h else f"{m}m"

        counts = self._nodes.get_counts()
        self.statusBar().showMessage(
            f"Uptime: {uptime_str}  |  "
            f"Pkts/min: {recent}  |  "
            f"Packets: {self._total_packets}  |  "
            f"Known: {counts['total']}  |  "
            f"Visible: {counts['visible']}"
        )

    # ── Public handlers ───────────────────────────────────────────────────────

    def on_status(self, text: str) -> None:
        self.statusBar().showMessage(text)

    def on_error(self, msg: str) -> None:
        self.log(f"ERROR: {msg}")

    def log(self, msg: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        self._log.append(f"[{ts}] {msg}")

    def refresh_stats(self, stats: dict) -> None:
        self._nodes.update_stats(stats)

    def load_history(self, packets: List[MQTTPacket], nodes: List[Node]) -> None:
        """Populate all widgets with historical data on startup."""
        for p in reversed(packets):
            self._feed.add_packet(p)
            self._msgs.add_packet(p)
            self._telem.add_packet(p)
        self._feed.flush_display()   # one rebuild after all history loaded
        self._nodes.load_nodes(nodes)
        self._map.load_nodes(nodes)
        self._update_vis_stats()
        self.log(f"Loaded {len(packets)} packets and {len(nodes)} nodes from history.")

    # ── Watchlist wiring ──────────────────────────────────────────────────────

    def _on_watchlist_add(self, node_id: str, label: str) -> None:
        try:
            self._app.storage.add_watchlist(node_id, label)
            self._nodes.set_watchlist_ids(
                {e["node_id"] for e in self._app.storage.get_watchlist()}
            )
            self.log(f"Watchlist: added {label} ({node_id})")
        except Exception as exc:
            QMessageBox.warning(self, "Watchlist Error", str(exc))

    def _on_watchlist_remove(self, node_id: str) -> None:
        try:
            self._app.storage.remove_watchlist(node_id)
            self._nodes.set_watchlist_ids(
                {e["node_id"] for e in self._app.storage.get_watchlist()}
            )
            self.log(f"Watchlist: removed {node_id}")
        except Exception as exc:
            QMessageBox.warning(self, "Watchlist Error", str(exc))

    # ── Internal ──────────────────────────────────────────────────────────────

    def _export_db_jsonl(self) -> None:
        path, _ = QFileDialog.getSaveFileName(
            self, "Export All Packets JSONL", "mesh_history.jsonl",
            "JSONL (*.jsonl);;All Files (*)"
        )
        if not path:
            return
        try:
            self._app.storage.export_jsonl(path)
            QMessageBox.information(self, "Export Complete", f"Saved to:\n{path}")
        except Exception as exc:
            QMessageBox.warning(self, "Export Error", str(exc))

    def closeEvent(self, event):
        self._app.source_manager.disconnect_all()
        self._app.save_sources(self._app.source_manager.all_configs())
        super().closeEvent(event)
