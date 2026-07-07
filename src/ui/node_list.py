import csv
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QAbstractItemView, QButtonGroup, QDialog, QDialogButtonBox,
    QFileDialog, QGroupBox, QHBoxLayout, QHeaderView,
    QInputDialog, QLabel, QLineEdit, QMessageBox, QPushButton,
    QRadioButton, QTableWidget, QTableWidgetItem, QVBoxLayout, QWidget,
)

from models import Node
from storage import export_node_reference, export_node_reference_csv

# 13 columns matching Node.to_display_row()
COLUMNS = [
    "Name", "Node ID", "Status", "Sources",
    "Location", "Dist", "Last Seen", "First Seen",
    "Pkts", "Pos", "Telem", "Msg", "Hardware",
]

_SORT_ROLE = Qt.UserRole + 1   # numeric sort key; Qt.UserRole is reserved for node_id


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


def _sort_val(node: Node, col: int):
    """Return a numeric sort key for columns that need it, else None."""
    if col == 5:   # Dist
        return node.distance_miles if node.distance_miles is not None else -1.0
    if col == 6:   # Last Seen
        return node.last_heard.timestamp() if node.last_heard else 0.0
    if col == 7:   # First Seen
        return node.first_seen.timestamp() if node.first_seen else 0.0
    if col == 8:   return float(node.packet_count)
    if col == 9:   return float(node.position_count)
    if col == 10:  return float(node.telemetry_count)
    if col == 11:  return float(node.message_count)
    return None

_STATUS_COLORS = {
    "Active":         "#1a9a1a",
    "Recent":         "#4a7fcc",
    "Stale":          "#aa7700",
    "Old":            "#888888",
    "Reference Only": "#9944cc",
    "Unknown":        "#999999",
}

def _sources_tip(node) -> str:
    tip = f"Sources: {', '.join(node.sources_seen)}"
    if node.last_map_seen:
        tip += f"\nMap report: {node.last_map_seen.strftime('%Y-%m-%d %H:%M')}"
    return tip


_FILTERS = [
    ("All",       "all"),
    ("MQTT",      "mqtt"),
    ("MAP",       "map"),
    ("Both",      "both"),
    ("Local",     "local"),
    ("Stale/Old", "stale"),
]


class NodeListWidget(QWidget):
    import_reference_requested = Signal(str)
    watchlist_add_requested    = Signal(str, str)   # node_id, label
    watchlist_remove_requested = Signal(str)         # node_id

    # Class-level flag: privacy warning shown once per session
    _privacy_warning_shown: bool = False

    def __init__(self, app=None, parent=None):
        super().__init__(parent)
        self._app = app
        self._nodes: Dict[str, Node] = {}
        self._filter = "all"
        self._watchlist_ids: Set[str] = set()
        self._visibility_hours: Optional[int] = 168   # 7 days default

        # In-place update state
        self._item_by_id: Dict[str, QTableWidgetItem] = {}  # node_id → col-0 item (row() is always current)
        self._needs_rebuild: bool = False
        self._pending_inplace: Set[str] = set()

        self._setup_ui()

    # ── Setup ─────────────────────────────────────────────────────────────────

    def _setup_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(4, 4, 4, 4)
        root.setSpacing(4)
        root.addLayout(self._build_stats_bar())
        root.addLayout(self._build_filter_bar())
        root.addWidget(self._build_table())

    def _build_stats_bar(self) -> QHBoxLayout:
        bar = QHBoxLayout()
        self._stat_labels: Dict[str, QLabel] = {}

        def _stat(key: str, prefix: str, tooltip: str = "") -> None:
            pfx = QLabel(prefix)
            if tooltip:
                pfx.setToolTip(tooltip)
            bar.addWidget(pfx)
            lbl = QLabel("—")
            lbl.setStyleSheet("font-size: 11px; padding: 0 3px;")
            if tooltip:
                lbl.setToolTip(tooltip)
            self._stat_labels[key] = lbl
            bar.addWidget(lbl)

        _stat("total",       "Known:")
        _stat("visible",     " Visible:")
        _stat("hidden",      " Hidden:")
        _stat("gps_visible", " GPS:")
        _stat("mqtt",        " MQTT:")
        _stat("map",         " MAP:",
              "Nodes with a successfully decoded Map Report in the database.\n"
              "May include data from previous sessions.\n"
              "Hover a node's Sources column for the exact timestamp.")
        _stat("local",       " Local:")
        _stat("active",      " Active:")

        bar.addStretch()
        return bar

    def _build_filter_bar(self) -> QHBoxLayout:
        bar = QHBoxLayout()

        self._filter_btns: Dict[str, QPushButton] = {}
        for label, key in _FILTERS:
            btn = QPushButton(label)
            btn.setCheckable(True)
            btn.setFixedHeight(24)
            btn.clicked.connect(lambda checked, k=key: self._set_filter(k))
            self._filter_btns[key] = btn
            bar.addWidget(btn)
        self._filter_btns["all"].setChecked(True)

        bar.addWidget(QLabel("Search:"))
        self._search = QLineEdit()
        self._search.setPlaceholderText("name / id…")
        self._search.setFixedWidth(160)
        self._search.textChanged.connect(self._refresh)
        bar.addWidget(self._search)

        bar.addStretch()
        self._count_lbl = QLabel("0 nodes")
        bar.addWidget(self._count_lbl)

        imp_btn = QPushButton("Import Reference…")
        imp_btn.setFixedHeight(24)
        imp_btn.clicked.connect(self._on_import_clicked)
        bar.addWidget(imp_btn)

        exp_csv = QPushButton("Export CSV")
        exp_csv.setFixedHeight(24)
        exp_csv.clicked.connect(self._export_csv)
        bar.addWidget(exp_csv)

        exp_json = QPushButton("Export JSON")
        exp_json.setFixedHeight(24)
        exp_json.clicked.connect(self._export_json)
        bar.addWidget(exp_json)

        exp_ref = QPushButton("Export Reference…")
        exp_ref.setFixedHeight(24)
        exp_ref.clicked.connect(self._export_reference)
        bar.addWidget(exp_ref)

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
        hdr.setSectionResizeMode(0, QHeaderView.Interactive)
        hdr.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(4, QHeaderView.Stretch)
        hdr.setSectionResizeMode(5, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(6, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(7, QHeaderView.ResizeToContents)
        for i in range(8, 13):
            hdr.setSectionResizeMode(i, QHeaderView.ResizeToContents)
        t.setColumnWidth(0, 180)
        t.setContextMenuPolicy(Qt.CustomContextMenu)
        t.customContextMenuRequested.connect(self._on_context_menu)
        t.cellDoubleClicked.connect(self._on_cell_double_clicked)
        t.setSortingEnabled(True)
        t.horizontalHeader().setSortIndicator(6, Qt.DescendingOrder)  # default: Last Seen desc
        self._table = t
        return t

    # ── Public API ────────────────────────────────────────────────────────────

    def set_visibility_hours(self, hours: Optional[int]) -> None:
        self._visibility_hours = hours
        self._refresh()

    def upsert_node(self, node: Node) -> None:
        """Update in-memory dict; track whether an in-place update or full rebuild is needed."""
        query = self._search.text().lower()
        was_visible = node.node_id in self._item_by_id
        self._nodes[node.node_id] = node
        now_visible = self._passes_filter(node) and self._passes_search(node, query)

        if was_visible and now_visible:
            self._pending_inplace.add(node.node_id)
        elif was_visible != now_visible:
            self._needs_rebuild = True

    def flush_table(self) -> None:
        """Apply pending node updates. Full rebuild only when row count changes."""
        if self._needs_rebuild:
            self._pending_inplace.clear()
            self._refresh()
            return

        if not self._pending_inplace:
            return

        # In-place cell updates; disable sorting so row positions don't shift mid-update
        self._table.setSortingEnabled(False)
        self._table.setUpdatesEnabled(False)
        for node_id in self._pending_inplace:
            node = self._nodes.get(node_id)
            if node is None:
                continue
            item0 = self._item_by_id.get(node_id)
            if item0 is None:
                continue
            row = item0.row()
            for col, val in enumerate(node.to_display_row()):
                item = self._table.item(row, col)
                if item is None:
                    item = _SortItem()
                    self._table.setItem(row, col, item)
                item.setText(val)
                item.setData(Qt.UserRole, node_id)
                sv = _sort_val(node, col)
                if sv is not None:
                    item.setData(_SORT_ROLE, sv)
                if col == 2:
                    item.setToolTip(f"Status: {val}")
                elif col == 3:
                    item.setToolTip(_sources_tip(node))
        self._table.setUpdatesEnabled(True)
        self._table.setSortingEnabled(True)
        self._pending_inplace.clear()

        # Refresh count label and stats bar (cheap — no table iteration)
        self._count_lbl.setText(
            f"{len(self._row_by_id)} shown / {len(self._nodes)} known"
        )
        self._refresh_stats_bar()

    def load_nodes(self, nodes: List[Node]) -> None:
        self._nodes = {n.node_id: n for n in nodes}
        self._item_by_id = {}
        self._needs_rebuild = False
        self._pending_inplace.clear()
        self._refresh()

    def update_stats(self, stats: dict) -> None:
        for key, lbl in self._stat_labels.items():
            lbl.setText(str(stats.get(key, "—")))

    def set_watchlist_ids(self, ids: Set[str]) -> None:
        self._watchlist_ids = ids

    def get_counts(self) -> dict:
        all_nodes = list(self._nodes.values())
        visible    = [n for n in all_nodes if self._is_visible(n)]
        gps_all    = [n for n in all_nodes if n.latitude is not None]
        gps_vis    = [n for n in visible   if n.latitude is not None]
        mqtt_ids   = {n.node_id for n in all_nodes if "mqtt_json" in n.sources_seen}
        # MAP requires a confirmed decoded Map Report (timestamped), not just any non-mqtt source
        map_ids    = {n.node_id for n in all_nodes if n.last_map_seen is not None}
        return {
            "total":       len(all_nodes),
            "visible":     len(visible),
            "hidden":      len(all_nodes) - len(visible),
            "gps_visible": len(gps_vis),
            "gps_total":   len(gps_all),
            "mqtt":        len(mqtt_ids),
            "map":         len(map_ids),
            "local":       sum(1 for n in visible if n.is_local),
            "active":      sum(1 for n in visible if n.status == "Active"),
        }

    # ── Visibility + filter helpers ───────────────────────────────────────────

    def _cutoff(self) -> Optional[datetime]:
        if self._visibility_hours is None:
            return None
        return datetime.now() - timedelta(hours=self._visibility_hours)

    def _is_visible(self, node: Node) -> bool:
        cutoff = self._cutoff()
        if cutoff is None:
            return True
        if node.last_heard is None:
            return False
        return node.last_heard >= cutoff

    def _passes_filter(self, node: Node) -> bool:
        if not self._is_visible(node):
            return False
        f = self._filter
        if f == "all":
            return True
        if f == "mqtt":
            return "mqtt_json" in node.sources_seen
        if f == "map":
            return node.last_map_seen is not None
        if f == "both":
            return "mqtt_json" in node.sources_seen and node.last_map_seen is not None
        if f == "local":
            return node.is_local
        if f == "stale":
            return node.status in ("Stale", "Old", "Reference Only")
        return True

    def _passes_search(self, node: Node, query: str) -> bool:
        if not query:
            return True
        return (
            query in node.node_id.lower()
            or (node.long_name  and query in node.long_name.lower())
            or (node.short_name and query in node.short_name.lower())
            or (node.location_name and query in node.location_name.lower())
        )

    # ── Internal ──────────────────────────────────────────────────────────────

    def _set_filter(self, key: str) -> None:
        self._filter = key
        for k, btn in self._filter_btns.items():
            btn.setChecked(k == key)
        self._refresh()

    def _refresh(self) -> None:
        query = self._search.text().lower()
        nodes = [
            n for n in self._nodes.values()
            if self._passes_filter(n) and self._passes_search(n, query)
        ]

        self._table.setSortingEnabled(False)
        self._table.setUpdatesEnabled(False)
        self._table.setRowCount(len(nodes))
        self._item_by_id.clear()
        for row, node in enumerate(nodes):
            for col, val in enumerate(node.to_display_row()):
                item = _SortItem(val)
                item.setData(Qt.UserRole, node.node_id)
                sv = _sort_val(node, col)
                if sv is not None:
                    item.setData(_SORT_ROLE, sv)
                if col == 2:
                    item.setToolTip(f"Status: {val}")
                elif col == 3:
                    item.setToolTip(_sources_tip(node))
                self._table.setItem(row, col, item)
                if col == 0:
                    self._item_by_id[node.node_id] = item
        self._table.setUpdatesEnabled(True)
        self._table.setSortingEnabled(True)

        self._needs_rebuild = False
        self._pending_inplace.clear()

        self._count_lbl.setText(f"{len(nodes)} shown / {len(self._nodes)} known")
        self._refresh_stats_bar()

    def _refresh_stats_bar(self) -> None:
        counts = self.get_counts()
        for key, lbl in self._stat_labels.items():
            val = counts.get(key, "—")
            if key == "gps_visible":
                lbl.setText(f"{val}/{counts.get('gps_total', 0)}")
            else:
                lbl.setText(str(val))

    # ── Context menu ──────────────────────────────────────────────────────────

    def _on_context_menu(self, pos) -> None:
        from PySide6.QtWidgets import QMenu
        item = self._table.itemAt(pos)
        if item is None:
            return
        node_id = item.data(Qt.UserRole)
        if not node_id:
            return

        menu = QMenu(self)
        if node_id in self._watchlist_ids:
            act = menu.addAction(f"Remove {node_id} from Watchlist")
            act.triggered.connect(lambda: self.watchlist_remove_requested.emit(node_id))
        else:
            act = menu.addAction(f"Add {node_id} to Watchlist")
            act.triggered.connect(lambda: self._add_to_watchlist(node_id))
        menu.exec(self._table.viewport().mapToGlobal(pos))

    def _add_to_watchlist(self, node_id: str) -> None:
        node = self._nodes.get(node_id)
        default_label = node.display_name() if node else node_id
        label, ok = QInputDialog.getText(
            self, "Add to Watchlist", "Label:", text=default_label
        )
        if ok:
            self.watchlist_add_requested.emit(node_id, label)

    def _on_cell_double_clicked(self, row: int, col: int) -> None:
        """Double-click on any cell — jump to the node on the Map tab."""
        if self._app is None:
            return
        item = self._table.item(row, 0)
        if item is None:
            return
        node_id = item.data(Qt.UserRole)
        if node_id:
            self._app.jump_to_node(node_id)

    def _on_import_clicked(self) -> None:
        path, _ = QFileDialog.getOpenFileName(
            self, "Import Reference Nodes",
            "", "Node files (*.json *.jsonl *.csv);;All Files (*)"
        )
        if path:
            self.import_reference_requested.emit(path)

    def _export_reference(self) -> None:
        """Show filter/format dialog, optional privacy warning, then save file."""
        # ── Filter + format dialog ────────────────────────────────────────────
        dlg = QDialog(self)
        dlg.setWindowTitle("Export Node Reference")
        dlg_lay = QVBoxLayout(dlg)

        filter_group = QGroupBox("Filter")
        fg_lay = QVBoxLayout(filter_group)
        filter_bg = QButtonGroup(dlg)
        rb_all     = QRadioButton("All known nodes")
        rb_visible = QRadioButton("Visible nodes only (current visibility window)")
        rb_active  = QRadioButton("Active/recent nodes only")
        rb_gps     = QRadioButton("Nodes with GPS only")
        rb_all.setChecked(True)
        for rb in (rb_all, rb_visible, rb_active, rb_gps):
            filter_bg.addButton(rb)
            fg_lay.addWidget(rb)
        dlg_lay.addWidget(filter_group)

        fmt_group = QGroupBox("Format")
        fg2_lay = QVBoxLayout(fmt_group)
        fmt_bg = QButtonGroup(dlg)
        rb_json = QRadioButton("JSON")
        rb_csv  = QRadioButton("CSV")
        rb_json.setChecked(True)
        for rb in (rb_json, rb_csv):
            fmt_bg.addButton(rb)
            fg2_lay.addWidget(rb)
        dlg_lay.addWidget(fmt_group)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(dlg.accept)
        buttons.rejected.connect(dlg.reject)
        dlg_lay.addWidget(buttons)

        if dlg.exec() != QDialog.Accepted:
            return

        # ── Privacy warning (once per session) ────────────────────────────────
        if not NodeListWidget._privacy_warning_shown:
            ret = QMessageBox.warning(
                self,
                "Privacy Notice",
                "This export may contain node names, approximate positions, "
                "last-seen timestamps, and other public MQTT-derived metadata. "
                "Share responsibly.\n\nContinue with export?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if ret != QMessageBox.Yes:
                return
            NodeListWidget._privacy_warning_shown = True

        # ── Build filtered node list ──────────────────────────────────────────
        if rb_all.isChecked():
            nodes = list(self._nodes.values())
            filter_label = "all_known"
        elif rb_visible.isChecked():
            nodes = [n for n in self._nodes.values() if self._is_visible(n)]
            filter_label = "visible_window"
        elif rb_active.isChecked():
            nodes = [n for n in self._nodes.values()
                     if n.status in ("Active", "Recent")]
            filter_label = "active_recent"
        else:
            nodes = [n for n in self._nodes.values() if n.latitude is not None]
            filter_label = "gps_only"

        use_json = rb_json.isChecked()

        # ── Save file dialog ──────────────────────────────────────────────────
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        if use_json:
            default_name = f"mesh_node_reference_{ts}.json"
            file_filter  = "JSON (*.json);;All Files (*)"
        else:
            default_name = f"mesh_node_reference_{ts}.csv"
            file_filter  = "CSV (*.csv);;All Files (*)"

        path, _ = QFileDialog.getSaveFileName(
            self, "Export Node Reference", default_name, file_filter
        )
        if not path:
            return

        # ── Write ─────────────────────────────────────────────────────────────
        try:
            if use_json:
                content = export_node_reference(nodes, filter_label)
                with open(path, "w", encoding="utf-8") as f:
                    f.write(content)
            else:
                content = export_node_reference_csv(nodes)
                with open(path, "w", newline="", encoding="utf-8") as f:
                    f.write(content)
            QMessageBox.information(
                self, "Export Complete",
                f"Exported {len(nodes)} nodes to:\n{path}"
            )
        except Exception as exc:
            QMessageBox.warning(self, "Export Error", str(exc))

    def _export_csv(self) -> None:
        path, _ = QFileDialog.getSaveFileName(
            self, "Export Nodes CSV", "nodes.csv", "CSV (*.csv)"
        )
        if not path:
            return
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(COLUMNS)
                for n in self._nodes.values():
                    w.writerow(n.to_display_row())
            QMessageBox.information(self, "Export Complete", f"Saved to:\n{path}")
        except Exception as exc:
            QMessageBox.warning(self, "Export Error", str(exc))

    def _export_json(self) -> None:
        path, _ = QFileDialog.getSaveFileName(
            self, "Export Nodes JSON", "nodes.json", "JSON (*.json)"
        )
        if not path:
            return
        out = []
        for n in self._nodes.values():
            out.append({
                "node_id": n.node_id,
                "display_name": n.display_name(),
                "long_name": n.long_name,
                "short_name": n.short_name,
                "hardware": n.hardware,
                "status": n.status,
                "source_label": n.source_label(),
                "sources": n.sources_seen,
                "location_name": n.location_name,
                "latitude": n.latitude,
                "longitude": n.longitude,
                "distance_miles": n.distance_miles,
                "is_local": n.is_local,
                "first_seen": n.first_seen.isoformat() if n.first_seen else None,
                "last_seen": n.last_heard.isoformat() if n.last_heard else None,
                "last_mqtt_seen": n.last_mqtt_seen.isoformat() if n.last_mqtt_seen else None,
                "last_map_seen": n.last_map_seen.isoformat() if n.last_map_seen else None,
                "last_position_seen": n.last_position_seen.isoformat() if n.last_position_seen else None,
                "packet_count": n.packet_count,
                "position_count": n.position_count,
                "telemetry_count": n.telemetry_count,
                "message_count": n.message_count,
            })
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(out, f, indent=2)
            QMessageBox.information(self, "Export Complete", f"Saved to:\n{path}")
        except Exception as exc:
            QMessageBox.warning(self, "Export Error", str(exc))
