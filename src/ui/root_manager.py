"""Root Topic Manager — two-pane model.

Left pane:  Available Discovered Roots  (known but NOT currently subscribed)
Right pane: Active Roots / Current Subscriptions  (live now)

Terminology: MQTT roots / root topics.  Meshtastic devices are nodes — not roots.
"""
from __future__ import annotations

import json
from typing import List, Optional, Set

from PySide6.QtCore import Qt, QTimer, Slot
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QComboBox, QDialog, QGroupBox, QHBoxLayout,
    QHeaderView, QInputDialog, QLabel, QLineEdit, QMessageBox,
    QPushButton, QSplitter, QTableWidget, QTableWidgetItem, QVBoxLayout,
    QWidget,
)

from root_classifier import activity_label
from source_manager import SOURCE_MQTT_JSON
from subscription_registry import ROOT_DERIVED, DIRECT, MAP_TYPE

# ── Left pane column indices (Available roots) ────────────────────────────────
_L_SEL      = 0
_L_ROOT     = 1
_L_REGION   = 2
_L_STATE    = 3
_L_TYPE     = 4
_L_ACTIVITY = 5
_L_TOTAL    = 6
_L_RECENT   = 7
_L_PPM      = 8
_L_CHANNELS = 9
_L_FIRST    = 10
_L_LAST     = 11
_L_AC       = 12   # auto-connect on startup
_L_NOTES    = 13

_L_HEADERS = [
    "☐", "Root Topic", "Region", "State", "Type", "Activity",
    "Total Pkts", "Recent", "P/m", "Channels",
    "First Seen", "Last Seen", "Auto-Conn", "Notes",
]

# ── Right pane column indices ─────────────────────────────────────────────────
_R_ROOT   = 0   # root topic or topic filter label
_R_STATE  = 1   # combined state text (Active | Staged | Auto-connect | Staged+AC | …)
_R_ACTIVE = 2   # ● if currently live in sub_registry
_R_STAGED = 3   # ● if staged=1 in DB
_R_AC     = 4   # ● if auto_connect=1 in DB
_R_SUBS   = 5   # MQTT subscription filter(s) (blank if not active)
_R_PKTS   = 6   # packets received this session (0 if not active)
_R_LAST   = 7   # last packet time (blank if not active)

_R_HEADERS = [
    "Root / Topic", "State", "Active", "Staged", "Auto-Conn",
    "MQTT Subscription(s)", "Pkts", "Last Pkt",
]


class _NumericItem(QTableWidgetItem):
    def __lt__(self, other: QTableWidgetItem) -> bool:
        try:
            return float(self.text().replace(",", "")) < float(
                other.text().replace(",", "")
            )
        except ValueError:
            return super().__lt__(other)


def _short_dt(val: Optional[str]) -> str:
    if not val:
        return ""
    s = str(val)
    # ISO datetime — show date + hour:min
    if "T" in s:
        date, rest = s.split("T", 1)
        return f"{date} {rest[:5]}"
    return s[:16]


def _combined_state(active: bool, staged: bool, ac: bool) -> str:
    if active and ac:    return "Active + AC"
    if active:           return "Active"
    if staged and ac:    return "Staged + AC"
    if staged:           return "Staged"
    if ac:               return "Auto-connect"
    return "—"


def _state_fg(active: bool, staged: bool, ac: bool) -> str:
    if active:  return "#1a7a1a"   # green
    if staged:  return "#8a5a00"   # amber
    if ac:      return "#1a4a8a"   # blue
    return "#555555"


class RootManagerDialog(QDialog):
    """Two-pane Root Topic Manager.

    Left  — Available: discovered roots NOT currently subscribed.
    Right — Active:    roots/topics with live MQTT subscriptions right now.
    """

    def __init__(self, app, parent=None):
        super().__init__(parent)
        self._app = app
        self.setWindowTitle("Root Topic Manager")
        self.setMinimumSize(1200, 580)
        self._build_ui()
        self._refresh()
        # Refresh every 3 s so the Active pane stays current
        self._timer = QTimer(self)
        self._timer.setInterval(3000)
        self._timer.timeout.connect(self._refresh)
        self._timer.start()
        # Connect to discovery signals
        try:
            app.discovery_result.connect(self._on_discovery_result)
            app.discovery_started.connect(self._on_disc_started)
            app.discovery_tick.connect(self._on_disc_tick)
            app.discovery_finished.connect(self._on_disc_finished)
            app.discovery_stopped.connect(self._on_disc_stopped)
            self._disc_connected = True
        except Exception:
            self._disc_connected = False

        # Connect test_mode_changed to update the Rollback button enable state
        try:
            app.test_mode_changed.connect(self._on_test_mode_changed)
            self._test_connected = True
        except Exception:
            self._test_connected = False
        # Sync button/label if discovery is already running when dialog opens
        if getattr(app, "disc_running", False):
            import time as _time
            started_at = getattr(app, "disc_started_at", None)
            dur        = getattr(app, "disc_duration_sec", 60)
            if started_at is not None:
                remaining = max(0, dur - int(_time.monotonic() - started_at))
                self._disc_lbl.setText(f"{remaining}s…")
            else:
                self._disc_lbl.setText(f"{dur}s…")
            self._disc_btn.setText("Stop Discovery")

    def closeEvent(self, event) -> None:
        self._timer.stop()
        if self._disc_connected:
            try:
                self._app.discovery_result.disconnect(self._on_discovery_result)
                self._app.discovery_started.disconnect(self._on_disc_started)
                self._app.discovery_tick.disconnect(self._on_disc_tick)
                self._app.discovery_finished.disconnect(self._on_disc_finished)
                self._app.discovery_stopped.disconnect(self._on_disc_stopped)
            except Exception:
                pass
        if getattr(self, "_test_connected", False):
            try:
                self._app.test_mode_changed.disconnect(self._on_test_mode_changed)
            except Exception:
                pass
        self._app.window.log("Root Manager closed")
        super().closeEvent(event)

    # ── UI construction ───────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setSpacing(4)

        # ── Safe Baseline Mode banner ─────────────────────────────────────────
        if getattr(self._app, "safe_mode", False):
            banner = QLabel(
                "Safe Baseline Mode is active.  Discovery and root management are fully available, "
                "but root changes are staged only — they will not affect live MQTT subscriptions "
                "until Normal Mode is enabled.\n"
                "Use Stage → to select roots for future production.  "
                "Use ← Unstage to remove them from staging."
            )
            banner.setWordWrap(True)
            banner.setStyleSheet(
                "font-weight: bold; font-size: 9pt; color: #6b3900;"
                " background: #fff3cc; padding: 6px 10px;"
                " border: 1px solid #d4a017; border-radius: 3px;"
            )
            root.addWidget(banner)

        # ── top filter bar (applies to left pane) ────────────────────────────
        root.addLayout(self._build_filter_bar())

        # ── two-pane splitter ─────────────────────────────────────────────────
        splitter = QSplitter(Qt.Horizontal)

        # Left pane
        left_box = QGroupBox("Available Discovered Roots  (not staged, auto-connect, or active)")
        left_lay = QVBoxLayout(left_box)
        left_lay.setContentsMargins(4, 4, 4, 4)
        self._left = self._make_table(_L_HEADERS)
        left_lay.addWidget(self._left)
        self._left_count = QLabel("")
        self._left_count.setStyleSheet("font-size: 9pt; color: #555;")
        left_lay.addWidget(self._left_count)
        splitter.addWidget(left_box)

        # Middle button column — labels differ by mode but all always enabled
        btn_col = QWidget()
        btn_lay = QVBoxLayout(btn_col)
        btn_lay.setAlignment(Qt.AlignVCenter)
        btn_lay.setSpacing(6)
        _safe = getattr(self._app, "safe_mode", False)

        if _safe:
            _btn_defs = [
                ("Stage →",           "Mark checked available roots as Staged (ready for future production)",
                 self._act_add),
                ("← Unstage",         "Remove selected staged/auto-connect roots from the right pane",
                 self._act_remove),
                ("Test Subscribe →",  "Subscribe the selected staged/auto-connect root on the live\n"
                                      "production connection for a controlled test.\n"
                                      "Adds only {root}/2/json/# — no other topics.",
                 self._act_test_subscribe),
                ("Stage Top 10 →",    "Stage the 10 busiest available roots",
                 lambda: self._act_add_top(10)),
                ("Stage Top 20 →",    "Stage the 20 busiest available roots",
                 lambda: self._act_add_top(20)),
                ("Stage Top 50 →",    "Stage the 50 busiest available roots",
                 lambda: self._act_add_top(50)),
                ("Clear Staged",      "Remove all staged roots from the right pane",
                 self._act_remove_all),
            ]
        else:
            _btn_defs = [
                ("Add →",        "Subscribe selected available roots to production",
                 self._act_add),
                ("← Remove",     "Unsubscribe selected active roots",
                 self._act_remove),
                ("Add Top 10 →", "Subscribe the 10 busiest available roots",
                 lambda: self._act_add_top(10)),
                ("Add Top 20 →", "Subscribe the 20 busiest available roots",
                 lambda: self._act_add_top(20)),
                ("Add Top 50 →", "Subscribe the 50 busiest available roots",
                 lambda: self._act_add_top(50)),
                ("Remove All",   "Unsubscribe all active roots",
                 self._act_remove_all),
            ]

        for label, tip, slot in _btn_defs:
            b = QPushButton(label)
            b.setFixedWidth(115)
            b.setToolTip(tip)
            b.clicked.connect(slot)
            btn_lay.addWidget(b)

        splitter.addWidget(btn_col)

        # Right pane
        right_box = QGroupBox("Active Subscriptions & Staged / Auto-Connect Roots")
        right_lay = QVBoxLayout(right_box)
        right_lay.setContentsMargins(4, 4, 4, 4)
        self._right = self._make_right_table()
        right_lay.addWidget(self._right)
        self._right_count = QLabel("")
        self._right_count.setStyleSheet("font-size: 9pt; color: #555;")
        right_lay.addWidget(self._right_count)
        splitter.addWidget(right_box)

        splitter.setSizes([600, 120, 480])
        root.addWidget(splitter, stretch=1)

        # ── bottom controls ───────────────────────────────────────────────────
        root.addLayout(self._build_bottom_bar())

    def _build_filter_bar(self) -> QHBoxLayout:
        bar = QHBoxLayout()
        bar.setSpacing(6)
        bar.addWidget(QLabel("Filter:"))
        self._filter_text = QLineEdit()
        self._filter_text.setPlaceholderText("root topic…")
        self._filter_text.setFixedWidth(180)
        self._filter_text.textChanged.connect(self._apply_filter)
        bar.addWidget(self._filter_text)

        for label, attr, items in [
            ("Region:", "_filter_region",
             ["All", "South", "Northeast", "Midwest", "West", "—"]),
            ("State:",  "_filter_state",
             ["All"] + sorted(["AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
                                "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
                                "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
                                "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
                                "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"])),
            ("Type:",   "_filter_type",
             ["All", "national", "state", "custom", "regional", "unknown"]),
            ("Activity:", "_filter_activity",
             ["All", "Active recently", "quiet", "low", "medium", "high", "firehose",
              "Top 10", "Top 20", "Top 50"]),
        ]:
            bar.addWidget(QLabel(label))
            cb = QComboBox()
            cb.addItems(items)
            cb.currentIndexChanged.connect(self._apply_filter)
            setattr(self, attr, cb)
            bar.addWidget(cb)

        clr = QPushButton("Clear")
        clr.setFixedWidth(50)
        clr.clicked.connect(self._clear_filter)
        bar.addWidget(clr)
        self._filter_lbl = QLabel("")
        self._filter_lbl.setStyleSheet("font-size: 9pt; color: #555;")
        bar.addWidget(self._filter_lbl)
        bar.addStretch()
        return bar

    def _build_bottom_bar(self) -> QHBoxLayout:
        bar = QHBoxLayout()
        bar.setSpacing(6)

        for label, tip, slot in [
            ("Set Auto-Connect",   "Mark selected active roots as auto-connect on startup",
             self._act_set_ac),
            ("Clear Auto-Connect", "Clear auto-connect on selected active roots",
             self._act_clear_ac),
            ("Save Active as Defaults",
             "Set currently active roots as the startup defaults (auto-connect=1 for each)",
             self._act_save_defaults),
            ("Clear All Defaults",
             "Remove auto-connect flag from all roots",
             self._act_clear_defaults),
            ("Add Root…",          "Manually add a root topic",   self._act_add_manual),
            ("Forget Selected",    "Delete selected available roots from the database",
             self._act_forget),
        ]:
            b = QPushButton(label)
            b.setFixedHeight(24)
            b.setToolTip(tip)
            b.clicked.connect(slot)
            bar.addWidget(b)

        # Rollback button — visible in Safe Mode; enabled only when test roots are active
        if getattr(self._app, "safe_mode", False):
            self._rollback_btn = QPushButton("Rollback to Safe Baseline")
            self._rollback_btn.setFixedHeight(24)
            self._rollback_btn.setEnabled(bool(getattr(self._app, "_test_roots", [])))
            self._rollback_btn.setToolTip(
                "Remove all test root subscriptions and return to exactly:\n"
                "  msh/US/2/json/#\n"
                "  msh/US/2/map/#"
            )
            self._rollback_btn.setStyleSheet(
                "QPushButton { color: #7a1a00; font-weight: bold; }"
                "QPushButton:enabled { background: #fff0e8; border: 1px solid #c0400a; }"
            )
            self._rollback_btn.clicked.connect(self._act_rollback_safe)
            bar.addWidget(self._rollback_btn)
        else:
            self._rollback_btn = None

        bar.addStretch()

        self._disc_btn = QPushButton("Discover Now")
        self._disc_btn.setFixedHeight(24)
        _safe_disc = getattr(self._app, "safe_mode", False)
        if _safe_disc:
            self._disc_btn.setToolTip(
                "Run a discovery scan — uses a separate MQTT connection.\n"
                "Safe Baseline Mode: results are saved to DB as Discovered; they won't\n"
                "affect production until you stage them and enable Normal Mode."
            )
        else:
            self._disc_btn.setToolTip(
                "Run a discovery scan — uses a separate MQTT connection; "
                "does not affect production subscriptions"
            )
        self._disc_btn.clicked.connect(self._act_discover)
        bar.addWidget(self._disc_btn)

        self._disc_lbl = QLabel("")
        self._disc_lbl.setStyleSheet("font-size: 9pt; color: #555;")
        bar.addWidget(self._disc_lbl)

        close_btn = QPushButton("Close")
        close_btn.setFixedHeight(24)
        close_btn.clicked.connect(self.accept)
        bar.addWidget(close_btn)
        return bar

    def _make_table(self, headers: List[str]) -> QTableWidget:
        t = QTableWidget()
        t.setColumnCount(len(headers))
        t.setHorizontalHeaderLabels(headers)
        t.verticalHeader().setVisible(False)
        t.verticalHeader().setDefaultSectionSize(20)
        t.setEditTriggers(QTableWidget.NoEditTriggers)
        t.setSelectionBehavior(QTableWidget.SelectRows)
        t.setSortingEnabled(True)
        t.setAlternatingRowColors(True)
        hdr = t.horizontalHeader()
        hdr.setSectionResizeMode(QHeaderView.Interactive)
        return t

    def _make_right_table(self) -> QTableWidget:
        t = self._make_table(_R_HEADERS)
        for col, w in [
            (_R_ROOT,   190), (_R_STATE, 110), (_R_ACTIVE, 48),
            (_R_STAGED,  52), (_R_AC,     70), (_R_SUBS,  200),
            (_R_PKTS,    50), (_R_LAST,   80),
        ]:
            t.setColumnWidth(col, w)
        return t

    # ── data refresh ──────────────────────────────────────────────────────────

    def _refresh(self) -> None:
        active_roots = self._app.sub_registry.get_active_roots()
        self._repopulate_left(active_roots)
        self._repopulate_right(active_roots)

    def _repopulate_left(self, active_roots: Set[str]) -> None:
        """Populate the Available pane — roots known in DB but not active, staged, or auto-connect."""
        all_rows = self._app.storage.get_all_mqtt_roots()
        # Exclude roots that are active, staged, or marked auto-connect (those belong in right pane)
        rows = [
            r for r in all_rows
            if r["root_topic"] not in active_roots
            and not r.get("staged")
            and not r.get("auto_connect")
        ]
        rows = self._filter_rows(rows)

        self._left.setSortingEnabled(False)
        self._left.setRowCount(len(rows))
        for trow, r in enumerate(rows):
            ppm    = float(r.get("packets_per_minute") or 0)
            act    = activity_label(ppm)
            total  = int(r.get("packet_count_total") or 0)
            recent = int(r.get("packet_count_recent") or 0)
            ac     = bool(r.get("auto_connect"))

            try:
                channels = json.loads(r.get("channels_seen") or "[]")
            except (ValueError, TypeError):
                channels = []

            def _item(txt, align=Qt.AlignLeft, num=False):
                it = _NumericItem(str(txt)) if num else QTableWidgetItem(str(txt))
                it.setTextAlignment(align | Qt.AlignVCenter)
                it.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                return it

            chk = QTableWidgetItem()
            chk.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsUserCheckable)
            chk.setCheckState(Qt.Unchecked)
            chk.setData(Qt.UserRole, r["root_topic"])
            self._left.setItem(trow, _L_SEL, chk)
            self._left.setItem(trow, _L_ROOT,     _item(r.get("root_topic") or ""))
            self._left.setItem(trow, _L_REGION,   _item(r.get("region") or ""))
            self._left.setItem(trow, _L_STATE,    _item(r.get("state_code") or ""))
            self._left.setItem(trow, _L_TYPE,     _item(r.get("root_type") or "?"))
            self._left.setItem(trow, _L_ACTIVITY, _item(act, Qt.AlignHCenter))
            self._left.setItem(trow, _L_TOTAL,
                _item(f"{total:,}" if total else "", Qt.AlignRight, num=True))
            self._left.setItem(trow, _L_RECENT,
                _item(f"{recent:,}" if recent else "", Qt.AlignRight, num=True))
            self._left.setItem(trow, _L_PPM,
                _item(f"{ppm:.1f}" if ppm else "", Qt.AlignRight, num=True))
            self._left.setItem(trow, _L_CHANNELS,
                _item(", ".join(sorted(channels)[:4])))
            self._left.setItem(trow, _L_FIRST,
                _item(_short_dt(r.get("first_seen"))))
            self._left.setItem(trow, _L_LAST,
                _item(_short_dt(r.get("last_seen"))))
            self._left.setItem(trow, _L_AC,
                _item("●" if ac else "○", Qt.AlignHCenter))
            self._left.setItem(trow, _L_NOTES,
                _item(r.get("notes") or ""))
            self._left.setRowHeight(trow, 20)

        self._left.setSortingEnabled(True)
        total_avail = sum(
            1 for r in all_rows
            if r["root_topic"] not in active_roots
            and not r.get("staged") and not r.get("auto_connect")
        )
        self._left_count.setText(
            f"{len(rows)} of {total_avail} available roots shown"
        )

    def _repopulate_right(self, active_roots: Set[str]) -> None:
        """Populate the right pane with all active, staged, and auto-connect entries."""
        subs      = self._app.sub_registry.get_all()
        src_stats = self._app.source_manager.get_stats(SOURCE_MQTT_JSON)
        connected = src_stats.get("connected", False)
        safe      = getattr(self._app, "safe_mode", False)

        db_by_root = {r["root_topic"]: r
                      for r in self._app.storage.get_all_mqtt_roots()}

        right_rows: List[dict] = []
        seen_labels: Set[str]  = set()

        # 1. Active subscriptions from sub_registry (the live production feeds)
        seen_roots: Set[str] = set()
        for s in subs:
            if s.sub_type == ROOT_DERIVED and s.parent_root:
                if s.parent_root not in seen_roots:
                    seen_roots.add(s.parent_root)
                    label  = s.parent_root
                    db_row = db_by_root.get(label, {})
                    seen_labels.add(label)
                    right_rows.append({
                        "label":    label,
                        "active":   True,
                        "staged":   bool(db_row.get("staged")),
                        "ac":       bool(db_row.get("auto_connect")),
                        "sub_type": ROOT_DERIVED,
                        "subs":     [x.topic_filter for x in subs
                                     if x.parent_root == s.parent_root],
                        "pkts":     sum(x.packet_count for x in subs
                                        if x.parent_root == s.parent_root),
                        "last":     max(
                            (x.last_packet for x in subs
                             if x.parent_root == s.parent_root
                             and x.last_packet is not None),
                            default=None,
                        ),
                    })
            elif s.sub_type in (DIRECT, MAP_TYPE, "raw"):
                label = s.topic_filter
                seen_labels.add(label)
                right_rows.append({
                    "label":    label,
                    "active":   True,
                    "staged":   False,
                    "ac":       False,
                    "sub_type": s.sub_type,
                    "subs":     [s.topic_filter],
                    "pkts":     s.packet_count,
                    "last":     s.last_packet,
                })

        # 2. Roots that are staged or auto-connect but not already listed as active
        for r in db_by_root.values():
            label     = r["root_topic"]
            is_staged = bool(r.get("staged"))
            is_ac     = bool(r.get("auto_connect"))
            if label in seen_labels:
                continue
            if is_staged or is_ac:
                seen_labels.add(label)
                right_rows.append({
                    "label":    label,
                    "active":   False,
                    "staged":   is_staged,
                    "ac":       is_ac,
                    "sub_type": "root",
                    "subs":     [],
                    "pkts":     0,
                    "last":     None,
                })

        # Sort: active first, then staged, then auto-connect, then alpha
        def _sort_key(row: dict) -> tuple:
            if row["active"]: return (0, row["label"])
            if row["staged"]: return (1, row["label"])
            return (2, row["label"])
        right_rows.sort(key=_sort_key)

        # Preserve current selection across rebuild
        prev_selected: Optional[str] = None
        for item in self._right.selectedItems():
            data = item.data(Qt.UserRole)
            if isinstance(data, dict):
                prev_selected = data.get("label")
                break

        self._right.setSortingEnabled(False)
        self._right.setRowCount(len(right_rows))

        for trow, r in enumerate(right_rows):
            active  = r["active"]
            staged  = r["staged"]
            ac      = r["ac"]
            label   = r["label"]
            st_text = _combined_state(active, staged, ac)
            st_fg   = _state_fg(active, staged, ac)

            # Tooltip on the State cell
            tip_parts: List[str] = []
            if active:
                tip_parts.append("Live: currently in production subscriptions.")
            else:
                tip_parts.append("Not currently connected.")
                if safe:
                    tip_parts.append(
                        "Safe Mode active: this root is not connected even if "
                        "staged or auto-connect."
                    )
            if staged:
                tip_parts.append("Staged: selected for future production use.")
            if ac:
                tip_parts.append(
                    "Auto-connect: will connect automatically when Normal Mode is enabled."
                )
            state_tip = "\n".join(tip_parts)

            row_data = {
                "label":    label,
                "active":   active,
                "staged":   staged,
                "ac":       ac,
                "sub_type": r["sub_type"],
            }

            def _item(txt, align=Qt.AlignLeft, _data=row_data):
                it = QTableWidgetItem(str(txt))
                it.setTextAlignment(align | Qt.AlignVCenter)
                it.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                it.setData(Qt.UserRole, _data)
                return it

            state_item = _item(st_text, Qt.AlignHCenter)
            state_item.setForeground(QColor(st_fg))
            state_item.setToolTip(state_tip)

            self._right.setItem(trow, _R_ROOT,   _item(label))
            self._right.setItem(trow, _R_STATE,  state_item)
            self._right.setItem(trow, _R_ACTIVE,
                _item("●" if active else "—", Qt.AlignHCenter))
            self._right.setItem(trow, _R_STAGED,
                _item("●" if staged else "—", Qt.AlignHCenter))
            self._right.setItem(trow, _R_AC,
                _item("●" if ac else "—", Qt.AlignHCenter))
            self._right.setItem(trow, _R_SUBS,
                _item("  |  ".join(r["subs"]) if r["subs"] else ""))
            self._right.setItem(trow, _R_PKTS,
                _item(str(r["pkts"]) if r["pkts"] else "", Qt.AlignRight))
            last_str = r["last"].strftime("%H:%M:%S") if r["last"] else ""
            self._right.setItem(trow, _R_LAST, _item(last_str))
            self._right.setRowHeight(trow, 20)

        self._right.setSortingEnabled(True)

        # Restore the previous selection if the row label is still present
        if prev_selected:
            for row in range(self._right.rowCount()):
                it = self._right.item(row, _R_ROOT)
                if it:
                    data = it.data(Qt.UserRole)
                    if isinstance(data, dict) and data.get("label") == prev_selected:
                        self._right.selectRow(row)
                        break

        n_active = sum(1 for r in right_rows if r["active"])
        n_staged = sum(1 for r in right_rows if r["staged"] and not r["active"])
        n_ac     = sum(1 for r in right_rows if r["ac"] and not r["staged"] and not r["active"])
        status   = "connected" if connected else "disconnected"
        parts: List[str] = []
        if n_active: parts.append(f"{n_active} active ({status})")
        if n_staged: parts.append(f"{n_staged} staged")
        if n_ac:     parts.append(f"{n_ac} auto-connect")
        self._right_count.setText(
            (", ".join(parts) or "none") + "  (click row to select)"
        )

    # ── filter ────────────────────────────────────────────────────────────────

    def _filter_rows(self, rows: list) -> list:
        from datetime import datetime as _dt, timedelta as _td
        from root_classifier import activity_label as _act_lbl
        text     = self._filter_text.text().strip().lower()
        region   = self._filter_region.currentText()
        state    = self._filter_state.currentText()
        rtype    = self._filter_type.currentText()
        activity = self._filter_activity.currentText()
        now      = _dt.now()

        # Apply text/region/state/type filters first (always)
        out = []
        for r in rows:
            if text and text not in (r.get("root_topic") or "").lower():
                continue
            if region != "All" and (r.get("region") or "") != region:
                continue
            if state != "All" and (r.get("state_code") or "") != state:
                continue
            if rtype != "All" and (r.get("root_type") or "") != rtype:
                continue
            out.append(r)

        # Top N: sort by p/m desc, cap at N
        if activity.startswith("Top "):
            try:
                n = int(activity.split()[1])
            except (IndexError, ValueError):
                n = 10
            out.sort(key=lambda r: float(r.get("packets_per_minute") or 0), reverse=True)
            out = out[:n]
            self._filter_lbl.setText(f"{len(out)} shown (top {n})")
            return out

        # Activity label filters
        if activity != "All":
            filtered = []
            for r in out:
                ppm = float(r.get("packets_per_minute") or 0)
                if activity == "Active recently":
                    ls = r.get("last_seen") or ""
                    try:
                        last = _dt.fromisoformat(ls[:19])
                        if (now - last) > _td(minutes=30):
                            continue
                    except (ValueError, TypeError):
                        continue
                else:
                    if _act_lbl(ppm) != activity:
                        continue
                filtered.append(r)
            out = filtered

        self._filter_lbl.setText(f"{len(out)} shown")
        return out

    def _apply_filter(self) -> None:
        active = self._app.sub_registry.get_active_roots()
        self._repopulate_left(active)

    def _clear_filter(self) -> None:
        self._filter_text.blockSignals(True)
        self._filter_text.clear()
        self._filter_text.blockSignals(False)
        self._filter_region.setCurrentIndex(0)
        self._filter_state.setCurrentIndex(0)
        self._filter_type.setCurrentIndex(0)
        self._filter_activity.setCurrentIndex(0)
        self._apply_filter()

    # ── selection helpers ─────────────────────────────────────────────────────

    def _checked_left_roots(self) -> List[str]:
        roots = []
        for row in range(self._left.rowCount()):
            chk = self._left.item(row, _L_SEL)
            if chk and chk.checkState() == Qt.Checked:
                roots.append(chk.data(Qt.UserRole))
        return roots

    def _selected_right_items(self) -> List[dict]:
        """Return row-data dicts for every selected row in the right pane.

        Each dict has: label, active, staged, ac, sub_type.
        """
        rows = {item.row() for item in self._right.selectedItems()}
        result = []
        for row in sorted(rows):
            it = self._right.item(row, _R_ROOT)
            if it:
                data = it.data(Qt.UserRole)
                if isinstance(data, dict):
                    result.append(data)
        return result

    def _all_right_labels(self) -> List[str]:
        labels = []
        for row in range(self._right.rowCount()):
            it = self._right.item(row, _R_ROOT)
            if it:
                data = it.data(Qt.UserRole)
                if isinstance(data, dict):
                    labels.append(data["label"])
        return labels

    # ── actions: Stage / Add  ─────────────────────────────────────────────────

    def _act_add(self) -> None:
        """Stage (Safe Baseline Mode) or subscribe (Normal Mode) checked available roots."""
        roots = self._checked_left_roots()
        if not roots:
            QMessageBox.information(self, "Nothing Selected",
                "Check at least one root in the Available pane first.")
            return

        _safe = getattr(self._app, "safe_mode", False)
        if _safe:
            for root in roots:
                self._app.storage.set_root_staged(root, True)
                self._app.window.log(f"Root staged: {root}")
        else:
            not_connected = []
            for root in roots:
                ok = self._app.subscribe_root(root)
                if not ok:
                    not_connected.append(root)
            if not_connected:
                QMessageBox.warning(
                    self, "Not Connected",
                    "The JSON source is not connected.  Connect first, then subscribe.\n\n"
                    "Roots not subscribed:\n" + "\n".join(not_connected),
                )
        self._refresh()

    def _act_add_top(self, n: int) -> None:
        """Stage or subscribe the top N available roots by recent p/m."""
        all_rows  = self._app.storage.get_all_mqtt_roots()
        active    = self._app.sub_registry.get_active_roots()
        _safe     = getattr(self._app, "safe_mode", False)

        # Available = not active, not staged, not auto-connect
        available = [
            r for r in all_rows
            if r["root_topic"] not in active
            and not r.get("staged") and not r.get("auto_connect")
        ]
        top = sorted(
            available,
            key=lambda r: float(r.get("packets_per_minute") or 0),
            reverse=True,
        )[:n]

        if _safe:
            for r in top:
                self._app.storage.set_root_staged(r["root_topic"], True)
                self._app.window.log(f"Root staged: {r['root_topic']}")
        else:
            for r in top:
                self._app.subscribe_root(r["root_topic"])
        self._refresh()

    def _act_remove(self) -> None:
        """Unstage / clear auto-connect (Safe Mode) or unsubscribe (Normal Mode)."""
        items = self._selected_right_items()
        if not items:
            _safe = getattr(self._app, "safe_mode", False)
            verb  = "← Unstage" if _safe else "← Remove"
            QMessageBox.information(
                self, "Nothing Selected",
                f"Click a row in the right pane to select it, then click {verb}."
            )
            return

        _safe = getattr(self._app, "safe_mode", False)

        if _safe:
            for item in items:
                label = item["label"]
                if item["active"]:
                    # Production feeds cannot be removed in Safe Mode
                    self._app.window.log(
                        f"Safe Mode: {label!r} is an active subscription — cannot remove."
                    )
                    continue
                if item["staged"]:
                    self._app.storage.set_root_staged(label, False)
                    self._app.window.log(f"Root unstaged: {label}")
                elif item["ac"]:
                    self._app.storage.set_root_auto_connect(label, False)
                    self._app.window.log(f"Auto-connect cleared: {label}")
        else:
            db_by_root = {r["root_topic"]: r
                          for r in self._app.storage.get_all_mqtt_roots()}

            for item in items:
                label    = item["label"]
                sub_type = item["sub_type"]

                if not item["active"]:
                    # Not live — just clear staging/AC flags
                    if item["staged"]:
                        self._app.storage.set_root_staged(label, False)
                        self._app.window.log(f"Root unstaged: {label}")
                    if item["ac"]:
                        self._app.storage.set_root_auto_connect(label, False)
                        self._app.window.log(f"Auto-connect cleared: {label}")
                    continue

                if sub_type == ROOT_DERIVED:
                    db_row = db_by_root.get(label, {})
                    if db_row.get("auto_connect"):
                        reply = QMessageBox.question(
                            self, "Disable Auto-Connect?",
                            f"'{label}' has auto-connect enabled.\n\n"
                            "Also disable auto-connect so it won't resubscribe at startup?",
                            QMessageBox.Yes | QMessageBox.No | QMessageBox.Cancel,
                            QMessageBox.Yes,
                        )
                        if reply == QMessageBox.Cancel:
                            continue
                        if reply == QMessageBox.Yes:
                            self._app.storage.set_root_auto_connect(label, False)
                    self._app.unsubscribe_root(label)

                elif sub_type == DIRECT:
                    QMessageBox.information(
                        self, "Direct Subscription",
                        f"'{label}' is the base direct subscription for the JSON source.\n\n"
                        "To change it, use Edit Source in the source panel.",
                    )

                elif sub_type in (MAP_TYPE, "map"):
                    QMessageBox.information(
                        self, "Map Source",
                        f"'{label}' is the Map Reports base subscription.\n\n"
                        "To disable it, uncheck the Map Reports source in the source panel.",
                    )

        self._refresh()

    def _act_remove_all(self) -> None:
        """Clear staged roots (Safe Baseline Mode) or unsubscribe all active (Normal Mode)."""
        _safe = getattr(self._app, "safe_mode", False)

        if _safe:
            staged = self._app.storage.get_staged_roots()
            if not staged:
                QMessageBox.information(self, "Nothing to Clear",
                    "No roots are currently staged.")
                return
            if QMessageBox.question(
                self, "Clear All Staged?",
                f"Remove all {len(staged)} staged root(s) from staging?\n"
                "(They will remain in the Available pane as Discovered.)",
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No,
            ) != QMessageBox.Yes:
                return
            for root in staged:
                self._app.storage.set_root_staged(root, False)
            self._app.window.log(f"Cleared {len(staged)} staged root(s).")
        else:
            if QMessageBox.question(
                self, "Remove All Active?",
                "Unsubscribe from ALL currently active root subscriptions?",
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No,
            ) != QMessageBox.Yes:
                return
            active_roots = list(self._app.sub_registry.get_active_roots())
            for root in active_roots:
                self._app.unsubscribe_root(root)

        self._refresh()

    # ── actions: Auto-connect ─────────────────────────────────────────────────

    def _act_set_ac(self) -> None:
        items = self._selected_right_items()
        if not items:
            QMessageBox.information(self, "Nothing Selected",
                "Click a row in the right pane first.")
            return
        for item in items:
            label = item["label"]
            self._app.storage.set_root_auto_connect(label, True)
            self._app.window.log(f"Auto-connect enabled: {label}")
        self._app._save_sources_config()
        self._refresh()

    def _act_clear_ac(self) -> None:
        items = self._selected_right_items()
        if not items:
            QMessageBox.information(self, "Nothing Selected",
                "Click a row in the right pane first.")
            return
        for item in items:
            label = item["label"]
            self._app.storage.set_root_auto_connect(label, False)
            self._app.window.log(f"Auto-connect cleared: {label}")
        self._app._save_sources_config()
        self._refresh()

    @Slot(bool)
    def _on_test_mode_changed(self, active: bool) -> None:
        """Enable/disable the Rollback button when test roots are added or removed."""
        if self._rollback_btn is not None:
            self._rollback_btn.setEnabled(active)
        self._refresh()

    def _act_test_subscribe(self) -> None:
        """Subscribe the selected staged/auto-connect root on the live production connection."""
        items = self._selected_right_items()
        # Only allow roots that are NOT already active
        candidates = [it for it in items if not it["active"]]
        if not candidates:
            QMessageBox.information(
                self, "Select a Staged or Auto-Connect Root",
                "Click a row in the right pane that is NOT already active, "
                "then click 'Test Subscribe →'.\n\n"
                "Only staged or auto-connect roots can be test-subscribed."
            )
            return
        for item in candidates:
            root = item["label"]
            # Only ROOT_DERIVED topics make sense; skip DIRECT / MAP_TYPE entries
            if item.get("sub_type") not in ("root", ROOT_DERIVED, None, "auto_connect", "staged"):
                QMessageBox.information(
                    self, "Cannot Test-Subscribe",
                    f"'{root}' is a direct subscription or map topic — not a root."
                )
                continue
            ok = self._app.test_subscribe_root(root)
            if not ok and not self._app._prod_mqtt.connected:
                QMessageBox.warning(
                    self, "Not Connected",
                    f"Test subscription for '{root}' was queued but the broker "
                    "is not currently connected.\n\n"
                    "The subscription will be sent automatically when the connection is restored."
                )
        self._refresh()

    def _act_rollback_safe(self) -> None:
        """Remove all test subscriptions and restore the Safe Baseline."""
        test_roots = list(getattr(self._app, "_test_roots", []))
        if not test_roots:
            QMessageBox.information(
                self, "Already at Safe Baseline",
                "No test root subscriptions are active."
            )
            return
        reply = QMessageBox.question(
            self, "Rollback to Safe Baseline?",
            f"Remove all test root subscriptions?\n\n"
            f"Roots to unsubscribe: {', '.join(test_roots)}\n\n"
            f"Live subscriptions after rollback:\n"
            f"  msh/US/2/json/#\n"
            f"  msh/US/2/map/#",
            QMessageBox.Yes | QMessageBox.Cancel,
            QMessageBox.Yes,
        )
        if reply == QMessageBox.Yes:
            self._app.rollback_to_safe_baseline()
            self._refresh()

    def _act_save_defaults(self) -> None:
        active_roots = list(self._app.sub_registry.get_active_roots())
        staged_roots = self._app.storage.get_staged_roots()
        # Set auto-connect on active + staged roots; clear all others
        promote = set(active_roots) | set(staged_roots)
        all_rows = self._app.storage.get_all_mqtt_roots()
        for r in all_rows:
            desired = r["root_topic"] in promote
            self._app.storage.set_root_auto_connect(r["root_topic"], desired)
        self._app._save_sources_config()
        self._app.window.log(
            f"Startup defaults updated: {len(promote)} root(s) — "
            + ", ".join(sorted(promote))
        )
        self._refresh()

    def _act_clear_defaults(self) -> None:
        if QMessageBox.question(
            self, "Clear All Defaults?",
            "Remove auto-connect flag from ALL roots?\n"
            "None will subscribe automatically at next startup.",
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No,
        ) != QMessageBox.Yes:
            return
        all_rows = self._app.storage.get_all_mqtt_roots()
        for r in all_rows:
            self._app.storage.set_root_auto_connect(r["root_topic"], False)
        self._app._save_sources_config()
        self._app.window.log("Startup defaults cleared — no roots will auto-connect.")
        self._refresh()

    # ── actions: Manage ───────────────────────────────────────────────────────

    def _act_add_manual(self) -> None:
        root, ok = QInputDialog.getText(
            self, "Add Root Topic", "Enter MQTT root topic (e.g. msh/US/SC):"
        )
        if not ok or not root.strip():
            return
        root = root.strip().rstrip("/")
        self._app.storage.add_manual_root(root, enabled=False, auto_connect=False,
                                          notes="Manually added")
        self._app.window.log(f"Root added (not subscribed): {root}")
        self._refresh()

    def _act_forget(self) -> None:
        roots = self._checked_left_roots()
        if not roots:
            QMessageBox.information(self, "Nothing Selected",
                "Check roots in the Available pane to forget.")
            return
        if QMessageBox.question(
            self, "Forget Roots?",
            f"Permanently delete {len(roots)} root(s) from the database?\n"
            + "\n".join(roots),
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No,
        ) != QMessageBox.Yes:
            return
        for root in roots:
            self._app.storage.delete_mqtt_root(root)
            self._app.window.log(f"Root forgotten: {root}")
        self._refresh()

    def _act_discover(self) -> None:
        if getattr(self._app, "disc_running", False):
            self._app.stop_discovery()
        else:
            dur = self._app.config.discovery_duration_seconds
            self._app.start_discovery(duration_sec=dur)

    @Slot(str, int)
    def _on_disc_started(self, topic: str, duration: int) -> None:
        self._disc_btn.setText("Stop Discovery")
        self._disc_lbl.setText(f"{duration}s…")

    @Slot(int, int, int)
    def _on_disc_tick(self, remaining: int, roots: int, packets: int) -> None:
        self._disc_lbl.setText(f"{remaining}s — {roots} root(s), {packets} pkt(s)")

    @Slot(int, int)
    def _on_disc_finished(self, roots: int, packets: int) -> None:
        self._disc_btn.setText("Discover Now")
        self._disc_lbl.setText(f"Found {roots} root(s)")
        self._refresh()

    @Slot()
    def _on_disc_stopped(self) -> None:
        self._disc_btn.setText("Discover Now")
        self._disc_lbl.setText("Stopped")
        self._refresh()

    @Slot(dict)
    def _on_discovery_result(self, result: dict) -> None:
        self._refresh()
