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

# ── Right pane column indices (Staged / Active roots) — row-selection ────────
_R_ROOT   = 0   # root topic or topic filter label
_R_STATE  = 1   # Staged / Auto-connect / Active
_R_AC     = 2   # auto-connect on startup
_R_SUBS   = 3   # MQTT subscription filter(s) (blank for staged/auto-connect)
_R_PKTS   = 4   # packets received (0 for staged)
_R_LAST   = 5   # last packet time (blank for staged)
_R_SINCE  = 6   # connected since (blank for staged/auto-connect)

_R_HEADERS = [
    "Root / Topic", "State", "Auto-Conn",
    "MQTT Subscription(s)", "Pkts", "Last Pkt", "Connected Since",
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


class RootManagerDialog(QDialog):
    """Two-pane Root Topic Manager.

    Left  — Available: discovered roots NOT currently subscribed.
    Right — Active:    roots/topics with live MQTT subscriptions right now.
    """

    def __init__(self, app, parent=None):
        super().__init__(parent)
        self._app = app
        self._disc_count = 0   # roots found since last Discover click
        self.setWindowTitle("Root Topic Manager")
        self.setMinimumSize(1200, 580)
        self._build_ui()
        self._refresh()
        # Refresh every 3 s so the Active pane stays current
        self._timer = QTimer(self)
        self._timer.setInterval(3000)
        self._timer.timeout.connect(self._refresh)
        self._timer.start()
        # Connect to discovery_result so we refresh after a discovery run
        try:
            app.discovery_result.connect(self._on_discovery_result)
            self._disc_connected = True
        except Exception:
            self._disc_connected = False

    def closeEvent(self, event) -> None:
        self._timer.stop()
        if self._disc_connected:
            try:
                self._app.discovery_result.disconnect(self._on_discovery_result)
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
                ("Stage →",        "Mark checked available roots as Staged (ready for future production)",
                 self._act_add),
                ("← Unstage",      "Remove selected staged/auto-connect roots from the right pane",
                 self._act_remove),
                ("Stage Top 10 →", "Stage the 10 busiest available roots",
                 lambda: self._act_add_top(10)),
                ("Stage Top 20 →", "Stage the 20 busiest available roots",
                 lambda: self._act_add_top(20)),
                ("Stage Top 50 →", "Stage the 50 busiest available roots",
                 lambda: self._act_add_top(50)),
                ("Clear Staged",   "Remove all staged roots from the right pane",
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
        right_box = QGroupBox("Staged / Active Roots")
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
            (_R_ROOT, 190), (_R_STATE, 90), (_R_AC, 70),
            (_R_SUBS, 220), (_R_PKTS, 55), (_R_LAST, 80), (_R_SINCE, 85),
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
        """Populate the right pane with staged, auto-connect, and active roots."""
        subs      = self._app.sub_registry.get_all()
        src_stats = self._app.source_manager.get_stats(SOURCE_MQTT_JSON)
        connected = src_stats.get("connected", False)
        cs        = src_stats.get("connected_since")
        since_str = _short_dt(cs.isoformat() if cs else "") if cs else "—"

        db_by_root = {r["root_topic"]: r
                      for r in self._app.storage.get_all_mqtt_roots()}

        right_rows: List[dict] = []

        # 1. Active subscriptions from sub_registry
        seen_roots: Set[str] = set()
        for s in subs:
            if s.sub_type == ROOT_DERIVED and s.parent_root:
                if s.parent_root not in seen_roots:
                    seen_roots.add(s.parent_root)
                    derived = [x.topic_filter for x in subs
                               if x.parent_root == s.parent_root]
                    db_row  = db_by_root.get(s.parent_root, {})
                    right_rows.append({
                        "label":   s.parent_root,
                        "state":   "Active",
                        "sub_key": (s.parent_root, ROOT_DERIVED),
                        "subs":    derived,
                        "pkts":    sum(x.packet_count for x in subs
                                       if x.parent_root == s.parent_root),
                        "last":    max(
                            (x.last_packet for x in subs
                             if x.parent_root == s.parent_root
                             and x.last_packet is not None),
                            default=None,
                        ),
                        "ac":     bool(db_row.get("auto_connect")),
                        "since":  since_str,
                    })
            elif s.sub_type in (DIRECT, MAP_TYPE, "raw"):
                right_rows.append({
                    "label":   s.topic_filter,
                    "state":   "Active",
                    "sub_key": (s.topic_filter, s.sub_type),
                    "subs":    [s.topic_filter],
                    "pkts":    s.packet_count,
                    "last":    s.last_packet,
                    "ac":      False,
                    "since":   since_str,
                })

        # 2. Staged roots (staged=1, not already active)
        for r in db_by_root.values():
            if r.get("staged") and r["root_topic"] not in active_roots:
                right_rows.append({
                    "label":   r["root_topic"],
                    "state":   "Staged",
                    "sub_key": (r["root_topic"], "staged"),
                    "subs":    [],
                    "pkts":    0,
                    "last":    None,
                    "ac":      bool(r.get("auto_connect")),
                    "since":   "",
                })

        # 3. Auto-connect roots that are not staged and not active
        for r in db_by_root.values():
            if (r.get("auto_connect")
                    and not r.get("staged")
                    and r["root_topic"] not in active_roots):
                right_rows.append({
                    "label":   r["root_topic"],
                    "state":   "Auto-connect",
                    "sub_key": (r["root_topic"], "auto_connect"),
                    "subs":    [],
                    "pkts":    0,
                    "last":    None,
                    "ac":      True,
                    "since":   "",
                })

        # State sort order: Active first, then Staged, then Auto-connect
        _state_order = {"Active": 0, "Staged": 1, "Auto-connect": 2}
        right_rows.sort(key=lambda r: (_state_order.get(r["state"], 9), r["label"]))

        self._right.setSortingEnabled(False)
        self._right.setRowCount(len(right_rows))
        for trow, r in enumerate(right_rows):
            def _item(txt, align=Qt.AlignLeft, num=False, _row=trow, _r=r):
                it = _NumericItem(str(txt)) if num else QTableWidgetItem(str(txt))
                it.setTextAlignment(align | Qt.AlignVCenter)
                it.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                it.setData(Qt.UserRole, _r["sub_key"])
                return it

            state_item = _item(r["state"], Qt.AlignHCenter)
            # Color-code state: Active=green, Staged=amber, Auto-connect=blue
            if r["state"] == "Active":
                state_item.setForeground(QColor("#1a7a1a"))
            elif r["state"] == "Staged":
                state_item.setForeground(QColor("#8a5a00"))
            else:
                state_item.setForeground(QColor("#1a4a8a"))

            self._right.setItem(trow, _R_ROOT, _item(r["label"]))
            self._right.setItem(trow, _R_STATE, state_item)
            self._right.setItem(trow, _R_AC,
                _item("●" if r["ac"] else "○", Qt.AlignHCenter))
            self._right.setItem(trow, _R_SUBS,
                _item("  |  ".join(r["subs"]) if r["subs"] else ""))
            self._right.setItem(trow, _R_PKTS,
                _item(str(r["pkts"]) if r["pkts"] else "", Qt.AlignRight, num=True))
            last_str = r["last"].strftime("%H:%M:%S") if r["last"] else ""
            self._right.setItem(trow, _R_LAST, _item(last_str))
            since_disp = (r["since"] if connected else "disconnected") if r["state"] == "Active" else ""
            self._right.setItem(trow, _R_SINCE, _item(since_disp))
            self._right.setRowHeight(trow, 20)

        self._right.setSortingEnabled(True)
        n_active  = sum(1 for r in right_rows if r["state"] == "Active")
        n_staged  = sum(1 for r in right_rows if r["state"] == "Staged")
        n_ac      = sum(1 for r in right_rows if r["state"] == "Auto-connect")
        status = "connected" if connected else "disconnected"
        parts = []
        if n_active:  parts.append(f"{n_active} active ({status})")
        if n_staged:  parts.append(f"{n_staged} staged")
        if n_ac:      parts.append(f"{n_ac} auto-connect")
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

    def _selected_right_items(self) -> List[tuple]:
        """Return (label, sub_key_type) for every selected row in the right pane.

        sub_key_type can be: "staged", "auto_connect", ROOT_DERIVED, DIRECT, MAP_TYPE, "raw".
        """
        rows = {item.row() for item in self._right.selectedItems()}
        result = []
        for row in sorted(rows):
            it = self._right.item(row, _R_ROOT)
            if it:
                data = it.data(Qt.UserRole)   # (label, sub_key_type)
                if data:
                    result.append(data)
        return result

    def _all_right_labels(self) -> List[str]:
        labels = []
        for row in range(self._right.rowCount()):
            it = self._right.item(row, _R_ROOT)
            if it:
                data = it.data(Qt.UserRole)
                if data:
                    labels.append(data[0])
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
        """Unstage (Safe Baseline Mode) or unsubscribe (Normal Mode) selected right-pane roots."""
        items = self._selected_right_items()   # [(label, sub_key_type), ...]
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
            for label, sub_key_type in items:
                if sub_key_type == "staged":
                    self._app.storage.set_root_staged(label, False)
                    self._app.window.log(f"Root unstaged: {label}")
                elif sub_key_type == "auto_connect":
                    self._app.storage.set_root_auto_connect(label, False)
                    self._app.window.log(f"Auto-connect cleared: {label}")
                # Active subs (DIRECT, MAP_TYPE, etc.) are not removable in Safe Baseline Mode
        else:
            db_by_root = {r["root_topic"]: r
                          for r in self._app.storage.get_all_mqtt_roots()}

            for label, sub_key_type in items:
                if sub_key_type == ROOT_DERIVED:
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

                elif sub_key_type == DIRECT:
                    reply = QMessageBox.question(
                        self, "Remove Direct Subscription?",
                        f"'{label}' is the base direct subscription for the JSON source.\n\n"
                        "Remove it? (Root-derived subscriptions will still work.)",
                        QMessageBox.Yes | QMessageBox.No, QMessageBox.No,
                    )
                    if reply == QMessageBox.Yes:
                        self._app.source_manager.unsubscribe_topics([label])
                        cfg = self._app.source_manager.get_config(SOURCE_MQTT_JSON)
                        if cfg and cfg.topic == label:
                            cfg.topic = ""
                            self._app._save_sources_config()

                elif sub_key_type in (MAP_TYPE, "map"):
                    QMessageBox.information(
                        self, "Map Source",
                        f"'{label}' is the Map Reports base subscription.\n\n"
                        "To disable it, uncheck the Map Reports source in the source panel.",
                    )

                elif sub_key_type == "staged":
                    self._app.storage.set_root_staged(label, False)
                    self._app.window.log(f"Root unstaged: {label}")

                elif sub_key_type == "auto_connect":
                    self._app.storage.set_root_auto_connect(label, False)
                    self._app.window.log(f"Auto-connect cleared: {label}")

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
        for label, _ in items:
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
        for label, _ in items:
            self._app.storage.set_root_auto_connect(label, False)
            self._app.window.log(f"Auto-connect cleared: {label}")
        self._app._save_sources_config()
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
        if self._app._disc_client and self._app._disc_client.running:
            QMessageBox.information(self, "Discovery Running",
                "A discovery scan is already in progress.")
            return
        dur = self._app.config.discovery_duration_seconds
        self._app.start_discovery(duration_sec=dur)
        self._disc_btn.setEnabled(False)
        self._disc_lbl.setText(f"Scanning {dur}s…")
        self._disc_count = 0
        QTimer.singleShot((dur + 8) * 1000, self._discovery_finished)

    def _discovery_finished(self) -> None:
        self._disc_btn.setEnabled(True)
        self._disc_lbl.setText("")
        self._refresh()

    @Slot(dict)
    def _on_discovery_result(self, result: dict) -> None:
        self._disc_count = len(result)
        self._disc_lbl.setText(f"Found {self._disc_count} root(s)")
        self._refresh()
