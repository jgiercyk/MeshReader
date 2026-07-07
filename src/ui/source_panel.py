"""Source manager panel — MQTT source table, active subscriptions, and Root Manager launch."""
import logging
import traceback
from datetime import datetime
from typing import Dict, List, Optional

from PySide6.QtCore import Qt, QTimer, Signal
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QCheckBox, QDialog, QDialogButtonBox, QFormLayout, QGroupBox,
    QHBoxLayout, QHeaderView, QLabel, QLineEdit, QMessageBox,
    QPushButton, QSpinBox, QTableWidget, QTableWidgetItem,
    QVBoxLayout, QWidget,
)

from source_manager import SOURCE_MQTT_RAW, SourceConfig, SourceManager

# column indices for the source table
_COL_ENABLED = 0
_COL_NAME    = 1
_COL_STATUS  = 2
_COL_TOPIC   = 3
_COL_DECODER = 4
_COL_LAST    = 5
_COL_RCVD    = 6
_COL_DEC     = 7
_COL_IGN     = 8
_COL_ERRORS  = 9

_HEADERS = ["✓", "Name", "Status", "Subscriptions", "Decoder",
            "Last Pkt", "Rcvd", "Dec", "Ign", "Err"]


class SourcePanel(QGroupBox):
    """Source manager panel — sources table + active subscriptions + Root Manager launch."""

    configs_changed = Signal()

    def __init__(self, source_manager: SourceManager, app=None, parent=None):
        super().__init__("MQTT Sources", parent)
        self._sm  = source_manager
        self._app = app
        self._disc_active = False
        # Per-source non-connected status: tag → (text, color)
        # Set by signal handler; read by _refresh_stats when not connected.
        self._force_status: Dict[str, tuple] = {}
        self._setup_ui()
        self._sm.source_status_changed.connect(self._on_source_status)
        self._refresh_timer = QTimer(self)
        self._refresh_timer.setInterval(2000)
        self._refresh_timer.timeout.connect(self._refresh_stats)
        self._refresh_timer.start()
        self._rebuild_rows()
        self._refresh_subs_label()

    # ── UI construction ───────────────────────────────────────────────────────

    def _setup_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(6, 4, 6, 4)
        root.setSpacing(3)

        # ── Mode indicator ────────────────────────────────────────────────────
        safe = getattr(self._app, "safe_mode", True) if self._app else True
        if safe:
            mode_text = (
                "Safe Baseline Mode — production is fixed to  msh/US/2/json/#  and  msh/US/2/map/#"
                "   |   Root Manager and Discovery are available; root changes are staged only"
            )
            mode_style = (
                "font-weight: bold; font-size: 9pt; color: #6b3900;"
                " background: #fff3cc; padding: 3px 8px;"
                " border: 1px solid #d4a017; border-radius: 3px;"
            )
        else:
            mode_text  = "Normal Mode — Root Manager controls production subscriptions"
            mode_style = (
                "font-weight: bold; font-size: 9pt; color: #1a5e1a;"
                " background: #e8f5e8; padding: 3px 8px;"
                " border: 1px solid #4a9a4a; border-radius: 3px;"
            )
        self._mode_lbl = QLabel(mode_text)
        self._mode_lbl.setStyleSheet(mode_style)
        root.addWidget(self._mode_lbl)

        # Sources table
        t = QTableWidget()
        t.setColumnCount(len(_HEADERS))
        t.setHorizontalHeaderLabels(_HEADERS)
        for col, tip in [
            (_COL_RCVD,   "Packets received this session"),
            (_COL_DEC,    "Packets decoded this session"),
            (_COL_IGN,    "Packets ignored this session"),
            (_COL_ERRORS, "Errors this session"),
        ]:
            hi = QTableWidgetItem(_HEADERS[col])
            hi.setToolTip(tip)
            t.setHorizontalHeaderItem(col, hi)
        t.verticalHeader().setVisible(False)
        t.verticalHeader().setDefaultSectionSize(22)
        t.setEditTriggers(QTableWidget.NoEditTriggers)
        t.setSelectionBehavior(QTableWidget.SelectRows)
        t.setMaximumHeight(105)
        t.setMinimumHeight(95)
        hdr = t.horizontalHeader()
        hdr.setSectionResizeMode(_COL_ENABLED, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(_COL_NAME,    QHeaderView.Interactive)
        hdr.setSectionResizeMode(_COL_STATUS,  QHeaderView.Interactive)
        hdr.setSectionResizeMode(_COL_TOPIC,   QHeaderView.Stretch)
        hdr.setSectionResizeMode(_COL_DECODER, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(_COL_LAST,    QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(_COL_RCVD,    QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(_COL_DEC,     QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(_COL_IGN,     QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(_COL_ERRORS,  QHeaderView.ResizeToContents)
        t.setColumnWidth(_COL_NAME,   130)
        t.setColumnWidth(_COL_STATUS, 220)
        self._table = t
        root.addWidget(t)

        # Source action buttons + overall status
        src_btns = QHBoxLayout()
        src_btns.setSpacing(6)
        for label, tip, slot in [
            ("Connect",      "Connect all enabled sources",  self._on_connect_all),
            ("Disconnect",   "Disconnect all sources",       self._on_disconnect_all),
            ("Restart",      "Force-stop all sources, clear stale state, reconnect immediately",
                             self._on_restart_all),
            ("Edit Source…", "Edit the selected source",     self._on_edit),
        ]:
            b = QPushButton(label)
            b.setFixedHeight(24)
            b.setToolTip(tip)
            b.clicked.connect(slot)
            src_btns.addWidget(b)
        src_btns.addStretch()
        self._overall_lbl = QLabel("Disconnected")
        self._overall_lbl.setStyleSheet("font-weight: bold; color: #cc3333;")
        src_btns.addWidget(self._overall_lbl)
        root.addLayout(src_btns)

        # Active subscriptions label (click-to-expand?)
        subs_row = QHBoxLayout()
        subs_row.setSpacing(4)
        subs_row.addWidget(QLabel("Live subs:"))
        self._subs_label = QLabel("—")
        self._subs_label.setStyleSheet("color: #333; font-size: 9pt;")
        subs_row.addWidget(self._subs_label, stretch=1)
        root.addLayout(subs_row)

        # Root Manager + Discover buttons
        mgr_row = QHBoxLayout()
        mgr_row.setSpacing(6)

        _safe = getattr(self._app, "safe_mode", True) if self._app else True
        root_mgr_tip = (
            "Open the Root Topic Manager — browse, filter, discover, and stage roots.\n"
            "Safe Baseline Mode: staged roots won't affect live production until Normal Mode is enabled."
            if _safe else
            "Open the Root Topic Manager to browse, filter, and manage all known "
            "and discovered MQTT roots"
        )
        root_mgr_btn = QPushButton("Root Manager…")
        root_mgr_btn.setFixedHeight(24)
        root_mgr_btn.setToolTip(root_mgr_tip)
        root_mgr_btn.clicked.connect(self._on_root_manager)
        mgr_row.addWidget(root_mgr_btn)

        self._disc_btn = QPushButton("Discover Now")
        self._disc_btn.setFixedHeight(24)
        if _safe:
            self._disc_btn.setToolTip(
                "Run a discovery scan — uses a separate MQTT connection.\n"
                "Safe Baseline Mode: discovered roots are saved to DB as Discovered/Staged;\n"
                "they will not affect live production until Normal Mode is enabled."
            )
        else:
            self._disc_btn.setToolTip(
                "Subscribe to msh/US/# for a short window to discover active region roots"
            )
        self._disc_btn.clicked.connect(self._on_discover)
        mgr_row.addWidget(self._disc_btn)

        self._disc_label = QLabel("")
        self._disc_label.setStyleSheet("font-size: 9pt; color: #555;")
        mgr_row.addWidget(self._disc_label)

        probe_btn = QPushButton("Topic Probe…")
        probe_btn.setFixedHeight(24)
        probe_btn.setToolTip(
            "Test candidate MQTT topics on an isolated connection to see which "
            "ones carry traffic — then apply the ones you want as production subscriptions."
        )
        probe_btn.clicked.connect(self._on_topic_probe)
        mgr_row.addWidget(probe_btn)

        subs_btn = QPushButton("Live Subs…")
        subs_btn.setFixedHeight(24)
        subs_btn.setToolTip(
            "Show all subscriptions currently registered in the subscription registry "
            "and at the MQTT broker level."
        )
        subs_btn.clicked.connect(self._on_live_subs)
        mgr_row.addWidget(subs_btn)

        mgr_row.addStretch()
        root.addLayout(mgr_row)

    # ── Root Manager launch ───────────────────────────────────────────────────

    def _on_root_manager(self) -> None:
        if self._app is None:
            return
        try:
            if self._app.window:
                self._app.window.log("Opening Root Manager")
            from ui.root_manager import RootManagerDialog
            dlg = RootManagerDialog(self._app, self)
            dlg.exec()
            self._refresh_subs_label()
        except Exception as exc:
            tb_str = traceback.format_exc()
            logging.error("Root Manager failed:\n%s", tb_str)
            msg = f"Root Manager failed: {exc}"
            try:
                if self._app.window:
                    self._app.window.log(msg)
            except Exception:
                pass
            QMessageBox.critical(
                self, "Root Manager Error",
                f"{exc}\n\nSee debug.log for details.",
            )

    # ── Discovery ────────────────────────────────────────────────────────────

    def _on_discover(self) -> None:
        if self._app is None or self._disc_active:
            return
        dur = self._app.config.discovery_duration_seconds
        ok = self._app.start_discovery(duration_sec=dur)
        if ok:
            self._disc_active = True
            self._disc_btn.setEnabled(False)
            self._disc_label.setText(f"Scanning {dur}s…")
            QTimer.singleShot((dur + 5) * 1000, self._reset_disc_btn)

    def _reset_disc_btn(self) -> None:
        self._disc_active = False
        self._disc_btn.setEnabled(True)
        self._disc_label.setText("")
        self._refresh_subs_label()

    def _on_topic_probe(self) -> None:
        if self._app is None:
            return
        try:
            from ui.topic_probe_dialog import TopicProbeDialog
            dlg = TopicProbeDialog(self._app, self)
            dlg.exec()
        except Exception as exc:
            import traceback as _tb
            logging.error("Topic Probe failed:\n%s", _tb.format_exc())
            QMessageBox.critical(self, "Topic Probe Error",
                f"{exc}\n\nSee debug.log for details.")

    def _on_live_subs(self) -> None:
        if self._app is None:
            return
        try:
            from ui.topic_probe_dialog import ActiveSubsDialog
            dlg = ActiveSubsDialog(self._app, self)
            dlg.exec()
        except Exception as exc:
            import traceback as _tb
            logging.error("Live Subs failed:\n%s", _tb.format_exc())
            QMessageBox.critical(self, "Live Subs Error",
                f"{exc}\n\nSee debug.log for details.")

    # ── Active subscriptions label ────────────────────────────────────────────

    def _refresh_subs_label(self) -> None:
        if self._app is None:
            return
        try:
            subs = self._app.sub_registry.get_all()
            live = [s.topic_filter for s in subs if s.sub_type != "discovery"]
            if live:
                self._subs_label.setText("  |  ".join(sorted(live)))
            else:
                self._subs_label.setText("(none active)")
        except Exception:
            self._subs_label.setText("—")

    # ── Sources table ─────────────────────────────────────────────────────────

    def _rebuild_rows(self) -> None:
        configs = self._sm.all_configs()
        self._table.setRowCount(len(configs))
        for row, cfg in enumerate(configs):
            cb = QCheckBox()
            cb.setChecked(cfg.enabled)
            cb.toggled.connect(
                lambda checked, tag=cfg.source_tag: self._on_toggle(tag, checked)
            )
            cw = QWidget()
            cl = QHBoxLayout(cw)
            cl.addWidget(cb)
            cl.setAlignment(Qt.AlignCenter)
            cl.setContentsMargins(0, 0, 0, 0)
            self._table.setCellWidget(row, _COL_ENABLED, cw)

            # Name cell — tooltip lists exact subscription strings
            topics = cfg.effective_topics()
            direct  = [cfg.topic] if cfg.topic and cfg.topic in topics else []
            derived = [t for t in topics if t not in direct]

            name_item = self._item(cfg.name)
            name_item.setData(Qt.UserRole, cfg.source_tag)
            if topics:
                tt: List[str] = []
                if direct:
                    tt.append("Direct:")
                    for t in direct:
                        tt.append(f"  {t}")
                if derived:
                    n_roots = len(cfg.mqtt_roots or [])
                    r_word = "root" if n_roots == 1 else "roots"
                    tt.append(f"Derived ({n_roots} {r_word}):" if n_roots else "Derived:")
                    for t in derived:
                        tt.append(f"  {t}")
                name_item.setToolTip("\n".join(tt))
            self._table.setItem(row, _COL_NAME, name_item)

            # Subscriptions column — show ACTUAL mqtt.subscribe() strings (never commas in one string)
            topic_text = ", ".join(topics) if topics else "(none)"
            topic_tip_lines: List[str] = []
            if direct:
                topic_tip_lines.append("Direct subscription(s):")
                for t in direct:
                    topic_tip_lines.append(f"  {t}")
            if derived:
                n_roots = len(cfg.mqtt_roots or [])
                topic_tip_lines.append(
                    f"Root-derived ({n_roots} root{'s' if n_roots != 1 else ''}):"
                )
                for t in derived:
                    topic_tip_lines.append(f"  {t}")
            topic_item = self._item(topic_text)
            topic_item.setToolTip("\n".join(topic_tip_lines))
            self._table.setItem(row, _COL_TOPIC, topic_item)

            self._table.setItem(row, _COL_DECODER, self._item(cfg.decoder))

            status = "Disabled" if not cfg.enabled else "Disconnected"
            color  = "#999999"  if not cfg.enabled else "#cc3333"
            si = self._item(status)
            si.setForeground(QColor(color))
            self._table.setItem(row, _COL_STATUS, si)

            for col in (_COL_LAST, _COL_RCVD, _COL_DEC, _COL_IGN, _COL_ERRORS):
                self._table.setItem(row, col,
                    self._item("--" if col == _COL_LAST else "0"))

    @staticmethod
    def _item(text: str) -> QTableWidgetItem:
        it = QTableWidgetItem(str(text))
        it.setTextAlignment(Qt.AlignVCenter | Qt.AlignLeft)
        return it

    def _row_for_tag(self, tag: str) -> int:
        for row in range(self._table.rowCount()):
            item = self._table.item(row, _COL_NAME)
            if item and item.data(Qt.UserRole) == tag:
                return row
        return -1

    def _set_status_cell(self, row: int, text: str, color: str,
                         tooltip: str = "") -> None:
        item = self._table.item(row, _COL_STATUS)
        if item:
            item.setText(text)
            item.setForeground(QColor(color))
            item.setToolTip(tooltip)

    # ── Source signal handlers ────────────────────────────────────────────────

    def _on_source_status(self, tag: str, text: str) -> None:
        """Handle a status update emitted by a source worker thread."""
        row = self._row_for_tag(tag)
        if row < 0:
            return
        lower = text.lower()

        if "error" in lower or "failed" in lower:
            color = "#cc6600"
            self._force_status[tag] = (text[:52], color)
        elif "disconnect" in lower:
            # "Disconnected" or "Disconnecting..." — use red, not green
            color = "#cc3333"
            self._force_status[tag] = (text[:52], color)
        elif "connecting" in lower or "reconnect" in lower or "resetting" in lower:
            color = "#aa8800"
            self._force_status[tag] = (text[:52], color)
        elif "connected" in lower:
            # Just connected — let _refresh_stats show the timer + sub count
            self._force_status.pop(tag, None)
            color = "#33aa33"
        else:
            color = "#555555"
            self._force_status[tag] = (text[:52], color)

        self._set_status_cell(row, text[:52], color)
        self._update_overall()

    def refresh_enabled_states(self) -> None:
        """Sync enabled checkboxes to cfg.enabled after an external config change.

        Call after modifying SourceConfig.enabled from outside the panel (e.g., probe apply).
        """
        for row in range(self._table.rowCount()):
            item = self._table.item(row, _COL_NAME)
            if not item:
                continue
            tag = item.data(Qt.UserRole)
            cfg = self._sm.get_config(tag)
            if cfg is None:
                continue
            cw = self._table.cellWidget(row, _COL_ENABLED)
            if cw:
                cb = cw.findChild(QCheckBox)
                if cb:
                    cb.blockSignals(True)
                    cb.setChecked(cfg.enabled)
                    cb.blockSignals(False)
            # If the cell still says "Disabled" but cfg is now enabled, advance it
            if cfg.enabled:
                cur = self._table.item(row, _COL_STATUS)
                if cur and cur.text() == "Disabled":
                    self._force_status[tag] = ("Connecting...", "#aa8800")
                    self._set_status_cell(row, "Connecting...", "#aa8800")
        self._update_overall()

    def _on_toggle(self, tag: str, enabled: bool) -> None:
        if enabled and tag == SOURCE_MQTT_RAW:
            result = QMessageBox.warning(
                self, "Enable Raw MQTT Source?",
                "The Raw (Advanced) source subscribes to  msh/US/2/#\n"
                "which captures ALL regional traffic at high volume.  It overlaps\n"
                "with other sources and can flood the packet feed.\n\n"
                "Enable only for short diagnostic sessions.\n\nEnable now?",
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No,
            )
            if result != QMessageBox.Yes:
                row = self._row_for_tag(tag)
                if row >= 0:
                    cw = self._table.cellWidget(row, _COL_ENABLED)
                    if cw:
                        cb = cw.findChild(QCheckBox)
                        if cb:
                            cb.blockSignals(True)
                            cb.setChecked(False)
                            cb.blockSignals(False)
                return
        cfg = self._sm.get_config(tag)
        if cfg is None:
            return
        new_cfg = SourceConfig(
            name=cfg.name, source_tag=cfg.source_tag, enabled=enabled,
            broker=cfg.broker, port=cfg.port, tls=cfg.tls,
            username=cfg.username, password=cfg.password,
            topic=cfg.topic, decoder=cfg.decoder, description=cfg.description,
            mqtt_roots=cfg.mqtt_roots,
        )
        self._sm.update_config(new_cfg)
        row = self._row_for_tag(tag)
        if row >= 0:
            if enabled:
                self._force_status[tag] = ("Connecting...", "#aa8800")
                self._set_status_cell(row, "Connecting...", "#aa8800")
            else:
                self._force_status.pop(tag, None)
                self._set_status_cell(row, "Disabled", "#999999")
        self.configs_changed.emit()
        self._update_overall()

    def _on_connect_all(self) -> None:
        for row in range(self._table.rowCount()):
            item = self._table.item(row, _COL_NAME)
            if item:
                tag = item.data(Qt.UserRole)
                cfg = self._sm.get_config(tag)
                if cfg and cfg.enabled:
                    self._force_status[tag] = ("Connecting...", "#aa8800")
                    self._set_status_cell(row, "Connecting...", "#aa8800")
        self._update_overall()
        self._sm.connect_all()

    def _on_disconnect_all(self) -> None:
        for row in range(self._table.rowCount()):
            item = self._table.item(row, _COL_NAME)
            if item:
                tag = item.data(Qt.UserRole)
                cfg = self._sm.get_config(tag)
                if cfg and cfg.enabled:
                    self._force_status[tag] = ("Disconnecting...", "#aa8800")
                    self._set_status_cell(row, "Disconnecting...", "#aa8800")
        self._update_overall()
        self._sm.disconnect_all()

    def _on_restart_all(self) -> None:
        """Force-stop all sources, clear stale state, reconnect immediately."""
        for row in range(self._table.rowCount()):
            item = self._table.item(row, _COL_NAME)
            if item:
                tag = item.data(Qt.UserRole)
                cfg = self._sm.get_config(tag)
                if cfg and cfg.enabled:
                    self._force_status[tag] = ("Restarting...", "#aa8800")
                    self._set_status_cell(row, "Restarting...", "#aa8800")
        self._update_overall()
        self._sm.restart_all()

    def _on_edit(self) -> None:
        row = self._table.currentRow()
        if row < 0:
            row = 0
        item = self._table.item(row, _COL_NAME)
        if not item:
            return
        tag = item.data(Qt.UserRole)
        cfg = self._sm.get_config(tag)
        if cfg is None:
            return
        dlg = _SourceEditDialog(cfg, self)
        if dlg.exec() == QDialog.Accepted:
            new_cfg = dlg.get_config()
            self._sm.update_config(new_cfg)
            topics = new_cfg.effective_topics()

            name_item = self._table.item(row, _COL_NAME)
            if name_item:
                name_item.setText(new_cfg.name)
                name_item.setToolTip(
                    "MQTT subscriptions:\n" + "\n".join(f"  {t}" for t in topics)
                )
            topic_item = self._table.item(row, _COL_TOPIC)
            if topic_item:
                if new_cfg.mqtt_roots:
                    topic_item.setText(", ".join(new_cfg.mqtt_roots))
                    topic_item.setToolTip(
                        f"{len(new_cfg.mqtt_roots)} root(s) → {len(topics)} subscriptions:\n"
                        + "\n".join(f"  {t}" for t in topics)
                    )
                else:
                    topic_item.setText(new_cfg.topic)
                    topic_item.setToolTip("")
            decoder_item = self._table.item(row, _COL_DECODER)
            if decoder_item:
                decoder_item.setText(new_cfg.decoder)
            cw = self._table.cellWidget(row, _COL_ENABLED)
            if cw:
                cb = cw.findChild(QCheckBox)
                if cb:
                    cb.blockSignals(True)
                    cb.setChecked(new_cfg.enabled)
                    cb.blockSignals(False)
            self.configs_changed.emit()

    # ── Stats refresh (every 2 s) ─────────────────────────────────────────────

    def _refresh_stats(self) -> None:
        for row in range(self._table.rowCount()):
            item = self._table.item(row, _COL_NAME)
            if not item:
                continue
            tag = item.data(Qt.UserRole)
            cfg = self._sm.get_config(tag)
            if cfg is None or not cfg.enabled:
                continue
            stats = self._sm.get_stats(tag)

            if stats.get("connected"):
                # ── Connected: show timer + subscription breakdown ───────────
                cs = stats.get("connected_since")
                dur = ""
                if cs and isinstance(cs, datetime):
                    delta = datetime.now() - cs
                    s = int(delta.total_seconds())
                    h, rem = divmod(s, 3600)
                    m = rem // 60
                    dur = (f"{h}h {m:02d}m — ") if h else f"{m}m — "

                topics = cfg.effective_topics()

                # Separate direct topic from root-derived subscriptions
                direct  = [cfg.topic] if cfg.topic and cfg.topic in topics else []
                derived = [t for t in topics if t not in direct]
                n_roots = len(cfg.mqtt_roots or [])

                parts: List[str] = []
                if direct:
                    parts.append(f"{len(direct)} direct")
                if derived and n_roots:
                    r_word = "root" if n_roots == 1 else "roots"
                    parts.append(f"{len(derived)} derived ({n_roots} {r_word})")
                elif derived:
                    parts.append(f"{len(derived)} derived")

                breakdown = ", ".join(parts)
                n = len(topics)
                status_text = (
                    f"Connected {dur}{n} sub{'s' if n != 1 else ''}"
                    + (f" ({breakdown})" if breakdown else "")
                )

                # Build detailed tooltip
                tt_lines: List[str] = []
                if direct:
                    tt_lines.append("Direct subscription:")
                    for t in direct:
                        tt_lines.append(f"  {t}")
                if cfg.mqtt_roots:
                    tt_lines.append(f"Derived from {n_roots} root{'s' if n_roots != 1 else ''}:")
                    for t in derived:
                        tt_lines.append(f"  {t}")
                tooltip = "\n".join(tt_lines) if tt_lines else "\n".join(topics)

                self._force_status.pop(tag, None)
                self._set_status_cell(row, status_text, "#33aa33", tooltip)

            else:
                # ── Not connected: show force_status or default Disconnected ─
                if tag in self._force_status:
                    txt, col = self._force_status[tag]
                else:
                    txt, col = "Disconnected", "#cc3333"
                cur = self._table.item(row, _COL_STATUS)
                if cur and cur.text().startswith("Connected"):
                    # Was showing connected; now it's not — update immediately
                    self._set_status_cell(row, txt, col)
                elif cur and not cur.text().startswith("Connected"):
                    self._set_status_cell(row, txt, col)

            # ── Packet counter columns ──────────────────────────────────────
            last = stats.get("last_packet")
            last_str = last.strftime("%H:%M:%S") if isinstance(last, datetime) else "--"
            for col, val in [
                (_COL_LAST,   last_str),
                (_COL_RCVD,   str(stats.get("packet_count",  0))),
                (_COL_DEC,    str(stats.get("decoded_count", 0))),
                (_COL_IGN,    str(stats.get("ignored_count", 0))),
                (_COL_ERRORS, str(stats.get("error_count",  0))),
            ]:
                it = self._table.item(row, col)
                if it:
                    it.setText(val)

            # ── Tooltip on Rcvd: per-root / per-channel breakdown ──────────
            root_counts    = stats.get("root_counts", {})
            channel_counts = stats.get("channel_counts", {})
            tips: List[str] = []
            if root_counts:
                tips.append("By root:")
                for r, c in sorted(root_counts.items(), key=lambda x: -x[1]):
                    tips.append(f"  {r}: {c}")
            if channel_counts:
                tips.append("By channel:")
                for ch, c in sorted(channel_counts.items(), key=lambda x: -x[1])[:8]:
                    tips.append(f"  {ch}: {c}")
            if tips:
                rcvd_item = self._table.item(row, _COL_RCVD)
                if rcvd_item:
                    rcvd_item.setToolTip("\n".join(tips))

            # ── Tooltip on Ign: per-reason breakdown ───────────────────────
            reasons = stats.get("ignore_reasons", {})
            if reasons:
                ign_item = self._table.item(row, _COL_IGN)
                if ign_item:
                    ign_item.setToolTip(
                        "\n".join(
                            f"{r}: {c}"
                            for r, c in sorted(reasons.items(), key=lambda x: -x[1])
                        )
                    )

        self._update_overall()

    def _update_overall(self) -> None:
        configs = self._sm.all_configs()
        enabled = [c for c in configs if c.enabled]
        connected = sum(
            1 for c in enabled
            if self._sm.get_stats(c.source_tag).get("connected")
        )
        n = len(enabled)
        if n == 0:
            self._overall_lbl.setText("No sources enabled")
            self._overall_lbl.setStyleSheet("font-weight: bold; color: #999999;")
        elif connected == 0:
            self._overall_lbl.setText("Disconnected")
            self._overall_lbl.setStyleSheet("font-weight: bold; color: #cc3333;")
        elif connected < n:
            self._overall_lbl.setText(f"Partial ({connected}/{n})")
            self._overall_lbl.setStyleSheet("font-weight: bold; color: #aa8800;")
        else:
            self._overall_lbl.setText(f"Connected ({connected}/{n})")
            self._overall_lbl.setStyleSheet("font-weight: bold; color: #33aa33;")

    def update_last_packet(self, dt: datetime) -> None:
        pass

    def update_rate(self, rate: float) -> None:
        pass


# ── Source edit dialog ────────────────────────────────────────────────────────

class _SourceEditDialog(QDialog):
    def __init__(self, cfg: SourceConfig, parent=None):
        super().__init__(parent)
        self._original = cfg
        self.setWindowTitle("Edit Source: " + cfg.name)
        self.setMinimumWidth(480)
        self._setup_ui(cfg)

    def _setup_ui(self, cfg: SourceConfig) -> None:
        layout = QVBoxLayout(self)
        form   = QFormLayout()

        self._name     = QLineEdit(cfg.name)
        self._tag_lbl  = QLabel(cfg.source_tag)
        self._broker   = QLineEdit(cfg.broker)
        self._port     = QSpinBox()
        self._port.setRange(1, 65535)
        self._port.setValue(cfg.port)
        self._tls      = QCheckBox("Enable TLS")
        self._tls.setChecked(cfg.tls)
        self._username = QLineEdit(cfg.username)
        self._password = QLineEdit(cfg.password)
        self._password.setEchoMode(QLineEdit.Password)
        self._topic    = QLineEdit(cfg.topic)
        self._decoder  = QLineEdit(cfg.decoder)
        self._enabled  = QCheckBox("Enabled")
        self._enabled.setChecked(cfg.enabled)
        self._desc     = QLineEdit(cfg.description)

        form.addRow("Name:",           self._name)
        form.addRow("Source Tag:",     self._tag_lbl)
        form.addRow("Broker:",         self._broker)
        form.addRow("Port:",           self._port)
        form.addRow("",                self._tls)
        form.addRow("Username:",       self._username)
        form.addRow("Password:",       self._password)
        form.addRow("Fallback Topic:", self._topic)
        form.addRow("Decoder:",        self._decoder)
        form.addRow("",                self._enabled)
        form.addRow("Description:",    self._desc)

        if cfg.mqtt_roots:
            topics = cfg.effective_topics()
            topics_lbl = QLabel("\n".join(topics))
            topics_lbl.setStyleSheet("color: #555; font-size: 9pt;")
            topics_lbl.setToolTip(
                "Subscriptions derived from auto-connect roots.\n"
                "Open Root Manager to add or remove roots."
            )
            form.addRow("Active subs:", topics_lbl)

        layout.addLayout(form)
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def get_config(self) -> SourceConfig:
        return SourceConfig(
            name=self._name.text().strip() or self._original.name,
            source_tag=self._original.source_tag,
            enabled=self._enabled.isChecked(),
            broker=self._broker.text().strip(),
            port=self._port.value(),
            tls=self._tls.isChecked(),
            username=self._username.text().strip(),
            password=self._password.text(),
            topic=self._topic.text().strip(),
            decoder=self._decoder.text().strip() or self._original.decoder,
            description=self._desc.text().strip(),
            mqtt_roots=self._original.mqtt_roots,
        )
