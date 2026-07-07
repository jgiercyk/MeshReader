"""Topic Probe Dialog and Live Active Subscriptions Inspector.

TopicProbeDialog:
  - Uses a completely isolated MQTT connection via TopicProbeClient.
  - Probe packets are NEVER stored in DB, nodes, map, or packet table.
  - All candidate topics are always probed simultaneously (Option B design).
  - Checkboxes in the "Apply?" column control which working topics go to production.
  - Table refreshes only from the 1-second tick QTimer — never per-packet.
  - No blocking calls on the Qt/UI thread.

ActiveSubsDialog:
  - Read-only inspector of the subscription registry and MQTT client state.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Optional

from PySide6.QtCore import Qt, QTimer, Slot
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QDialog, QGroupBox, QHBoxLayout, QHeaderView, QLabel, QMessageBox,
    QPushButton, QSpinBox, QTableWidget, QTableWidgetItem, QVBoxLayout,
)

from source_manager import SOURCE_MQTT_JSON, SOURCE_MQTT_MAP
from topic_probe_client import TopicProbeClient

# ── Candidate topics — all are always probed; checkboxes only control apply ──────

PROBE_TOPICS = [
    "msh/US/2/json/#",
    "msh/us/2/json/#",
    "msh/US/SC/2/json/#",
    "msh/us/sc/2/json/#",
    "msh/US/2/map/#",
]

# ── Column indices ──────────────────────────────────────────────────────────────

_P_APPLY = 0   # "Apply?" — does NOT control which topics are probed
_P_FILT  = 1
_P_STAT  = 2
_P_PKTS  = 3
_P_CHAN  = 4
_P_FST   = 5
_P_LST   = 6

_P_HEADERS = ["Apply?", "Topic Filter", "Status", "Packets", "Channels", "First Seen", "Last Seen"]


class TopicProbeDialog(QDialog):
    """Run a timed probe across all candidate topics, then apply the working ones."""

    def __init__(self, app, parent=None):
        super().__init__(parent)
        self._app          = app
        self._probe: Optional[TopicProbeClient] = None
        self._probe_running = False   # explicit flag; avoids thread.is_alive() race
        self._countdown    = 0

        # 1-second tick: drives countdown label and table refresh.
        # This is the ONLY place table refreshes happen — never per-packet.
        self._tick_timer = QTimer(self)
        self._tick_timer.setInterval(1000)
        self._tick_timer.timeout.connect(self._on_tick)

        # Fallback: if _on_probe_done doesn't arrive within 3 s of countdown=0,
        # force-complete so the UI doesn't hang waiting for a stuck paho disconnect.
        self._fallback_timer = QTimer(self)
        self._fallback_timer.setSingleShot(True)
        self._fallback_timer.setInterval(3000)
        self._fallback_timer.timeout.connect(self._on_fallback_timeout)

        self.setWindowTitle("Topic Probe — MQTT Subscription Tester")
        self.setMinimumSize(940, 380)
        self._build_ui()
        logging.info("Probe: dialog opened")

    # ── UI construction ────────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setSpacing(6)

        info = QLabel(
            "<b>PROBE = diagnostic only.</b>  Uses a separate MQTT connection — "
            "packets are never stored or shown in the normal packet/node/map views.  "
            "All candidate topics are probed simultaneously.  "
            "After the probe, check (✓) the topics you want to apply to production, "
            "then click <b>Apply Checked as Production ▶ Restart</b>."
        )
        info.setWordWrap(True)
        info.setStyleSheet("color: #444; font-size: 9pt;")
        layout.addWidget(info)

        # Results table
        self._tbl = QTableWidget()
        self._tbl.setColumnCount(len(_P_HEADERS))
        self._tbl.setHorizontalHeaderLabels(_P_HEADERS)
        self._tbl.setRowCount(len(PROBE_TOPICS))
        self._tbl.verticalHeader().setVisible(False)
        self._tbl.verticalHeader().setDefaultSectionSize(22)
        self._tbl.setEditTriggers(QTableWidget.NoEditTriggers)
        self._tbl.setSelectionBehavior(QTableWidget.SelectRows)
        self._tbl.setAlternatingRowColors(True)

        for col, w in [(_P_APPLY, 50), (_P_FILT, 210), (_P_STAT, 85),
                       (_P_PKTS, 65), (_P_CHAN, 160), (_P_FST, 82), (_P_LST, 82)]:
            self._tbl.setColumnWidth(col, w)
        self._tbl.horizontalHeader().setSectionResizeMode(_P_CHAN, QHeaderView.Stretch)

        for row, topic in enumerate(PROBE_TOPICS):
            chk = QTableWidgetItem()
            chk.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsUserCheckable)
            chk.setCheckState(Qt.Unchecked)
            chk.setData(Qt.UserRole, topic)
            self._tbl.setItem(row, _P_APPLY, chk)
            self._tbl.setItem(row, _P_FILT, QTableWidgetItem(topic))
            for col in [_P_STAT, _P_PKTS, _P_CHAN, _P_FST, _P_LST]:
                self._tbl.setItem(row, col, QTableWidgetItem("—"))
            self._tbl.setRowHeight(row, 22)

        # Checkbox toggles → re-evaluate Apply button
        self._tbl.itemChanged.connect(self._on_table_changed)
        layout.addWidget(self._tbl, stretch=1)

        # Controls bar
        bar = QHBoxLayout()
        bar.addWidget(QLabel("Duration:"))
        self._dur = QSpinBox()
        self._dur.setRange(15, 120)
        self._dur.setValue(45)
        self._dur.setSuffix(" s")
        self._dur.setFixedWidth(72)
        bar.addWidget(self._dur)

        self._start_btn = QPushButton("Start Probe")
        self._start_btn.setFixedHeight(26)
        self._start_btn.clicked.connect(self._start)
        bar.addWidget(self._start_btn)

        self._stop_btn = QPushButton("Stop")
        self._stop_btn.setFixedHeight(26)
        self._stop_btn.setEnabled(False)
        self._stop_btn.clicked.connect(self._stop)
        bar.addWidget(self._stop_btn)

        self._cd_lbl = QLabel("")
        self._cd_lbl.setMinimumWidth(160)
        self._cd_lbl.setStyleSheet("font-weight: bold;")
        bar.addWidget(self._cd_lbl)
        bar.addStretch()

        self._apply_btn = QPushButton("Apply Checked as Production  ▶  Restart")
        self._apply_btn.setFixedHeight(26)
        self._apply_btn.setEnabled(False)
        self._apply_btn.setToolTip("Run the probe first.")
        self._apply_btn.clicked.connect(self._apply)
        bar.addWidget(self._apply_btn)

        close_btn = QPushButton("Close")
        close_btn.setFixedHeight(26)
        close_btn.clicked.connect(self.accept)
        bar.addWidget(close_btn)
        layout.addLayout(bar)

    # ── probe lifecycle ────────────────────────────────────────────────────────

    def _start(self) -> None:
        if self._probe and self._probe.running:
            return   # guard against double-start

        json_cfg = self._app.source_manager.get_config(SOURCE_MQTT_JSON)
        if json_cfg is None:
            QMessageBox.warning(self, "No JSON Source", "No JSON source is configured.")
            return

        dur = self._dur.value()
        self._countdown = dur
        self._fallback_timer.stop()
        self._tick_timer.stop()

        # Reset table to blank
        self._tbl.blockSignals(True)
        for row in range(self._tbl.rowCount()):
            chk = self._tbl.item(row, _P_APPLY)
            if chk:
                chk.setCheckState(Qt.Unchecked)
            for col in [_P_STAT, _P_PKTS, _P_CHAN, _P_FST, _P_LST]:
                it = self._tbl.item(row, col)
                if it:
                    it.setText("—")
                    it.setForeground(QColor("#000000"))
            stat = self._tbl.item(row, _P_STAT)
            if stat:
                stat.setText("waiting")
        self._tbl.blockSignals(False)

        self._start_btn.setEnabled(False)
        self._stop_btn.setEnabled(True)
        self._apply_btn.setEnabled(False)
        self._apply_btn.setToolTip("Stop or wait for probe to finish before applying.")
        self._cd_lbl.setText(f"{dur}s…")

        # on_packet_seen is intentionally None — no per-packet UI callback.
        # The tick timer calls _refresh_table() once per second instead.
        # This prevents event-queue flooding that caused the previous freeze.
        self._probe = TopicProbeClient(
            broker=json_cfg.broker,
            port=json_cfg.port,
            username=json_cfg.username,
            password=json_cfg.password,
            topics=PROBE_TOPICS,
            on_packet_seen=None,
            on_log=self._on_probe_log,
            on_done=self._on_probe_done,
        )
        self._probe_running = True
        logging.info(
            "Probe: started  broker=%s:%d  topics=%s  duration=%ds  client_id=%s",
            json_cfg.broker, json_cfg.port, PROBE_TOPICS, dur, self._probe._client_id,
        )
        self._probe.start(dur)
        self._tick_timer.start()

    def _stop(self) -> None:
        logging.info("Probe: stopping (user requested)")
        self._tick_timer.stop()
        self._fallback_timer.stop()
        self._probe_running = False
        if self._probe:
            self._probe.stop()   # non-blocking — just sets the event
        self._cd_lbl.setText("Stopped")
        self._start_btn.setEnabled(True)
        self._stop_btn.setEnabled(False)
        self._refresh_table()
        self._update_apply_btn()

    # ── tick timer (runs on the Qt main thread, 1 Hz) ─────────────────────────

    @Slot()
    def _on_tick(self) -> None:
        self._countdown -= 1
        logging.debug("Probe: tick seconds_remaining=%d", self._countdown)
        self._refresh_table()

        if self._countdown <= 0:
            self._tick_timer.stop()
            self._cd_lbl.setText("0s — finishing…")
            self._start_btn.setEnabled(True)
            self._stop_btn.setEnabled(False)
            # Fallback: if probe done-callback doesn't arrive within 3 s, force-complete
            self._fallback_timer.start()
            logging.info("Probe: countdown reached 0; awaiting done callback (3 s fallback armed)")
        else:
            self._cd_lbl.setText(f"{self._countdown}s remaining")

    @Slot()
    def _on_fallback_timeout(self) -> None:
        """3 s elapsed after countdown=0 with no done signal — force-complete."""
        logging.warning("Probe: fallback timeout — force-completing (probe loop may be stuck)")
        self._probe_running = False
        self._cd_lbl.setText("Complete (timeout)")
        self._refresh_table()
        self._update_apply_btn()

    # ── table refresh ─────────────────────────────────────────────────────────

    def _refresh_table(self) -> None:
        if self._probe is None:
            return
        snap = self._probe.snapshot()
        for row, topic in enumerate(PROBE_TOPICS):
            data = snap.get(topic)
            if data is None:
                continue
            n  = data["packet_count"]
            ch = sorted(data.get("channels", set()))
            fs = (data.get("first_seen") or "")[:16].replace("T", " ")
            ls = (data.get("last_seen")  or "")[:16].replace("T", " ")
            has_data = n > 0
            color    = "#005500" if has_data else "#888888"

            for col, txt in [
                (_P_STAT, "receiving" if has_data else "quiet"),
                (_P_PKTS, str(n)),
                (_P_CHAN, ", ".join(ch[:6])),
                (_P_FST,  fs or "—"),
                (_P_LST,  ls or "—"),
            ]:
                it = self._tbl.item(row, col)
                if it:
                    it.setText(txt)
                    it.setForeground(QColor(color if col == _P_STAT else "#000000"))

    # ── probe callbacks (called from worker threads — must not block) ──────────

    def _on_probe_log(self, msg: str) -> None:
        logging.info("Probe: %s", msg)
        QTimer.singleShot(0, lambda: self._app.window.log(f"[Probe] {msg}"))

    def _on_probe_done(self, results: dict) -> None:
        """Called from probe worker thread — marshals to Qt main thread via singleShot."""
        def _work():
            self._fallback_timer.stop()
            self._tick_timer.stop()
            self._probe_running = False
            self._refresh_table()

            total = sum(d["packet_count"] for d in results.values())
            working = sum(1 for d in results.values() if d["packet_count"] > 0)
            self._cd_lbl.setText(f"Complete — {total} pkt(s) on {working} topic(s)")
            self._start_btn.setEnabled(True)
            self._stop_btn.setEnabled(False)

            # Auto-check topics that got packets; uncheck quiet ones
            self._tbl.blockSignals(True)
            for row, topic in enumerate(PROBE_TOPICS):
                data = results.get(topic)
                got_pkts = bool(data and data["packet_count"] > 0)
                chk = self._tbl.item(row, _P_APPLY)
                if chk:
                    chk.setCheckState(Qt.Checked if got_pkts else Qt.Unchecked)
            self._tbl.blockSignals(False)

            self._update_apply_btn()
            logging.info(
                "Probe: complete  working_topics=%d  total_pkts=%d",
                working, total,
            )
        QTimer.singleShot(0, _work)

    # ── Apply button enable/disable ────────────────────────────────────────────

    def _on_table_changed(self, item) -> None:
        if item.column() == _P_APPLY:
            self._update_apply_btn()

    def _update_apply_btn(self) -> None:
        if self._probe is None:
            enabled, reason = False, "no probe run yet"
            tip = "Run the probe first."
        elif self._probe_running:
            enabled, reason = False, "probe still running"
            tip = "Stop or wait for probe to finish before applying."
        else:
            snap = self._probe.snapshot()
            has_data = False
            for row in range(self._tbl.rowCount()):
                chk = self._tbl.item(row, _P_APPLY)
                if chk and chk.checkState() == Qt.Checked:
                    topic = chk.data(Qt.UserRole)
                    if snap.get(topic, {}).get("packet_count", 0) > 0:
                        has_data = True
                        break
            if has_data:
                enabled, reason = True, "enabled"
                tip = (
                    "REPLACES the production subscription with the checked working topic(s).\n"
                    "  /2/json/# topics → MQTT JSON source\n"
                    "  /map/# topics → MQTT Map Reports source\n"
                    "Source(s) restart immediately.\n\n"
                    "Probe = diagnostic only.  Production = what the app actually uses."
                )
            else:
                enabled, reason = False, "no checked rows with packets"
                tip = (
                    "Check at least one topic that received packets "
                    "(Status: receiving, Packets > 0) during the probe."
                )

        self._apply_btn.setEnabled(enabled)
        self._apply_btn.setToolTip(tip)
        logging.info("Probe: Apply button %s", reason)

    # ── apply to production ────────────────────────────────────────────────────

    def _apply(self) -> None:
        snap = self._probe.snapshot() if self._probe else {}
        json_topics: List[str] = []
        map_topics:  List[str] = []
        skipped:     List[str] = []

        for row in range(self._tbl.rowCount()):
            chk = self._tbl.item(row, _P_APPLY)
            if not (chk and chk.checkState() == Qt.Checked):
                continue
            topic = chk.data(Qt.UserRole)
            pkts  = snap.get(topic, {}).get("packet_count", 0)
            if pkts == 0:
                skipped.append(f"{topic}  (0 packets — skipped)")
                continue
            if topic.endswith("/2/json/#"):
                json_topics.append(topic)
            elif "/map/" in topic or topic.endswith("/map/#"):
                map_topics.append(topic)

        if not json_topics and not map_topics:
            msg = "No checked topics received packets during the probe."
            if skipped:
                msg += "\n\nChecked but quiet:\n" + "\n".join(f"  {t}" for t in skipped)
            QMessageBox.information(self, "Nothing to Apply", msg)
            return

        def _log(msg: str) -> None:
            self._app.window.log(f"[Probe] {msg}")
            logging.info("Probe apply: %s", msg)

        _log("Applying probe results to production")
        summary: List[str] = []

        # ── JSON source ───────────────────────────────────────────────────────
        if json_topics:
            cfg = self._app.source_manager.get_config(SOURCE_MQTT_JSON)
            if cfg:
                old_topic   = cfg.topic
                old_enabled = cfg.enabled
                # Always enable and set the confirmed working topic.
                # Must set enabled=True BEFORE reset_source — reset_source guards on it.
                cfg.enabled    = True
                cfg.topic      = json_topics[0]
                cfg.mqtt_roots = []   # direct topic covers all traffic; roots are redundant
                self._app._save_sources_config()
                _log(f"JSON source enabled  (was enabled={old_enabled})")
                _log(f"JSON production topic: {json_topics[0]}  (was: {old_topic or '(none)'})")
                _log("Restarting MQTT JSON")
                self._app.source_manager.reset_source(SOURCE_MQTT_JSON)
                summary.append(
                    f"MQTT JSON — ENABLED, production topic set to:\n"
                    f"  {json_topics[0]}\n"
                    f"  (was: {old_topic or '(none)'}  —  source restarting)"
                )
                if len(json_topics) > 1:
                    summary.append(
                        "Only the first JSON topic was applied (it covers all traffic):\n"
                        + "\n".join(f"  {t}  (not applied)" for t in json_topics[1:])
                    )

        # ── Map source ────────────────────────────────────────────────────────
        if map_topics:
            map_cfg = self._app.source_manager.get_config(SOURCE_MQTT_MAP)
            if map_cfg:
                old_enabled = map_cfg.enabled
                # Always enable and confirm the topic, then restart.
                # reset_source guards on enabled, so set it first.
                map_cfg.enabled = True
                map_cfg.topic   = map_topics[0]
                self._app._save_sources_config()
                _log(f"Map source enabled  (was enabled={old_enabled})")
                _log(f"Map production topic: {map_topics[0]}")
                _log("Restarting MQTT Map Reports")
                self._app.source_manager.reset_source(SOURCE_MQTT_MAP)
                summary.append(
                    f"MQTT Map Reports — ENABLED, production topic:\n"
                    f"  {map_topics[0]}  (source restarting)"
                )

        if skipped:
            summary.append("Skipped (0 packets during probe — not applied):\n"
                           + "\n".join(f"  {s}" for s in skipped))

        # Sync source panel checkboxes and status cells to the new enabled states.
        # reset_source() fires status signals, but the "Enabled" checkbox widget
        # is only updated by the panel's own toggle/edit paths — so we must push it.
        sp = getattr(getattr(self._app, "window", None), "_source_panel", None)
        if sp:
            sp.refresh_enabled_states()

        _log("Done — production sources restarting")
        QMessageBox.information(self, "Production Subscriptions Updated",
                                "\n\n".join(summary))

    # ── window close ──────────────────────────────────────────────────────────

    def closeEvent(self, event) -> None:
        logging.info("Probe: dialog closing")
        self._tick_timer.stop()
        self._fallback_timer.stop()
        if self._probe and self._probe.running:
            logging.info("Probe: stopping on window close")
            self._probe.stop()
        super().closeEvent(event)


# ── Live Active Subscriptions Inspector ───────────────────────────────────────────

_S_TAG   = 0
_S_TOPIC = 1
_S_TYPE  = 2
_S_ROOT  = 3
_S_PKTS  = 4
_S_LAST  = 5
_S_SINCE = 6

_S_HEADERS = ["Source", "Topic Filter", "Type", "Parent Root", "Packets", "Last Pkt", "Since"]

_C_TAG    = 0
_C_TOPIC  = 1
_C_SINCE  = 2
_C_HEADERS = ["Source", "MQTT-Level Topic", "Connected Since"]


class ActiveSubsDialog(QDialog):
    """Read-only inspector: subscription registry + MQTT-client active topic view."""

    def __init__(self, app, parent=None):
        super().__init__(parent)
        self._app = app
        self.setWindowTitle("Live Active Subscriptions")
        self.setMinimumSize(860, 480)
        self._build_ui()
        self._refresh()
        self._timer = QTimer(self)
        self._timer.setInterval(2000)
        self._timer.timeout.connect(self._refresh)
        self._timer.start()

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setSpacing(8)

        reg_box = QGroupBox("Subscription Registry  (authoritative in-memory view)")
        reg_lay = QVBoxLayout(reg_box)
        self._reg_tbl = self._make_table(_S_HEADERS)
        reg_lay.addWidget(self._reg_tbl)
        self._reg_count = QLabel("")
        self._reg_count.setStyleSheet("font-size: 9pt; color: #555;")
        reg_lay.addWidget(self._reg_count)
        root.addWidget(reg_box, stretch=2)

        cli_box = QGroupBox("MQTT Client  (topics actually subscribed to the broker)")
        cli_lay = QVBoxLayout(cli_box)
        self._cli_tbl = self._make_table(_C_HEADERS)
        cli_lay.addWidget(self._cli_tbl)
        self._cli_count = QLabel("")
        self._cli_count.setStyleSheet("font-size: 9pt; color: #555;")
        cli_lay.addWidget(self._cli_count)
        root.addWidget(cli_box, stretch=1)

        bar = QHBoxLayout()
        bar.addStretch()
        refresh_btn = QPushButton("Refresh Now")
        refresh_btn.setFixedHeight(24)
        refresh_btn.clicked.connect(self._refresh)
        bar.addWidget(refresh_btn)
        close = QPushButton("Close")
        close.setFixedHeight(24)
        close.clicked.connect(self.accept)
        bar.addWidget(close)
        root.addLayout(bar)

    def _make_table(self, headers: List[str]) -> QTableWidget:
        t = QTableWidget()
        t.setColumnCount(len(headers))
        t.setHorizontalHeaderLabels(headers)
        t.verticalHeader().setVisible(False)
        t.verticalHeader().setDefaultSectionSize(20)
        t.setEditTriggers(QTableWidget.NoEditTriggers)
        t.setSelectionBehavior(QTableWidget.SelectRows)
        t.setAlternatingRowColors(True)
        t.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        return t

    @Slot()
    def _refresh(self) -> None:
        self._refresh_registry()
        self._refresh_client()

    def _refresh_registry(self) -> None:
        subs = self._app.sub_registry.get_all()
        self._reg_tbl.setSortingEnabled(False)
        self._reg_tbl.setRowCount(len(subs))
        for row, s in enumerate(subs):
            last  = s.last_packet.strftime("%H:%M:%S") if s.last_packet else "—"
            since = s.subscribed_at.strftime("%H:%M:%S") if s.subscribed_at else "—"
            for col, txt in [
                (_S_TAG,   s.source_tag),
                (_S_TOPIC, s.topic_filter),
                (_S_TYPE,  s.sub_type),
                (_S_ROOT,  s.parent_root or ""),
                (_S_PKTS,  str(s.packet_count)),
                (_S_LAST,  last),
                (_S_SINCE, since),
            ]:
                it = QTableWidgetItem(txt)
                it.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                self._reg_tbl.setItem(row, col, it)
            self._reg_tbl.setRowHeight(row, 20)
        self._reg_tbl.setSortingEnabled(True)
        self._reg_count.setText(f"{len(subs)} registered subscription(s)")

    def _refresh_client(self) -> None:
        rows = []
        for cfg in self._app.source_manager.all_configs():
            if not cfg.enabled:
                continue
            stats = self._app.source_manager.get_stats(cfg.source_tag)
            cs    = stats.get("connected_since")
            since = cs.strftime("%H:%M:%S") if isinstance(cs, datetime) else "—"
            for topic in sorted(stats.get("active_topics", set())):
                rows.append((cfg.source_tag, topic, since))

        self._cli_tbl.setSortingEnabled(False)
        self._cli_tbl.setRowCount(len(rows))
        for row, (tag, topic, since) in enumerate(rows):
            for col, txt in [(_C_TAG, tag), (_C_TOPIC, topic), (_C_SINCE, since)]:
                it = QTableWidgetItem(txt)
                it.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                self._cli_tbl.setItem(row, col, it)
            self._cli_tbl.setRowHeight(row, 20)
        self._cli_tbl.setSortingEnabled(True)
        self._cli_count.setText(f"{len(rows)} topic(s) subscribed at broker")

    def closeEvent(self, event) -> None:
        self._timer.stop()
        super().closeEvent(event)
