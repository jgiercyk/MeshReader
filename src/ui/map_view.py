import json
import logging
import os
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from PySide6.QtCore import Qt, QUrl
from PySide6.QtWidgets import (
    QHBoxLayout, QLabel, QPushButton, QVBoxLayout, QWidget,
)

from intelligence import node_has_valid_position

try:
    from PySide6.QtWebEngineWidgets import QWebEngineView
    from PySide6.QtWebEngineCore import QWebEngineSettings
    _HAS_WEBENGINE = True
except ImportError:
    _HAS_WEBENGINE = False

from models import Node

# ── Static HTML page ──────────────────────────────────────────────────────────
# Loaded once; all marker updates use runJavaScript so zoom/pan is preserved.

_MAP_HTML = r"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
<title>Mesh Map</title>
<link rel="stylesheet"
      href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
      crossorigin=""/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
        crossorigin=""></script>
<style>
  html, body, #map { height: 100%; margin: 0; padding: 0; }
</style>
</head>
<body>
<div id="map"></div>
<script>
var meshMap = L.map('map', { scrollWheelZoom: true }).setView([20, 0], 2);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
  maxZoom: 19
}).addTo(meshMap);

var meshMarkers = {};

function addOrUpdateMarker(n) {
  if (n.lat == null || n.lon == null) return;
  if (n.lat < -90 || n.lat > 90 || n.lon < -180 || n.lon > 180) return;

  var loc      = n.location    ? '<br/><i>' + n.location + '</i>'          : '';
  var alt      = n.alt         ? '<br/>Alt: ' + n.alt + ' m'               : '';
  var hw       = n.hardware    ? '<br/>HW: ' + n.hardware                   : '';
  var dist     = n.distance    ? '<br/>Dist: ' + n.distance                 : '';
  var status   = n.status      ? '<br/>Status: <b>' + n.status + '</b>'    : '';
  var src      = n.sources     ? '<br/>Sources: ' + n.sources               : '';
  var first    = n.first_seen  ? '<br/>First seen: ' + n.first_seen         : '';
  var posAge   = n.pos_age     ? '<br/>Position: ' + n.pos_age              : '';
  var posStale = n.pos_stale   ? ' <span style="color:#cc6600">(stale)</span>' : '';
  var posSrc   = n.pos_src     ? '<br/>Pos source: <b>' + n.pos_src + '</b>' : '';
  var approx   = (n.pos_prec != null && n.pos_prec > 0 && n.pos_prec < 32)
    ? '<br/><span style="color:#cc6600">&#x26A0; Approximate location (' + n.pos_prec + ' bits)</span>'
    : '';

  var popup =
    '<b>' + (n.name || n.id) + '</b>' +
    '<br/>ID: ' + n.id +
    (n.short_name ? '<br/>Short: ' + n.short_name : '') +
    status + src + loc + dist + alt + hw +
    '<br/>Last type: ' + (n.last_type || '?') +
    (n.last_heard ? '<br/>Last seen: ' + n.last_heard : '') +
    posAge + posStale + posSrc + approx +
    first +
    '<br/>Pkts: ' + (n.packet_count || 0);

  if (meshMarkers[n.id]) {
    meshMarkers[n.id].setLatLng([n.lat, n.lon]);
    meshMarkers[n.id].setPopupContent(popup);
  } else {
    var label = n.short_name || (n.name ? n.name.split(' ')[0] : null);
    var m = L.marker([n.lat, n.lon]).addTo(meshMap).bindPopup(popup);
    if (label) {
      m.bindTooltip(label, { permanent: false, direction: 'top' });
    }
    meshMarkers[n.id] = m;
  }
}

function removeMarker(id) {
  if (meshMarkers[id]) {
    meshMap.removeLayer(meshMarkers[id]);
    delete meshMarkers[id];
  }
}

function fitAllNodes() {
  var latlngs = Object.values(meshMarkers).map(function(m) {
    return m.getLatLng();
  });
  if (latlngs.length === 1) {
    meshMap.setView(latlngs[0], 11);
  } else if (latlngs.length > 1) {
    meshMap.fitBounds(latlngs, { padding: [40, 40] });
  }
}

function clearMarkers() {
  Object.values(meshMarkers).forEach(function(m) {
    meshMap.removeLayer(m);
  });
  meshMarkers = {};
}

function jumpToNode(lat, lng, nodeId) {
  var currentZoom = meshMap.getZoom();
  var targetZoom = Math.max(currentZoom, 13);
  meshMap.setView([lat, lng], targetZoom);
  if (meshMarkers[nodeId]) {
    meshMarkers[nodeId].openPopup();
  }
}
</script>
</body>
</html>
"""


# ── Auto-focus subclass ───────────────────────────────────────────────────────

if _HAS_WEBENGINE:
    class _MapWebView(QWebEngineView):
        def enterEvent(self, event):
            self.setFocus()
            super().enterEvent(event)
else:
    _MapWebView = None  # type: ignore[assignment,misc]


# ── Widget ────────────────────────────────────────────────────────────────────

class MapViewWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._nodes: Dict[str, Node] = {}
        self._page_ready = False
        self._pending_js: List[str] = []
        self._html_path: Optional[str] = None
        self._last_positions: Dict[str, tuple] = {}   # node_id → (lat, lon)
        self._visibility_hours: Optional[int] = 168
        self._setup_ui()

    # ── Setup ─────────────────────────────────────────────────────────────────

    def _setup_ui(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(4, 4, 4, 4)
        root.setSpacing(4)

        bar = QHBoxLayout()
        self._info_lbl = QLabel("No nodes with GPS position yet.")
        bar.addWidget(self._info_lbl)
        bar.addStretch()

        refresh_btn = QPushButton("Refresh Map")
        refresh_btn.clicked.connect(self._refresh_all)
        bar.addWidget(refresh_btn)

        fit_btn = QPushButton("Fit All Nodes")
        fit_btn.clicked.connect(self._fit_all)
        bar.addWidget(fit_btn)
        root.addLayout(bar)

        if _HAS_WEBENGINE:
            self._view = _MapWebView()
            self._view.settings().setAttribute(
                QWebEngineSettings.WebAttribute.LocalContentCanAccessRemoteUrls, True
            )
            self._view.loadFinished.connect(self._on_load_finished)
            root.addWidget(self._view)
            self._load_page()
        else:
            lbl = QLabel(
                "Map requires PySide6 with Qt WebEngine.\n"
                "Reinstall PySide6 and restart the app.\n\n"
                "Nodes with GPS coordinates still appear in the Node List tab."
            )
            lbl.setAlignment(Qt.AlignCenter)
            root.addWidget(lbl)
            self._view = None  # type: ignore[assignment]

    def _load_page(self):
        if self._html_path is None:
            fd, self._html_path = tempfile.mkstemp(suffix=".html", prefix="mesh_map_")
            os.close(fd)
            with open(self._html_path, "w", encoding="utf-8") as f:
                f.write(_MAP_HTML)
        self._view.load(QUrl.fromLocalFile(self._html_path))

    # ── Page-ready handling ───────────────────────────────────────────────────

    def _on_load_finished(self, ok: bool) -> None:
        if not ok:
            return
        self._page_ready = True
        for js in self._pending_js:
            self._view.page().runJavaScript(js)
        self._pending_js.clear()

    def _run_js(self, js: str) -> None:
        if self._page_ready:
            self._view.page().runJavaScript(js)
        else:
            self._pending_js.append(js)

    # ── Public API ────────────────────────────────────────────────────────────

    def set_visibility_hours(self, hours: Optional[int]) -> None:
        """Set the visibility window and refresh the map — removes stale markers."""
        self._visibility_hours = hours
        self._refresh_all()

    def upsert_node(self, node: Node) -> None:
        self._nodes[node.node_id] = node
        self._update_label()
        if not node_has_valid_position(node) or not self._is_visible(node):
            # Position invalid or outside window — remove any stale marker
            if node.node_id in self._last_positions:
                del self._last_positions[node.node_id]
                self._run_js(f"removeMarker({json.dumps(node.node_id)});")
            return
        pos = (node.latitude, node.longitude)
        if self._last_positions.get(node.node_id) != pos:
            self._last_positions[node.node_id] = pos
            self._run_js(f"addOrUpdateMarker({self._node_json(node)});")

    def load_nodes(self, nodes: List[Node]) -> None:
        self._nodes = {n.node_id: n for n in nodes}
        self._last_positions = {}
        self._run_js("clearMarkers();")
        self._update_label()
        for node in nodes:
            if node_has_valid_position(node) and self._is_visible(node):
                self._last_positions[node.node_id] = (node.latitude, node.longitude)
                self._run_js(f"addOrUpdateMarker({self._node_json(node)});")

    # ── Internal ──────────────────────────────────────────────────────────────

    def _is_visible(self, node: Node) -> bool:
        if self._visibility_hours is None:
            return True
        if node.last_heard is None:
            return False
        cutoff = datetime.now() - timedelta(hours=self._visibility_hours)
        return node.last_heard >= cutoff

    def _refresh_all(self) -> None:
        """Clear all markers and re-add only visible nodes with valid GPS."""
        self._run_js("clearMarkers();")
        self._last_positions.clear()
        for node in self._nodes.values():
            if node_has_valid_position(node) and self._is_visible(node):
                self._last_positions[node.node_id] = (node.latitude, node.longitude)
                self._run_js(f"addOrUpdateMarker({self._node_json(node)});")

    def _fit_all(self) -> None:
        self._run_js("fitAllNodes();")

    def _update_label(self) -> None:
        all_with_gps = sum(1 for n in self._nodes.values() if node_has_valid_position(n))
        visible_with_gps = len(self._last_positions)
        total = len(self._nodes)
        self._info_lbl.setText(
            f"{visible_with_gps} visible GPS  /  {all_with_gps} total GPS  /  {total} known"
        )

    def jump_to(self, lat: float, lon: float, node_id: str) -> None:
        """Pan/zoom the Leaflet map to (lat, lon) and open the node's popup."""
        js = f"jumpToNode({lat}, {lon}, {json.dumps(node_id)});"
        self._run_js(js)

    @staticmethod
    def _pos_age_str(node: Node) -> str:
        if node.last_position_seen is None:
            return ""
        delta = datetime.now() - node.last_position_seen
        h = delta.total_seconds() / 3600
        if h < 1:
            return f"{int(delta.total_seconds() / 60)}m ago"
        if h < 24:
            return f"{h:.0f}h ago"
        return f"{h / 24:.1f}d ago"

    def _node_json(self, node: Node) -> str:
        if not node_has_valid_position(node):
            logging.error("marker attempted for node without valid position: %s", node.node_id)
        dist_str = f"{node.distance_miles:.1f} mi" if node.distance_miles is not None else ""
        pos_age  = self._pos_age_str(node)
        pos_stale = False
        if node.last_position_seen is not None:
            age_h = (datetime.now() - node.last_position_seen).total_seconds() / 3600
            pos_stale = age_h > 24
        data = {
            "id":           node.node_id,
            "name":         node.long_name or node.short_name or node.node_id,
            "short_name":   node.short_name or "",
            "lat":          node.latitude,
            "lon":          node.longitude,
            "alt":          int(node.altitude) if node.altitude is not None else None,
            "last_heard":   node.last_heard.strftime("%Y-%m-%d %H:%M") if node.last_heard else "",
            "first_seen":   node.first_seen.strftime("%Y-%m-%d %H:%M") if node.first_seen else "",
            "last_type":    node.last_packet_type or "",
            "hardware":     node.hardware or "",
            "location":     node.location_name or "",
            "status":       node.status,
            "sources":      node.source_label(),
            "distance":     dist_str,
            "packet_count": node.packet_count,
            "pos_age":      pos_age,
            "pos_stale":    pos_stale,
            "pos_src":      node.position_source or "",
            "pos_prec":     node.position_precision,
        }
        return json.dumps(data)
