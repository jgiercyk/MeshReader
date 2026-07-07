"""
Background reverse geocoder using Nominatim (OpenStreetMap).

- Results are cached in SQLite, keyed to 2 decimal places (~1 km).
- Nominatim rate limit: 1 request per second.
- All network I/O happens on a daemon thread; results come back as Qt signals
  on the main thread.
- Never crashes the app: all network/parse errors return a coordinate string.
"""

import json
import threading
import time
import urllib.error
import urllib.request
from typing import Dict, List, Optional, Tuple

from PySide6.QtCore import QObject, Signal

_ROUND = 2          # cache key precision (~1 km at mid-latitudes)
_RATE  = 1.15       # seconds between Nominatim requests
_UA    = "MeshCommandPost/1.0 (meshtastic read-only monitor; contact via github)"


class ReverseGeocoder(QObject):
    """Emits location_ready(node_id, location_name) on the main thread."""

    location_ready = Signal(str, str)   # node_id, location_name

    def __init__(self, storage, parent=None):
        super().__init__(parent)
        self._storage = storage
        self._lock = threading.Lock()
        # (lat_key, lon_key) -> [node_id, ...]  — waiting for this key to resolve
        self._pending: Dict[Tuple[float, float], List[str]] = {}
        self._queue: List[Tuple[float, float]] = []
        self._last_req = 0.0
        threading.Thread(target=self._worker, daemon=True, name="geocoder").start()

    # ── Public API ───────────────────────────────────────────────────────────

    def request(self, node_id: str, lat: float, lon: float) -> None:
        """Queue a reverse-geocode lookup for node_id. No-op if already cached."""
        lat_k = round(lat, _ROUND)
        lon_k = round(lon, _ROUND)

        cached = self._storage.get_geocode(lat_k, lon_k)
        if cached is not None:
            self.location_ready.emit(node_id, cached)
            return

        key = (lat_k, lon_k)
        with self._lock:
            if key not in self._pending:
                self._pending[key] = []
                self._queue.append(key)
            if node_id not in self._pending[key]:
                self._pending[key].append(node_id)

    # ── Worker thread ────────────────────────────────────────────────────────

    def _worker(self) -> None:
        while True:
            key: Optional[Tuple[float, float]] = None
            with self._lock:
                if self._queue:
                    key = self._queue.pop(0)
            if key is None:
                time.sleep(0.25)
                continue

            lat_k, lon_k = key

            wait = _RATE - (time.time() - self._last_req)
            if wait > 0:
                time.sleep(wait)

            name = self._fetch(lat_k, lon_k)
            self._last_req = time.time()
            self._storage.set_geocode(lat_k, lon_k, name)

            with self._lock:
                node_ids = self._pending.pop(key, [])

            for nid in node_ids:
                self.location_ready.emit(nid, name)

    def _fetch(self, lat: float, lon: float) -> str:
        try:
            url = (
                f"https://nominatim.openstreetmap.org/reverse"
                f"?format=json&lat={lat}&lon={lon}&zoom=10"
            )
            req = urllib.request.Request(url, headers={"User-Agent": _UA})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
            return _format_name(data)
        except Exception:
            return f"{lat:.2f}°, {lon:.2f}°"


def _format_name(data: dict) -> str:
    addr = data.get("address", {})
    city = next(
        (addr[k] for k in ("city", "town", "village", "hamlet", "suburb", "county")
         if k in addr),
        "",
    )
    state = addr.get("state", "")
    if city and state:
        return f"{city}, {state}"
    if city:
        return city
    if state:
        return state
    return (data.get("display_name") or "Unknown location")[:60]
