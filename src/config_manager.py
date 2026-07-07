import json
from pathlib import Path
from typing import Any, List

_DEFAULTS: dict = {
    "home_latitude": 34.8526,
    "home_longitude": -82.3940,
    "local_radius_miles": 50.0,
    "active_minutes": 15,
    "recent_hours": 8,
    "old_days": 7,
    "geocoding_enabled": True,
    "reference_layer_enabled": True,
    "visibility_hours": 168,   # 7 days; 0 or None = All Known
    "packet_retain_hours": 48,
    "packet_retain_max_rows": 15_000,
    "mqtt_roots": ["msh/US/SC"],   # fallback for fresh installs; msh/US not auto-subscribed
    "discovery_interval_minutes": 0,     # 0 = disabled (default: off for stability)
    "discovery_duration_seconds": 60,
}


class ConfigManager:
    def __init__(self, path: Path):
        self._path = path
        self._data: dict = {}
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            try:
                with open(self._path, encoding="utf-8") as f:
                    self._data = json.load(f)
            except Exception:
                self._data = {}

    def save(self) -> None:
        try:
            with open(self._path, "w", encoding="utf-8") as f:
                json.dump(self._data, f, indent=2)
        except Exception:
            pass

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, _DEFAULTS.get(key, default))

    def set(self, key: str, value: Any) -> None:
        self._data[key] = value
        self.save()

    @staticmethod
    def normalize_root(root: str) -> str:
        """Normalize an MQTT root: forward slashes, no trailing slash."""
        return root.replace("\\", "/").strip().rstrip("/")

    @property
    def mqtt_roots(self) -> List[str]:
        raw = self.get("mqtt_roots")
        if isinstance(raw, list):
            normed = [ConfigManager.normalize_root(r) for r in raw if r and str(r).strip()]
            return normed if normed else ["msh/US/SC"]
        return ["msh/US/SC"]

    def set_mqtt_roots(self, roots: List[str]) -> None:
        normed = [ConfigManager.normalize_root(r) for r in roots if r and str(r).strip()]
        self.set("mqtt_roots", normed)

    @property
    def home_lat(self) -> float:
        return float(self.get("home_latitude"))

    @property
    def home_lon(self) -> float:
        return float(self.get("home_longitude"))

    @property
    def local_radius(self) -> float:
        return float(self.get("local_radius_miles"))

    @property
    def active_minutes(self) -> int:
        return int(self.get("active_minutes"))

    @property
    def recent_hours(self) -> int:
        return int(self.get("recent_hours"))

    @property
    def old_days(self) -> int:
        return int(self.get("old_days"))

    @property
    def visibility_hours(self):
        """Hours for the node/map visibility window.  None = show all known nodes."""
        val = self.get("visibility_hours")
        if val is None or val == 0:
            return None
        return int(val)

    @property
    def packet_retain_hours(self) -> int:
        return int(self.get("packet_retain_hours"))

    @property
    def packet_retain_max_rows(self) -> int:
        return int(self.get("packet_retain_max_rows"))

    @property
    def discovery_interval_minutes(self) -> int:
        """Minutes between automatic discovery runs. 0 = disabled."""
        return int(self.get("discovery_interval_minutes"))

    @property
    def discovery_duration_seconds(self) -> int:
        """How long each discovery run subscribes broadly."""
        return int(self.get("discovery_duration_seconds"))
