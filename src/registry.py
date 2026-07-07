"""
Central in-memory node registry.  Single source of truth for UI display.
Updated by app.py after every node upsert; read by all UI widgets.
"""

from typing import Dict, List, Optional

from models import Node

_EM = "—"   # em dash


class NodeRegistry:
    def __init__(self):
        self._nodes: Dict[str, Node] = {}

    # ── Maintenance ───────────────────────────────────────────────────────────

    def update(self, node: Node) -> None:
        self._nodes[node.node_id] = node

    def load_many(self, nodes: List[Node]) -> None:
        for n in nodes:
            self._nodes[n.node_id] = n

    def get(self, node_id: str) -> Optional[Node]:
        return self._nodes.get(node_id)

    def all_nodes(self) -> List[Node]:
        return list(self._nodes.values())

    # ── Display helpers ───────────────────────────────────────────────────────

    def node_display(self, node_id: str) -> str:
        """Best available label: 'Name — !id — Location'."""
        if not node_id:
            return ""
        node = self._nodes.get(node_id)
        loc = self._location_str(node)
        name = (node.long_name or node.short_name or "") if node else ""
        if name:
            return f"{name} {_EM} {node_id} {_EM} {loc}"
        return f"{node_id} {_EM} {loc}"

    def node_display_int(self, num: Optional[int]) -> str:
        """Same as node_display but accepts the raw integer node number."""
        if num is None:
            return ""
        if num == 4294967295:
            return "BROADCAST"
        return self.node_display(f"!{num:08x}")

    def location_str(self, node_id: str) -> str:
        return self._location_str(self._nodes.get(node_id))

    # ── Filtering ─────────────────────────────────────────────────────────────

    def get_filtered(
        self,
        source_filter: str = "all",
        local_only: bool = False,
        stale_only: bool = False,
    ) -> List[Node]:
        nodes = list(self._nodes.values())

        if source_filter == "mqtt":
            nodes = [n for n in nodes if "mqtt_json" in n.sources_seen]
        elif source_filter == "map":
            nodes = [n for n in nodes if any(s != "mqtt_json" for s in n.sources_seen)]
        elif source_filter == "both":
            nodes = [n for n in nodes
                     if "mqtt_json" in n.sources_seen
                     and any(s != "mqtt_json" for s in n.sources_seen)]
        elif source_filter == "mqtt_only":
            nodes = [n for n in nodes
                     if "mqtt_json" in n.sources_seen
                     and not any(s != "mqtt_json" for s in n.sources_seen)]
        elif source_filter == "map_only":
            nodes = [n for n in nodes
                     if "mqtt_json" not in n.sources_seen and n.sources_seen]

        if local_only:
            nodes = [n for n in nodes if n.is_local]

        if stale_only:
            nodes = [n for n in nodes if n.status in ("Stale", "Old", "Reference Only")]

        return nodes

    # ── Stats ─────────────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        nodes = list(self._nodes.values())
        total = len(nodes)
        mqtt_ids = {n.node_id for n in nodes if "mqtt_json" in n.sources_seen}
        map_ids  = {n.node_id for n in nodes if any(s != "mqtt_json" for s in n.sources_seen)}
        both     = mqtt_ids & map_ids
        return {
            "total":        total,
            "mqtt":         len(mqtt_ids),
            "map":          len(map_ids),
            "both":         len(both),
            "mqtt_only":    len(mqtt_ids - map_ids),
            "map_only":     len(map_ids - mqtt_ids),
            "local":        sum(1 for n in nodes if n.is_local),
            "local_active": sum(1 for n in nodes if n.is_local and n.status == "Active"),
            "has_gps":      sum(1 for n in nodes if n.latitude is not None),
            "active":       sum(1 for n in nodes if n.status == "Active"),
            "stale":        sum(1 for n in nodes if n.status in ("Stale", "Old")),
            "reference":    sum(1 for n in nodes if n.status == "Reference Only"),
        }

    # ── Internal ──────────────────────────────────────────────────────────────

    @staticmethod
    def _location_str(node: Optional[Node]) -> str:
        if node is None:
            return "Unknown location"
        if node.location_name:
            return node.location_name
        if node.latitude is not None and node.longitude is not None:
            return f"{node.latitude:.4f}, {node.longitude:.4f}"
        return "Unknown location"
