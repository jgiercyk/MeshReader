"""Authoritative in-memory registry of actual live MQTT subscriptions.

Every subscribe() call registers here. Every unsubscribe() call removes here.
The UI reads from this registry — never from calculated desired state.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from threading import Lock
from typing import Dict, List, Optional, Set

# Sub-type constants — also used as display labels
DIRECT       = "direct"
ROOT_DERIVED = "root-derived"
MAP_TYPE     = "map"
RAW_TYPE     = "raw"
DISCOVERY    = "discovery"


@dataclass
class ActiveSub:
    topic_filter: str
    source_tag:   str
    sub_type:     str           # one of the constants above
    parent_root:  Optional[str] = None  # set for ROOT_DERIVED entries
    packet_count: int           = 0
    last_packet:  Optional[datetime] = None
    subscribed_at: datetime     = field(default_factory=datetime.now)


class SubscriptionRegistry:
    """Thread-safe, append-only registry of every currently-subscribed topic filter.

    Updated by the MQTT worker threads via callbacks; read by the main/UI thread.
    Lock protects all mutations; reads snapshot the dict to avoid holding the lock
    across Qt paint cycles.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._subs: Dict[str, ActiveSub] = {}   # topic_filter → ActiveSub

    # ── write API ────────────────────────────────────────────────────────────

    def register(
        self,
        topic: str,
        source_tag: str,
        sub_type: str,
        parent_root: Optional[str] = None,
    ) -> None:
        with self._lock:
            self._subs[topic] = ActiveSub(
                topic_filter=topic,
                source_tag=source_tag,
                sub_type=sub_type,
                parent_root=parent_root,
            )
        logging.debug("SubRegistry +%s  [%s]", topic, sub_type)

    def unregister(self, topic: str) -> None:
        with self._lock:
            self._subs.pop(topic, None)
        logging.debug("SubRegistry -%s", topic)

    def unregister_source(self, source_tag: str) -> List[str]:
        """Remove all subscriptions for a source. Returns the removed topics."""
        with self._lock:
            gone = [t for t, s in self._subs.items() if s.source_tag == source_tag]
            for t in gone:
                del self._subs[t]
        return gone

    def record_packet(self, topic: str) -> None:
        """Increment the packet counter on the subscription that matches this topic."""
        with self._lock:
            for t, s in self._subs.items():
                if topic_matches(topic, t):
                    s.packet_count += 1
                    s.last_packet = datetime.now()
                    break

    # ── read API ─────────────────────────────────────────────────────────────

    def get_active_roots(self) -> Set[str]:
        """Return the set of parent_roots for all live ROOT_DERIVED subscriptions."""
        with self._lock:
            return {
                s.parent_root
                for s in self._subs.values()
                if s.parent_root and s.sub_type == ROOT_DERIVED
            }

    def is_root_active(self, root: str) -> bool:
        with self._lock:
            return any(
                s.parent_root == root
                for s in self._subs.values()
                if s.sub_type == ROOT_DERIVED
            )

    def get_all(self) -> List[ActiveSub]:
        with self._lock:
            return list(self._subs.values())

    def snapshot(self) -> Dict[str, ActiveSub]:
        with self._lock:
            return dict(self._subs)

    def count(self) -> int:
        with self._lock:
            return len(self._subs)

    def clear_all(self) -> None:
        """Remove every entry from the registry. Call before a clean reconnect."""
        with self._lock:
            self._subs.clear()
        logging.debug("SubRegistry cleared")


# ── MQTT topic pattern matching ───────────────────────────────────────────────

def topic_matches(topic: str, pattern: str) -> bool:
    """Return True if a concrete topic string matches an MQTT subscription pattern.

    Supports '#' (multi-level wildcard) and '+' (single-level wildcard).
    """
    if not topic or not pattern:
        return False
    if "#" not in pattern and "+" not in pattern:
        return topic == pattern
    return _match_parts(topic.split("/"), pattern.split("/"), 0, 0)


def _match_parts(t: list, p: list, ti: int, pi: int) -> bool:
    while pi < len(p):
        seg = p[pi]
        if seg == "#":
            return True
        if ti >= len(t):
            return False
        if seg != "+" and seg != t[ti]:
            return False
        ti += 1
        pi += 1
    return ti == len(t)
