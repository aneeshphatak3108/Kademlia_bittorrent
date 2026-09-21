"""Local key/value store with TTL expiration and republish bookkeeping."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, List, Tuple

from kademlia.constants import TTL_DEFAULT


@dataclass
class StoredItem:
    value: bytes
    stored_at: float
    expires_at: float
    last_republished: float


class DataStore:
    def __init__(self):
        self._items: Dict[bytes, StoredItem] = {}

    def put(self, key: bytes, value: bytes, ttl: int = TTL_DEFAULT) -> None:
        now = time.monotonic()
        self._items[key] = StoredItem(
            value=value,
            stored_at=now,
            expires_at=now + ttl,
            last_republished=now,
        )

    def get(self, key: bytes) -> "bytes | None":
        item = self._items.get(key)
        if item is None:
            return None
        if time.monotonic() >= item.expires_at:
            return None
        return item.value

    def __contains__(self, key: bytes) -> bool:
        return self.get(key) is not None

    def purge_expired(self) -> List[bytes]:
        now = time.monotonic()
        expired = [key for key, item in self._items.items() if now >= item.expires_at]
        for key in expired:
            del self._items[key]
        return expired

    def items_due_for_republish(self, interval: float) -> List[Tuple[bytes, bytes, float]]:
        """Returns (key, value, remaining_ttl) for items due to be re-pushed
        to the network. remaining_ttl (time until this item's own expiry) is
        included so republishing doesn't silently reset a short-TTL item's
        lifetime to the default TTL on the peers it's re-sent to."""
        now = time.monotonic()
        due = []
        for key, item in self._items.items():
            if now >= item.expires_at:
                continue
            if now - item.last_republished >= interval:
                due.append((key, item.value, item.expires_at - now))
        return due

    def mark_republished(self, key: bytes) -> None:
        item = self._items.get(key)
        if item is not None:
            item.last_republished = time.monotonic()

    def snapshot(self) -> Dict[bytes, bytes]:
        now = time.monotonic()
        return {k: v.value for k, v in self._items.items() if now < v.expires_at}
