"""K-buckets and the Kademlia routing table."""

from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import List, Optional

from kademlia.constants import ID_BITS, K
from kademlia.identifier import NodeID


@dataclass
class Contact:
    node_id: NodeID
    ip: str
    port: int
    last_seen: float = field(default_factory=time.monotonic)

    def __eq__(self, other):
        return isinstance(other, Contact) and self.node_id == other.node_id

    def __hash__(self):
        return hash(self.node_id)

    def __repr__(self):
        return f"Contact({self.node_id}, {self.ip}:{self.port})"


class KBucket:
    """Holds up to K contacts, ordered least-recently-seen first."""

    def __init__(self, capacity: int = K):
        self.capacity = capacity
        self.contacts: "OrderedDict[NodeID, Contact]" = OrderedDict()
        self.replacement_cache: "OrderedDict[NodeID, Contact]" = OrderedDict()
        self.last_touched: float = time.monotonic()

    def is_full(self) -> bool:
        return len(self.contacts) >= self.capacity

    def has_contact(self, node_id: NodeID) -> bool:
        return node_id in self.contacts

    def get_contacts(self) -> List[Contact]:
        return list(self.contacts.values())

    def add_contact(self, contact: Contact) -> Optional[Contact]:
        """Insert or refresh a contact as most-recently-seen.

        Returns the least-recently-seen contact if the bucket is full and
        `contact` is new (caller should ping it before evicting), else None.
        """
        self.last_touched = time.monotonic()
        if contact.node_id in self.contacts:
            del self.contacts[contact.node_id]
            self.contacts[contact.node_id] = contact
            return None
        if not self.is_full():
            self.contacts[contact.node_id] = contact
            return None
        self._offer_replacement(contact)
        return next(iter(self.contacts.values()))

    def _offer_replacement(self, contact: Contact) -> None:
        if contact.node_id in self.replacement_cache:
            del self.replacement_cache[contact.node_id]
        self.replacement_cache[contact.node_id] = contact
        while len(self.replacement_cache) > self.capacity:
            self.replacement_cache.popitem(last=False)

    def remove_contact(self, node_id: NodeID) -> None:
        self.contacts.pop(node_id, None)
        if self.replacement_cache:
            replacement_id, replacement = self.replacement_cache.popitem(last=True)
            self.contacts[replacement_id] = replacement

    def promote_replacement_after_failed_ping(self, node_id: NodeID) -> None:
        """The LRS contact `node_id` failed to respond to a ping: evict it
        and, if available, promote the most recently seen replacement."""
        self.remove_contact(node_id)


class RoutingTable:
    """Fixed array of ID_BITS k-buckets, indexed by XOR-distance bit length."""

    def __init__(self, owner_id: NodeID):
        self.owner_id = owner_id
        self.buckets: List[KBucket] = [KBucket() for _ in range(ID_BITS)]

    def bucket_for(self, node_id: NodeID) -> int:
        return self.owner_id.bucket_index(node_id)

    def add_contact(self, contact: Contact) -> Optional[Contact]:
        if contact.node_id == self.owner_id:
            return None
        index = self.bucket_for(contact.node_id)
        return self.buckets[index].add_contact(contact)

    def remove_contact(self, node_id: NodeID) -> None:
        if node_id == self.owner_id:
            return
        index = self.bucket_for(node_id)
        self.buckets[index].remove_contact(node_id)

    def find_closest(self, target: NodeID, count: int = K) -> List[Contact]:
        all_contacts = self.all_contacts()
        all_contacts.sort(key=lambda c: target.distance(c.node_id))
        return all_contacts[:count]

    def all_contacts(self) -> List[Contact]:
        result: List[Contact] = []
        for bucket in self.buckets:
            result.extend(bucket.get_contacts())
        return result

    def buckets_needing_refresh(self, idle_threshold: float) -> List[int]:
        now = time.monotonic()
        return [
            i
            for i, bucket in enumerate(self.buckets)
            if now - bucket.last_touched >= idle_threshold
        ]

    def empty_buckets(self) -> List[int]:
        return [i for i, bucket in enumerate(self.buckets) if not bucket.contacts]
