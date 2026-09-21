"""160-bit node/key identifiers and the Kademlia XOR distance metric."""

from __future__ import annotations

import os
import random

from kademlia.constants import ID_BITS

ID_BYTES = ID_BITS // 8


class NodeID:
    """An immutable 160-bit identifier, used for both node IDs and storage keys."""

    __slots__ = ("_int", "_bytes")

    def __init__(self, value: bytes):
        if len(value) != ID_BYTES:
            raise ValueError(f"NodeID must be exactly {ID_BYTES} bytes, got {len(value)}")
        self._bytes = bytes(value)
        self._int = int.from_bytes(self._bytes, byteorder="big")

    @classmethod
    def random(cls) -> "NodeID":
        return cls(os.urandom(ID_BYTES))

    @classmethod
    def from_int(cls, value: int) -> "NodeID":
        return cls(value.to_bytes(ID_BYTES, byteorder="big"))

    @classmethod
    def random_in_bucket_range(cls, bucket_index: int, own_id: "NodeID") -> "NodeID":
        """Generate a random ID whose XOR distance from own_id falls in bucket
        `bucket_index` (i.e. distance.bit_length() - 1 == bucket_index)."""
        if not 0 <= bucket_index < ID_BITS:
            raise ValueError(f"bucket_index must be in [0, {ID_BITS}), got {bucket_index}")
        # A distance with highest set bit == bucket_index: fix that bit to 1,
        # randomize the lower bits.
        low_bits = random.getrandbits(bucket_index) if bucket_index > 0 else 0
        distance = (1 << bucket_index) | low_bits
        return cls.from_int(own_id._int ^ distance)

    @property
    def bytes(self) -> bytes:
        return self._bytes

    @property
    def as_int(self) -> int:
        return self._int

    def distance(self, other: "NodeID") -> int:
        return self._int ^ other._int

    def bucket_index(self, other: "NodeID") -> int:
        """Index of the bucket `other` belongs to, relative to self."""
        d = self.distance(other)
        if d == 0:
            raise ValueError("distance is zero; identical IDs have no bucket index")
        return d.bit_length() - 1

    def __eq__(self, other):
        return isinstance(other, NodeID) and self._bytes == other._bytes

    def __hash__(self):
        return hash(self._bytes)

    def __repr__(self):
        return f"NodeID({self._bytes.hex()})"

    def __str__(self):
        return self._bytes.hex()
