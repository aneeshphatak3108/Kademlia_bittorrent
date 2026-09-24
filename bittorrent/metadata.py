"""Parsing of .torrent files (BEP3 format, bencoded).

The `info` dict is the part that matters: its SHA1 is the info_hash, which is
both the torrent's identity and -- being 20 bytes, like a NodeID -- the DHT key
peers rendezvous on (see bittorrent/discovery.py).

Single-file torrents only for now; multi-file (the `files` list) is not
supported, and parsing one raises rather than silently mis-reading it.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import List

from kademlia.bencode import BencodeError, bencode_decode, bencode_encode
from kademlia.identifier import ID_BYTES


class MetadataError(Exception):
    """Raised on a malformed or unsupported .torrent file."""


@dataclass
class TorrentMetadata:
    info_hash: bytes  # 20 bytes -- SHA1 of the bencoded info dict
    name: str
    piece_length: int
    piece_hashes: List[bytes]  # one 20-byte SHA1 per piece
    total_length: int

    @property
    def piece_count(self) -> int:
        return len(self.piece_hashes)

    def piece_size(self, index: int) -> int:
        """Length of piece `index` -- the final piece is usually short."""
        if not 0 <= index < self.piece_count:
            raise IndexError(f"piece index {index} out of range (have {self.piece_count})")
        if index < self.piece_count - 1:
            return self.piece_length
        remainder = self.total_length - self.piece_length * (self.piece_count - 1)
        return remainder

    def block_count(self, index: int, block_size: int) -> int:
        size = self.piece_size(index)
        return (size + block_size - 1) // block_size

    @classmethod
    def from_bytes(cls, raw: bytes) -> "TorrentMetadata":
        try:
            torrent = bencode_decode(raw)
        except BencodeError as exc:
            raise MetadataError(f"not valid bencode: {exc}") from exc
        if not isinstance(torrent, dict):
            raise MetadataError("torrent file must decode to a dict")

        info = torrent.get(b"info")
        if not isinstance(info, dict):
            raise MetadataError("torrent file has no 'info' dict")
        if b"files" in info:
            raise MetadataError("multi-file torrents are not supported")

        name = _require(info, b"name", bytes).decode("utf-8", errors="replace")
        piece_length = _require(info, b"piece length", int)
        pieces = _require(info, b"pieces", bytes)
        total_length = _require(info, b"length", int)

        if piece_length <= 0:
            raise MetadataError(f"piece length must be positive, got {piece_length}")
        if total_length < 0:
            raise MetadataError(f"length must be non-negative, got {total_length}")
        if len(pieces) % ID_BYTES != 0:
            raise MetadataError(f"'pieces' is not a multiple of {ID_BYTES} bytes")

        piece_hashes = [pieces[i : i + ID_BYTES] for i in range(0, len(pieces), ID_BYTES)]
        expected = (total_length + piece_length - 1) // piece_length if total_length else 0
        if len(piece_hashes) != expected:
            raise MetadataError(
                f"piece count mismatch: {len(piece_hashes)} hashes for {total_length} bytes "
                f"at {piece_length} bytes/piece (expected {expected})"
            )

        # info_hash is SHA1 over the bencoded info dict. Re-encoding is safe
        # here because bencode is canonical (dict keys sorted), so a
        # well-formed input round-trips to identical bytes.
        info_hash = hashlib.sha1(bencode_encode(info)).digest()

        return cls(
            info_hash=info_hash,
            name=name,
            piece_length=piece_length,
            piece_hashes=piece_hashes,
            total_length=total_length,
        )

    @classmethod
    def from_file(cls, path: str) -> "TorrentMetadata":
        with open(path, "rb") as fh:
            return cls.from_bytes(fh.read())


def build_torrent(data: bytes, name: str, piece_length: int) -> bytes:
    """Build a bencoded .torrent for `data`. Inverse of TorrentMetadata.from_bytes."""
    if piece_length <= 0:
        raise MetadataError(f"piece length must be positive, got {piece_length}")
    pieces = b"".join(
        hashlib.sha1(data[off : off + piece_length]).digest()
        for off in range(0, len(data), piece_length)
    )
    info = {
        b"name": name.encode("utf-8"),
        b"piece length": piece_length,
        b"pieces": pieces,
        b"length": len(data),
    }
    return bencode_encode({b"info": info})


def _require(d: dict, key: bytes, kind: type):
    if key not in d:
        raise MetadataError(f"info dict missing required key {key!r}")
    value = d[key]
    if not isinstance(value, kind):
        raise MetadataError(f"{key!r} should be {kind.__name__}, got {type(value).__name__}")
    return value
