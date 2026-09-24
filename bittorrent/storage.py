"""On-disk piece storage.

Two decisions from sys_design.md §1 are baked in here:

* Blocks are written straight to their final offset as they arrive, rather
  than buffered in memory until a piece completes. That makes duplicate block
  deliveries (endgame mode, late retries) harmless by construction -- same
  bytes, same offset -- and means the file itself is the bookkeeping.
* No separate persisted bitfield. On startup we re-hash what's on disk and
  trust only what verifies. Slower to resume than a `.resume` file, but it has
  no write-ordering hazard: a half-written piece simply fails its hash and is
  treated as absent.

File I/O is blocking, so every method that touches the disk hops onto a thread
via run_in_executor -- calling open()/write() directly on the event loop would
stall every peer connection for the duration of the syscall.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
from typing import Set

from bittorrent.metadata import TorrentMetadata


class PieceStore:
    def __init__(self, path: str, metadata: TorrentMetadata):
        self.path = path
        self.metadata = metadata
        self._have: Set[int] = set()

    # -- lifecycle --

    async def open(self, allocate: bool = True) -> None:
        """Create the backing file if needed, then verify whatever is already there."""
        await _to_thread(self._open_sync, allocate)
        await self.rescan()

    def _open_sync(self, allocate: bool) -> None:
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        if not os.path.exists(self.path):
            with open(self.path, "wb") as fh:
                if allocate and self.metadata.total_length:
                    # Sparse allocation: seek past the end and write one byte, so
                    # the file has the right size without writing out its contents.
                    fh.seek(self.metadata.total_length - 1)
                    fh.write(b"\0")

    async def rescan(self) -> Set[int]:
        """Re-hash every piece on disk; `have` becomes exactly what verifies."""
        self._have = await _to_thread(self._rescan_sync)
        return set(self._have)

    def _rescan_sync(self) -> Set[int]:
        verified = set()
        if not os.path.exists(self.path):
            return verified
        if os.path.getsize(self.path) < self.metadata.total_length:
            return verified
        with open(self.path, "rb") as fh:
            for index in range(self.metadata.piece_count):
                fh.seek(index * self.metadata.piece_length)
                chunk = fh.read(self.metadata.piece_size(index))
                if hashlib.sha1(chunk).digest() == self.metadata.piece_hashes[index]:
                    verified.add(index)
        return verified

    # -- block level I/O --

    async def write_block(self, index: int, begin: int, data: bytes) -> None:
        self._check_block(index, begin, len(data))
        await _to_thread(self._write_sync, index * self.metadata.piece_length + begin, data)

    def _write_sync(self, offset: int, data: bytes) -> None:
        with open(self.path, "r+b") as fh:
            fh.seek(offset)
            fh.write(data)

    async def read_block(self, index: int, begin: int, length: int) -> bytes:
        self._check_block(index, begin, length)
        return await _to_thread(
            self._read_sync, index * self.metadata.piece_length + begin, length
        )

    def _read_sync(self, offset: int, length: int) -> bytes:
        with open(self.path, "rb") as fh:
            fh.seek(offset)
            return fh.read(length)

    def _check_block(self, index: int, begin: int, length: int) -> None:
        size = self.metadata.piece_size(index)  # raises IndexError on a bad index
        if begin < 0 or length < 0 or begin + length > size:
            raise ValueError(
                f"block [{begin}, {begin + length}) out of range for piece {index} (size {size})"
            )

    # -- piece level --

    async def verify_piece(self, index: int) -> bool:
        """Hash what's on disk for `index`; record it as have only if it matches."""
        data = await self.read_block(index, 0, self.metadata.piece_size(index))
        ok = hashlib.sha1(data).digest() == self.metadata.piece_hashes[index]
        if ok:
            self._have.add(index)
        else:
            self._have.discard(index)
        return ok

    def has_piece(self, index: int) -> bool:
        return index in self._have

    @property
    def have(self) -> Set[int]:
        return set(self._have)

    @property
    def is_complete(self) -> bool:
        return len(self._have) == self.metadata.piece_count

    @property
    def bytes_downloaded(self) -> int:
        return sum(self.metadata.piece_size(i) for i in self._have)

    def bitfield(self) -> bytes:
        """`have` as a BEP3 bitfield: MSB of byte 0 is piece 0, spare bits zero."""
        count = self.metadata.piece_count
        field = bytearray((count + 7) // 8)
        for index in self._have:
            field[index // 8] |= 128 >> (index % 8)
        return bytes(field)


def parse_bitfield(field: bytes, piece_count: int) -> Set[int]:
    """Inverse of PieceStore.bitfield, ignoring the spare trailing bits."""
    pieces = set()
    for index in range(piece_count):
        if field[index // 8] & (128 >> (index % 8)):
            pieces.add(index)
    return pieces


async def _to_thread(fn, *args):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, fn, *args)
