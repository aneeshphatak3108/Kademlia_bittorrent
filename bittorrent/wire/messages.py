"""The peer wire protocol: handshake plus length-prefixed binary messages.

Deliberately *not* bencode. Bencode is the right tool for metadata and for DHT
RPCs, but this is the hot path carrying the actual file data -- a fixed-size
binary header is cheaper to parse and frames cleanly on a TCP stream.

    handshake: <pstrlen=19><"BitTorrent protocol"><8 reserved><info_hash><peer_id>
    message:   <4-byte big-endian length><1-byte id><payload>
               (length 0 is a keep-alive and carries no id)

Every length read off the wire is bounds-checked *before* anything is
allocated for it -- a peer claiming a 4GB message should be disconnected, not
handed 4GB of buffer (sys_design.md §8).
"""

from __future__ import annotations

import asyncio
import struct
from dataclasses import dataclass, field
from typing import Union

PROTOCOL_NAME = b"BitTorrent protocol"
HANDSHAKE_LENGTH = 1 + len(PROTOCOL_NAME) + 8 + 20 + 20  # 68
PEER_ID_LENGTH = 20
INFO_HASH_LENGTH = 20

# Generous ceiling: the largest legitimate message is a `piece` (~16 KiB block
# plus a 9-byte header) or a bitfield (piece_count / 8 bytes). 1 MiB covers a
# bitfield for ~8M pieces while still rejecting anything absurd outright.
MAX_MESSAGE_SIZE = 1024 * 1024

CHOKE = 0
UNCHOKE = 1
INTERESTED = 2
NOT_INTERESTED = 3
HAVE = 4
BITFIELD = 5
REQUEST = 6
PIECE = 7
CANCEL = 8


class WireError(Exception):
    """Malformed data from a peer -- the connection should be dropped."""


@dataclass
class Handshake:
    info_hash: bytes
    peer_id: bytes
    reserved: bytes = field(default=b"\x00" * 8)

    def encode(self) -> bytes:
        if len(self.info_hash) != INFO_HASH_LENGTH:
            raise WireError(f"info_hash must be {INFO_HASH_LENGTH} bytes")
        if len(self.peer_id) != PEER_ID_LENGTH:
            raise WireError(f"peer_id must be {PEER_ID_LENGTH} bytes")
        return (
            bytes([len(PROTOCOL_NAME)])
            + PROTOCOL_NAME
            + self.reserved
            + self.info_hash
            + self.peer_id
        )

    @classmethod
    def decode(cls, raw: bytes) -> "Handshake":
        if len(raw) != HANDSHAKE_LENGTH:
            raise WireError(f"handshake must be {HANDSHAKE_LENGTH} bytes, got {len(raw)}")
        if raw[0] != len(PROTOCOL_NAME) or raw[1 : 1 + len(PROTOCOL_NAME)] != PROTOCOL_NAME:
            raise WireError("handshake does not identify the BitTorrent protocol")
        offset = 1 + len(PROTOCOL_NAME)
        return cls(
            reserved=raw[offset : offset + 8],
            info_hash=raw[offset + 8 : offset + 28],
            peer_id=raw[offset + 28 : offset + 48],
        )


@dataclass
class KeepAlive:
    pass


@dataclass
class Choke:
    pass


@dataclass
class Unchoke:
    pass


@dataclass
class Interested:
    pass


@dataclass
class NotInterested:
    pass


@dataclass
class Have:
    index: int


@dataclass
class Bitfield:
    field: bytes


@dataclass
class Request:
    index: int
    begin: int
    length: int


@dataclass
class Piece:
    index: int
    begin: int
    block: bytes


@dataclass
class Cancel:
    index: int
    begin: int
    length: int


Message = Union[
    KeepAlive, Choke, Unchoke, Interested, NotInterested,
    Have, Bitfield, Request, Piece, Cancel,
]

_EMPTY = {CHOKE: Choke, UNCHOKE: Unchoke, INTERESTED: Interested, NOT_INTERESTED: NotInterested}
_EMPTY_IDS = {Choke: CHOKE, Unchoke: UNCHOKE, Interested: INTERESTED, NotInterested: NOT_INTERESTED}


def encode(msg: Message) -> bytes:
    if isinstance(msg, KeepAlive):
        return struct.pack(">I", 0)

    msg_id = _EMPTY_IDS.get(type(msg))
    if msg_id is not None:
        payload = b""
    elif isinstance(msg, Have):
        msg_id, payload = HAVE, struct.pack(">I", msg.index)
    elif isinstance(msg, Bitfield):
        msg_id, payload = BITFIELD, msg.field
    elif isinstance(msg, Request):
        msg_id, payload = REQUEST, struct.pack(">III", msg.index, msg.begin, msg.length)
    elif isinstance(msg, Cancel):
        msg_id, payload = CANCEL, struct.pack(">III", msg.index, msg.begin, msg.length)
    elif isinstance(msg, Piece):
        msg_id, payload = PIECE, struct.pack(">II", msg.index, msg.begin) + msg.block
    else:
        raise WireError(f"cannot encode {type(msg)!r}")

    return struct.pack(">I", len(payload) + 1) + bytes([msg_id]) + payload


def decode(body: bytes) -> Message:
    """Decode one message body (the bytes after the length prefix)."""
    if not body:
        return KeepAlive()

    msg_id, payload = body[0], body[1:]

    if msg_id in _EMPTY:
        if payload:
            raise WireError(f"message id {msg_id} takes no payload, got {len(payload)} bytes")
        return _EMPTY[msg_id]()
    if msg_id == HAVE:
        if len(payload) != 4:
            raise WireError(f"have takes a 4-byte index, got {len(payload)} bytes")
        return Have(index=struct.unpack(">I", payload)[0])
    if msg_id == BITFIELD:
        return Bitfield(field=payload)
    if msg_id in (REQUEST, CANCEL):
        if len(payload) != 12:
            raise WireError(f"message id {msg_id} takes 12 bytes, got {len(payload)}")
        index, begin, length = struct.unpack(">III", payload)
        cls = Request if msg_id == REQUEST else Cancel
        return cls(index=index, begin=begin, length=length)
    if msg_id == PIECE:
        if len(payload) < 8:
            raise WireError(f"piece needs at least an 8-byte header, got {len(payload)}")
        index, begin = struct.unpack(">II", payload[:8])
        return Piece(index=index, begin=begin, block=payload[8:])

    raise WireError(f"unknown message id {msg_id}")


async def read_handshake(reader: asyncio.StreamReader) -> Handshake:
    raw = await reader.readexactly(HANDSHAKE_LENGTH)
    return Handshake.decode(raw)


async def read_message(reader: asyncio.StreamReader) -> Message:
    """Read one length-prefixed message, rejecting absurd lengths up front."""
    header = await reader.readexactly(4)
    (length,) = struct.unpack(">I", header)
    if length > MAX_MESSAGE_SIZE:
        raise WireError(f"peer claims a {length}-byte message (cap is {MAX_MESSAGE_SIZE})")
    if length == 0:
        return KeepAlive()
    return decode(await reader.readexactly(length))


async def write_message(writer: asyncio.StreamWriter, msg: Message) -> None:
    """Send a message, applying backpressure if the peer is slow to drain."""
    writer.write(encode(msg))
    await writer.drain()
