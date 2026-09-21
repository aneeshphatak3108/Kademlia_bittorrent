"""KRPC-style (BEP5-inspired) message shapes for the 4 Kademlia RPCs, bencoded on the wire.

Envelope: {"t": <tx-id>, "y": "q"|"r"|"e", ...}
  query:    adds "q" (method name) and "a" (args dict, always includes "id")
  response: adds "r" (dict, always includes "id")
  error:    adds "e" = [code, message]
"""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass, field
from typing import List, Optional, Union

from kademlia.bencode import BencodeError, bencode_decode, bencode_encode
from kademlia.identifier import ID_BYTES, NodeID
from kademlia.routing_table import Contact

COMPACT_NODE_SIZE = ID_BYTES + 4 + 2  # id + IPv4 + port


class MalformedMessageError(Exception):
    """Raised when a received datagram doesn't decode into a valid message.

    `tid` is set whenever the transaction ID was successfully parsed before
    the failure, so the protocol layer can still reply with a proper KRPC
    error envelope instead of silently dropping the datagram.
    """

    tid: Optional[bytes] = None


def encode_compact_nodes(contacts: List[Contact]) -> bytes:
    out = bytearray()
    for c in contacts:
        try:
            ip_bytes = ipaddress.IPv4Address(c.ip).packed
        except ipaddress.AddressValueError as exc:
            raise MalformedMessageError(f"non-IPv4 address in compact node encode: {c.ip}") from exc
        out += c.node_id.bytes + ip_bytes + c.port.to_bytes(2, "big")
    return bytes(out)


def decode_compact_nodes(data: bytes) -> List[Contact]:
    if len(data) % COMPACT_NODE_SIZE != 0:
        raise MalformedMessageError("compact node data is not a multiple of the record size")
    contacts = []
    for offset in range(0, len(data), COMPACT_NODE_SIZE):
        chunk = data[offset : offset + COMPACT_NODE_SIZE]
        node_id = NodeID(chunk[:ID_BYTES])
        ip = str(ipaddress.IPv4Address(chunk[ID_BYTES : ID_BYTES + 4]))
        port = int.from_bytes(chunk[ID_BYTES + 4 : ID_BYTES + 6], "big")
        contacts.append(Contact(node_id=node_id, ip=ip, port=port))
    return contacts


@dataclass
class PingQuery:
    tid: bytes
    sender_id: NodeID


@dataclass
class PingResponse:
    tid: bytes
    responder_id: NodeID


@dataclass
class StoreQuery:
    tid: bytes
    sender_id: NodeID
    key: bytes
    value: bytes
    ttl: Optional[int] = None


@dataclass
class StoreResponse:
    tid: bytes
    responder_id: NodeID
    status: str = "ok"


@dataclass
class FindNodeQuery:
    tid: bytes
    sender_id: NodeID
    target: NodeID


@dataclass
class FindNodeResponse:
    tid: bytes
    responder_id: NodeID
    nodes: List[Contact] = field(default_factory=list)


@dataclass
class FindValueQuery:
    tid: bytes
    sender_id: NodeID
    key: bytes


@dataclass
class FindValueResponse:
    tid: bytes
    responder_id: NodeID
    value: Optional[bytes] = None
    nodes: Optional[List[Contact]] = None


@dataclass
class ErrorMessage:
    tid: bytes
    code: int
    message: str


Query = Union[PingQuery, StoreQuery, FindNodeQuery, FindValueQuery]
Response = Union[PingResponse, StoreResponse, FindNodeResponse, FindValueResponse]
Message = Union[Query, Response, ErrorMessage]

_QUERY_METHOD_NAMES = {
    PingQuery: b"ping",
    StoreQuery: b"store",
    FindNodeQuery: b"find_node",
    FindValueQuery: b"find_value",
}


def encode(msg: Message) -> bytes:
    if isinstance(msg, PingQuery):
        envelope = {b"t": msg.tid, b"y": b"q", b"q": b"ping", b"a": {b"id": msg.sender_id.bytes}}
    elif isinstance(msg, StoreQuery):
        args = {b"id": msg.sender_id.bytes, b"key": msg.key, b"value": msg.value}
        if msg.ttl is not None:
            args[b"ttl"] = msg.ttl
        envelope = {b"t": msg.tid, b"y": b"q", b"q": b"store", b"a": args}
    elif isinstance(msg, FindNodeQuery):
        envelope = {
            b"t": msg.tid,
            b"y": b"q",
            b"q": b"find_node",
            b"a": {b"id": msg.sender_id.bytes, b"target": msg.target.bytes},
        }
    elif isinstance(msg, FindValueQuery):
        envelope = {
            b"t": msg.tid,
            b"y": b"q",
            b"q": b"find_value",
            b"a": {b"id": msg.sender_id.bytes, b"key": msg.key},
        }
    elif isinstance(msg, PingResponse):
        envelope = {b"t": msg.tid, b"y": b"r", b"r": {b"id": msg.responder_id.bytes}}
    elif isinstance(msg, StoreResponse):
        envelope = {
            b"t": msg.tid,
            b"y": b"r",
            b"r": {b"id": msg.responder_id.bytes, b"status": msg.status.encode("ascii")},
        }
    elif isinstance(msg, FindNodeResponse):
        envelope = {
            b"t": msg.tid,
            b"y": b"r",
            b"r": {b"id": msg.responder_id.bytes, b"nodes": encode_compact_nodes(msg.nodes)},
        }
    elif isinstance(msg, FindValueResponse):
        r = {b"id": msg.responder_id.bytes}
        if msg.value is not None:
            r[b"value"] = msg.value
        else:
            r[b"nodes"] = encode_compact_nodes(msg.nodes or [])
        envelope = {b"t": msg.tid, b"y": b"r", b"r": r}
    elif isinstance(msg, ErrorMessage):
        envelope = {b"t": msg.tid, b"y": b"e", b"e": [msg.code, msg.message.encode("utf-8")]}
    else:
        raise MalformedMessageError(f"unknown message type: {type(msg)!r}")
    return bencode_encode(envelope)


def decode(raw: bytes) -> Message:
    try:
        envelope = bencode_decode(raw)
    except BencodeError as exc:
        raise MalformedMessageError(str(exc)) from exc

    if not isinstance(envelope, dict):
        raise MalformedMessageError("top-level bencoded message must be a dict")

    tid = _require_bytes(envelope, b"t")
    y = _require_bytes(envelope, b"y")

    try:
        if y == b"q":
            return _decode_query(envelope, tid)
        if y == b"r":
            return _decode_response(envelope, tid)
        if y == b"e":
            return _decode_error(envelope, tid)
        raise MalformedMessageError(f"unknown message type marker: {y!r}")
    except MalformedMessageError as exc:
        exc.tid = tid
        raise


def _decode_query(envelope: dict, tid: bytes) -> Query:
    q = _require_bytes(envelope, b"q")
    a = envelope.get(b"a")
    if not isinstance(a, dict):
        raise MalformedMessageError("query missing 'a' args dict")
    sender_id = NodeID(_require_bytes(a, b"id"))

    if q == b"ping":
        return PingQuery(tid=tid, sender_id=sender_id)
    if q == b"store":
        key = _require_bytes(a, b"key")
        value = _require_bytes(a, b"value")
        ttl = a.get(b"ttl")
        return StoreQuery(tid=tid, sender_id=sender_id, key=key, value=value, ttl=ttl)
    if q == b"find_node":
        target = NodeID(_require_bytes(a, b"target"))
        return FindNodeQuery(tid=tid, sender_id=sender_id, target=target)
    if q == b"find_value":
        key = _require_bytes(a, b"key")
        return FindValueQuery(tid=tid, sender_id=sender_id, key=key)
    raise MalformedMessageError(f"unknown query method: {q!r}")


def _decode_response(envelope: dict, tid: bytes) -> Response:
    r = envelope.get(b"r")
    if not isinstance(r, dict):
        raise MalformedMessageError("response missing 'r' dict")
    responder_id = NodeID(_require_bytes(r, b"id"))

    if b"value" in r:
        return FindValueResponse(tid=tid, responder_id=responder_id, value=_require_bytes(r, b"value"))
    if b"nodes" in r:
        nodes = decode_compact_nodes(_require_bytes(r, b"nodes"))
        # Ambiguous between FindNodeResponse and FindValueResponse(nodes=...)
        # on the wire; caller (protocol layer) knows which query it sent and
        # can treat FindNodeResponse.nodes / FindValueResponse.nodes uniformly.
        return FindNodeResponse(tid=tid, responder_id=responder_id, nodes=nodes)
    if b"status" in r:
        status = _require_bytes(r, b"status").decode("ascii")
        return StoreResponse(tid=tid, responder_id=responder_id, status=status)
    return PingResponse(tid=tid, responder_id=responder_id)


def _decode_error(envelope: dict, tid: bytes) -> ErrorMessage:
    e = envelope.get(b"e")
    if not (isinstance(e, list) and len(e) == 2):
        raise MalformedMessageError("malformed error field 'e'")
    code, message = e
    if not isinstance(code, int) or not isinstance(message, bytes):
        raise MalformedMessageError("malformed error field 'e' contents")
    return ErrorMessage(tid=tid, code=code, message=message.decode("utf-8", errors="replace"))


def _require_bytes(d: dict, key: bytes) -> bytes:
    if key not in d:
        raise MalformedMessageError(f"missing required field: {key!r}")
    value = d[key]
    if not isinstance(value, bytes):
        raise MalformedMessageError(f"expected bytes for {key!r}, got {type(value)!r}")
    return value
