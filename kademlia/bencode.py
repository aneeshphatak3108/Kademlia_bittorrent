"""Minimal bencode codec (BitTorrent's serialization format, BEP 3).

Supports the four bencode types: byte strings, integers, lists, and
dictionaries (with byte-string keys, sorted on encode per spec). Shared
between the Kademlia DHT wire protocol and the future BitTorrent layer.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple, Union

Bencodable = Union[bytes, int, list, dict]


class BencodeError(Exception):
    """Raised on malformed bencoded data."""


def bencode_encode(obj: Bencodable) -> bytes:
    out = bytearray()
    _encode(obj, out)
    return bytes(out)


def _encode(obj: Any, out: bytearray) -> None:
    if isinstance(obj, bool):
        raise BencodeError("bencode does not support bool; use int")
    if isinstance(obj, int):
        out += b"i" + str(obj).encode("ascii") + b"e"
    elif isinstance(obj, (bytes, bytearray)):
        out += str(len(obj)).encode("ascii") + b":" + bytes(obj)
    elif isinstance(obj, str):
        raw = obj.encode("utf-8")
        out += str(len(raw)).encode("ascii") + b":" + raw
    elif isinstance(obj, list):
        out += b"l"
        for item in obj:
            _encode(item, out)
        out += b"e"
    elif isinstance(obj, dict):
        out += b"d"
        keys = sorted(obj.keys(), key=_dict_key_sort_bytes)
        for key in keys:
            _encode(_as_key_bytes(key), out)
            _encode(obj[key], out)
        out += b"e"
    else:
        raise BencodeError(f"unsupported type for bencode: {type(obj)!r}")


def _as_key_bytes(key: Any) -> bytes:
    if isinstance(key, (bytes, bytearray)):
        return bytes(key)
    if isinstance(key, str):
        return key.encode("utf-8")
    raise BencodeError(f"dict keys must be bytes or str, got {type(key)!r}")


def _dict_key_sort_bytes(key: Any) -> bytes:
    return _as_key_bytes(key)


def bencode_decode(data: bytes) -> Bencodable:
    value, offset = _decode(data, 0)
    if offset != len(data):
        raise BencodeError("trailing data after top-level bencoded value")
    return value


def _decode(data: bytes, offset: int) -> Tuple[Bencodable, int]:
    if offset >= len(data):
        raise BencodeError("unexpected end of data")
    marker = data[offset : offset + 1]
    if marker == b"i":
        return _decode_int(data, offset)
    if marker == b"l":
        return _decode_list(data, offset)
    if marker == b"d":
        return _decode_dict(data, offset)
    if marker.isdigit():
        return _decode_bytes(data, offset)
    raise BencodeError(f"invalid bencode marker {marker!r} at offset {offset}")


def _decode_int(data: bytes, offset: int) -> Tuple[int, int]:
    end = data.find(b"e", offset)
    if end == -1:
        raise BencodeError("unterminated integer")
    token = data[offset + 1 : end]
    if token == b"" or token == b"-" or (token.startswith(b"0") and token != b"0") or token.startswith(b"-0"):
        raise BencodeError(f"malformed integer token {token!r}")
    try:
        value = int(token)
    except ValueError as exc:
        raise BencodeError(f"malformed integer token {token!r}") from exc
    return value, end + 1


def _decode_bytes(data: bytes, offset: int) -> Tuple[bytes, int]:
    colon = data.find(b":", offset)
    if colon == -1:
        raise BencodeError("malformed byte string length")
    length_token = data[offset:colon]
    if not length_token.isdigit():
        raise BencodeError(f"malformed byte string length {length_token!r}")
    length = int(length_token)
    start = colon + 1
    end = start + length
    if end > len(data):
        raise BencodeError("byte string length exceeds available data")
    return data[start:end], end


def _decode_list(data: bytes, offset: int) -> Tuple[List[Bencodable], int]:
    items: List[Bencodable] = []
    offset += 1
    while True:
        if offset >= len(data):
            raise BencodeError("unterminated list")
        if data[offset : offset + 1] == b"e":
            return items, offset + 1
        value, offset = _decode(data, offset)
        items.append(value)


def _decode_dict(data: bytes, offset: int) -> Tuple[Dict[bytes, Bencodable], int]:
    result: Dict[bytes, Bencodable] = {}
    offset += 1
    while True:
        if offset >= len(data):
            raise BencodeError("unterminated dict")
        if data[offset : offset + 1] == b"e":
            return result, offset + 1
        key, offset = _decode(data, offset)
        if not isinstance(key, bytes):
            raise BencodeError("dict keys must decode to byte strings")
        value, offset = _decode(data, offset)
        result[key] = value
