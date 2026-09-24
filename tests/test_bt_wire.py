import asyncio
import struct

import pytest

from bittorrent.wire import messages as wire


def roundtrip(msg):
    framed = wire.encode(msg)
    (length,) = struct.unpack(">I", framed[:4])
    assert length == len(framed) - 4, "length prefix disagrees with the framed body"
    return wire.decode(framed[4:])


def test_handshake_roundtrip():
    hs = wire.Handshake(info_hash=b"i" * 20, peer_id=b"p" * 20)
    raw = hs.encode()
    assert len(raw) == wire.HANDSHAKE_LENGTH == 68
    decoded = wire.Handshake.decode(raw)
    assert decoded.info_hash == b"i" * 20
    assert decoded.peer_id == b"p" * 20


def test_handshake_rejects_wrong_protocol_name():
    raw = bytearray(wire.Handshake(info_hash=b"i" * 20, peer_id=b"p" * 20).encode())
    raw[1:20] = b"NotBitTorrent proto"
    with pytest.raises(wire.WireError, match="BitTorrent protocol"):
        wire.Handshake.decode(bytes(raw))


def test_handshake_rejects_wrong_length():
    with pytest.raises(wire.WireError):
        wire.Handshake.decode(b"\x13" + wire.PROTOCOL_NAME)  # truncated


def test_handshake_rejects_bad_field_sizes():
    with pytest.raises(wire.WireError):
        wire.Handshake(info_hash=b"short", peer_id=b"p" * 20).encode()
    with pytest.raises(wire.WireError):
        wire.Handshake(info_hash=b"i" * 20, peer_id=b"short").encode()


@pytest.mark.parametrize(
    "msg",
    [
        wire.Choke(), wire.Unchoke(), wire.Interested(), wire.NotInterested(),
        wire.Have(index=42),
        wire.Bitfield(field=b"\xff\x0f"),
        wire.Request(index=1, begin=2, length=3),
        wire.Cancel(index=4, begin=5, length=6),
        wire.Piece(index=7, begin=8, block=b"payload-bytes"),
    ],
)
def test_message_roundtrip(msg):
    assert roundtrip(msg) == msg


def test_keepalive_is_a_bare_zero_length():
    assert wire.encode(wire.KeepAlive()) == struct.pack(">I", 0)
    assert isinstance(wire.decode(b""), wire.KeepAlive)


def test_piece_carries_an_arbitrary_length_block():
    block = bytes(range(256)) * 4
    decoded = roundtrip(wire.Piece(index=3, begin=16384, block=block))
    assert decoded.block == block
    assert (decoded.index, decoded.begin) == (3, 16384)


def test_decode_rejects_unknown_message_id():
    with pytest.raises(wire.WireError, match="unknown message id"):
        wire.decode(bytes([99]))


def test_decode_rejects_wrong_payload_sizes():
    with pytest.raises(wire.WireError):
        wire.decode(bytes([wire.HAVE]) + b"\x00\x00")  # have needs 4 bytes
    with pytest.raises(wire.WireError):
        wire.decode(bytes([wire.REQUEST]) + b"\x00" * 11)  # request needs 12
    with pytest.raises(wire.WireError):
        wire.decode(bytes([wire.PIECE]) + b"\x00" * 4)  # piece needs an 8-byte header
    with pytest.raises(wire.WireError):
        wire.decode(bytes([wire.CHOKE]) + b"junk")  # choke takes no payload


async def test_read_message_over_a_stream():
    reader = asyncio.StreamReader()
    reader.feed_data(wire.encode(wire.Have(index=9)))
    reader.feed_data(wire.encode(wire.Request(index=1, begin=2, length=3)))
    reader.feed_data(wire.encode(wire.KeepAlive()))
    reader.feed_eof()

    assert await wire.read_message(reader) == wire.Have(index=9)
    assert await wire.read_message(reader) == wire.Request(index=1, begin=2, length=3)
    assert isinstance(await wire.read_message(reader), wire.KeepAlive)


async def test_read_message_rejects_an_absurd_length_before_allocating():
    # A hostile peer claiming a huge message must be refused outright, not
    # handed a buffer that size.
    reader = asyncio.StreamReader()
    reader.feed_data(struct.pack(">I", wire.MAX_MESSAGE_SIZE + 1))
    reader.feed_eof()
    with pytest.raises(wire.WireError, match="cap is"):
        await wire.read_message(reader)


async def test_read_handshake_over_a_stream():
    reader = asyncio.StreamReader()
    reader.feed_data(wire.Handshake(info_hash=b"a" * 20, peer_id=b"b" * 20).encode())
    reader.feed_eof()
    hs = await wire.read_handshake(reader)
    assert hs.info_hash == b"a" * 20
