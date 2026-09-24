import hashlib

import pytest

from bittorrent.metadata import TorrentMetadata, build_torrent
from bittorrent.storage import PieceStore, parse_bitfield

DATA = bytes(range(256)) * 4  # 1024 bytes
PIECE_LENGTH = 256


def metadata(data: bytes = DATA, piece_length: int = PIECE_LENGTH) -> TorrentMetadata:
    return TorrentMetadata.from_bytes(build_torrent(data, "t.bin", piece_length))


async def test_open_creates_sparse_file_of_the_right_size(tmp_path):
    meta = metadata()
    store = PieceStore(str(tmp_path / "out.bin"), meta)
    await store.open()
    assert (tmp_path / "out.bin").stat().st_size == meta.total_length
    assert store.have == set()  # nothing verifies yet -- it's all zeroes


async def test_write_verify_and_read_back(tmp_path):
    meta = metadata()
    store = PieceStore(str(tmp_path / "out.bin"), meta)
    await store.open()

    await store.write_block(0, 0, DATA[:128])
    assert await store.verify_piece(0) is False  # only half the piece written
    await store.write_block(0, 128, DATA[128:256])
    assert await store.verify_piece(0) is True

    assert store.has_piece(0)
    assert await store.read_block(0, 0, 256) == DATA[:256]


async def test_complete_download_then_rescan_recovers_state(tmp_path):
    meta = metadata()
    path = str(tmp_path / "out.bin")
    store = PieceStore(path, meta)
    await store.open()
    for index in range(meta.piece_count):
        offset = index * meta.piece_length
        await store.write_block(index, 0, DATA[offset : offset + meta.piece_size(index)])
        assert await store.verify_piece(index)
    assert store.is_complete
    assert store.bytes_downloaded == meta.total_length

    # A fresh store over the same file re-derives `have` by re-hashing (no
    # persisted bitfield) -- this is the crash-recovery path.
    reopened = PieceStore(path, meta)
    await reopened.open()
    assert reopened.is_complete
    assert reopened.have == set(range(meta.piece_count))


async def test_rescan_treats_corrupt_piece_as_absent(tmp_path):
    meta = metadata()
    path = str(tmp_path / "out.bin")
    store = PieceStore(path, meta)
    await store.open()
    for index in range(meta.piece_count):
        offset = index * meta.piece_length
        await store.write_block(index, 0, DATA[offset : offset + meta.piece_size(index)])
        await store.verify_piece(index)

    await store.write_block(2, 0, b"\xff" * 256)  # corrupt piece 2 on disk
    recovered = await store.rescan()
    assert 2 not in recovered
    assert recovered == {0, 1, 3}


async def test_last_short_piece_verifies(tmp_path):
    data = b"q" * 1000
    meta = metadata(data, 256)
    store = PieceStore(str(tmp_path / "out.bin"), meta)
    await store.open()
    last = meta.piece_count - 1
    await store.write_block(last, 0, data[last * 256 :])
    assert await store.verify_piece(last) is True


async def test_write_out_of_range_rejected(tmp_path):
    meta = metadata()
    store = PieceStore(str(tmp_path / "out.bin"), meta)
    await store.open()
    with pytest.raises(ValueError):
        await store.write_block(0, 200, b"x" * 100)  # runs past the 256-byte piece
    with pytest.raises(IndexError):
        await store.write_block(99, 0, b"x")


async def test_bitfield_roundtrip(tmp_path):
    meta = metadata()
    store = PieceStore(str(tmp_path / "out.bin"), meta)
    await store.open()
    await store.write_block(1, 0, DATA[256:512])
    await store.verify_piece(1)

    field = store.bitfield()
    assert len(field) == (meta.piece_count + 7) // 8
    assert parse_bitfield(field, meta.piece_count) == {1}


def test_parse_bitfield_ignores_spare_trailing_bits():
    # 4 pieces => 1 byte, with 4 spare bits that must not be read as pieces.
    assert parse_bitfield(bytes([0b10110000]), 4) == {0, 2, 3}
    assert parse_bitfield(bytes([0b11111111]), 4) == {0, 1, 2, 3}


async def test_piece_hashes_line_up_with_metadata(tmp_path):
    meta = metadata()
    store = PieceStore(str(tmp_path / "out.bin"), meta)
    await store.open()
    await store.write_block(0, 0, DATA[:256])
    assert hashlib.sha1(await store.read_block(0, 0, 256)).digest() == meta.piece_hashes[0]
