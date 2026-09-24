import hashlib

import pytest

from bittorrent.metadata import MetadataError, TorrentMetadata, build_torrent
from kademlia.bencode import bencode_decode, bencode_encode


def make(data: bytes = b"x" * 1000, name: str = "test.bin", piece_length: int = 256) -> bytes:
    return build_torrent(data, name, piece_length)


def test_roundtrip_basic_fields():
    data = b"x" * 1000
    meta = TorrentMetadata.from_bytes(make(data, "test.bin", 256))
    assert meta.name == "test.bin"
    assert meta.piece_length == 256
    assert meta.total_length == 1000
    assert meta.piece_count == 4  # 256 + 256 + 256 + 232


def test_info_hash_is_sha1_of_bencoded_info_dict():
    raw = make()
    meta = TorrentMetadata.from_bytes(raw)
    info = bencode_decode(raw)[b"info"]
    assert meta.info_hash == hashlib.sha1(bencode_encode(info)).digest()
    assert len(meta.info_hash) == 20


def test_piece_hashes_match_the_data():
    data = bytes(range(256)) * 4  # 1024 bytes
    meta = TorrentMetadata.from_bytes(make(data, "d.bin", 256))
    for index in range(meta.piece_count):
        chunk = data[index * 256 : (index + 1) * 256]
        assert meta.piece_hashes[index] == hashlib.sha1(chunk).digest()


def test_last_piece_is_short():
    meta = TorrentMetadata.from_bytes(make(b"x" * 1000, "t.bin", 256))
    assert meta.piece_size(0) == 256
    assert meta.piece_size(2) == 256
    assert meta.piece_size(3) == 232  # 1000 - 768
    assert sum(meta.piece_size(i) for i in range(meta.piece_count)) == 1000


def test_exact_multiple_has_no_short_piece():
    meta = TorrentMetadata.from_bytes(make(b"y" * 512, "t.bin", 256))
    assert meta.piece_count == 2
    assert meta.piece_size(1) == 256


def test_piece_size_rejects_bad_index():
    meta = TorrentMetadata.from_bytes(make())
    with pytest.raises(IndexError):
        meta.piece_size(meta.piece_count)


def test_block_count():
    meta = TorrentMetadata.from_bytes(make(b"z" * 1000, "t.bin", 256))
    assert meta.block_count(0, 100) == 3  # 100 + 100 + 56
    assert meta.block_count(3, 100) == 3  # last piece is 232 bytes


def test_rejects_non_bencode():
    with pytest.raises(MetadataError):
        TorrentMetadata.from_bytes(b"definitely not bencode")


def test_rejects_missing_info():
    with pytest.raises(MetadataError):
        TorrentMetadata.from_bytes(bencode_encode({b"announce": b"x"}))


def test_rejects_multifile():
    info = {
        b"name": b"dir",
        b"piece length": 256,
        b"pieces": b"\x00" * 20,
        b"files": [{b"length": 10, b"path": [b"a"]}],
    }
    with pytest.raises(MetadataError, match="multi-file"):
        TorrentMetadata.from_bytes(bencode_encode({b"info": info}))


def test_rejects_truncated_pieces_field():
    info = {b"name": b"t", b"piece length": 256, b"pieces": b"\x00" * 19, b"length": 256}
    with pytest.raises(MetadataError, match="multiple"):
        TorrentMetadata.from_bytes(bencode_encode({b"info": info}))


def test_rejects_piece_count_mismatch():
    # Two hashes claimed, but the length only accounts for one piece.
    info = {b"name": b"t", b"piece length": 256, b"pieces": b"\x00" * 40, b"length": 256}
    with pytest.raises(MetadataError, match="piece count mismatch"):
        TorrentMetadata.from_bytes(bencode_encode({b"info": info}))


def test_rejects_nonpositive_piece_length():
    with pytest.raises(MetadataError):
        build_torrent(b"abc", "t", 0)


def test_from_file(tmp_path):
    path = tmp_path / "t.torrent"
    path.write_bytes(make())
    meta = TorrentMetadata.from_file(str(path))
    assert meta.name == "test.bin"
