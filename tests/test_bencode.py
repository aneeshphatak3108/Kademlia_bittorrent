import pytest

from kademlia.bencode import BencodeError, bencode_decode, bencode_encode


def test_encode_bytes():
    assert bencode_encode(b"spam") == b"4:spam"


def test_encode_int():
    assert bencode_encode(42) == b"i42e"
    assert bencode_encode(-3) == b"i-3e"
    assert bencode_encode(0) == b"i0e"


def test_encode_list():
    assert bencode_encode([b"a", 1, b"bc"]) == b"l1:ai1e2:bce"


def test_encode_dict_sorts_keys():
    assert bencode_encode({b"z": 1, b"a": 2}) == b"d1:ai2e1:zi1ee"


def test_roundtrip_nested():
    obj = {b"id": b"x" * 20, b"nodes": [{b"a": 1}, {b"b": [1, 2, 3]}], b"n": -7}
    assert bencode_decode(bencode_encode(obj)) == obj


def test_decode_rejects_leading_zero_int():
    with pytest.raises(BencodeError):
        bencode_decode(b"i03e")


def test_decode_rejects_negative_zero():
    with pytest.raises(BencodeError):
        bencode_decode(b"i-0e")


def test_decode_rejects_trailing_garbage():
    with pytest.raises(BencodeError):
        bencode_decode(b"i1ee")


def test_decode_rejects_truncated_string():
    with pytest.raises(BencodeError):
        bencode_decode(b"5:ab")
