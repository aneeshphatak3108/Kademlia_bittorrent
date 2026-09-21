import pytest

from kademlia.identifier import NodeID
from kademlia.routing_table import Contact
from kademlia.rpc import messages
from kademlia.rpc.messages import (
    COMPACT_NODE_SIZE,
    ErrorMessage,
    FindNodeQuery,
    FindNodeResponse,
    FindValueQuery,
    FindValueResponse,
    MalformedMessageError,
    PingQuery,
    PingResponse,
    StoreQuery,
    StoreResponse,
    decode_compact_nodes,
    encode_compact_nodes,
)


def make_contact(port=4000):
    return Contact(node_id=NodeID.random(), ip="127.0.0.1", port=port)


def test_ping_round_trip():
    sender = NodeID.random()
    query = PingQuery(tid=b"\x01\x02", sender_id=sender)
    decoded = messages.decode(messages.encode(query))
    assert isinstance(decoded, PingQuery)
    assert decoded.tid == b"\x01\x02"
    assert decoded.sender_id == sender

    responder = NodeID.random()
    response = PingResponse(tid=b"\x01\x02", responder_id=responder)
    decoded = messages.decode(messages.encode(response))
    assert isinstance(decoded, PingResponse)
    assert decoded.responder_id == responder


def test_store_query_round_trip_with_and_without_ttl():
    sender = NodeID.random()
    query = StoreQuery(tid=b"ab", sender_id=sender, key=b"k" * 20, value=b"some value", ttl=3600)
    decoded = messages.decode(messages.encode(query))
    assert isinstance(decoded, StoreQuery)
    assert decoded.key == b"k" * 20
    assert decoded.value == b"some value"
    assert decoded.ttl == 3600

    query_no_ttl = StoreQuery(tid=b"ab", sender_id=sender, key=b"k" * 20, value=b"v")
    decoded = messages.decode(messages.encode(query_no_ttl))
    assert decoded.ttl is None


def test_store_response_round_trip():
    responder = NodeID.random()
    response = StoreResponse(tid=b"ab", responder_id=responder)
    decoded = messages.decode(messages.encode(response))
    assert isinstance(decoded, StoreResponse)
    assert decoded.status == "ok"


def test_find_node_round_trip():
    sender = NodeID.random()
    target = NodeID.random()
    query = FindNodeQuery(tid=b"zz", sender_id=sender, target=target)
    decoded = messages.decode(messages.encode(query))
    assert isinstance(decoded, FindNodeQuery)
    assert decoded.target == target

    contacts = [make_contact(port=p) for p in range(5000, 5005)]
    response = FindNodeResponse(tid=b"zz", responder_id=sender, nodes=contacts)
    decoded = messages.decode(messages.encode(response))
    assert isinstance(decoded, FindNodeResponse)
    assert [c.node_id for c in decoded.nodes] == [c.node_id for c in contacts]
    assert [(c.ip, c.port) for c in decoded.nodes] == [(c.ip, c.port) for c in contacts]


def test_find_value_round_trip_value_found():
    sender = NodeID.random()
    query = FindValueQuery(tid=b"vv", sender_id=sender, key=b"k" * 20)
    decoded = messages.decode(messages.encode(query))
    assert isinstance(decoded, FindValueQuery)
    assert decoded.key == b"k" * 20

    response = FindValueResponse(tid=b"vv", responder_id=sender, value=b"the value")
    decoded = messages.decode(messages.encode(response))
    assert isinstance(decoded, FindValueResponse)
    assert decoded.value == b"the value"


def test_find_value_round_trip_fallback_nodes():
    sender = NodeID.random()
    contacts = [make_contact(port=6000)]
    response = FindValueResponse(tid=b"vv", responder_id=sender, nodes=contacts)
    decoded = messages.decode(messages.encode(response))
    # Ambiguous on the wire with FindNodeResponse -- both carry {id, nodes}.
    # The RPC-wrapper layer (Node.rpc_find_value) knows it sent a find_value
    # query and reads `.nodes`/`.value` generically either way.
    assert getattr(decoded, "nodes", None) is not None
    assert [c.node_id for c in decoded.nodes] == [contacts[0].node_id]


def test_error_message_round_trip():
    error = ErrorMessage(tid=b"ee", code=203, message="Protocol Error")
    decoded = messages.decode(messages.encode(error))
    assert isinstance(decoded, ErrorMessage)
    assert decoded.code == 203
    assert decoded.message == "Protocol Error"
    assert decoded.tid == b"ee"


def test_compact_node_encode_decode_round_trip():
    contacts = [make_contact(port=p) for p in (1, 2, 3, 65535)]
    packed = encode_compact_nodes(contacts)
    assert len(packed) == COMPACT_NODE_SIZE * len(contacts)
    unpacked = decode_compact_nodes(packed)
    assert [c.node_id for c in unpacked] == [c.node_id for c in contacts]
    assert [(c.ip, c.port) for c in unpacked] == [(c.ip, c.port) for c in contacts]


def test_compact_node_decode_rejects_truncated_data():
    with pytest.raises(MalformedMessageError):
        decode_compact_nodes(b"\x00" * (COMPACT_NODE_SIZE - 1))


def test_decode_rejects_non_dict_envelope():
    from kademlia.bencode import bencode_encode

    with pytest.raises(MalformedMessageError):
        messages.decode(bencode_encode([1, 2, 3]))


def test_decode_missing_tid_has_no_tid_to_recover():
    from kademlia.bencode import bencode_encode

    raw = bencode_encode({b"y": b"q", b"q": b"ping", b"a": {b"id": b"x" * 20}})
    with pytest.raises(MalformedMessageError) as exc_info:
        messages.decode(raw)
    assert exc_info.value.tid is None


def test_decode_malformed_query_preserves_tid_for_error_reply():
    from kademlia.bencode import bencode_encode

    # Valid envelope/tid, but store query missing required 'value' field.
    raw = bencode_encode(
        {b"t": b"XY", b"y": b"q", b"q": b"store", b"a": {b"id": b"x" * 20, b"key": b"k" * 20}}
    )
    with pytest.raises(MalformedMessageError) as exc_info:
        messages.decode(raw)
    assert exc_info.value.tid == b"XY"


def test_decode_unknown_query_method_preserves_tid():
    from kademlia.bencode import bencode_encode

    raw = bencode_encode({b"t": b"ZZ", b"y": b"q", b"q": b"delete_everything", b"a": {b"id": b"x" * 20}})
    with pytest.raises(MalformedMessageError) as exc_info:
        messages.decode(raw)
    assert exc_info.value.tid == b"ZZ"
