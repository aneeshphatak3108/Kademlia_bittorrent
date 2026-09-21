import asyncio

import pytest

from kademlia.bencode import bencode_encode
from kademlia.identifier import NodeID
from kademlia.routing_table import Contact
from kademlia.rpc.messages import PingQuery, PingResponse
from kademlia.rpc.protocol import KademliaProtocol, RPCErrorResponse, RPCTimeoutError


class StubNode:
    """Minimal stand-in for Node: only implements what KademliaProtocol needs."""

    def __init__(self):
        self.id = NodeID.random()

    async def handle_query(self, msg, addr):
        if isinstance(msg, PingQuery):
            return PingResponse(tid=msg.tid, responder_id=self.id)
        return None


async def make_protocol(stub_node):
    loop = asyncio.get_event_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: KademliaProtocol(stub_node), local_addr=("127.0.0.1", 0)
    )
    return transport, protocol


async def test_ping_round_trip():
    node_a, node_b = StubNode(), StubNode()
    transport_a, protocol_a = await make_protocol(node_a)
    transport_b, protocol_b = await make_protocol(node_b)
    try:
        addr_b = transport_b.get_extra_info("sockname")
        contact_b = Contact(node_id=node_b.id, ip=addr_b[0], port=addr_b[1])

        query = PingQuery(tid=protocol_a.new_transaction_id(), sender_id=node_a.id)
        response, _from_addr = await protocol_a.send_query(contact_b, query)

        assert isinstance(response, PingResponse)
        assert response.responder_id == node_b.id
    finally:
        transport_a.close()
        transport_b.close()


async def test_query_times_out_when_no_response():
    node_a = StubNode()
    transport_a, protocol_a = await make_protocol(node_a)
    try:
        unreachable = Contact(node_id=NodeID.random(), ip="127.0.0.1", port=1)
        query = PingQuery(tid=protocol_a.new_transaction_id(), sender_id=node_a.id)
        with pytest.raises(RPCTimeoutError):
            await protocol_a.send_query(unreachable, query, timeout=0.2)
    finally:
        transport_a.close()


async def test_malformed_query_gets_protocol_error_reply():
    node_a, node_b = StubNode(), StubNode()
    transport_a, protocol_a = await make_protocol(node_a)
    transport_b, protocol_b = await make_protocol(node_b)
    try:
        addr_b = transport_b.get_extra_info("sockname")
        # A syntactically valid KRPC envelope (recoverable tid) but a store
        # query missing its required 'value' field.
        raw = bencode_encode(
            {b"t": b"Q1", b"y": b"q", b"q": b"store", b"a": {b"id": node_a.id.bytes, b"key": b"k" * 20}}
        )

        future = asyncio.get_event_loop().create_future()
        protocol_a._pending[b"Q1"] = future
        transport_a.sendto(raw, addr_b)

        with pytest.raises(RPCErrorResponse) as exc_info:
            await asyncio.wait_for(future, timeout=2.0)
        assert exc_info.value.error.code == 203
    finally:
        transport_a.close()
        transport_b.close()


async def test_transaction_ids_do_not_collide_with_pending():
    node_a = StubNode()
    transport_a, protocol_a = await make_protocol(node_a)
    try:
        seen = set()
        for _ in range(50):
            tid = protocol_a.new_transaction_id()
            assert tid not in seen
            protocol_a._pending[tid] = asyncio.get_event_loop().create_future()
            seen.add(tid)
        for fut in protocol_a._pending.values():
            fut.cancel()
    finally:
        transport_a.close()
