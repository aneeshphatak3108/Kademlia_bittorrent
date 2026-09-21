import asyncio

from kademlia.constants import K
from kademlia.identifier import NodeID
from kademlia.rpc.messages import ErrorMessage, FindValueQuery, StoreQuery

FAST = dict(rpc_timeout=1.0, rpc_retries=1)
FAST_REPUBLISH = dict(rpc_timeout=1.0, rpc_retries=1, expiry_sweep_interval=0.15, republish_interval=0.2)


async def build_mesh(make_node, n, **kwargs):
    seed = await make_node(**kwargs)
    nodes = [seed]
    for _ in range(n - 1):
        node = await make_node(**kwargs)
        await node.join([nodes[-1].contact])
        nodes.append(node)
    return nodes


async def test_bootstrap_single_seed(make_node):
    a = await make_node(**FAST)
    b = await make_node(**FAST)

    await b.join([a.contact])

    a_ids = {c.node_id for c in a.routing_table_snapshot()}
    b_ids = {c.node_id for c in b.routing_table_snapshot()}
    assert b.id in a_ids
    assert a.id in b_ids


async def test_store_and_get(make_node):
    nodes = await build_mesh(make_node, 10, **FAST)
    key = NodeID.random().bytes
    value = b"hello distributed world"

    acked = await nodes[0].store(key, value)
    assert acked >= 1

    result = await nodes[-1].find_value(key)
    assert result == value


async def test_lookup_convergence(make_node):
    nodes = await build_mesh(make_node, 10, **FAST)
    target = NodeID.random()

    found = await nodes[0].find_node(target)
    found_ids = {c.node_id for c in found}

    other_ids = {n.id for n in nodes if n is not nodes[0]}
    ground_truth = set(sorted(other_ids, key=lambda nid: target.distance(nid))[:K])

    assert found_ids == ground_truth


async def test_node_leave_timeout(make_node):
    nodes = await build_mesh(make_node, 6, **FAST)
    victim = nodes[3]
    victim_id = victim.id

    key = NodeID.random().bytes
    value = b"still retrievable after a peer leaves"
    await nodes[0].store(key, value)

    await victim.stop()
    survivors = [n for n in nodes if n is not victim]

    result = await survivors[0].find_value(key)
    assert result == value

    found = await survivors[0].find_node(NodeID.random())
    assert victim_id not in {c.node_id for c in found}


async def test_key_ttl_expiry(make_node):
    nodes = await build_mesh(make_node, 5, **FAST)
    key = NodeID.random().bytes

    await nodes[0].store(key, b"short lived", ttl=1)
    assert await nodes[-1].find_value(key) == b"short lived"

    await asyncio.sleep(1.3)
    assert await nodes[-1].find_value(key) is None


async def test_invalid_key_length_gets_protocol_error(make_node):
    node = await make_node(**FAST)
    stranger = NodeID.random()

    bad_store = StoreQuery(tid=b"xy", sender_id=stranger, key=b"too-short", value=b"v")
    response = await node.handle_query(bad_store, ("127.0.0.1", 9999))
    assert isinstance(response, ErrorMessage)
    assert response.code == 203

    bad_find_value = FindValueQuery(tid=b"zz", sender_id=stranger, key=b"also-too-short")
    response = await node.handle_query(bad_find_value, ("127.0.0.1", 9999))
    assert isinstance(response, ErrorMessage)
    assert response.code == 203


async def test_republish(make_node):
    nodes = await build_mesh(make_node, 5, **FAST_REPUBLISH)
    key = NodeID.random().bytes
    value = b"republished to new joiners"

    await nodes[0].store(key, value, ttl=100)

    new_node = await make_node(**FAST_REPUBLISH)
    await new_node.join([nodes[0].contact])

    await asyncio.sleep(1.0)  # let at least one republish sweep run

    assert await new_node.find_value(key) == value


async def test_republish_forwards_remaining_ttl_not_default(make_node):
    # Exercises exactly what the republish loop does (Node._expiry_and_republish_loop
    # calls rpc_store(contact, key, value, ttl=remaining_ttl)) without depending on
    # live background-loop timing: if it forwarded ttl=None instead, the receiver
    # would apply TTL_DEFAULT (24h) and this key would never expire.
    a = await make_node(**FAST)
    b = await make_node(**FAST)
    await b.join([a.contact])

    key = NodeID.random().bytes
    value = b"short window"

    assert await a.rpc_store(b.contact, key, value, ttl=1)
    assert await b.find_value(key) == value

    await asyncio.sleep(1.3)
    assert await b.find_value(key) is None
