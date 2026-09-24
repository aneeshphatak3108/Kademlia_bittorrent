import hashlib

import pytest

from bittorrent.discovery import (
    AnnouncePeerQuery,
    GetPeersQuery,
    GetPeersResponse,
    PeerDiscovery,
    PeerTable,
    decode_peers,
    encode_peers,
)
from kademlia.identifier import NodeID
from kademlia.rpc import messages as krpc

INFO_HASH = hashlib.sha1(b"a torrent").digest()
FAST = dict(rpc_timeout=1.0, rpc_retries=1)


async def build_mesh(make_node, n):
    seed = await make_node(**FAST)
    nodes = [seed]
    for _ in range(n - 1):
        node = await make_node(**FAST)
        await node.join([nodes[-1].contact])
        nodes.append(node)
    return nodes


# -- wire format --


def test_peer_record_roundtrip():
    peers = [("127.0.0.1", 6881), ("192.168.1.5", 65535)]
    assert decode_peers(encode_peers(peers)) == peers


def test_decode_peers_rejects_truncated_data():
    with pytest.raises(krpc.MalformedMessageError):
        decode_peers(b"\x00" * 5)


def test_announce_query_roundtrips_through_the_dht_codec():
    msg = AnnouncePeerQuery(
        tid=b"aa", sender_id=NodeID.random(), info_hash=INFO_HASH, port=6881
    )
    decoded = krpc.decode(krpc.encode(msg))
    assert isinstance(decoded, AnnouncePeerQuery)
    assert decoded.info_hash == INFO_HASH
    assert decoded.port == 6881


def test_get_peers_response_roundtrips():
    msg = GetPeersResponse(
        tid=b"bb", responder_id=NodeID.random(), peers=[("10.0.0.1", 7000)]
    )
    decoded = krpc.decode(krpc.encode(msg))
    assert isinstance(decoded, GetPeersResponse)
    assert decoded.peers == [("10.0.0.1", 7000)]


def test_registering_extensions_did_not_break_builtin_messages():
    ping = krpc.PingQuery(tid=b"zz", sender_id=NodeID.random())
    assert isinstance(krpc.decode(krpc.encode(ping)), krpc.PingQuery)


def test_announce_query_rejects_bad_info_hash():
    raw = krpc.encode(
        AnnouncePeerQuery(tid=b"aa", sender_id=NodeID.random(), info_hash=INFO_HASH, port=1)
    )
    tampered = raw.replace(b"20:" + INFO_HASH, b"3:abc")
    with pytest.raises(krpc.MalformedMessageError):
        krpc.decode(tampered)


def test_announce_query_rejects_bad_port():
    msg = AnnouncePeerQuery(
        tid=b"aa", sender_id=NodeID.random(), info_hash=INFO_HASH, port=99999
    )
    with pytest.raises(krpc.MalformedMessageError, match="invalid announce port"):
        krpc.decode(krpc.encode(msg))


# -- the per-node table --


def test_peer_table_accumulates_rather_than_overwrites():
    table = PeerTable()
    table.add(INFO_HASH, ("1.1.1.1", 1))
    table.add(INFO_HASH, ("2.2.2.2", 2))
    assert set(table.get(INFO_HASH)) == {("1.1.1.1", 1), ("2.2.2.2", 2)}


def test_peer_table_readd_is_idempotent():
    table = PeerTable()
    table.add(INFO_HASH, ("1.1.1.1", 1))
    table.add(INFO_HASH, ("1.1.1.1", 1))
    assert table.get(INFO_HASH) == [("1.1.1.1", 1)]


def test_peer_table_keeps_torrents_separate():
    other = hashlib.sha1(b"another").digest()
    table = PeerTable()
    table.add(INFO_HASH, ("1.1.1.1", 1))
    table.add(other, ("2.2.2.2", 2))
    assert table.get(INFO_HASH) == [("1.1.1.1", 1)]
    assert table.get(other) == [("2.2.2.2", 2)]


def test_peer_table_entries_expire():
    table = PeerTable(ttl=-1)  # already expired on insert
    table.add(INFO_HASH, ("1.1.1.1", 1))
    assert table.get(INFO_HASH) == []


# -- end to end over a real DHT --


async def test_announce_then_discover_across_the_dht(make_node):
    nodes = await build_mesh(make_node, 6)
    services = [PeerDiscovery(n) for n in nodes]

    acked = await services[0].announce(INFO_HASH, port=6881)
    assert acked >= 1, "no node accepted the announcement"

    found = await services[-1].get_peers(INFO_HASH)
    ports = {port for _host, port in found}
    assert 6881 in ports, f"announced peer not discoverable, got {found}"


async def test_multiple_peers_are_all_discoverable(make_node):
    nodes = await build_mesh(make_node, 6)
    services = [PeerDiscovery(n) for n in nodes]

    for index, service in enumerate(services[:3]):
        assert await service.announce(INFO_HASH, port=7000 + index) >= 1

    found = await services[-1].get_peers(INFO_HASH)
    ports = {port for _host, port in found}
    assert {7000, 7001, 7002} <= ports, f"expected all three announcers, got {ports}"


async def test_get_peers_includes_announcements_made_to_us(make_node):
    """A lookup never returns the searcher itself, so a node has to read its
    own table -- otherwise, in a small swarm, an announcement made directly to
    us would sit locally unread and the peer would look undiscoverable."""
    nodes = await build_mesh(make_node, 2)
    services = [PeerDiscovery(n) for n in nodes]

    # nodes[0] announces; with two nodes the only recipient is nodes[1].
    assert await services[0].announce(INFO_HASH, port=6881) >= 1
    found = await services[1].get_peers(INFO_HASH)
    assert 6881 in {port for _host, port in found}, found


async def test_unannounced_torrent_finds_nobody(make_node):
    nodes = await build_mesh(make_node, 4)
    services = [PeerDiscovery(n) for n in nodes]
    await services[0].announce(INFO_HASH, port=6881)

    other = hashlib.sha1(b"a different torrent").digest()
    assert await services[-1].get_peers(other) == set()


async def test_announce_uses_the_udp_source_address_not_a_claimed_one(make_node):
    # The announcing peer controls only the port; the address must come from
    # the datagram itself, so a peer can't announce someone else's IP.
    nodes = await build_mesh(make_node, 4)
    services = [PeerDiscovery(n) for n in nodes]
    await services[0].announce(INFO_HASH, port=6881)

    found = await services[-1].get_peers(INFO_HASH)
    assert found, "nothing discovered"
    assert all(host == "127.0.0.1" for host, _port in found), found
