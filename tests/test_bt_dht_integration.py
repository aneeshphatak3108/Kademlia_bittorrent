"""The full stack: peers discover each other through the DHT, then transfer.

Everything below goes through real sockets -- UDP for the DHT, TCP for the
peer wire protocol -- with no manual wiring of peer addresses. This is the
test that proves the two layers actually compose.
"""

import hashlib
import os

import pytest

from bittorrent.discovery import PeerDiscovery
from bittorrent.metadata import TorrentMetadata, build_torrent
from bittorrent.session import TorrentSession

PIECE_LENGTH = 32 * 1024
FAST_DHT = dict(rpc_timeout=1.0, rpc_retries=1)
# Announce/discover aggressively so the test doesn't wait on production timers.
FAST_BT = dict(announce_interval=0.5, discovery_interval=0.5, unchoke_interval=0.2)


@pytest.fixture
async def sessions():
    started = []
    yield started.append
    for session in started:
        await session.stop()


async def build_dht(make_node, n):
    """A DHT where *every* node serves peer announcements.

    This matters: any node can end up among the K closest to a given
    info_hash, so a node without the discovery handlers installed would
    reject announcements routed to it. Nodes hold announcements for torrents
    they aren't downloading themselves -- that's the infrastructure role.
    """
    seed = await make_node(**FAST_DHT)
    nodes = [seed]
    for _ in range(n - 1):
        node = await make_node(**FAST_DHT)
        await node.join([nodes[-1].contact])
        nodes.append(node)
    for node in nodes:
        PeerDiscovery(node)
    return nodes


async def test_leecher_finds_seeder_through_the_dht(tmp_path, make_node, sessions):
    data = os.urandom(PIECE_LENGTH * 4)
    meta = TorrentMetadata.from_bytes(build_torrent(data, "f.bin", PIECE_LENGTH))
    nodes = await build_dht(make_node, 5)

    seed_path = tmp_path / "seed.bin"
    seed_path.write_bytes(data)
    seeder = TorrentSession(
        meta, str(seed_path), node=nodes[0], host="127.0.0.1", **FAST_BT
    )
    await seeder.start()
    sessions(seeder)

    leecher = TorrentSession(
        meta, str(tmp_path / "leech.bin"), node=nodes[-1], host="127.0.0.1", **FAST_BT
    )
    await leecher.start()
    sessions(leecher)

    # No add_peer() anywhere: the seeder announces itself to the DHT, the
    # leecher's discovery loop looks the info_hash up and dials what it finds.
    assert await leecher.wait_complete(timeout=45), leecher.progress()
    got = (tmp_path / "leech.bin").read_bytes()
    assert hashlib.sha256(got).hexdigest() == hashlib.sha256(data).hexdigest()


async def test_two_leechers_both_complete_via_dht(tmp_path, make_node, sessions):
    data = os.urandom(PIECE_LENGTH * 3)
    meta = TorrentMetadata.from_bytes(build_torrent(data, "f.bin", PIECE_LENGTH))
    nodes = await build_dht(make_node, 6)

    seed_path = tmp_path / "seed.bin"
    seed_path.write_bytes(data)
    seeder = TorrentSession(meta, str(seed_path), node=nodes[0], host="127.0.0.1", **FAST_BT)
    await seeder.start()
    sessions(seeder)

    leechers = []
    for i, node in enumerate(nodes[1:3], start=1):
        leech = TorrentSession(
            meta, str(tmp_path / f"leech{i}.bin"), node=node, host="127.0.0.1", **FAST_BT
        )
        await leech.start()
        sessions(leech)
        leechers.append(leech)

    for i, leech in enumerate(leechers, start=1):
        assert await leech.wait_complete(timeout=45), leech.progress()
        assert (tmp_path / f"leech{i}.bin").read_bytes() == data


async def test_announcement_is_visible_to_other_nodes(tmp_path, make_node, sessions):
    """The seeder's session announces itself; an unrelated node can find it."""
    data = os.urandom(PIECE_LENGTH)
    meta = TorrentMetadata.from_bytes(build_torrent(data, "f.bin", PIECE_LENGTH))
    nodes = await build_dht(make_node, 5)

    seed_path = tmp_path / "seed.bin"
    seed_path.write_bytes(data)
    seeder = TorrentSession(meta, str(seed_path), node=nodes[0], host="127.0.0.1", **FAST_BT)
    await seeder.start()
    sessions(seeder)

    observer = PeerDiscovery(nodes[-1])
    for _ in range(40):  # let the announce loop run at least once
        peers = await observer.get_peers(meta.info_hash)
        if peers:
            break
    assert peers, "seeder never became discoverable through the DHT"
    assert seeder.port in {port for _host, port in peers}
