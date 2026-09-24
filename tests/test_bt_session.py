"""End-to-end transfers, in-process but over real TCP sockets."""

import asyncio
import hashlib
import os

import pytest

from bittorrent.metadata import TorrentMetadata, build_torrent
from bittorrent.session import TorrentSession

PIECE_LENGTH = 32 * 1024  # a couple of blocks per piece
FAST = dict(announce_interval=3600, discovery_interval=3600, unchoke_interval=0.2)


def make_payload(size: int) -> bytes:
    return os.urandom(size)


async def seeder(tmp_path, data: bytes, meta: TorrentMetadata, name="seed.bin") -> TorrentSession:
    """A session that already holds the whole file."""
    path = tmp_path / name
    path.write_bytes(data)
    session = TorrentSession(meta, str(path), host="127.0.0.1", **FAST)
    await session.start()
    assert session.is_complete, "seeder should start out complete"
    return session


async def leecher(tmp_path, meta: TorrentMetadata, name="leech.bin") -> TorrentSession:
    session = TorrentSession(meta, str(tmp_path / name), host="127.0.0.1", **FAST)
    await session.start()
    return session


@pytest.fixture
async def sessions():
    started = []

    def track(session):
        started.append(session)
        return session

    yield track
    for session in started:
        await session.stop()


async def test_single_piece_transfer(tmp_path, sessions):
    data = make_payload(PIECE_LENGTH)
    meta = TorrentMetadata.from_bytes(build_torrent(data, "f.bin", PIECE_LENGTH))

    seed = sessions(await seeder(tmp_path, data, meta))
    leech = sessions(await leecher(tmp_path, meta))

    assert await leech.add_peer("127.0.0.1", seed.port)
    assert await leech.wait_complete(timeout=20), leech.progress()
    assert (tmp_path / "leech.bin").read_bytes() == data


async def test_multi_piece_transfer_matches_sha256(tmp_path, sessions):
    data = make_payload(PIECE_LENGTH * 6 + 1234)  # a short final piece too
    meta = TorrentMetadata.from_bytes(build_torrent(data, "f.bin", PIECE_LENGTH))
    assert meta.piece_count == 7

    seed = sessions(await seeder(tmp_path, data, meta))
    leech = sessions(await leecher(tmp_path, meta))

    await leech.add_peer("127.0.0.1", seed.port)
    assert await leech.wait_complete(timeout=30), leech.progress()

    got = (tmp_path / "leech.bin").read_bytes()
    assert hashlib.sha256(got).hexdigest() == hashlib.sha256(data).hexdigest()


async def test_download_from_two_seeders(tmp_path, sessions):
    data = make_payload(PIECE_LENGTH * 6)
    meta = TorrentMetadata.from_bytes(build_torrent(data, "f.bin", PIECE_LENGTH))

    seed_a = sessions(await seeder(tmp_path, data, meta, name="seed_a.bin"))
    seed_b = sessions(await seeder(tmp_path, data, meta, name="seed_b.bin"))
    leech = sessions(await leecher(tmp_path, meta))

    await leech.add_peer("127.0.0.1", seed_a.port)
    await leech.add_peer("127.0.0.1", seed_b.port)
    assert await leech.wait_complete(timeout=30), leech.progress()
    assert (tmp_path / "leech.bin").read_bytes() == data


async def test_leecher_becomes_a_seeder_for_a_third_peer(tmp_path, sessions):
    """The classic swarm property: once B has the data it can serve C itself."""
    data = make_payload(PIECE_LENGTH * 4)
    meta = TorrentMetadata.from_bytes(build_torrent(data, "f.bin", PIECE_LENGTH))

    seed = sessions(await seeder(tmp_path, data, meta))
    middle = sessions(await leecher(tmp_path, meta, name="middle.bin"))
    await middle.add_peer("127.0.0.1", seed.port)
    assert await middle.wait_complete(timeout=30), middle.progress()

    await seed.stop()  # original seeder leaves entirely

    last = sessions(await leecher(tmp_path, meta, name="last.bin"))
    await last.add_peer("127.0.0.1", middle.port)
    assert await last.wait_complete(timeout=30), last.progress()
    assert (tmp_path / "last.bin").read_bytes() == data


async def test_partial_progress_resumes_after_restart(tmp_path, sessions):
    """A restarted leecher re-hashes what's on disk and only fetches the rest."""
    data = make_payload(PIECE_LENGTH * 4)
    meta = TorrentMetadata.from_bytes(build_torrent(data, "f.bin", PIECE_LENGTH))

    # Pre-seed the output file with the first two pieces already correct.
    partial = bytearray(len(data))
    partial[: PIECE_LENGTH * 2] = data[: PIECE_LENGTH * 2]
    out = tmp_path / "resumed.bin"
    out.write_bytes(bytes(partial))

    seed = sessions(await seeder(tmp_path, data, meta))
    resumed = TorrentSession(meta, str(out), host="127.0.0.1", **FAST)
    await resumed.start()
    sessions(resumed)

    assert resumed.store.have == {0, 1}, "should have recovered the valid pieces from disk"
    await resumed.add_peer("127.0.0.1", seed.port)
    assert await resumed.wait_complete(timeout=30), resumed.progress()
    assert out.read_bytes() == data


async def test_incoming_connection_is_accepted(tmp_path, sessions):
    """The seeder dials the leecher, rather than the other way around."""
    data = make_payload(PIECE_LENGTH * 2)
    meta = TorrentMetadata.from_bytes(build_torrent(data, "f.bin", PIECE_LENGTH))

    seed = sessions(await seeder(tmp_path, data, meta))
    leech = sessions(await leecher(tmp_path, meta))

    assert await seed.add_peer("127.0.0.1", leech.port)
    assert await leech.wait_complete(timeout=30), leech.progress()
    assert (tmp_path / "leech.bin").read_bytes() == data


async def test_peer_with_a_different_torrent_is_rejected(tmp_path, sessions):
    data_a = make_payload(PIECE_LENGTH)
    data_b = make_payload(PIECE_LENGTH)
    meta_a = TorrentMetadata.from_bytes(build_torrent(data_a, "a.bin", PIECE_LENGTH))
    meta_b = TorrentMetadata.from_bytes(build_torrent(data_b, "b.bin", PIECE_LENGTH))
    assert meta_a.info_hash != meta_b.info_hash

    seed_a = sessions(await seeder(tmp_path, data_a, meta_a, name="a_seed.bin"))
    other = sessions(await leecher(tmp_path, meta_b, name="b_leech.bin"))

    assert await other.add_peer("127.0.0.1", seed_a.port) is False
    assert other.peers == {}


async def test_mutual_dial_yields_one_connection_not_two(tmp_path, sessions):
    """Both peers dialing each other must collapse to a single connection.

    Outbound connections are keyed by the peer's listening port, inbound ones
    only expose the remote's ephemeral source port -- so address-based dedup
    silently fails here and the pair ends up connected twice.
    """
    data = make_payload(PIECE_LENGTH * 2)
    meta = TorrentMetadata.from_bytes(build_torrent(data, "f.bin", PIECE_LENGTH))

    seed = sessions(await seeder(tmp_path, data, meta))
    leech = sessions(await leecher(tmp_path, meta))

    await asyncio.gather(
        leech.add_peer("127.0.0.1", seed.port),
        seed.add_peer("127.0.0.1", leech.port),
    )
    await asyncio.sleep(0.5)  # let both handshakes settle

    for session, label in ((seed, "seeder"), (leech, "leecher")):
        peer_ids = [c.peer_id for c in session.active_peers()]
        assert len(peer_ids) == len(set(peer_ids)), f"{label} has duplicate peers: {peer_ids}"
        assert len(peer_ids) <= 1, f"{label} kept {len(peer_ids)} connections to one peer"

    # and the transfer still works over whichever connection survived
    assert await leech.wait_complete(timeout=30), leech.progress()
    assert (tmp_path / "leech.bin").read_bytes() == data


async def test_never_connects_to_itself(tmp_path, sessions):
    data = make_payload(PIECE_LENGTH)
    meta = TorrentMetadata.from_bytes(build_torrent(data, "f.bin", PIECE_LENGTH))
    seed = sessions(await seeder(tmp_path, data, meta))

    await seed.add_peer("127.0.0.1", seed.port)
    await asyncio.sleep(0.2)
    assert seed.active_peers() == []


async def test_progress_reporting(tmp_path, sessions):
    data = make_payload(PIECE_LENGTH * 3)
    meta = TorrentMetadata.from_bytes(build_torrent(data, "f.bin", PIECE_LENGTH))

    seed = sessions(await seeder(tmp_path, data, meta))
    before = seed.progress()
    assert before["complete"] is True
    assert before["pieces_have"] == before["pieces_total"] == 3
    assert before["bytes_have"] == len(data)

    leech = sessions(await leecher(tmp_path, meta))
    assert leech.progress()["pieces_have"] == 0
    await leech.add_peer("127.0.0.1", seed.port)
    assert await leech.wait_complete(timeout=30)
    assert leech.progress()["complete"] is True


async def test_seeder_serves_multiple_leechers_concurrently(tmp_path, sessions):
    data = make_payload(PIECE_LENGTH * 4)
    meta = TorrentMetadata.from_bytes(build_torrent(data, "f.bin", PIECE_LENGTH))

    seed = sessions(await seeder(tmp_path, data, meta))
    leeches = [sessions(await leecher(tmp_path, meta, name=f"l{i}.bin")) for i in range(3)]
    for leech in leeches:
        await leech.add_peer("127.0.0.1", seed.port)

    results = await asyncio.gather(*(l.wait_complete(timeout=40) for l in leeches))
    assert all(results), [l.progress() for l in leeches]
    for i in range(3):
        assert (tmp_path / f"l{i}.bin").read_bytes() == data
