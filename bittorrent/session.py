"""A TorrentSession: everything for one torrent, in one object.

Deliberately per-torrent (piece store, picker, peer set, choking state), so
running several torrents at once is just several of these sharing one DHT
node -- the DHT is generic infrastructure keyed by info_hash and has no notion
of "torrent" at all.

Event flow: PeerConnection owns one peer's wire conversation and calls back
into here for anything needing a cross-peer decision (what to request next,
who to unchoke). This object owns the shared state; connections own only their
own socket.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Dict, List, Optional, Tuple

from bittorrent import constants
from bittorrent.choking import ChokeManager
from bittorrent.discovery import PeerDiscovery
from bittorrent.metadata import TorrentMetadata
from bittorrent.picker import PiecePicker
from bittorrent.storage import PieceStore, parse_bitfield
from bittorrent.wire import messages as wire
from bittorrent.wire.connection import ACTIVE, PeerConnection

logger = logging.getLogger(__name__)

Peer = Tuple[str, int]

# Ways setting up a peer can legitimately fail. IncompleteReadError is in here
# because a peer that rejects our handshake (wrong torrent, say) just closes
# the socket -- and it derives from EOFError, not OSError, so it would escape a
# connection-error-only except clause.
PEER_SETUP_ERRORS = (
    wire.WireError,
    asyncio.IncompleteReadError,
    asyncio.TimeoutError,
    ConnectionError,
    OSError,
)


class TorrentSession:
    def __init__(
        self,
        metadata: TorrentMetadata,
        path: str,
        node=None,
        host: str = "0.0.0.0",
        port: int = 0,
        peer_id: Optional[bytes] = None,
        discovery: Optional[PeerDiscovery] = None,
        announce_interval: float = constants.ANNOUNCE_INTERVAL,
        discovery_interval: float = constants.PEER_DISCOVERY_INTERVAL,
        unchoke_interval: float = constants.UNCHOKE_INTERVAL,
    ):
        self.metadata = metadata
        self.peer_id = peer_id or os.urandom(20)
        self.host = host
        self.port = port

        self.store = PieceStore(path, metadata)
        self.picker = PiecePicker(metadata)
        self.choking = ChokeManager(self)
        self.node = node
        self.discovery = discovery or (PeerDiscovery(node) if node is not None else None)

        self.peers: Dict[str, PeerConnection] = {}
        self._failed_peers: Dict[str, float] = {}  # key -> retry-not-before
        self._server: Optional[asyncio.AbstractServer] = None
        self._tasks: List[asyncio.Task] = []
        self._complete = asyncio.Event()
        self._connect_semaphore = asyncio.Semaphore(constants.MAX_CONCURRENT_CONNECT_ATTEMPTS)

        self.announce_interval = announce_interval
        self.discovery_interval = discovery_interval
        self.unchoke_interval = unchoke_interval

    # -- lifecycle --

    async def start(self) -> None:
        await self.store.open()
        for index in self.store.have:
            self.picker.piece_verified(index)

        self._server = await asyncio.start_server(self._on_incoming, self.host, self.port)
        self.port = self._server.sockets[0].getsockname()[1]
        if self.store.is_complete:
            self._complete.set()

        loop = asyncio.get_event_loop()
        self._tasks = [
            loop.create_task(self._announce_loop()),
            loop.create_task(self._discovery_loop()),
            loop.create_task(self._choke_loop()),
            loop.create_task(self._timeout_loop()),
        ]
        logger.info(
            "session for %s listening on %s:%d (%d/%d pieces)",
            self.metadata.name, self.host, self.port,
            len(self.store.have), self.metadata.piece_count,
        )

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        for conn in list(self.peers.values()):
            await conn.close()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def wait_complete(self, timeout: Optional[float] = None) -> bool:
        try:
            await asyncio.wait_for(self._complete.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    # -- progress --

    @property
    def is_complete(self) -> bool:
        return self.store.is_complete

    def progress(self) -> dict:
        return {
            "name": self.metadata.name,
            "info_hash": self.metadata.info_hash.hex(),
            "pieces_total": self.metadata.piece_count,
            "pieces_have": len(self.store.have),
            "bytes_total": self.metadata.total_length,
            "bytes_have": self.store.bytes_downloaded,
            "complete": self.store.is_complete,
            "peers": [
                {
                    "key": c.key,
                    "choked_by_peer": c.peer_choking,
                    "choking_peer": c.am_choking,
                    "downloaded": c.bytes_downloaded,
                    "uploaded": c.bytes_uploaded,
                    "pieces": len(c.peer_pieces),
                }
                for c in self.active_peers()
            ],
        }

    def active_peers(self) -> List[PeerConnection]:
        return [c for c in self.peers.values() if c.state == ACTIVE]

    # -- peer setup --

    async def _on_incoming(self, reader, writer) -> None:
        host, port = writer.get_extra_info("peername")[:2]
        conn = PeerConnection(self, host, port, reader=reader, writer=writer)
        try:
            await conn.accept()
        except PEER_SETUP_ERRORS as exc:
            logger.debug("inbound handshake from %s:%d failed: %s", host, port, exc)
            writer.close()
            return
        await self._register(conn)

    async def add_peer(self, host: str, port: int) -> bool:
        """Dial a peer discovered via the DHT. True if it joined."""
        key = f"{host}:{port}"
        if key in self.peers or self._is_self(host, port):
            return False
        if self._failed_peers.get(key, 0) > time.monotonic():
            return False  # still in backoff from an earlier failure
        if len(self.peers) >= constants.MAX_PEER_CONNECTIONS:
            return False

        conn = PeerConnection(self, host, port)
        async with self._connect_semaphore:
            try:
                await conn.connect()
            except PEER_SETUP_ERRORS as exc:
                logger.debug("connecting to %s failed: %s", key, exc)
                self._failed_peers[key] = time.monotonic() + constants.PEER_RETRY_BACKOFF
                return False
        await self._register(conn)
        return True

    def _is_self(self, host: str, port: int) -> bool:
        return port == self.port and host in ("127.0.0.1", "localhost", self.host)

    def _find_duplicate(self, conn: PeerConnection) -> Optional[PeerConnection]:
        """An existing connection to the same peer, identified by peer_id.

        Addresses can't be used for this: an outbound connection is keyed by
        the peer's *listening* port, while an inbound one only reveals the
        remote's ephemeral source port, so the same peer looks like two
        different addresses.
        """
        if conn.peer_id is None:
            return None
        for other in self.peers.values():
            if other is not conn and other.peer_id == conn.peer_id:
                return other
        return None

    async def _register(self, conn: PeerConnection) -> None:
        if conn.peer_id == self.peer_id:
            logger.debug("refusing a connection to ourselves")
            await conn.close()
            return

        duplicate = self._find_duplicate(conn)
        if duplicate is not None:
            # Both peers dialed each other. Both sides apply this same rule to
            # the same pair of ids, so they agree on which connection survives
            # instead of each dropping a different one and killing both.
            keep_outbound = self.peer_id > conn.peer_id
            if conn.initiated_by_us == keep_outbound:
                logger.debug("duplicate peer %s: dropping the older connection", conn.key)
                await duplicate.close()
            else:
                logger.debug("duplicate peer %s: keeping the existing connection", conn.key)
                await conn.close()
                return

        self.peers[conn.key] = conn
        conn.start()
        if self.store.have:
            await conn.send_bitfield(self.store.bitfield())
        logger.debug("peer %s joined (%d total)", conn.key, len(self.peers))

    # -- wire callbacks --

    async def on_bitfield(self, conn: PeerConnection, field: bytes) -> None:
        try:
            pieces = parse_bitfield(field, self.metadata.piece_count)
        except IndexError:
            logger.warning("peer %s sent a short bitfield", conn.key)
            await conn.close()
            return
        self.picker.remove_peer(conn.peer_pieces)  # in case of a re-sent bitfield
        conn.peer_pieces = pieces
        self.picker.add_peer(pieces)
        await self._update_interest(conn)
        await self._maybe_request(conn)

    async def on_have(self, conn: PeerConnection, index: int) -> None:
        if not 0 <= index < self.metadata.piece_count:
            logger.warning("peer %s announced out-of-range piece %d", conn.key, index)
            return
        self.picker.peer_got_piece(index)
        await self._update_interest(conn)
        await self._maybe_request(conn)

    async def on_unchoked(self, conn: PeerConnection) -> None:
        await self._maybe_request(conn)

    async def on_piece(self, conn: PeerConnection, index: int, begin: int, block: bytes) -> None:
        try:
            await self.store.write_block(index, begin, block)
        except (ValueError, IndexError) as exc:
            logger.warning("peer %s sent an invalid block: %s", conn.key, exc)
            conn.failures += 1
            return

        # Guard the side effects: a duplicate delivery (endgame, or a late
        # answer to a timed-out request) must not re-trigger verification.
        if self.store.has_piece(index):
            await self._maybe_request(conn)
            return

        if self.picker.block_received(index, begin):
            await self._piece_complete(conn, index)
        await self._maybe_request(conn)

    async def _piece_complete(self, conn: PeerConnection, index: int) -> None:
        if await self.store.verify_piece(index):
            self.picker.piece_verified(index)
            logger.debug(
                "piece %d verified (%d/%d)",
                index, len(self.store.have), self.metadata.piece_count,
            )
            for peer in self.active_peers():
                await peer.send_have(index)
                await self._update_interest(peer)
            if self.store.is_complete:
                logger.info("download complete: %s", self.metadata.name)
                self._complete.set()
        else:
            # Can't tell which contributor sent bad data (hashes are
            # piece-granular), so penalise the one that finished it and retry
            # the whole piece elsewhere.
            logger.warning("piece %d failed its hash check", index)
            self.picker.piece_failed(index)
            conn.failures += 1
            if conn.failures >= constants.MAX_PEER_FAILURES:
                self._failed_peers[conn.key] = time.monotonic() + constants.PEER_RETRY_BACKOFF
                await conn.close()

    async def on_request(self, conn: PeerConnection, index: int, begin: int, length: int) -> None:
        if conn.am_choking:
            return  # choked peers get no data -- that's what choking means
        if length > wire.MAX_MESSAGE_SIZE or not self.store.has_piece(index):
            return
        try:
            block = await self.store.read_block(index, begin, length)
        except (ValueError, IndexError) as exc:
            logger.debug("bad request from %s: %s", conn.key, exc)
            return
        conn.bytes_uploaded += len(block)
        await conn.send(wire.Piece(index=index, begin=begin, block=block))

    async def on_peer_disconnected(self, conn: PeerConnection) -> None:
        self.peers.pop(conn.key, None)
        self.picker.remove_peer(conn.peer_pieces)
        self.picker.release_peer(conn.key)  # its claims become pickable again
        self.choking.forget(conn.key)
        logger.debug("peer %s left (%d remain)", conn.key, len(self.peers))

    # -- requesting --

    async def _update_interest(self, conn: PeerConnection) -> None:
        wanted = bool(conn.peer_pieces - self.store.have)
        await conn.set_interested(wanted)

    async def _maybe_request(self, conn: PeerConnection) -> None:
        if conn.state != ACTIVE or conn.peer_choking or not conn.am_interested:
            return
        slots = conn.free_request_slots
        if slots <= 0:
            return
        # pick() claims the blocks synchronously before we await on sending,
        # so a concurrent peer coroutine can't claim the same ones.
        for index, begin, length in self.picker.pick(conn.peer_pieces, conn.key, slots):
            await conn.request_block(index, begin, length)

    # -- background loops --

    async def _announce_loop(self) -> None:
        try:
            while True:
                acked = 0
                if self.discovery is not None:
                    try:
                        acked = await self.discovery.announce(self.metadata.info_hash, self.port)
                        logger.debug("announced to %d node(s)", acked)
                    except Exception:
                        logger.exception("announce failed")
                # Nothing accepted it (we may still be alone in the DHT) --
                # retry soon rather than going undiscoverable for minutes.
                delay = self.announce_interval if acked else min(
                    constants.ANNOUNCE_RETRY_INTERVAL, self.announce_interval
                )
                await asyncio.sleep(delay)
        except asyncio.CancelledError:
            pass

    async def _discovery_loop(self) -> None:
        try:
            while True:
                if self.discovery is not None and len(self.peers) < constants.MAX_PEER_CONNECTIONS:
                    try:
                        peers = await self.discovery.get_peers(self.metadata.info_hash)
                        for host, port in peers:
                            await self.add_peer(host, port)
                    except Exception:
                        logger.exception("peer discovery failed")
                # Look harder while we still want data and have nobody to get it from.
                starving = not self.peers and not self.store.is_complete
                delay = min(
                    constants.PEER_DISCOVERY_RETRY_INTERVAL, self.discovery_interval
                ) if starving else self.discovery_interval
                await asyncio.sleep(delay)
        except asyncio.CancelledError:
            pass

    async def _choke_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.unchoke_interval)
                try:
                    await self.choking.tick()
                except Exception:
                    logger.exception("choke round failed")
        except asyncio.CancelledError:
            pass

    async def _timeout_loop(self) -> None:
        """Reassign blocks a peer accepted but never answered.

        A timeout means "busy", not "gone", so the block goes to a *different*
        peer rather than being re-requested from the same one.
        """
        try:
            while True:
                await asyncio.sleep(5.0)
                now = time.monotonic()
                for conn in self.active_peers():
                    for index, begin in conn.expired_requests(now):
                        conn.pending.pop((index, begin), None)
                        self.picker.release_block(index, begin, conn.key)
                        conn.failures += 1
                    if conn.failures >= constants.MAX_PEER_FAILURES:
                        self._failed_peers[conn.key] = now + constants.PEER_RETRY_BACKOFF
                        await conn.close()
                for conn in self.active_peers():
                    await self._maybe_request(conn)
        except asyncio.CancelledError:
            pass
