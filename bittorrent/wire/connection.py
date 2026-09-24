"""One TCP connection to one peer.

Owns the wire-level conversation (handshake, choke/interest state, message
read loop, request pipelining) and reports everything upward to the session,
which owns the cross-peer decisions (what to request, who to unchoke).

The lifecycle is an explicit state machine -- CONNECTING -> HANDSHAKING ->
ACTIVE -> CLOSED -- so out-of-order messages (a `piece` arriving before the
handshake, say) are rejected rather than half-processed.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, Optional, Set, Tuple

from bittorrent import constants
from bittorrent.wire import messages as wire

logger = logging.getLogger(__name__)

CONNECTING = "connecting"
HANDSHAKING = "handshaking"
ACTIVE = "active"
CLOSED = "closed"


class PeerConnection:
    def __init__(self, session, host: str, port: int, reader=None, writer=None, peer_id=None):
        self.session = session
        self.host = host
        self.port = port
        self._reader: Optional[asyncio.StreamReader] = reader
        self._writer: Optional[asyncio.StreamWriter] = writer
        self.peer_id: Optional[bytes] = peer_id

        self.state = CONNECTING if reader is None else HANDSHAKING
        # Which side dialed. Needed to break the tie when both peers dial each
        # other and end up with two connections for the same pair.
        self.initiated_by_us = reader is None
        self.peer_pieces: Set[int] = set()

        # Four-way state, as in BEP3. "am_*" is our view of them; "peer_*" theirs of us.
        self.am_choking = True
        self.am_interested = False
        self.peer_choking = True
        self.peer_interested = False

        # Outstanding block requests we've sent: (index, begin) -> sent-at time.
        self.pending: Dict[Tuple[int, int], float] = {}
        self.bytes_downloaded = 0  # cumulative; the choke manager reads deltas
        self.bytes_uploaded = 0
        self.failures = 0
        self.last_message_at = time.monotonic()

        self._read_task: Optional[asyncio.Task] = None

    @property
    def key(self) -> str:
        return f"{self.host}:{self.port}"

    def __repr__(self) -> str:
        return f"<PeerConnection {self.key} {self.state}>"

    # -- setup --

    async def connect(self) -> None:
        """Dial out, handshake, and start the read loop."""
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port), timeout=constants.CONNECT_TIMEOUT
        )
        self.state = HANDSHAKING
        await self._handshake(initiate=True)

    async def accept(self) -> None:
        """Finish setting up a connection someone else dialed to us."""
        await self._handshake(initiate=False)

    async def _handshake(self, initiate: bool) -> None:
        ours = wire.Handshake(
            info_hash=self.session.metadata.info_hash, peer_id=self.session.peer_id
        )
        if initiate:
            self._writer.write(ours.encode())
            await self._writer.drain()

        theirs = await asyncio.wait_for(
            wire.read_handshake(self._reader), timeout=constants.HANDSHAKE_TIMEOUT
        )
        if theirs.info_hash != self.session.metadata.info_hash:
            raise wire.WireError(
                f"peer {self.key} handshook for a different torrent "
                f"({theirs.info_hash.hex()[:16]}...)"
            )

        if not initiate:
            self._writer.write(ours.encode())
            await self._writer.drain()

        self.peer_id = theirs.peer_id
        self.state = ACTIVE

    def start(self) -> None:
        self._read_task = asyncio.get_event_loop().create_task(self._read_loop())

    # -- outbound --

    async def send(self, msg) -> None:
        if self.state == CLOSED or self._writer is None:
            return
        try:
            await wire.write_message(self._writer, msg)
        except (ConnectionError, OSError) as exc:
            logger.debug("send to %s failed: %s", self.key, exc)
            await self.close()

    async def send_bitfield(self, field: bytes) -> None:
        await self.send(wire.Bitfield(field=field))

    async def send_have(self, index: int) -> None:
        await self.send(wire.Have(index=index))

    async def set_interested(self, interested: bool) -> None:
        if self.am_interested == interested:
            return
        self.am_interested = interested
        await self.send(wire.Interested() if interested else wire.NotInterested())

    async def set_choking(self, choking: bool) -> None:
        if self.am_choking == choking:
            return
        self.am_choking = choking
        await self.send(wire.Choke() if choking else wire.Unchoke())

    async def request_block(self, index: int, begin: int, length: int) -> None:
        self.pending[(index, begin)] = time.monotonic()
        await self.send(wire.Request(index=index, begin=begin, length=length))

    async def cancel_block(self, index: int, begin: int, length: int) -> None:
        self.pending.pop((index, begin), None)
        await self.send(wire.Cancel(index=index, begin=begin, length=length))

    @property
    def free_request_slots(self) -> int:
        return max(0, constants.MAX_PIPELINED_REQUESTS - len(self.pending))

    # -- inbound --

    async def _read_loop(self) -> None:
        try:
            while self.state == ACTIVE:
                msg = await wire.read_message(self._reader)
                self.last_message_at = time.monotonic()
                await self._handle(msg)
        except (asyncio.IncompleteReadError, ConnectionError, OSError) as exc:
            logger.debug("peer %s disconnected: %s", self.key, exc)
        except wire.WireError as exc:
            logger.warning("protocol violation from %s: %s", self.key, exc)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("unexpected error on connection to %s", self.key)
        finally:
            await self.close()

    async def _handle(self, msg) -> None:
        if isinstance(msg, wire.KeepAlive):
            return
        if isinstance(msg, wire.Choke):
            self.peer_choking = True
            # Anything outstanding won't be answered while choked -- hand the
            # claims back so other peers can pick those blocks up.
            self._release_pending()
        elif isinstance(msg, wire.Unchoke):
            self.peer_choking = False
            await self.session.on_unchoked(self)
        elif isinstance(msg, wire.Interested):
            self.peer_interested = True
        elif isinstance(msg, wire.NotInterested):
            self.peer_interested = False
        elif isinstance(msg, wire.Have):
            self.peer_pieces.add(msg.index)
            await self.session.on_have(self, msg.index)
        elif isinstance(msg, wire.Bitfield):
            await self.session.on_bitfield(self, msg.field)
        elif isinstance(msg, wire.Request):
            await self.session.on_request(self, msg.index, msg.begin, msg.length)
        elif isinstance(msg, wire.Piece):
            self.pending.pop((msg.index, msg.begin), None)
            self.bytes_downloaded += len(msg.block)
            await self.session.on_piece(self, msg.index, msg.begin, msg.block)
        elif isinstance(msg, wire.Cancel):
            pass  # we serve requests immediately, so there's nothing queued to cancel

    def _release_pending(self) -> None:
        for index, begin in list(self.pending):
            self.session.picker.release_block(index, begin, self.key)
        self.pending.clear()

    def expired_requests(self, now: float):
        """Requests older than the timeout, for the session's sweep."""
        return [
            (index, begin)
            for (index, begin), sent_at in self.pending.items()
            if now - sent_at > constants.BLOCK_REQUEST_TIMEOUT
        ]

    # -- teardown --

    async def close(self) -> None:
        if self.state == CLOSED:
            return
        self.state = CLOSED
        self._release_pending()

        # close() is also called from _read_loop's own finally block, so only
        # cancel the read task when we're not currently running inside it --
        # otherwise the connection would cancel itself mid-teardown.
        task, self._read_task = self._read_task, None
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

        if self._writer is not None:
            try:
                self._writer.close()
            except Exception:
                pass
        await self.session.on_peer_disconnected(self)
