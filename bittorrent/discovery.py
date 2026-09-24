"""Finding peers for a torrent through the DHT.

The torrent's info_hash is 20 bytes, exactly like a NodeID, so "who is in this
swarm" becomes "which nodes are closest to this key" -- the DHT's existing
iterative lookup answers it unchanged.

What the DHT's own STORE *cannot* do is hold a peer list. `DataStore.put`
replaces a single value per key, so a second peer announcing to the same node
would overwrite the first. Swarm membership needs additive semantics, so this
module registers two extension RPCs with their own per-node table:

    ANNOUNCE_PEER(info_hash, port) -> "add me to the set for this info_hash"
    GET_PEERS(info_hash)           -> "who's in that set?"

Every announcing peer sends ANNOUNCE_PEER to *all* of the K closest nodes it
finds (not one each), and since closeness to a fixed key doesn't depend on who
is asking, every peer's announcement converges on the same K nodes. Each of
those nodes therefore accumulates the whole peer list independently -- K-way
replication, not a partition -- so querying a few of them is redundancy
against a missed announcement rather than reassembly of fragments.

Announcements are a *liveness lease*, not durable data: a node never
republishes someone else's announcement (it cannot honestly assert that peer
is still up), entries expire on a short TTL, and each peer re-announces itself
on a timer. A peer that goes away simply stops renewing and falls out.
"""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import struct
import time
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple

from bittorrent import constants
from kademlia.identifier import ID_BYTES, NodeID
from kademlia.node import Node
from kademlia.rpc import messages as krpc
from kademlia.rpc.protocol import RPCErrorResponse, RPCTimeoutError

logger = logging.getLogger(__name__)

ANNOUNCE_PEER = b"announce_peer"
GET_PEERS = b"get_peers"
PEER_RECORD_SIZE = 6  # 4-byte IPv4 + 2-byte port, as in BEP5's compact form

Peer = Tuple[str, int]


# -- message types -----------------------------------------------------------


@dataclass
class AnnouncePeerQuery:
    tid: bytes
    sender_id: NodeID
    info_hash: bytes
    port: int


@dataclass
class AnnouncePeerResponse:
    tid: bytes
    responder_id: NodeID


@dataclass
class GetPeersQuery:
    tid: bytes
    sender_id: NodeID
    info_hash: bytes


@dataclass
class GetPeersResponse:
    tid: bytes
    responder_id: NodeID
    peers: List[Peer] = field(default_factory=list)


def encode_peers(peers: List[Peer]) -> bytes:
    out = bytearray()
    for host, port in peers:
        out += ipaddress.IPv4Address(host).packed + struct.pack(">H", port)
    return bytes(out)


def decode_peers(raw: bytes) -> List[Peer]:
    if len(raw) % PEER_RECORD_SIZE != 0:
        raise krpc.MalformedMessageError("peer list is not a multiple of the record size")
    peers = []
    for offset in range(0, len(raw), PEER_RECORD_SIZE):
        chunk = raw[offset : offset + PEER_RECORD_SIZE]
        peers.append((str(ipaddress.IPv4Address(chunk[:4])), struct.unpack(">H", chunk[4:])[0]))
    return peers


def _validate_info_hash(raw: bytes) -> bytes:
    if len(raw) != ID_BYTES:
        raise krpc.MalformedMessageError(f"info_hash must be {ID_BYTES} bytes, got {len(raw)}")
    return raw


krpc.register_query(
    ANNOUNCE_PEER,
    AnnouncePeerQuery,
    lambda m: {b"info_hash": m.info_hash, b"port": m.port},
    lambda tid, sender_id, a: AnnouncePeerQuery(
        tid=tid,
        sender_id=sender_id,
        info_hash=_validate_info_hash(a.get(b"info_hash", b"")),
        port=_require_port(a),
    ),
)
krpc.register_query(
    GET_PEERS,
    GetPeersQuery,
    lambda m: {b"info_hash": m.info_hash},
    lambda tid, sender_id, a: GetPeersQuery(
        tid=tid, sender_id=sender_id, info_hash=_validate_info_hash(a.get(b"info_hash", b""))
    ),
)
krpc.register_response(
    b"announce_peer",
    AnnouncePeerResponse,
    lambda m: {},
    lambda tid, responder_id, r: AnnouncePeerResponse(tid=tid, responder_id=responder_id),
)
krpc.register_response(
    b"get_peers",
    GetPeersResponse,
    lambda m: {b"peers": encode_peers(m.peers)},
    lambda tid, responder_id, r: GetPeersResponse(
        tid=tid, responder_id=responder_id, peers=decode_peers(r.get(b"peers", b""))
    ),
)


def _require_port(a: dict) -> int:
    port = a.get(b"port")
    if not isinstance(port, int) or not 0 < port < 65536:
        raise krpc.MalformedMessageError(f"invalid announce port: {port!r}")
    return port


# -- the per-node announcement table -----------------------------------------


class PeerTable:
    """Peers announced to this node, per info_hash, each on a short TTL."""

    def __init__(self, ttl: float = constants.ANNOUNCE_TTL):
        self.ttl = ttl
        self._peers: Dict[bytes, Dict[Peer, float]] = {}

    def add(self, info_hash: bytes, peer: Peer) -> None:
        # Re-announcing is just a set-add with a refreshed expiry: idempotent,
        # no dedup logic needed.
        self._peers.setdefault(info_hash, {})[peer] = time.monotonic() + self.ttl

    def get(self, info_hash: bytes) -> List[Peer]:
        now = time.monotonic()
        entries = self._peers.get(info_hash, {})
        live = [peer for peer, expires in entries.items() if expires > now]
        for peer in [p for p, e in entries.items() if e <= now]:
            del entries[peer]
        return live

    def purge_expired(self) -> None:
        now = time.monotonic()
        for info_hash in list(self._peers):
            entries = self._peers[info_hash]
            for peer in [p for p, e in entries.items() if e <= now]:
                del entries[peer]
            if not entries:
                del self._peers[info_hash]

    def __len__(self) -> int:
        return sum(len(v) for v in self._peers.values())


# -- the discovery service ---------------------------------------------------


class PeerDiscovery:
    """Serves ANNOUNCE_PEER/GET_PEERS for a node, and drives lookups for a torrent."""

    def __init__(self, node: Node, ttl: float = constants.ANNOUNCE_TTL):
        self.node = node
        self.table = PeerTable(ttl=ttl)
        node.register_query_handler(AnnouncePeerQuery, self._handle_announce)
        node.register_query_handler(GetPeersQuery, self._handle_get_peers)

    # -- serving --

    def _handle_announce(self, msg: AnnouncePeerQuery, addr):
        # The IP is taken from the datagram's source address, never from the
        # message: a peer can claim any port (its TCP listener differs from its
        # DHT socket) but must not be able to announce someone else's address.
        self.table.add(msg.info_hash, (addr[0], msg.port))
        return AnnouncePeerResponse(tid=msg.tid, responder_id=self.node.id)

    def _handle_get_peers(self, msg: GetPeersQuery, addr):
        return GetPeersResponse(
            tid=msg.tid, responder_id=self.node.id, peers=self.table.get(msg.info_hash)
        )

    # -- driving --

    async def announce(self, info_hash: bytes, port: int) -> int:
        """Tell the K closest nodes that we're in this swarm. Returns the ack count.

        Fans out concurrently, like Node.store() does -- one slow or dead
        contact shouldn't hold up the rest.
        """
        contacts = await self.node.find_node(NodeID(info_hash))
        if not contacts:
            return 0
        results = await asyncio.gather(
            *(self._announce_one(c, info_hash, port) for c in contacts)
        )
        return sum(1 for ok in results if ok)

    async def _announce_one(self, contact, info_hash: bytes, port: int) -> bool:
        try:
            await self.node.send_extension_query(
                contact,
                lambda tid: AnnouncePeerQuery(
                    tid=tid, sender_id=self.node.id, info_hash=info_hash, port=port
                ),
            )
            return True
        except (RPCTimeoutError, RPCErrorResponse) as exc:
            logger.debug("announce to %s failed: %s", contact, exc)
            return False

    async def get_peers(self, info_hash: bytes) -> Set[Peer]:
        """Union the peer lists held by the nodes closest to this info_hash.

        Includes our *own* table. A lookup never returns the searcher itself,
        so without this a node would ignore announcements made directly to it
        -- which is exactly where they land in a small swarm, and would leave
        the answer sitting locally unread.
        """
        peers: Set[Peer] = set(self.table.get(info_hash))
        contacts = await self.node.find_node(NodeID(info_hash))
        if contacts:
            results = await asyncio.gather(
                *(self._get_peers_one(c, info_hash) for c in contacts)
            )
            for found in results:
                peers.update(found)
        return peers

    async def _get_peers_one(self, contact, info_hash: bytes) -> List[Peer]:
        try:
            response = await self.node.send_extension_query(
                contact,
                lambda tid: GetPeersQuery(tid=tid, sender_id=self.node.id, info_hash=info_hash),
            )
        except (RPCTimeoutError, RPCErrorResponse) as exc:
            logger.debug("get_peers from %s failed: %s", contact, exc)
            return []
        return list(getattr(response, "peers", ()))
