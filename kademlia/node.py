"""The public Kademlia DHT node: wires together routing table, storage, the
UDP RPC transport, and the iterative lookup engine behind a small async API
(`start`, `stop`, `join`, `ping`, `store`, `find_node`, `find_value`).
"""

from __future__ import annotations

import asyncio
import logging
import math
from typing import Dict, List, Optional, Tuple, Union

from kademlia import lookup
from kademlia.constants import (
    BUCKET_REFRESH_INTERVAL,
    EXPIRY_SWEEP_INTERVAL,
    MAINTENANCE_TICK_INTERVAL,
    REPUBLISH_INTERVAL,
    RPC_RETRIES,
    RPC_TIMEOUT,
    TTL_DEFAULT,
)
from kademlia.identifier import ID_BYTES, NodeID
from kademlia.routing_table import Contact, RoutingTable
from kademlia.rpc.messages import (
    ErrorMessage,
    FindNodeQuery,
    FindNodeResponse,
    FindValueQuery,
    FindValueResponse,
    PingQuery,
    PingResponse,
    Query,
    Response,
    StoreQuery,
    StoreResponse,
)
from kademlia.rpc.protocol import KademliaProtocol, RPCErrorResponse, RPCTimeoutError
from kademlia.storage import DataStore

logger = logging.getLogger(__name__)

MAX_JOIN_BUCKET_REFRESHES = 20


class DHTError(Exception):
    """Base class for errors raised by the Kademlia DHT layer."""


class BootstrapError(DHTError):
    """Raised when a node fails to join the network via any bootstrap contact."""


class Node:
    def __init__(
        self,
        host: str,
        port: int,
        node_id: Optional[NodeID] = None,
        *,
        rpc_timeout: float = RPC_TIMEOUT,
        rpc_retries: int = RPC_RETRIES,
        refresh_interval: float = BUCKET_REFRESH_INTERVAL,
        republish_interval: float = REPUBLISH_INTERVAL,
        expiry_sweep_interval: float = EXPIRY_SWEEP_INTERVAL,
        maintenance_tick_interval: float = MAINTENANCE_TICK_INTERVAL,
    ):
        self.host = host
        self.port = port
        self.id = node_id or NodeID.random()

        self.routing_table = RoutingTable(self.id)
        self.storage = DataStore()

        # Overridable at construction time (mainly for tests) so background
        # maintenance and RPC timing don't have to run at paper-scale
        # (hour-long) intervals to be exercised end-to-end.
        self.rpc_timeout = rpc_timeout
        self.rpc_retries = rpc_retries
        self.refresh_interval = refresh_interval
        self.republish_interval = republish_interval
        self.expiry_sweep_interval = expiry_sweep_interval
        self.maintenance_tick_interval = maintenance_tick_interval

        self.transport: Optional[asyncio.DatagramTransport] = None
        self.protocol: Optional[KademliaProtocol] = None
        self._refresh_task: Optional[asyncio.Task] = None
        self._expiry_task: Optional[asyncio.Task] = None

    # -- lifecycle --

    async def start(self) -> None:
        loop = asyncio.get_event_loop()
        self.transport, self.protocol = await loop.create_datagram_endpoint(
            lambda: KademliaProtocol(self), local_addr=(self.host, self.port)
        )
        sockname = self.transport.get_extra_info("sockname")
        if sockname:
            self.port = sockname[1]
        self._refresh_task = asyncio.create_task(self._refresh_loop())
        self._expiry_task = asyncio.create_task(self._expiry_and_republish_loop())

    async def stop(self) -> None:
        for task in (self._refresh_task, self._expiry_task):
            if task is not None:
                task.cancel()
        pending = [t for t in (self._refresh_task, self._expiry_task) if t is not None]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        if self.transport is not None:
            self.transport.close()

    @property
    def contact(self) -> Contact:
        return Contact(node_id=self.id, ip=self.host, port=self.port)

    def routing_table_snapshot(self) -> List[Contact]:
        return self.routing_table.all_contacts()

    def local_store_snapshot(self) -> Dict[bytes, bytes]:
        return self.storage.snapshot()

    # -- public DHT API --

    async def join(self, bootstrap_contacts: List[Contact]) -> None:
        if not bootstrap_contacts:
            return  # first/seed node of the network

        responded = False
        for contact in bootstrap_contacts:
            if await self.rpc_ping(contact):
                responded = True
        if not responded:
            raise BootstrapError("no bootstrap contact responded")

        await lookup.iterative_find_node(self, self.id)

        for idx in self.routing_table.empty_buckets()[:MAX_JOIN_BUCKET_REFRESHES]:
            target = NodeID.random_in_bucket_range(idx, self.id)
            await lookup.iterative_find_node(self, target)

    async def ping(self, contact: Contact) -> bool:
        return await self.rpc_ping(contact)

    async def store(self, key: bytes, value: bytes, ttl: int = TTL_DEFAULT) -> int:
        self.storage.put(key, value, ttl=ttl)
        target = NodeID(key)
        closest = await lookup.iterative_find_node(self, target)
        if not closest:
            return 1
        acked = await asyncio.gather(*(self.rpc_store(c, key, value, ttl) for c in closest))
        return sum(1 for ok in acked if ok) + 1

    async def find_node(self, target_id: NodeID) -> List[Contact]:
        return await lookup.iterative_find_node(self, target_id)

    async def find_value(self, key: bytes) -> Optional[bytes]:
        local = self.storage.get(key)
        if local is not None:
            return local
        result = await lookup.iterative_find_value(self, NodeID(key))
        return result.value

    # -- routing table maintenance --

    def add_contact(self, contact: Contact) -> None:
        if contact.node_id == self.id:
            return
        least_recently_seen = self.routing_table.add_contact(contact)
        if least_recently_seen is not None:
            asyncio.create_task(self._verify_and_replace(least_recently_seen, contact))

    async def _verify_and_replace(self, least_recently_seen: Contact, candidate: Contact) -> None:
        alive = await self.rpc_ping(least_recently_seen)
        if alive:
            self.routing_table.add_contact(least_recently_seen)  # touch as most-recently-seen
        else:
            self.routing_table.remove_contact(least_recently_seen.node_id)
            self.routing_table.add_contact(candidate)

    def on_contact_unresponsive(self, contact: Contact) -> None:
        self.routing_table.remove_contact(contact.node_id)

    # -- outgoing RPC wrappers (single-attempt-per-call, with retry) --

    async def _send_with_retry(self, contact: Contact, build_query, retries: Optional[int] = None, timeout: Optional[float] = None) -> Response:
        assert self.protocol is not None, "node not started"
        retries = self.rpc_retries if retries is None else retries
        timeout = self.rpc_timeout if timeout is None else timeout
        last_exc: Optional[Exception] = None
        for _ in range(retries + 1):
            query = build_query(self.protocol.new_transaction_id())
            try:
                response, _addr = await self.protocol.send_query(contact, query, timeout=timeout)
                return response
            except (RPCTimeoutError, RPCErrorResponse) as exc:
                last_exc = exc
        assert last_exc is not None
        raise last_exc

    def _trusted_responder(self, contact: Contact, response: Response) -> Contact:
        """The contact info to add to the routing table: the address we
        actually sent to, paired with the ID the peer itself declared in its
        response (never the caller-supplied contact.node_id, which may be a
        placeholder -- e.g. an unauthenticated bootstrap host:port)."""
        return Contact(node_id=response.responder_id, ip=contact.ip, port=contact.port)

    async def rpc_ping(self, contact: Contact) -> bool:
        try:
            response = await self._send_with_retry(contact, lambda tid: PingQuery(tid=tid, sender_id=self.id))
        except (RPCTimeoutError, RPCErrorResponse):
            self.on_contact_unresponsive(contact)
            return False
        self.add_contact(self._trusted_responder(contact, response))
        return True

    async def rpc_store(self, contact: Contact, key: bytes, value: bytes, ttl: Optional[int] = None) -> bool:
        # bencode has no float type; round up so a fractional ttl never truncates to an already-expired 0
        wire_ttl = max(1, math.ceil(ttl)) if ttl is not None else None
        try:
            response = await self._send_with_retry(contact, lambda tid: StoreQuery(tid=tid, sender_id=self.id, key=key, value=value, ttl=wire_ttl))
        except (RPCTimeoutError, RPCErrorResponse):
            self.on_contact_unresponsive(contact)
            return False
        self.add_contact(self._trusted_responder(contact, response))
        return True

    async def rpc_find_node(self, contact: Contact, target: NodeID) -> List[Contact]:
        response = await self._send_with_retry(contact, lambda tid: FindNodeQuery(tid=tid, sender_id=self.id, target=target))
        self.add_contact(self._trusted_responder(contact, response))
        return getattr(response, "nodes", [])

    async def rpc_find_value(self, contact: Contact, key: bytes) -> Tuple[List[Contact], Optional[bytes]]:
        response = await self._send_with_retry(contact, lambda tid: FindValueQuery(tid=tid, sender_id=self.id, key=key))
        self.add_contact(self._trusted_responder(contact, response))
        value = getattr(response, "value", None)
        if value is not None:
            return [], value
        return getattr(response, "nodes", []), None

    # -- incoming query handling --

    async def handle_query(self, msg: Query, addr: Tuple[str, int]) -> Optional[Union[Response, ErrorMessage]]:
        sender = Contact(node_id=msg.sender_id, ip=addr[0], port=addr[1])
        self.add_contact(sender)

        if isinstance(msg, PingQuery):
            return PingResponse(tid=msg.tid, responder_id=self.id)

        if isinstance(msg, StoreQuery):
            if len(msg.key) != ID_BYTES:
                return ErrorMessage(tid=msg.tid, code=203, message=f"key must be exactly {ID_BYTES} bytes")
            ttl = msg.ttl if msg.ttl is not None else TTL_DEFAULT
            self.storage.put(msg.key, msg.value, ttl=ttl)
            return StoreResponse(tid=msg.tid, responder_id=self.id)

        if isinstance(msg, FindNodeQuery):
            closest = self.routing_table.find_closest(msg.target)
            return FindNodeResponse(tid=msg.tid, responder_id=self.id, nodes=closest)

        if isinstance(msg, FindValueQuery):
            if len(msg.key) != ID_BYTES:
                return ErrorMessage(tid=msg.tid, code=203, message=f"key must be exactly {ID_BYTES} bytes")
            value = self.storage.get(msg.key)
            if value is not None:
                return FindValueResponse(tid=msg.tid, responder_id=self.id, value=value)
            closest = self.routing_table.find_closest(NodeID(msg.key))
            return FindValueResponse(tid=msg.tid, responder_id=self.id, nodes=closest)

        logger.warning("unhandled query type: %r", msg)
        return None

    # -- background maintenance --

    async def _refresh_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.maintenance_tick_interval)
                for idx in self.routing_table.buckets_needing_refresh(self.refresh_interval):
                    target = NodeID.random_in_bucket_range(idx, self.id)
                    try:
                        await lookup.iterative_find_node(self, target)
                    except Exception:
                        logger.exception("bucket refresh failed for bucket %d", idx)
        except asyncio.CancelledError:
            pass

    async def _expiry_and_republish_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self.expiry_sweep_interval)
                self.storage.purge_expired()
                for key, value, remaining_ttl in self.storage.items_due_for_republish(self.republish_interval):
                    try:
                        closest = await lookup.iterative_find_node(self, NodeID(key))
                        await asyncio.gather(
                            *(self.rpc_store(c, key, value, ttl=remaining_ttl) for c in closest),
                            return_exceptions=True,
                        )
                        self.storage.mark_republished(key)
                    except Exception:
                        logger.exception("republish failed for key %r", key)
        except asyncio.CancelledError:
            pass
