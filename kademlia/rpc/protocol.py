"""asyncio UDP transport: sends/receives bencoded KRPC datagrams, correlates
responses to pending queries by transaction ID, and dispatches incoming
queries to the owning Node's handlers.

Retry-with-backoff is intentionally NOT implemented here -- callers (Node's
RPC wrapper methods) own retry policy. This layer does a single
send + timeout per call.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import TYPE_CHECKING, Dict, Optional, Tuple

from kademlia.constants import RPC_TIMEOUT
from kademlia.rpc import messages
from kademlia.rpc.messages import ErrorMessage, MalformedMessageError, Query, Response
from kademlia.routing_table import Contact

if TYPE_CHECKING:
    from kademlia.node import Node

logger = logging.getLogger(__name__)

TID_SIZE = 2


class RPCTimeoutError(Exception):
    """A query received no matching response within the timeout window."""


class RPCErrorResponse(Exception):
    """The remote peer replied with an explicit KRPC error message."""

    def __init__(self, error: ErrorMessage):
        super().__init__(f"remote error {error.code}: {error.message}")
        self.error = error


class KademliaProtocol(asyncio.DatagramProtocol):
    def __init__(self, node: "Node"):
        self.node = node
        self.transport: Optional[asyncio.DatagramTransport] = None
        self._pending: Dict[bytes, "asyncio.Future[Tuple[Response, Tuple[str, int]]]"] = {}
        self._timeout_handles: Dict[bytes, asyncio.TimerHandle] = {}

    # -- asyncio.DatagramProtocol callbacks --

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        try:
            msg = messages.decode(data)
        except MalformedMessageError as exc:
            logger.debug("malformed datagram from %s: %s", addr, exc)
            if exc.tid is not None and self.transport is not None:
                error = ErrorMessage(tid=exc.tid, code=203, message=f"Protocol Error: {exc}")
                self.transport.sendto(messages.encode(error), addr)
            return

        if isinstance(msg, ErrorMessage):
            self._resolve_pending(msg.tid, exc=RPCErrorResponse(msg))
        elif isinstance(msg, (messages.PingResponse, messages.StoreResponse, messages.FindNodeResponse, messages.FindValueResponse)):
            self._resolve_pending(msg.tid, result=(msg, addr))
        else:
            asyncio.get_event_loop().create_task(self._handle_query(msg, addr))

    def error_received(self, exc: Exception) -> None:
        logger.debug("UDP error: %s", exc)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        for tid, future in list(self._pending.items()):
            if not future.done():
                future.set_exception(RPCTimeoutError("transport closed"))
        self._pending.clear()
        for handle in self._timeout_handles.values():
            handle.cancel()
        self._timeout_handles.clear()

    # -- outgoing queries --

    def new_transaction_id(self) -> bytes:
        while True:
            tid = os.urandom(TID_SIZE)
            if tid not in self._pending:
                return tid

    def send_query(self, contact: Contact, query_msg: Query, timeout: float = RPC_TIMEOUT) -> "asyncio.Future":
        assert self.transport is not None, "protocol not yet connected"
        loop = asyncio.get_event_loop()
        future: "asyncio.Future" = loop.create_future()
        self._pending[query_msg.tid] = future
        self.transport.sendto(messages.encode(query_msg), (contact.ip, contact.port))

        def on_timeout():
            fut = self._pending.pop(query_msg.tid, None)
            self._timeout_handles.pop(query_msg.tid, None)
            if fut is not None and not fut.done():
                fut.set_exception(RPCTimeoutError(f"no response from {contact.ip}:{contact.port}"))

        self._timeout_handles[query_msg.tid] = loop.call_later(timeout, on_timeout)
        return future

    def _resolve_pending(self, tid: bytes, result=None, exc: Optional[Exception] = None) -> None:
        future = self._pending.pop(tid, None)
        handle = self._timeout_handles.pop(tid, None)
        if handle is not None:
            handle.cancel()
        if future is None or future.done():
            return
        if exc is not None:
            future.set_exception(exc)
        else:
            future.set_result(result)

    # -- incoming queries --

    async def _handle_query(self, msg: Query, addr: Tuple[str, int]) -> None:
        try:
            response = await self.node.handle_query(msg, addr)
        except Exception:
            logger.exception("error handling incoming query %r from %s", msg, addr)
            return
        if response is not None and self.transport is not None:
            self.transport.sendto(messages.encode(response), addr)

    def send_response(self, addr: Tuple[str, int], response: Response) -> None:
        if self.transport is not None:
            self.transport.sendto(messages.encode(response), addr)
