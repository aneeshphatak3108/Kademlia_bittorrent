"""Iterative node/value lookup, the core Kademlia routing algorithm.

One engine (`iterative_lookup`) backs both FIND_NODE and FIND_VALUE:
each round queries up to ALPHA not-yet-queried contacts from an
ever-refined shortlist, in parallel, and folds newly discovered contacts
back in. Termination follows the paper: stop once the K closest contacts
seen so far have all been queried and no closer contact has been found.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from kademlia.constants import ALPHA, K, MAX_LOOKUP_ROUNDS
from kademlia.identifier import NodeID
from kademlia.routing_table import Contact
from kademlia.rpc.protocol import RPCErrorResponse, RPCTimeoutError

if TYPE_CHECKING:
    from kademlia.node import Node

FIND_NODE = "find_node"
FIND_VALUE = "find_value"


@dataclass
class LookupResult:
    closest_nodes: List[Contact]
    value: Optional[bytes] = None


def _closest(contacts: Dict[NodeID, Contact], target: NodeID, count: int = K) -> List[Contact]:
    ordered = sorted(contacts.values(), key=lambda c: target.distance(c.node_id))
    return ordered[:count]


async def _query_one(
    node: "Node", contact: Contact, target: NodeID, rpc: str
) -> Tuple[List[Contact], Optional[bytes]]:
    if rpc == FIND_NODE:
        nodes = await node.rpc_find_node(contact, target)
        return nodes, None
    nodes, value = await node.rpc_find_value(contact, target.bytes)
    return nodes, value


async def iterative_lookup(node: "Node", target: NodeID, rpc: str, alpha: int = ALPHA) -> LookupResult:
    seed = node.routing_table.find_closest(target, K)
    contacts: Dict[NodeID, Contact] = {c.node_id: c for c in seed}
    queried: set = set()
    failed: set = set()  # permanently excluded from this lookup, even if re-offered by a stale peer response

    if not contacts:
        return LookupResult(closest_nodes=[])

    best_distance = min(target.distance(nid) for nid in contacts)

    for _ in range(MAX_LOOKUP_ROUNDS):
        top_k = _closest(contacts, target, K)
        batch = [c for c in top_k if c.node_id not in queried][:alpha]
        if not batch:
            break
        queried.update(c.node_id for c in batch)

        results = await asyncio.gather(
            *(_query_one(node, c, target, rpc) for c in batch), return_exceptions=True
        )

        for contact, outcome in zip(batch, results):
            if isinstance(outcome, (RPCTimeoutError, RPCErrorResponse, OSError)):
                contacts.pop(contact.node_id, None)
                failed.add(contact.node_id)
                node.on_contact_unresponsive(contact)
                continue
            if isinstance(outcome, BaseException):
                raise outcome

            new_nodes, value = outcome
            if value is not None:
                return LookupResult(closest_nodes=_closest(contacts, target, K), value=value)

            for nc in new_nodes:
                if nc.node_id == node.id or nc.node_id in failed or nc.node_id in contacts:
                    continue
                contacts[nc.node_id] = nc

        current_best = min((target.distance(nid) for nid in contacts), default=best_distance)
        if current_best < best_distance:
            best_distance = current_best
        else:
            top_k = _closest(contacts, target, K)
            if all(c.node_id in queried for c in top_k):
                break

    return LookupResult(closest_nodes=_closest(contacts, target, K))


async def iterative_find_node(node: "Node", target: NodeID, alpha: int = ALPHA) -> List[Contact]:
    result = await iterative_lookup(node, target, FIND_NODE, alpha=alpha)
    return result.closest_nodes


async def iterative_find_value(node: "Node", key: NodeID, alpha: int = ALPHA) -> LookupResult:
    return await iterative_lookup(node, key, FIND_VALUE, alpha=alpha)
