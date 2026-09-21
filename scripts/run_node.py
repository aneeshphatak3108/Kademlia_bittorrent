#!/usr/bin/env python3
"""Launches one Kademlia DHT node as a real OS process, bound to a real UDP
port. A small line-delimited JSON control channel (plain TCP, bound to
--control-host, which defaults to 127.0.0.1/localhost-only) lets a harness
script (see harness.py) drive it: store/get/find_node/dump routing
table/shutdown. Used for Tier 2 (real multi-process) test scenarios, and for
driving individual nodes by hand -- including nodes on separate machines on
the same WiFi (see test_dht.md), where --host is set to the machine's real
LAN IP but --control-host is left at its localhost default.
"""

from __future__ import annotations

import argparse
import asyncio
import binascii
import json
import sys
from typing import Optional

sys.path.insert(0, __file__.rsplit("/scripts/", 1)[0])  # allow running from anywhere

from kademlia.identifier import NodeID
from kademlia.logging_conf import configure_logging
from kademlia.node import BootstrapError, Node
from kademlia.routing_table import Contact


def parse_bootstrap(spec: Optional[str]) -> list:
    if not spec:
        return []
    contacts = []
    for entry in spec.split(","):
        entry = entry.strip()
        if not entry:
            continue
        host, port = entry.rsplit(":", 1)
        contacts.append(Contact(node_id=NodeID.random(), ip=host, port=int(port)))
    return contacts


class ControlServer:
    def __init__(self, node: Node, shutdown_event: asyncio.Event):
        self.node = node
        self.shutdown_event = shutdown_event

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                try:
                    request = json.loads(line)
                    response = await self.dispatch(request)
                except Exception as exc:  # control channel is trusted (localhost harness only)
                    response = {"ok": False, "error": str(exc)}
                writer.write((json.dumps(response) + "\n").encode("utf-8"))
                await writer.drain()
        finally:
            writer.close()

    async def dispatch(self, request: dict) -> dict:
        cmd = request.get("cmd")
        if cmd == "ping":
            return {"ok": True}
        if cmd == "store":
            key = binascii.unhexlify(request["key"])
            value = binascii.unhexlify(request["value"])
            ttl = request.get("ttl")
            kwargs = {} if ttl is None else {"ttl": ttl}
            acked = await self.node.store(key, value, **kwargs)
            return {"ok": True, "acked": acked}
        if cmd == "get":
            key = binascii.unhexlify(request["key"])
            value = await self.node.find_value(key)
            return {"ok": True, "value": None if value is None else binascii.hexlify(value).decode("ascii")}
        if cmd == "find_node":
            target = NodeID(binascii.unhexlify(request["target"]))
            contacts = await self.node.find_node(target)
            return {"ok": True, "nodes": [[str(c.node_id), c.ip, c.port] for c in contacts]}
        if cmd == "dump_routing_table":
            contacts = self.node.routing_table_snapshot()
            return {"ok": True, "contacts": [[str(c.node_id), c.ip, c.port] for c in contacts]}
        if cmd == "dump_store":
            items = self.node.local_store_snapshot()
            return {"ok": True, "items": {k.hex(): v.hex() for k, v in items.items()}}
        if cmd == "node_id":
            return {"ok": True, "id": str(self.node.id)}
        if cmd == "shutdown":
            self.shutdown_event.set()
            return {"ok": True}
        return {"ok": False, "error": f"unknown command: {cmd!r}"}


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run one Kademlia DHT node process.")
    parser.add_argument("--host", default="127.0.0.1", help="address to bind the DHT UDP socket to (use the machine's real LAN IP to be reachable from other machines)")
    parser.add_argument("--port", type=int, required=True, help="UDP port for DHT traffic")
    parser.add_argument("--control-port", type=int, required=True, help="TCP port for the control channel")
    parser.add_argument(
        "--control-host",
        default="127.0.0.1",
        help="address to bind the control channel to (default: localhost-only, regardless of --host)",
    )
    parser.add_argument("--bootstrap", default=None, help="comma-separated host:port list of peers to join via")
    parser.add_argument("--log-file", default=None)
    args = parser.parse_args()

    label = f"{args.host}:{args.port}"
    configure_logging(label, logfile=args.log_file)

    node = Node(args.host, args.port)
    await node.start()

    bootstrap_contacts = parse_bootstrap(args.bootstrap)
    if bootstrap_contacts:
        try:
            await node.join(bootstrap_contacts)
        except BootstrapError as exc:
            print(f"READY {node.id} {args.host}:{node.port} JOIN_FAILED {exc}", flush=True)
            await node.stop()
            sys.exit(1)

    print(f"READY {node.id} {args.host}:{node.port}", flush=True)

    shutdown_event = asyncio.Event()
    control = ControlServer(node, shutdown_event)
    server = await asyncio.start_server(control.handle_client, args.control_host, args.control_port)

    async with server:
        await shutdown_event.wait()

    server.close()
    await server.wait_closed()
    await node.stop()


if __name__ == "__main__":
    asyncio.run(main())
