#!/usr/bin/env python3
"""Run one BitTorrent peer: a DHT node plus a TorrentSession for one torrent.

Seeds if the target file is already complete, downloads it otherwise -- the
same code path either way, since "seeding" is just "already have every piece".

Peers find each other purely through the DHT (no tracker): the session
announces itself under the torrent's info_hash and periodically looks up who
else has announced.

Like run_node.py, this exposes a line-delimited JSON control channel bound to
localhost, so a harness (or ctl.py) can ask it for progress.

    python scripts/run_peer.py --torrent f.torrent --path out.bin \
        --dht-port 9000 --bt-port 6881 --control-port 9001 \
        [--bootstrap host:port]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bittorrent.discovery import PeerDiscovery
from bittorrent.metadata import TorrentMetadata
from bittorrent.session import TorrentSession
from kademlia.identifier import NodeID
from kademlia.logging_conf import configure_logging
from kademlia.node import BootstrapError, Node
from kademlia.routing_table import Contact


def parse_bootstrap(spec):
    if not spec:
        return []
    contacts = []
    for entry in spec.split(","):
        entry = entry.strip()
        if not entry:
            continue
        host, port = entry.rsplit(":", 1)
        contacts.append(
            Contact(node_id=NodeID.random(), ip=socket.gethostbyname(host), port=int(port))
        )
    return contacts


class ControlServer:
    def __init__(self, session: TorrentSession, shutdown: asyncio.Event):
        self.session = session
        self.shutdown = shutdown

    async def handle_client(self, reader, writer) -> None:
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                try:
                    response = await self.dispatch(json.loads(line))
                except Exception as exc:  # trusted, localhost-only channel
                    response = {"ok": False, "error": str(exc)}
                writer.write((json.dumps(response) + "\n").encode())
                await writer.drain()
        finally:
            writer.close()

    async def dispatch(self, request: dict) -> dict:
        cmd = request.get("cmd")
        if cmd == "ping":
            return {"ok": True}
        if cmd == "progress":
            return {"ok": True, **self.session.progress()}
        if cmd == "complete":
            return {"ok": True, "complete": self.session.is_complete}
        if cmd == "peers":
            return {"ok": True, "peers": [c.key for c in self.session.active_peers()]}
        if cmd == "add_peer":
            added = await self.session.add_peer(request["host"], int(request["port"]))
            return {"ok": True, "added": added}
        if cmd == "shutdown":
            self.shutdown.set()
            return {"ok": True}
        return {"ok": False, "error": f"unknown command: {cmd!r}"}


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run one BitTorrent peer.")
    parser.add_argument("--torrent", required=True, help=".torrent file describing the download")
    parser.add_argument("--path", required=True, help="where the data lives (or will be written)")
    parser.add_argument("--host", default="127.0.0.1", help="address to bind the DHT + BT sockets to")
    parser.add_argument("--dht-port", type=int, required=True, help="UDP port for the DHT")
    parser.add_argument("--bt-port", type=int, default=0, help="TCP port for peer connections")
    parser.add_argument("--control-port", type=int, required=True)
    parser.add_argument("--control-host", default="127.0.0.1")
    parser.add_argument("--bootstrap", default=None, help="comma-separated host:port DHT peers")
    parser.add_argument("--log-file", default=None)
    parser.add_argument("--seed-forever", action="store_true",
                        help="keep running after completing (default: keep seeding anyway)")
    args = parser.parse_args()

    configure_logging(f"{args.host}:{args.dht_port}", logfile=args.log_file)
    metadata = TorrentMetadata.from_file(args.torrent)

    node = Node(args.host, args.dht_port)
    await node.start()

    bootstrap = parse_bootstrap(args.bootstrap)
    if bootstrap:
        try:
            await node.join(bootstrap)
        except BootstrapError as exc:
            print(f"JOIN_FAILED {exc}", flush=True)
            await node.stop()
            sys.exit(1)

    # Every node serves announcements, including for torrents it isn't
    # downloading -- any node can be among the K closest to some info_hash.
    discovery = PeerDiscovery(node)

    session = TorrentSession(
        metadata, args.path, node=node, discovery=discovery,
        host=args.host, port=args.bt_port,
    )
    await session.start()

    print(
        f"READY {node.id} dht={args.host}:{node.port} bt={args.host}:{session.port} "
        f"info_hash={metadata.info_hash.hex()} "
        f"pieces={len(session.store.have)}/{metadata.piece_count}",
        flush=True,
    )

    shutdown = asyncio.Event()
    control = ControlServer(session, shutdown)
    server = await asyncio.start_server(control.handle_client, args.control_host, args.control_port)

    async def announce_completion():
        await session.wait_complete()
        print(f"COMPLETE {metadata.name} {metadata.total_length} bytes", flush=True)

    watcher = asyncio.get_event_loop().create_task(announce_completion())

    async with server:
        await shutdown.wait()

    watcher.cancel()
    server.close()
    await server.wait_closed()
    await session.stop()
    await node.stop()


if __name__ == "__main__":
    asyncio.run(main())
