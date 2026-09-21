#!/usr/bin/env python3
"""Tier 2 end-to-end harness: launches N real `run_node.py` OS processes,
bootstraps them into a mesh, drives a handful of smoke scenarios over their
control channels, and kills a few mid-test to check the survivors still
converge. Validates the real UDP/process-boundary/bencode path, which the
pytest Tier 1 suite (tests/test_network_scenarios.py) does not exercise
(there, everything runs in one process/event loop).

Usage: python scripts/harness.py [--nodes N]
"""

from __future__ import annotations

import argparse
import binascii
import contextlib
import json
import os
import secrets
import socket
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUN_NODE = os.path.join(ROOT, "scripts", "run_node.py")
LOG_DIR = os.path.join(ROOT, "logs")

BASE_PORT = 19000


class HarnessNode:
    def __init__(self, index: int, host: str = "127.0.0.1"):
        self.index = index
        self.host = host
        self.dht_port = BASE_PORT + index * 2
        self.control_port = BASE_PORT + index * 2 + 1
        self.process: subprocess.Popen = None
        self.node_id: str = None

    def start(self, bootstrap: "HarnessNode" = None) -> None:
        args = [
            sys.executable,
            RUN_NODE,
            "--host",
            self.host,
            "--port",
            str(self.dht_port),
            "--control-port",
            str(self.control_port),
            "--log-file",
            os.path.join(LOG_DIR, f"node_{self.dht_port}.log"),
        ]
        if bootstrap is not None:
            args += ["--bootstrap", f"{bootstrap.host}:{bootstrap.dht_port}"]
        self.process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        line = self.process.stdout.readline().strip()
        if not line.startswith("READY"):
            raise RuntimeError(f"node {self.index} failed to start: {line!r}")
        parts = line.split()
        self.node_id = parts[1]

    def terminate(self) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()

    def send_command(self, request: dict, timeout: float = 5.0) -> dict:
        with socket_connect(self.host, self.control_port, timeout) as sock:
            sock.sendall((json.dumps(request) + "\n").encode("utf-8"))
            data = b""
            sock.settimeout(timeout)
            while not data.endswith(b"\n"):
                chunk = sock.recv(65536)
                if not chunk:
                    break
                data += chunk
        return json.loads(data.decode("utf-8"))


@contextlib.contextmanager
def socket_connect(host: str, port: int, timeout: float):
    sock = socket.create_connection((host, port), timeout=timeout)
    try:
        yield sock
    finally:
        sock.close()


def hexlify(b: bytes) -> str:
    return binascii.hexlify(b).decode("ascii")


def build_mesh(n: int) -> list:
    os.makedirs(LOG_DIR, exist_ok=True)
    nodes = [HarnessNode(0)]
    nodes[0].start()
    print(f"  node 0 up: id={nodes[0].node_id} port={nodes[0].dht_port}")
    for i in range(1, n):
        node = HarnessNode(i)
        node.start(bootstrap=nodes[-1])
        print(f"  node {i} up: id={node.node_id} port={node.dht_port}")
        nodes.append(node)
    return nodes


def run_scenarios(nodes: list) -> None:
    print("\n[scenario] store on node 0, get from last node")
    key = secrets.token_bytes(20)
    value = b"harness end-to-end smoke test value"
    resp = nodes[0].send_command({"cmd": "store", "key": hexlify(key), "value": hexlify(value)})
    assert resp["ok"], resp
    print(f"  stored, acked by {resp['acked']} node(s)")

    resp = nodes[-1].send_command({"cmd": "get", "key": hexlify(key)})
    assert resp["ok"], resp
    got = binascii.unhexlify(resp["value"]) if resp["value"] else None
    assert got == value, f"expected {value!r}, got {got!r}"
    print("  get-after-put across processes: OK")

    print("\n[scenario] kill a few nodes, confirm survivors still converge")
    victims = nodes[1:3] if len(nodes) > 4 else nodes[1:2]
    for v in victims:
        print(f"  killing node {v.index} (id={v.node_id})")
        v.terminate()
    survivors = [n for n in nodes if n not in victims]
    time.sleep(1.0)  # let RPC timeouts to the dead nodes clear out

    resp = survivors[0].send_command({"cmd": "get", "key": hexlify(key)}, timeout=15.0)
    assert resp["ok"], resp
    got = binascii.unhexlify(resp["value"]) if resp["value"] else None
    assert got == value, f"expected value to survive churn, got {got!r}"
    print("  lookup still converges after node churn: OK")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodes", type=int, default=10)
    args = parser.parse_args()

    print(f"Launching {args.nodes}-node mesh via {RUN_NODE} ...")
    nodes = build_mesh(args.nodes)
    try:
        run_scenarios(nodes)
        print("\nAll Tier 2 scenarios passed.")
    finally:
        for n in nodes:
            n.terminate()


if __name__ == "__main__":
    main()
