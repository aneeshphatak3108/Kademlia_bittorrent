#!/usr/bin/env python3
"""Tier 3 end-to-end harness: launches N Kademlia DHT nodes as real Docker
containers on a shared user-defined network, each with its own container IP
and filesystem (unlike Tier 1/2, which run every "node" on 127.0.0.1,
distinguished only by port). The repo is bind-mounted into every container
at /app, so code edits on the host are picked up immediately -- no image
rebuild needed between runs.

Mirrors harness.py's shape (build a mesh, run scenarios, tear down) with
`docker run`/`docker exec`/`docker kill` standing in for
subprocess.Popen/socket/terminate.

Requires the current user to have permission to talk to the Docker daemon
(e.g. be in the `docker` group) -- see docker_test.md.

Usage: python scripts/docker_harness.py [--nodes N]
"""

from __future__ import annotations

import argparse
import binascii
import json
import os
import secrets
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(ROOT, "logs")

IMAGE_TAG = "kademlia-dht-node:latest"
NETWORK_NAME = "kademlia-test-net"
DHT_PORT = 9000
CONTROL_PORT = 9001


def run(args: list, **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(args, capture_output=True, text=True, **kwargs)


class DockerNode:
    def __init__(self, index: int):
        self.index = index
        self.name = f"node{index}"
        self.node_id: str = None

    def start(self, bootstrap: "DockerNode" = None) -> None:
        args = [
            "docker", "run", "-d",
            "--name", self.name,
            "--network", NETWORK_NAME,
            "-v", f"{ROOT}:/app",
            IMAGE_TAG,
            "python", "/app/scripts/run_node.py",
            "--host", "0.0.0.0",
            "--port", str(DHT_PORT),
            "--control-port", str(CONTROL_PORT),
            "--log-file", f"/app/logs/{self.name}.log",
        ]
        if bootstrap is not None:
            args += ["--bootstrap", f"{bootstrap.name}:{DHT_PORT}"]
        result = run(args)
        if result.returncode != 0:
            raise RuntimeError(f"docker run failed for {self.name}: {result.stderr}")
        self.node_id = self._wait_for_ready()

    def _wait_for_ready(self, timeout: float = 15.0) -> str:
        deadline = time.time() + timeout
        while time.time() < deadline:
            logs = run(["docker", "logs", self.name]).stdout
            for line in logs.splitlines():
                if line.startswith("READY"):
                    return line.split()[1]
            time.sleep(0.2)
        raise RuntimeError(f"container {self.name} did not print READY within {timeout}s")

    def send_command(self, request: dict, timeout: float = 5.0) -> dict:
        result = run(
            ["docker", "exec", self.name, "python", "/app/ctl.py", str(CONTROL_PORT), json.dumps(request)],
            timeout=timeout,
        )
        if result.returncode != 0:
            raise RuntimeError(f"docker exec on {self.name} failed: {result.stderr}")
        return json.loads(result.stdout.strip())

    def kill(self) -> None:
        run(["docker", "kill", self.name])

    def remove(self) -> None:
        run(["docker", "rm", "-f", self.name])


def hexlify(b: bytes) -> str:
    return binascii.hexlify(b).decode("ascii")


def build_image() -> None:
    print(f"Building {IMAGE_TAG} ...")
    result = run(["docker", "build", "-t", IMAGE_TAG, ROOT])
    if result.returncode != 0:
        raise RuntimeError(f"docker build failed:\n{result.stderr}")


def setup_network() -> None:
    run(["docker", "network", "rm", NETWORK_NAME])  # clean up any stale run, ignore failure
    result = run(["docker", "network", "create", NETWORK_NAME])
    if result.returncode != 0:
        raise RuntimeError(f"docker network create failed: {result.stderr}")


def build_mesh(nodes: list) -> None:
    # `nodes` is pre-created (names known) before this runs, so a failure
    # partway through still leaves every container name discoverable for
    # cleanup -- see main().
    os.makedirs(LOG_DIR, exist_ok=True)
    nodes[0].start()
    print(f"  {nodes[0].name} up: id={nodes[0].node_id}")
    for i in range(1, len(nodes)):
        nodes[i].start(bootstrap=nodes[i - 1])
        print(f"  {nodes[i].name} up: id={nodes[i].node_id}")


def find_non_holders(nodes: list, key: bytes) -> list:
    """Nodes that do NOT have `key` in local storage -- querying one of
    these forces a genuine iterative_find_value() network lookup, instead
    of a trivial local hit. (The origin node always has a local copy
    unconditionally -- Node.store()'s first line -- and with K >= node
    count every node would too, which is why this is determined by asking
    each node directly rather than assumed.)"""
    non_holders = []
    for n in nodes:
        resp = n.send_command({"cmd": "dump_store"})
        assert resp["ok"], resp
        if hexlify(key) not in resp["items"]:
            non_holders.append(n)
    return non_holders


def run_scenarios(nodes: list) -> None:
    print("\n[scenario] store on node0, then force a real network lookup from a node with no local copy")
    key = secrets.token_bytes(20)
    value = b"docker harness end-to-end smoke test value"
    resp = nodes[0].send_command({"cmd": "store", "key": hexlify(key), "value": hexlify(value)})
    assert resp["ok"], resp
    print(f"  stored, acked by {resp['acked']} node(s)")

    non_holders = find_non_holders(nodes, key)
    print(f"  {len(non_holders)}/{len(nodes)} nodes have no local copy ({resp['acked']} nodes were acked)")
    assert len(non_holders) >= 4, (
        f"only {len(non_holders)} non-holder nodes available -- need at least 4 "
        "(1 to query now, 2 to kill, 1 to re-query after) to make this scenario "
        "meaningful; raise --nodes or lower K in kademlia/constants.py"
    )

    query_target, *rest = non_holders
    resp = query_target.send_command({"cmd": "get", "key": hexlify(key)})
    assert resp["ok"], resp
    got = binascii.unhexlify(resp["value"]) if resp["value"] else None
    assert got == value, f"expected {value!r}, got {got!r}"
    print(f"  {query_target.name} (no local copy) found it via a real network lookup: OK")

    print("\n[scenario] docker kill non-holder nodes, confirm another non-holder still finds it")
    victims, remaining = rest[:2], rest[2:]
    for v in victims:
        print(f"  docker kill {v.name} (id={v.node_id})")
        v.kill()
    time.sleep(1.0)  # let RPC timeouts to the dead nodes clear out

    resp = remaining[0].send_command({"cmd": "get", "key": hexlify(key)}, timeout=15.0)
    assert resp["ok"], resp
    got = binascii.unhexlify(resp["value"]) if resp["value"] else None
    assert got == value, f"expected value to survive churn, got {got!r}"
    print(f"  {remaining[0].name} (no local copy) still finds it via network lookup after churn: OK")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodes", type=int, default=8)
    args = parser.parse_args()

    if run(["docker", "info"]).returncode != 0:
        print("Cannot reach the Docker daemon (permission denied or not running).", file=sys.stderr)
        print("See docker_test.md for how to grant this user access to Docker.", file=sys.stderr)
        sys.exit(1)

    build_image()
    setup_network()

    nodes = [DockerNode(i) for i in range(args.nodes)]
    for n in nodes:
        n.remove()  # clear any stale container with this name from a previous failed run

    print(f"\nLaunching {args.nodes}-node mesh ...")
    try:
        build_mesh(nodes)
        run_scenarios(nodes)
        print("\nAll Tier 3 scenarios passed.")
    finally:
        for n in nodes:
            n.remove()
        run(["docker", "network", "rm", NETWORK_NAME])


if __name__ == "__main__":
    main()
