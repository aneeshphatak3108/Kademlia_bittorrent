#!/usr/bin/env python3
"""End-to-end BitTorrent test across real Docker containers.

One container seeds a file, the others start empty and must find it purely
through the DHT (no peer addresses are ever passed in), then download it. Each
container has its own IP and filesystem, so this exercises the real
UDP-for-DHT + TCP-for-data path rather than loopback with different ports.

Success is checked by SHA256 of the downloaded file against the original.

Usage: python scripts/bt_harness.py [--leechers N] [--size BYTES]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORK_DIR = os.path.join(ROOT, "bt_work")
LOG_DIR = os.path.join(ROOT, "logs")

IMAGE_TAG = "kademlia-dht-node:latest"
NETWORK_NAME = "bittorrent-test-net"
DHT_PORT = 9000
BT_PORT = 6881
CONTROL_PORT = 9001

DEFAULT_SIZE = 2 * 1024 * 1024
PIECE_LENGTH = 128 * 1024


def run(args, **kwargs):
    return subprocess.run(args, capture_output=True, text=True, **kwargs)


class PeerContainer:
    def __init__(self, name: str, data_file: str):
        self.name = name
        self.data_file = data_file  # path inside /app/bt_work

    def start(self, bootstrap: "PeerContainer" = None) -> None:
        args = [
            "docker", "run", "-d",
            "--name", self.name,
            "--network", NETWORK_NAME,
            "-v", f"{ROOT}:/app",
            IMAGE_TAG,
            "python", "/app/scripts/run_peer.py",
            "--torrent", "/app/bt_work/payload.torrent",
            "--path", f"/app/bt_work/{self.data_file}",
            "--host", "0.0.0.0",
            "--dht-port", str(DHT_PORT),
            "--bt-port", str(BT_PORT),
            "--control-port", str(CONTROL_PORT),
            "--log-file", f"/app/logs/{self.name}.log",
        ]
        if bootstrap is not None:
            args += ["--bootstrap", f"{bootstrap.name}:{DHT_PORT}"]
        result = run(args)
        if result.returncode != 0:
            raise RuntimeError(f"docker run failed for {self.name}: {result.stderr}")
        self._wait_for_ready()

    def _wait_for_ready(self, timeout: float = 20.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            logs = run(["docker", "logs", self.name]).stdout
            for line in logs.splitlines():
                if line.startswith("READY"):
                    return
                if line.startswith("JOIN_FAILED"):
                    raise RuntimeError(f"{self.name}: {line}")
            time.sleep(0.2)
        raise RuntimeError(
            f"{self.name} never printed READY:\n{run(['docker', 'logs', self.name]).stdout}"
        )

    def command(self, request: dict, timeout: float = 20.0) -> dict:
        result = run(
            ["docker", "exec", self.name, "python", "/app/ctl.py",
             str(CONTROL_PORT), json.dumps(request)],
            timeout=timeout,
        )
        if result.returncode != 0:
            raise RuntimeError(f"docker exec on {self.name} failed: {result.stderr}")
        return json.loads(result.stdout.strip())

    def remove(self) -> None:
        run(["docker", "rm", "-f", self.name])


def prepare_payload(size: int) -> str:
    """Create the file to share plus its .torrent, and return the SHA256."""
    os.makedirs(WORK_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)
    payload = os.path.join(WORK_DIR, "payload.bin")
    data = os.urandom(size)
    with open(payload, "wb") as fh:
        fh.write(data)

    result = run([
        sys.executable, os.path.join(ROOT, "scripts", "make_torrent.py"), payload,
        "--out", os.path.join(WORK_DIR, "payload.torrent"),
        "--piece-length", str(PIECE_LENGTH),
    ])
    if result.returncode != 0:
        raise RuntimeError(f"make_torrent failed: {result.stderr}")

    # The seeder serves from its own copy, so the original stays untouched.
    with open(os.path.join(WORK_DIR, "seed.bin"), "wb") as fh:
        fh.write(data)
    return hashlib.sha256(data).hexdigest()


def setup_network() -> None:
    run(["docker", "network", "rm", NETWORK_NAME])
    result = run(["docker", "network", "create", NETWORK_NAME])
    if result.returncode != 0:
        raise RuntimeError(f"docker network create failed: {result.stderr}")


def build_image() -> None:
    print(f"Building {IMAGE_TAG} ...")
    result = run(["docker", "build", "-t", IMAGE_TAG, ROOT])
    if result.returncode != 0:
        raise RuntimeError(f"docker build failed:\n{result.stderr}")


def wait_for_completion(leechers, timeout: float) -> None:
    deadline = time.time() + timeout
    pending = list(leechers)
    while pending and time.time() < deadline:
        for peer in list(pending):
            progress = peer.command({"cmd": "progress"})
            if progress.get("complete"):
                print(f"  {peer.name}: complete ({progress['bytes_have']} bytes)")
                pending.remove(peer)
            else:
                print(
                    f"  {peer.name}: {progress['pieces_have']}/{progress['pieces_total']} pieces, "
                    f"{len(progress['peers'])} peer(s)"
                )
        if pending:
            time.sleep(2.0)
    if pending:
        raise AssertionError(f"timed out waiting for {[p.name for p in pending]}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leechers", type=int, default=2)
    parser.add_argument("--size", type=int, default=DEFAULT_SIZE)
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args()

    if run(["docker", "info"]).returncode != 0:
        print("Cannot reach the Docker daemon (permission denied or not running).", file=sys.stderr)
        print("See docker_test.md for how to grant this user access to Docker.", file=sys.stderr)
        sys.exit(1)

    expected_sha = prepare_payload(args.size)
    print(f"payload: {args.size} bytes, sha256={expected_sha}")

    build_image()
    setup_network()

    seeder = PeerContainer("bt-seed", "seed.bin")
    leechers = [PeerContainer(f"bt-leech{i}", f"leech{i}.bin") for i in range(args.leechers)]
    for peer in [seeder] + leechers:
        peer.remove()  # clear anything stale from a previous run

    try:
        print("\nStarting seeder ...")
        seeder.start()
        print(f"  {seeder.name} up (has the complete file)")

        print(f"\nStarting {len(leechers)} leecher(s) -- no peer addresses given, DHT only ...")
        for peer in leechers:
            peer.start(bootstrap=seeder)
            print(f"  {peer.name} up (empty)")

        print("\nWaiting for downloads to complete ...")
        wait_for_completion(leechers, timeout=args.timeout)

        print("\nVerifying downloaded files against the original ...")
        for peer in leechers:
            path = os.path.join(WORK_DIR, peer.data_file)
            with open(path, "rb") as fh:
                got = hashlib.sha256(fh.read()).hexdigest()
            assert got == expected_sha, f"{peer.name}: sha256 {got} != {expected_sha}"
            print(f"  {peer.name}: sha256 matches")

        print("\nAll BitTorrent scenarios passed.")
    finally:
        for peer in [seeder] + leechers:
            peer.remove()
        run(["docker", "network", "rm", NETWORK_NAME])


if __name__ == "__main__":
    main()
