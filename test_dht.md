# Testing the Kademlia DHT

Quick reference for every way to run and exercise this codebase: the
automated test suite, the real multi-process harness, and driving individual
node processes by hand.

## 0. One-time setup

```bash
cd /home/aneesh/Desktop/Bit_torrent_grind
python3 -m venv .venv              # already done if you've set this up before
source .venv/bin/activate
pip install -e . pytest pytest-asyncio
```

Every command below assumes the venv is active (`source .venv/bin/activate`).

## 1. Automated test suite (Tier 1 — pytest)

Runs everything in one process, but over real UDP sockets on `127.0.0.1`
(different port per node). This is what you'll run most often.

```bash
pytest tests/ -q                              # run everything
pytest tests/ -v                              # verbose, one line per test
pytest tests/test_bencode.py -q                # just the bencode codec tests
pytest tests/test_routing_table.py -q          # just k-bucket/routing table tests
pytest tests/test_messages.py -q               # just wire message encode/decode tests
pytest tests/test_rpc_protocol.py -q           # just the UDP transport/tx-id tests
pytest tests/test_store_expiry.py -q           # just TTL/DataStore tests
pytest tests/test_network_scenarios.py -q      # just the full-system scenarios
pytest tests/ -k republish -v                  # any test with "republish" in its name
pytest tests/ -x                               # stop at the first failure
```

Expect ~49 tests, all passing, in well under 10 seconds.

### What `test_network_scenarios.py` actually checks

| Test | What it proves |
|---|---|
| `test_bootstrap_single_seed` | A node joining via one bootstrap contact ends up in that contact's routing table and vice versa |
| `test_store_and_get` | A value stored from one node is retrievable from a different node across a 10-node mesh |
| `test_lookup_convergence` | `find_node(target)` returns exactly the true K-closest nodes, checked against a brute-force ground truth |
| `test_node_leave_timeout` | A stopped/unreachable node gets evicted from routing tables and stops appearing in lookup results |
| `test_key_ttl_expiry` | A value with a short TTL becomes unretrievable after it expires |
| `test_republish` | A node that joins *after* the initial store still receives the value via the periodic republish sweep |
| `test_republish_forwards_remaining_ttl_not_default` | Republishing a short-lived key doesn't silently reset it to the 24h default TTL on the receiving peer |
| `test_invalid_key_length_gets_protocol_error` | A malformed `STORE`/`FIND_VALUE` (wrong-length key) gets a proper KRPC error reply, not a crash |

## 2. Real multi-process harness (Tier 2)

Launches N *actual separate OS processes* (`scripts/run_node.py`), each with
its own real UDP socket, and drives them over their control channels. This
is the test that proves the wire protocol works across a genuine process
boundary, not just inside one Python interpreter.

```bash
python scripts/harness.py                  # default: 10-node mesh
python scripts/harness.py --nodes 20       # bigger mesh
```

It will:
1. Start node 0 as a seed, then nodes 1..N-1 each bootstrapping off the
   previous one, printing each node's id/port as it comes up.
2. Store a value on node 0, fetch it back from the last node — proves
   get-after-put works across process boundaries.
3. Kill a couple of nodes mid-test (`SIGTERM`) and confirm a lookup from a
   surviving node still converges and finds the value — proves churn
   handling works for real, not just in a mocked timeout.

It exits non-zero (and prints an `AssertionError`) if anything fails, and
always cleans up (terminates) every process it started, even on failure.

Logs for each node process land in `logs/node_<port>.log` — check these
first if the harness fails and you need to see what a specific node did.

## 3. Driving a single node by hand

Useful for manual exploration, or debugging something the automated tests
don't cover. `scripts/run_node.py` starts one node and opens a small
line-delimited-JSON **control channel** over plain TCP on `127.0.0.1` (kept
separate from the DHT's own UDP port).

### Start a couple of nodes

```bash
# Terminal 1 — seed node
python scripts/run_node.py --port 9000 --control-port 9001
# prints: READY <node_id_hex> 127.0.0.1:9000

# Terminal 2 — second node, joins via the seed
python scripts/run_node.py --port 9002 --control-port 9003 --bootstrap 127.0.0.1:9000
```

Flags: `--host` (default `127.0.0.1`; the address the DHT's UDP socket binds
to), `--port` (DHT UDP port), `--control-port` (TCP control port),
`--control-host` (default `127.0.0.1`; the address the control channel binds
to — stays localhost-only by default *even if* `--host` is a LAN IP, see
section 4 below), `--bootstrap host:port[,host:port...]` (peers to join via —
omit for a seed node), `--log-file <path>` (optional).

### Send it commands

The control channel takes one JSON object per line, and replies with one
JSON object per line. `key`/`value` are hex-encoded (JSON has no native byte
string). Easiest way to talk to it is a short Python snippet — save this as
`ctl.py` in the project root:

```python
#!/usr/bin/env python3
import json, socket, sys

port = int(sys.argv[1])
request = json.loads(sys.argv[2])

with socket.create_connection(("127.0.0.1", port), timeout=10) as sock:
    sock.sendall((json.dumps(request) + "\n").encode())
    print(sock.recv(65536).decode().strip())
```

Then, from a third terminal:

```bash
# what's my node id?
python ctl.py 9001 '{"cmd": "node_id"}'

# store a value (key must be exactly 20 bytes = 40 hex chars)
python ctl.py 9001 '{"cmd": "store", "key": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "value": "68656c6c6f"}'

# fetch it back from the OTHER node
python ctl.py 9003 '{"cmd": "get", "key": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}'

# see what a node currently knows about
python ctl.py 9001 '{"cmd": "dump_routing_table"}'
python ctl.py 9001 '{"cmd": "dump_store"}'

# find_node against an arbitrary target id
python ctl.py 9001 '{"cmd": "find_node", "target": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}'

# shut a node down cleanly
python ctl.py 9001 '{"cmd": "shutdown"}'
```

Handy one-liners for generating hex values from Python:

```bash
python -c "import os; print(os.urandom(20).hex())"     # random 20-byte key
python -c "print(b'hello world'.hex())"                  # value from text
python -c "print(bytes.fromhex('68656c6c6f').decode())"  # text from hex
```

### Full command reference (control channel)

| `cmd` | Required fields | Returns |
|---|---|---|
| `ping` | — | `{"ok": true}` |
| `node_id` | — | `{"ok": true, "id": "<hex>"}` |
| `store` | `key` (40 hex chars), `value` (hex); optional `ttl` (seconds) | `{"ok": true, "acked": <N>}` |
| `get` | `key` (40 hex chars) | `{"ok": true, "value": "<hex>" or null}` |
| `find_node` | `target` (40 hex chars) | `{"ok": true, "nodes": [[id, ip, port], ...]}` |
| `dump_routing_table` | — | `{"ok": true, "contacts": [[id, ip, port], ...]}` |
| `dump_store` | — | `{"ok": true, "items": {key_hex: value_hex, ...}}` |
| `shutdown` | — | `{"ok": true}`, then the process exits |

Any unknown `cmd`, or a handler that raises, comes back as
`{"ok": false, "error": "<message>"}` instead of crashing the connection.

## 4. Running across multiple machines on the same WiFi

The wire protocol has no code-level restriction to a single machine — a
node's address is always derived from the real UDP source address of the
packets it sends (`Node.handle_query` / `Node._trusted_responder` in
`kademlia/node.py`), never self-reported. The only thing that's local-only by
default is the **control channel** — `--control-host` defaults to
`127.0.0.1`/localhost-only for safety, since it's unauthenticated and includes
a `shutdown` command. That's intentional: keep the control channel local, and
open only the DHT's UDP port between machines.

`scripts/harness.py` won't help here — it launches subprocesses on the local
machine only. For real machines, run `run_node.py` directly on each one.

### Step 1 — find each machine's LAN IP

- Linux: `hostname -I` or `ip addr show` (look for the address on your WiFi
  interface, e.g. `wlan0`/`wlp0s20f3` — ignore `docker0`/`172.17.x.x` if
  Docker is installed, and ignore `127.0.0.1`)
- macOS: `ipconfig getifaddr en0` (or `en1` for some Macs)
- Windows: `ipconfig`, look for "IPv4 Address" under your WiFi adapter

Example: this machine's WiFi address is `192.168.29.155` — yours will differ.

### Step 2 — open the firewall for the DHT's UDP port (not the control port)

- Linux (ufw): `sudo ufw allow 9000/udp`
- macOS: the first inbound connection will trigger a firewall prompt — allow
  it for Python
- Windows: allow Python through the firewall for private networks when
  prompted

The control port (`--control-port`) does **not** need to be opened — it's
bound to `127.0.0.1` by default and isn't reachable from other machines
regardless of firewall rules; run `ctl.py` locally on whichever machine you
want to inspect.

### Step 3 — run one node per machine

```bash
# Machine A (LAN IP 192.168.29.155) — the seed
python scripts/run_node.py --host 192.168.29.155 --port 9000 --control-port 9001
# prints: READY <node_id_hex> 192.168.29.155:9000
```

Copy the `host:port` from that `READY` line, then on the other machine:

```bash
# Machine B (LAN IP 192.168.29.200, for example) — joins via A
python scripts/run_node.py --host 192.168.29.200 --port 9000 --control-port 9001 \
    --bootstrap 192.168.29.155:9000
```

Add more machines the same way, each `--bootstrap`-ing off any node already
in the mesh.

### Step 4 — verify it actually crossed the network

Run `ctl.py` (see section 3) **on machine A** to store a value, then run it
**on machine B** to fetch it:

```bash
# on machine A
python ctl.py 9001 '{"cmd": "store", "key": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "value": "68656c6c6f"}'

# on machine B
python ctl.py 9001 '{"cmd": "get", "key": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}'
# {"ok": true, "value": "68656c6c6f"}  <- proves it round-tripped over the real network
```

### Pitfalls

- **`--host 127.0.0.1` (the default) will not work across machines** — it's
  the loopback address, meaning "this same machine," on every machine. You
  must pass each machine's real LAN IP explicitly.
- **Don't use `--host 0.0.0.0`** either, even though it "listens on
  everything" — the `READY` line and any bootstrap string built from it would
  then say `0.0.0.0:<port>`, which isn't a valid address for another machine
  to dial. Always pass the specific LAN IP from step 1.
- **Docker/VPN/virtual interfaces**: if `hostname -I`/`ip addr` lists more
  than one address (common with Docker installed, e.g. `docker0` at
  `172.17.x.x`, or an active VPN), make sure you're using the one on your
  actual WiFi interface, not a virtual bridge — the virtual one won't be
  reachable from other machines on the WiFi.
- **Guest/isolated WiFi networks**: some routers (common on guest networks,
  coffee shops, offices) enable "client isolation" / "AP isolation," which
  blocks devices on the same WiFi from reaching each other entirely. No
  firewall change on your machines can fix this — it has to be disabled on
  the router, or you need a network without it.

## 5. Quick sanity checklist

If you've changed something and want a fast "did I break anything" pass:

```bash
pytest tests/ -q && python scripts/harness.py --nodes 10
```

Both exit 0 with no failures if the DHT layer is healthy.



Machine A (say its IP is 192.168.29.155) — this is the seed, so no --bootstrap:

# Terminal 1 on machine A — start the node, leave this running
cd /home/aneesh/Desktop/Bit_torrent_grind
source .venv/bin/activate
python scripts/run_node.py --host 192.168.29.155 --port 9000 --control-port 9001

# Terminal 2 on machine A — once it's up, store a value
python ctl.py 9001 '{"cmd": "store", "key": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "value": "68656c6c6f"}'

Machine B (say its IP is 192.168.29.200) — joins via A's IP:

# Terminal 1 on machine B — start the node, bootstrapping off A
cd /home/aneesh/Desktop/Bit_torrent_grind
source .venv/bin/activate
python scripts/run_node.py --host 192.168.29.200 --port 9000 --control-port 9001 --bootstrap 192.168.29.155:9000

# Terminal 2 on machine B — fetch the value A stored
python ctl.py 9001 '{"cmd": "get", "key": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}'

Two things this requires that aren't in those commands themselves:
1. ctl.py (the small helper script from test_dht.md section 3) needs to exist on both machines — it's just a few lines, copy it over.
2. Machine B must be started after machine A is already up and listening (since B's --bootstrap dials A directly on startup).`