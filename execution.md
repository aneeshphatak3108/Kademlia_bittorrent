# Running the BitTorrent Layer

Commands and steps, in the order you'd actually use them. See `bittorrent.md`
for how it works, `test_dht.md` for the DHT layer's own testing tiers.

## 0. Setup

```bash
cd /home/aneesh/Desktop/Bit_torrent_grind
source .venv/bin/activate
```

Everything except the Docker harness needs the venv active.

## 1. Automated tests

```bash
pytest tests/ -q                            # everything: DHT + BitTorrent (140 tests)
pytest tests/ -q -k bt_                     # just the BitTorrent layer
```

Individual suites:

```bash
pytest tests/test_bt_metadata.py -q         # .torrent parsing, info_hash, piece sizes
pytest tests/test_bt_storage.py -q          # block I/O, hash verification, resume-by-rehash
pytest tests/test_bt_picker.py -q           # rarest-first, ties, random-first, endgame
pytest tests/test_bt_wire.py -q             # handshake + message framing, bad-input rejection
pytest tests/test_bt_discovery.py -q        # ANNOUNCE_PEER/GET_PEERS over a real DHT
pytest tests/test_bt_session.py -q          # end-to-end transfers over real TCP
pytest tests/test_bt_dht_integration.py -q  # full stack: discovery via DHT, then transfer
```

The two that matter most:

- `test_bt_session.py` — a seeder and leecher in one process but over real
  sockets, checking the downloaded bytes match by SHA256. Includes a leecher
  becoming a seeder for a third peer, resume-after-restart, and one seeder
  serving three leechers at once.
- `test_bt_dht_integration.py` — no peer addresses are passed anywhere. The
  seeder announces to the DHT, the leecher looks the info_hash up, finds it,
  dials it, and downloads. This is the proof the two layers compose.

## 2. Make a torrent

```bash
# any file will do
head -c 2000000 /dev/urandom > /tmp/payload.bin

python scripts/make_torrent.py /tmp/payload.bin --piece-length 131072
```

Prints the info_hash, size, and piece count, and writes
`/tmp/payload.bin.torrent`. `--out` sets a different output path.

## 3. Two peers by hand (one machine)

The seeder already has the file; the leecher starts with nothing and must
find the seeder through the DHT. **No peer addresses are exchanged** — only
a DHT bootstrap address.

```bash
# Terminal 1 — seeder. Its --path already holds the complete file.
cp /tmp/payload.bin /tmp/seed_copy.bin
python scripts/run_peer.py \
    --torrent /tmp/payload.bin.torrent \
    --path /tmp/seed_copy.bin \
    --dht-port 9000 --bt-port 6881 --control-port 9001
```

It prints, and stays running:

```
READY <node_id> dht=127.0.0.1:9000 bt=127.0.0.1:6881 info_hash=<hex> pieces=7/7
COMPLETE payload.bin 2000000 bytes
```

```bash
# Terminal 2 — leecher. --path does not exist yet; it gets created.
python scripts/run_peer.py \
    --torrent /tmp/payload.bin.torrent \
    --path /tmp/downloaded.bin \
    --dht-port 9100 --bt-port 6981 --control-port 9101 \
    --bootstrap 127.0.0.1:9000
```

Starts at `pieces=0/7` and prints `COMPLETE ...` within a few seconds.

```bash
# Terminal 3 — check on it, then verify
python ctl.py 9101 '{"cmd": "progress"}'
sha256sum /tmp/payload.bin /tmp/downloaded.bin   # the two hashes must match
```

Shut them down cleanly:

```bash
python ctl.py 9101 '{"cmd": "shutdown"}'
python ctl.py 9001 '{"cmd": "shutdown"}'
```

### run_peer.py flags

| Flag | Meaning |
|---|---|
| `--torrent` | the `.torrent` file (required) |
| `--path` | where the data is, or will be written (required) |
| `--dht-port` | UDP port for the DHT (required) |
| `--bt-port` | TCP port for peer connections (0 = pick one) |
| `--control-port` | TCP port for the JSON control channel (required) |
| `--host` | address to bind to; use the real LAN IP across machines, `0.0.0.0` in Docker |
| `--control-host` | defaults to `127.0.0.1` — the control channel stays local even when `--host` doesn't |
| `--bootstrap` | `host:port[,host:port]` of DHT nodes to join through; omit for the first peer |
| `--log-file` | where to write logs |

### Control channel commands

```bash
python ctl.py <control-port> '{"cmd": "progress"}'   # pieces, bytes, per-peer stats
python ctl.py <control-port> '{"cmd": "complete"}'   # just the boolean
python ctl.py <control-port> '{"cmd": "peers"}'      # currently connected peers
python ctl.py <control-port> '{"cmd": "add_peer", "host": "127.0.0.1", "port": 6881}'
python ctl.py <control-port> '{"cmd": "shutdown"}'
```

`add_peer` dials a peer directly, bypassing the DHT — useful for isolating
whether a problem is in discovery or in the transfer itself.

## 4. Across Docker containers

Each container gets its own IP and filesystem, so this exercises the real
UDP-for-DHT plus TCP-for-data path rather than loopback-with-different-ports.
Needs Docker access first — see `docker_test.md` §1 if `docker ps` fails.

```bash
python scripts/bt_harness.py                      # 1 seeder, 2 leechers, 2 MB
python scripts/bt_harness.py --leechers 4 --size 8000000
```

No venv needed (it only shells out to `docker`). It will:

1. Generate a random payload plus its `.torrent` in `bt_work/`.
2. Build the image and create a Docker network.
3. Start `bt-seed` holding the complete file.
4. Start `bt-leech0..N` empty, given **only** a DHT bootstrap address —
   they must discover the seeder through the DHT themselves.
5. Poll progress until every leecher completes.
6. Verify each downloaded file's SHA256 against the original.
7. Tear down every container and the network, pass or fail.

Ends with `All BitTorrent scenarios passed.`

### Across real machines on the same WiFi

Same as §3, but bind to each machine's real LAN IP (find it with
`hostname -I`) and open the **UDP** DHT port and the **TCP** BT port between
them. The control port stays localhost-only and needs no firewall rule.

```bash
# Machine A (192.168.29.155) — seeder
python scripts/run_peer.py --torrent payload.torrent --path payload.bin \
    --host 192.168.29.155 --dht-port 9000 --bt-port 6881 --control-port 9001

# Machine B — leecher, bootstrapping off A's DHT port
python scripts/run_peer.py --torrent payload.torrent --path downloaded.bin \
    --host 192.168.29.200 --dht-port 9000 --bt-port 6881 --control-port 9001 \
    --bootstrap 192.168.29.155:9000
```

Both machines need the repo and the same `.torrent` file. Firewall, e.g.:

```bash
sudo ufw allow 9000/udp     # DHT
sudo ufw allow 6881/tcp     # peer wire protocol
```

The `docker_test.md` pitfalls all apply here too — loopback/`0.0.0.0` as
`--host` won't work across machines, and WiFi client isolation blocks
everything regardless of firewall rules.

## 5. Tuning

`bittorrent/constants.py`:

| Constant | Default | Effect |
|---|---|---|
| `BLOCK_SIZE` | 16 KiB | request granularity within a piece |
| `MAX_PIPELINED_REQUESTS` | 8 | outstanding requests per peer; raise for high-latency links |
| `UNCHOKE_SLOTS` | 4 | how many peers we upload to at once |
| `UNCHOKE_INTERVAL` | 10 s | how often tit-for-tat is recalculated |
| `OPTIMISTIC_UNCHOKE_INTERVAL` | 30 s | how often the newcomer slot rotates |
| `ENDGAME_THRESHOLD` | 8 blocks | when to start duplicating the stragglers |
| `RANDOM_FIRST_PIECES` | 2 | pieces picked randomly before rarest-first kicks in |
| `ANNOUNCE_TTL` | 30 min | how long a node keeps a peer announcement |
| `MAX_PEER_CONNECTIONS` | 50 | connection ceiling |

`kademlia/constants.py` still governs the DHT (`K`, `ALPHA`, RPC timeouts).
Note `K` is currently **3**, not the paper's 20.

## 6. Troubleshooting

**Leecher sits at 0 pieces with no peers.** Discovery isn't finding the
seeder. Check `{"cmd": "peers"}` on both sides, then bypass the DHT with
`add_peer` — if that works, the problem is discovery (bootstrap address,
DHT connectivity), not the transfer.

```bash
python ctl.py 9101 '{"cmd": "add_peer", "host": "127.0.0.1", "port": 6881}'
```

**Peers connect but nothing transfers.** Usually choking: check
`choked_by_peer` in `{"cmd": "progress"}`. A peer with nothing to offer waits
for the optimistic unchoke slot, up to `OPTIMISTIC_UNCHOKE_INTERVAL`.

**`info_hash` mismatch.** Both sides must use the *same* `.torrent` file —
regenerating it from the same data produces the same info_hash, but any
difference in piece length or name changes it, and peers will refuse each
other's handshake.

**Logs.** `--log-file` per peer; the Docker harness writes `logs/<name>.log`.

**Logs from a failed Docker run** are the first place to look — they survive
container removal because `logs/` is bind-mounted from the host:

```bash
cat logs/bt-seed.log
cat logs/bt-leech0.log
```

**Stale state after an interrupted run** — see §7.

## 7. Cleanup

A normal `bt_harness.py` run cleans up after itself: containers and the
network are removed in a `finally` block whether it passed or failed. These
commands are for the abnormal case — Ctrl-C mid-run, closed terminal, machine
slept — where its own teardown never got to run.

### Containers and network

```bash
# Remove leftover BitTorrent harness containers (bt-seed, bt-leech0, ...)
docker ps -aq --filter "name=^bt-" | xargs -r docker rm -f

# Remove the BitTorrent test network
docker network rm bittorrent-test-net 2>/dev/null

# Confirm both are gone
docker ps -a --filter "name=^bt-"
docker network ls
```

`xargs -r` skips the command entirely when nothing matches, instead of
calling `docker rm` with no arguments and erroring.

If you've also been running the DHT-only harness (`docker_harness.py`), it
uses different names, so clean those separately:

```bash
docker ps -aq --filter "name=^node[0-9]+$" | xargs -r docker rm -f
docker network rm kademlia-test-net 2>/dev/null
```

**If you need `sudo` for Docker**, wrap the *whole* pipeline — don't just
prefix `sudo`:

```bash
sudo sh -c 'docker ps -aq --filter "name=^bt-" | xargs -r docker rm -f'
```

`sudo docker rm -f $(docker ps -aq ...)` does **not** work: the shell
evaluates `$(...)` before sudo applies, so the inner `docker ps` runs as your
normal user and fails with "permission denied", leaving the outer command
with no arguments.

Better still, stop needing sudo. If `sudo groupadd docker` /
`sudo usermod -aG docker $USER` have already been run (see `docker_test.md`
§1), the membership only takes effect in a *new* session — check with
`getent group docker`, then open a fresh terminal or run `newgrp docker`.

### Generated files

`bt_harness.py` writes the payload, the `.torrent`, and every peer's copy of
the data into `bt_work/`. At the default 2 MB that's ~8 MB per run, and it
grows fast with `--size` and `--leechers`, so it's worth clearing:

```bash
rm -rf bt_work/          # payload, .torrent, and each peer's downloaded copy
rm -f logs/*.log         # per-peer logs (shared with the DHT harness)
```

### Full reset

Only if you want the image rebuilt from scratch next run (slower one time,
since it re-pulls `python:3.8-slim`):

```bash
docker rmi kademlia-dht-node:latest 2>/dev/null
```

Note this image is shared with `docker_harness.py` (the DHT tier), so
removing it affects both.

### Everything at once

```bash
docker ps -aq --filter "name=^bt-" | xargs -r docker rm -f
docker ps -aq --filter "name=^node[0-9]+$" | xargs -r docker rm -f
docker network rm bittorrent-test-net kademlia-test-net 2>/dev/null
rm -rf bt_work/ logs/*.log
```

Under sudo, as one root shell:

```bash
sudo sh -c 'docker ps -aq --filter "name=^bt-" | xargs -r docker rm -f
            docker ps -aq --filter "name=^node[0-9]+$" | xargs -r docker rm -f
            docker network rm bittorrent-test-net kademlia-test-net 2>/dev/null'
rm -rf bt_work/ logs/*.log
```

## 8. Quick check after changing something

```bash
pytest tests/ -q && python scripts/bt_harness.py
```
