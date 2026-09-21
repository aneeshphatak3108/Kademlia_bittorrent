# Kademlia DHT Layer — Code Guide

This explains how the code in `kademlia/` fits together. It's the lower layer
described in `claude.md`: a pure Kademlia DHT (arbitrary key/value, no
BitTorrent semantics yet). Read this top to bottom once, then use it as a map.

## Mental model in one paragraph

Every node has a random 160-bit ID. Nodes know about a limited set of other
nodes (their **routing table**), organized into **buckets** by how "far" (XOR
distance) those nodes' IDs are from the node's own ID. To find something —
another node, or a stored value — a node asks the closest peers it currently
knows about who *they* know that's even closer, repeatedly, until it converges
on the true closest nodes to the target. That repeated narrowing is the
**iterative lookup**, and it's the one algorithm nearly everything else is
built on: `STORE` uses it to find *where* to put a value, `FIND_VALUE` uses it
to find *who has* a value, and joining the network uses it (searching for your
own ID) to populate your routing table.

## Directory layout

```
kademlia/
  identifier.py      160-bit NodeID + XOR distance math
  bencode.py          bencode codec (the wire serialization format)
  constants.py        all the tunable numbers (K, ALPHA, timeouts, TTLs...)
  routing_table.py    KBucket + RoutingTable (who we know about)
  storage.py           DataStore (our local key/value copies, with TTL)
  rpc/
    messages.py         the 4 RPCs as dataclasses + bencode <-> object conversion
    protocol.py          asyncio UDP socket handling, request/response matching
  lookup.py            the iterative lookup algorithm (used by find_node & find_value)
  node.py              the public Node class — wires everything above together
  logging_conf.py      per-node logging setup, used by scripts/run_node.py

scripts/
  run_node.py          launches one Node as a real OS process with a control channel
  harness.py           spins up N run_node.py processes, drives a multi-process test

tests/                 pytest suite (see "Testing" below)
```

Read them in roughly this order if you're new to the code:
`identifier.py` → `routing_table.py` → `storage.py` → `rpc/messages.py` →
`rpc/protocol.py`.py` → `node.py`.

## Walking through a `store()` call

This is the best way to see how the pieces connect. Say you call
`await my_node.store(key, value)` (`node.py:144`):

1. It stores the value **locally first** (`self.storage.put(...)`) — every
   node keeps its own copy of anything it originates, which simplifies
   republishing later.
2. It calls `lookup.iterative_find_node(self, NodeID(key))` to find the K=20
   nodes in the whole network closest to the key (treating the key like a
   node ID — same 160-bit space, same distance metric).
3. Inside that lookup (`lookup.py:49`), it starts from whatever's already in
   the local routing table, asks the `ALPHA=3` closest-of-those for *their*
   closest-known nodes (a `FIND_NODE` RPC), merges the new nodes it hears
   about into the candidate pool, and repeats — always querying the
   closest not-yet-queried candidates — until nothing gets any closer and
   the closest K have all been asked. That's what "iterative" means: each
   round tightens the search based on the previous round's answers.
4. Back in `store()`, once it has the true K closest live nodes, it sends a
   `STORE` RPC to each of them concurrently (`asyncio.gather`) and counts
   how many acknowledged.

`find_value(key)` (`node.py:156`) is the mirror image: check local storage
first, and if not found, run the same iterative lookup but with `FIND_VALUE`
RPCs instead of `FIND_NODE` — any peer that actually has the value short-
circuits the search by returning it directly instead of more node contacts.

## The routing table (`routing_table.py`)

- `NodeID.bucket_index(other)` (`identifier.py`) computes which of 160
  buckets a contact belongs in: bucket *i* holds contacts whose XOR distance
  from us has its highest set bit at position *i*. Bucket 0 = almost
  identical ID (very "close"), bucket 159 = almost the whole ID differs.
- Each `KBucket` holds at most `K=20` contacts, ordered **least-recently-seen
  first**. When a bucket is full and a *new* contact shows up, we don't just
  evict the oldest one — we ping it first (`Node._verify_and_replace`,
  `node.py:172`). If it's still alive, it stays (Kademlia trusts nodes with
  long uptime over new arrivals, since long-lived nodes are statistically
  more likely to keep being reliable). If it's dead, it's evicted and the new
  contact takes its place.
- `RoutingTable.find_closest(target, count)` just scans every bucket and
  sorts by XOR distance — simple, and plenty fast at the scale this project
  runs at.

## The wire protocol (`rpc/messages.py`)

Every message on the wire is a **bencoded dict** (bencode = BitTorrent's
serialization format — see `bencode.py`; chosen so the future BitTorrent
layer can reuse the same codec). The envelope is modeled on BEP 5's KRPC:

```
{"t": <2-byte transaction id>, "y": "q" | "r" | "e", ...}
```

- `y="q"` (query) adds `"q"` (method name: `ping` / `store` / `find_node` /
  `find_value`) and `"a"` (args dict, always includes the sender's `id`).
- `y="r"` (response) adds `"r"` (a dict, always includes the responder's
  `id`).
- `y="e"` (error) adds `"e": [code, message]` — sent back when a query is
  well-formed enough to recover its transaction id but otherwise invalid
  (e.g. a `store` missing its `value` field, or a `key` that isn't exactly
  20 bytes). See `MalformedMessageError.tid` and `protocol.py:54`.

`FIND_NODE`/`FIND_VALUE` responses carry a **compact node list**: each
contact packed as 26 raw bytes (20-byte ID + 4-byte IPv4 + 2-byte port),
concatenated — `encode_compact_nodes`/`decode_compact_nodes`. IPv6 isn't
supported (the project scope assumes everyone's on the same WiFi).

`STORE`'s `key` must be exactly 20 bytes, because it's placed in the *same*
ID space as node IDs — that's what lets `find_closest` be reused to answer
"which nodes should hold this key" the same way it answers "which nodes are
closest to this ID."

## The transport (`rpc/protocol.py`)

`KademliaProtocol` is a single `asyncio.DatagramProtocol` per node — one UDP
socket, shared by every outgoing RPC and every incoming query.

- **Outgoing**: `send_query()` generates a transaction id, remembers a
  `Future` for it in `self._pending`, sends the datagram, and schedules a
  timeout (`loop.call_later`). When a matching response (or explicit error)
  arrives, `datagram_received` resolves that `Future`. Retries live one
  level up, in `Node._send_with_retry` (`node.py:185`) — the protocol layer
  itself only does one send + one timeout.
- **Incoming**: a query gets handed to `Node.handle_query` (`node.py:241`),
  which dispatches on message type, updates the routing table with the
  sender, and returns whatever response (or `ErrorMessage`) should be sent
  back.

Because it's all `asyncio`, one socket comfortably handles many concurrent
in-flight RPCs (bounded per-lookup by `ALPHA`) — no threads or locks needed;
everything runs on one event loop, and routing-table/storage mutations
always happen between `await` points.

## Storage & expiry (`storage.py`)

`DataStore` is a plain dict of `key -> StoredItem(value, stored_at,
expires_at, last_republished)`. Two background loops in `node.py` drive it:

- `_refresh_loop` (`node.py:273`): every `maintenance_tick_interval`, refresh
  any routing-table bucket that's been idle too long, by doing a
  `find_node` lookup for a random ID that would fall in that bucket. Keeps
  the routing table populated even in quiet parts of the ID space.
- `_expiry_and_republish_loop` (`node.py:286`): every
  `expiry_sweep_interval`, purge anything past its `expires_at`, and for
  anything due (`last_republished` older than `republish_interval`),
  re-run the STORE placement algorithm so the value keeps living on the
  current K closest nodes even as the network's membership changes.
  Important subtlety: it forwards the item's **remaining** TTL
  (`expires_at - now`), not the original TTL — otherwise every republish
  would reset a short-lived key back to a fresh full TTL on every peer it
  touches.

All the interval/timeout constants above are `Node.__init__` keyword
arguments (defaulting to `constants.py`'s paper-scale values, e.g. 1 hour
refresh/republish, 24 hour default TTL) specifically so tests can shrink them
without touching the real logic — see the `FAST`/`FAST_REPUBLISH` dicts in
`tests/test_network_scenarios.py`.

## Joining the network (`Node.join`, `node.py:124`)

1. Ping every bootstrap contact given; if *none* respond (and the list
   wasn't empty), raise `BootstrapError`. An empty list means "I'm the first
   node" — a valid, deliberate no-op.
2. Run an iterative `find_node` lookup for **your own ID**. This is the
   standard Kademlia trick: searching for yourself naturally discovers and
   populates nearby buckets across the ID space, not just around whoever you
   bootstrapped through.
3. Refresh any buckets that are still empty afterward (capped at
   `MAX_JOIN_BUCKET_REFRESHES`), so a small network still ends up
   reasonably well-connected.

One correctness detail worth knowing: when we ping/query a contact we don't
fully trust yet (e.g. a bootstrap address we only know as host:port, not by
ID), we never add *our own guess* at their node ID to the routing table —
we add whatever ID they actually declared in their response
(`Node._trusted_responder`, `node.py:200`).

## Testing

Two tiers, matching `tests/`:

- **Tier 1** (`pytest`, everything in one process): fast, runs real UDP
  sockets on localhost across many `Node` objects sharing one event loop.
  `tests/test_network_scenarios.py` has the full-system scenarios
  (bootstrap, store/get, lookup-convergence-vs-brute-force, node churn, TTL
  expiry, republish); the other `test_*.py` files unit-test individual
  modules. Run with:
  ```
  source .venv/bin/activate && pytest tests/ -q
  ```
- **Tier 2** (`scripts/harness.py`): launches real, separate OS processes
  (`scripts/run_node.py`) talking over real UDP ports, to validate the
  actual process-boundary/wire-serialization path that Tier 1 can't exercise
  (Tier 1 nodes share a process, so a bencode bug could theoretically be
  masked by Python objects staying in memory). Run with:
  ```
  source .venv/bin/activate && python scripts/harness.py --nodes 10
  ```

## Known, deliberate simplifications

- Fixed 160-bucket routing table instead of the dynamic bucket-splitting
  some implementations use — simpler, and correct at this project's scale.
- No distinction between "original publisher" and "replica holder"
  republishing (the paper has two separate timers for this) — collapsed
  into one republish sweep per key-holder.
- No NAT traversal — out of scope per `claude.md` (same-WiFi assumption).
- No authentication/Sybil resistance — this is a teaching/from-scratch
  implementation, not a hardened production DHT.
