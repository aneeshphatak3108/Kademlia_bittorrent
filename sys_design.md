# BitTorrent Layer — System Design Notes

## How to use this document

This is a working-through of the system-design decisions for the BitTorrent
layer, before any of it gets built. Two purposes: (1) reach an actual
decision on each question before coding starts, and (2) double as prep for
being asked about this project's design in a system-design-interview style
setting — so each section tries to state the *tradeoff*, not just the
answer, and several sections end with the kind of follow-up question an
interviewer would likely ask next.

Nothing here is final. Where a recommendation is given, it's a starting
position for discussion, not a decision already made.

## What the DHT layer already gives us (verified against the actual code, not memory)

Worth grounding everything below in exactly what exists today, since several
design choices in this document depend on precise details of it:

- `Node.store(key, value, ttl)` (`kademlia/node.py:145`): stores locally
  first, runs `iterative_find_node` to find the current closest nodes to
  `key`, then sends `STORE` to **all** of them concurrently via
  `asyncio.gather`, and counts how many acknowledged.
- `DataStore.put(key, value, ttl)` (`kademlia/storage.py:24`): **one key maps
  to exactly one value** — each call *replaces* whatever was there. This is
  a deliberate, correct design for a generic arbitrary-KV store, and it's
  the source of a real problem for peer-list storage, covered in §2 below.
- `iterative_lookup` (`kademlia/lookup.py:49`): actively queries outward —
  starts from whatever's in the local routing table, then keeps asking
  closer and closer nodes, until no closer node can be found and the
  current K-closest-known set has all been queried. This is *why* different
  callers converge on the same answer regardless of their starting
  point — it doesn't just report local knowledge, it searches until it
  provably can't do better.
- Constants today (`kademlia/constants.py`): `K=20`, `ALPHA=3`,
  `RPC_TIMEOUT=5.0s`, `RPC_RETRIES=2`, `REPUBLISH_INTERVAL=3600s` (1h),
  `TTL_DEFAULT=86400s` (24h), `EXPIRY_SWEEP_INTERVAL=300s`. These are tuned
  for *durable arbitrary data*. Several BT-layer decisions below need their
  own, different constants — carried through explicitly rather than assumed.
- Republishing (`Node._expiry_and_republish_loop`, `node.py`): whichever
  node currently *holds* a value is responsible for periodically re-running
  `find_node` and re-pushing it to the *current* closest set, because that
  set drifts as the network churns. This detail matters a lot for §2.

---

## 0. Foundational questions (these shape everything that follows)

### 0.1 Protocol fidelity — real BEP-compliant BitTorrent, or our own protocol inspired by it?

| Option | What it buys | What it costs |
|---|---|---|
| Strict BEP compliance (BEP3 wire protocol, BEP9 metadata exchange, BEP5 `announce_peer`/`get_peers`) | Interoperates with real BitTorrent clients on the same swarm | Significant extra spec-compliance work (extension protocol negotiation, exact byte-for-byte framing, edge cases that exist only for legacy-client compatibility) that doesn't teach anything about the underlying system |
| Our own protocol, inspired by BitTorrent's mechanisms but not byte-compatible | Full control of both sides, easier to reason about and debug end-to-end, no compliance busywork | Can't talk to real clients; if that's ever wanted later, it's a rewrite of the wire format, not the algorithms |

**Recommendation:** spirit-compliant, not byte-compliant. Reuse
BitTorrent's *mechanisms* (piece/block splitting, rarest-first, choke/
unchoke, SHA1 piece hashes, length-prefixed binary framing for the hot-path
data-transfer protocol) because they're well-proven answers to real
problems — but don't chase literal interop with real clients. Keep using
bencode for anything metadata-shaped (consistent with why it was chosen for
the DHT layer), but the piece/block transfer protocol itself should be a
simple length-prefixed binary framing, not bencode — bencode is the wrong
tool for a hot-path bulk-data protocol (per-message encoding overhead, no
fixed-size framing to exploit).

*Likely follow-up:* "What would it take to add real-client interop later?"
— Mainly the wire format (message IDs, reserved bytes, extension
negotiation) and the `.torrent`/magnet-link metadata format; the algorithmic
core (piece selection, choke/unchoke, the DHT integration) wouldn't need to
change, since those are already modeled on the real mechanisms.

### 0.2 Single torrent per process, or multi-torrent?

`claude.md` doesn't specify. This affects nearly every later section:
resource limits become per-torrent *and* global, scheduling needs a
torrent-level layer above the piece-level layer, persistence needs a
torrent-id namespace.

**Recommendation:** design the core abstractions (piece manager, peer
connection, piece picker) as *per-torrent* objects from the start, so
multi-torrent is later just "run more than one instance" — but build and
test single-torrent first. Naming this now avoids baking in
single-torrent-only assumptions (global, non-namespaced state) that would
be painful to unwind later, without paying the cost of solving cross-torrent
resource arbitration before the single-torrent mechanics even work.

*Likely follow-up:* "If you had 10 torrents running, how would you share a
fixed upload-bandwidth budget across them?" — This is genuinely unsolved by
the recommendation above (it only says "design for it," not "here's the
algorithm") — worth having an answer ready: some kind of weighted
allocation across per-torrent token buckets, revisited when multi-torrent
is actually built.

### 0.3 Where does peer discovery plug into the DHT layer?

A torrent's `info_hash` (20 bytes — same shape as a Kademlia key) is what
peers need to rendezvous on. The question is: how do many different peers
all become discoverable under that one key, given `DataStore.put` replaces
a single value per key rather than accumulating a list?

**A tempting wrong answer, worth naming explicitly because it's an easy
trap:** "just reuse `store()`/`find_value()` as-is, with the stored value
being a list that peers append to." This doesn't actually solve anything —
`DataStore.put(key, value)` still *replaces* whatever was at that key on
each individual node. If peer A appends itself to the list and calls
`store()`, and peer B does the same moments later on the *same* node, B's
`put()` overwrites A's list-with-B-appended with its own
list-with-only-B — unless there's a read-modify-write update, which
`DataStore` deliberately doesn't support (and adding it reopens the
concurrency questions the base DHT layer already solved by *not* needing
read-modify-write). Whether the value is a single contact or "a list" is
irrelevant — the overwrite problem is about the storage primitive's
semantics, not the value's shape.

**Another tempting wrong answer:** "have each of the K closest nodes store
one peer's announcement" — i.e., node 1 holds peer 1's entry, node 2 holds
peer 2's entry, and so on, a 1-to-1 partition. This is also wrong, and for a
different reason: it gives no way to reconstruct the full list from a
lookup. If you query 3 of the 20 nodes, you'd get back (at most) 3 peers'
worth of entries — not "up to 3 nodes' worth of a shared near-complete
picture," since each node genuinely only knows about the one peer assigned
to it under this scheme. There is no redundancy in it at all, and it's not
actually how the mechanism this document ends up recommending works.

**The actual mechanism — full replication via fan-out, not partitioning:**
add two new BT-layer-only RPCs, `ANNOUNCE_PEER(info_hash, my_contact)` and
`GET_PEERS(info_hash)`, following the same wire-transport patterns the DHT
layer already uses (bencoded, UDP, tx-id correlated, timeout/retry —
`kademlia/rpc/protocol.py`'s machinery is directly reusable, just with a new
message shape). Each DHT node keeps its own local table, separate from
`DataStore`:

```python
peer_announcements: Dict[info_hash, Set[PeerContact]]   # ADDS, never overwrites
```

Announcing works exactly like `Node.store()` already does (`node.py:145`)
— find the current K closest nodes to `info_hash`, then fan out
`ANNOUNCE_PEER` to **all** of them concurrently (`asyncio.gather`), not to
one each. Since (in a reasonably stable network) *every* peer's
`find_node(info_hash)` converges on that same K-closest set — because
`iterative_lookup` actively searches for the true closest nodes rather than
reporting local knowledge — every peer's announcement fans out to the same
set of nodes. Each of those nodes therefore independently accumulates the
(approximately) *complete* peer list on its own, via ordinary set-add. This
is K-way replication of the whole list, not a partition of it: querying a
few of the K nodes on the `GET_PEERS` side is redundancy against any single
node having missed an announcement (timing, brief unreachability), not
reassembly of disjoint fragments — even a single one of the K nodes, if it's
been reachable the whole time, should already hold essentially the full list
on its own.

**No transport connections needed for any of this, and this is worth being
explicit about since it's an easy thing to conflate:** `ANNOUNCE_PEER`/
`GET_PEERS` are DHT control-plane RPCs — single UDP datagram out, one back,
exactly like `PING`/`STORE`/`FIND_NODE`/`FIND_VALUE` today. This is a
*completely separate* transport from the actual file-piece transfer
protocol (§0.1's TCP peer-wire protocol). Fanning out to K=20 (or 50, or
whatever) nodes costs 20 (or 50) small UDP datagrams sent concurrently, not
20 held-open connections.

**Guarantee level — stated precisely, not oversold:** there's no hard
guarantee, and there shouldn't be — this is the same probabilistic,
eventually-consistent bet the base DHT's own `STORE` replication already
makes, not a weaker version of it. What it rests on:
1. **Redundancy margin** (K nodes, query several not just one).
2. **Active convergence** — `iterative_lookup`'s termination condition means
   different peers reliably find the *same* true-closest set in a stable
   network, which is what makes "everyone's announce lands on the same
   nodes" true in the first place (this is the same property
   `test_lookup_convergence` already checks for plain `find_node`).
3. **Periodic re-announcement** — covered next, because it's not optional,
   it's load-bearing.

**Who re-announces, and why this differs from the base layer's republish —
this is the second place the earlier draft got imprecise, worth being
careful about:**

- In the base DHT, *whichever node holds a copy* re-publishes it forward as
  the closest set drifts (`Node._expiry_and_republish_loop`) — reasonable,
  because the data is inert; any holder can honestly keep re-asserting
  "here are these bytes," they don't change and don't need anyone's
  ongoing consent.
- A peer announcement is not inert data — it's a **liveness claim** ("I am
  online right now, serving this torrent"). A DHT node holding your old
  announcement cannot honestly keep re-asserting that on your behalf; it has
  no way to know if you're actually still around. So, unlike the base
  layer's `STORE`, **holding nodes should not republish `ANNOUNCE_PEER`
  entries on a peer's behalf.** The *announcing peer itself* must
  periodically re-run `find_node(info_hash)` and re-send `ANNOUNCE_PEER` to
  the current closest set. If it stops, its entry should simply expire and
  drop out — correct behavior, not a bug, since a peer that hasn't
  reaffirmed liveness recently isn't a good peer to try connecting to.
- Practical consequence: `ANNOUNCE_PEER` entries need their **own**, much
  shorter TTL/re-announce-interval than the base layer's `TTL_DEFAULT`
  (24h) / `REPUBLISH_INTERVAL` (1h) — those are tuned for durable arbitrary
  data. Real BEP5 uses roughly 30 minutes for peer-announce entries,
  functioning more like a heartbeat/lease than durable storage. This project
  should use a similarly short, separate constant, not reuse the DHT
  layer's existing ones.

*Likely follow-ups:*
- "What if the network partitions and two halves each think they have the
  complete peer list?" — Neither half's list is wrong, exactly, but neither
  is complete; this is the same split-brain-ish weakness any
  eventually-consistent, gossip/redundancy-based system has, and it's why
  §0.3 above deliberately doesn't claim a hard guarantee. Healing happens
  passively once connectivity is restored and re-announces start landing on
  the (now-merged) correct closest set again.
- "Why not just use a tracker (a single known server) instead of all this?"
  — Because the whole point of this project is a Kademlia-based (trackerless)
  design; a tracker would trivially solve this at the cost of the single
  point of failure/control that DHT-based discovery exists to avoid.
- "How would a malicious node exploit `ANNOUNCE_PEER`?" — It could refuse to
  store real announcements while claiming to (silent data loss, mitigated
  only by redundancy across K nodes), or flood fake announcements for peers
  that don't exist (mitigated by peers being unreachable when actually
  dialed, so bad entries cost a wasted connection attempt, not worse) —
  covered more generally in §8.

---

## 1. Persistent state vs RAM

**Guiding principle, carried over from how the DHT layer already behaves:**
persist only what's *ground-truth work product* or *prohibitively expensive
to recompute*; everything else (session/scheduling/network state) lives in
RAM, disposable, rebuilt from the network or from persisted ground truth
after a restart.

| State | Persist? | Reasoning |
|---|---|---|
| Torrent metadata (info_hash, piece hashes, piece length, file list) | Yes | Immutable; cheap to re-parse from the original metadata file on every start, so no need for a separate derived copy |
| Downloaded piece bytes | Yes | This is the actual deliverable |
| "Which pieces do we have" bitfield | Recommend: derive from disk at startup, don't separately persist (below) | — |
| In-flight block-level request state | No — RAM only | Ephemeral; treat any not-fully-verified piece as entirely absent after a crash |
| Known peers for this swarm | No (soft-persist as a hint at most) | Cheaply rediscoverable via the DHT |
| Rate limiter / bandwidth accounting | No — RAM only | Fine to reset on restart |
| Choke/unchoke state, per-peer stats | No — RAM only | Rebuilt as connections re-form |

**The bitfield tradeoff is the interesting one:**

- **(A) Re-derive on startup:** for each piece fully present on disk by
  byte-range, re-hash and verify; mark "have" only on match. Self-healing
  (silently fixes disk/bitfield divergence), zero extra persisted state,
  zero write-ordering subtlety to get wrong. Cost: an O(file size) SHA1 pass
  every startup — real but bounded (SHA1 is roughly GB/s on modern
  hardware, so a few GB is a few seconds).
- **(B) Persist the bitfield** (a `.resume`-style file, as Transmission/
  libtorrent do): instant resume. Cost: a new failure mode — the bitfield
  write and the data write must be ordered correctly (data durably written
  *before* the bitfield says "have"), or a crash between the two can leave
  the bitfield asserting a piece is complete/verified when it isn't,
  risking serving corrupt data to other peers.

**Recommendation:** (A) first. No write-ordering/fsync subtlety to get
wrong — correct by construction. (B) is a legitimate optimization to add
later, and *specifically* becomes safer to add once (A)'s hash-verification
path already exists as a fallback/double-check, rather than being the only
source of truth from day one.

**Write-back policy:** write each block directly to its final on-disk
offset as it arrives, not buffered until the whole piece completes — piece
sizes (256KB–4MB) are large enough that buffering doesn't meaningfully
reduce syscall count, and writing at the final offset immediately makes
duplicate-block writes (endgame mode, §7) naturally harmless (same bytes,
same offset). Only the "have piece" bit flips, and only after hash
verification, once all of a piece's blocks are in.

*Likely follow-up:* "How would you extend this to support pausing and
resuming a torrent mid-session without restarting the process?" — That's
actually a *lighter* problem than full crash recovery: no persistence needed
at all, just stop issuing new requests / stop accepting new connections and
keep all in-RAM state (bitfield, peer set) alive, since the process never
died.

---

## 2. Concurrency & race conditions

Same execution model as the DHT layer: single-threaded `asyncio`. Classic
multi-threaded data races don't apply, but a real, different race class
does:

**Check-then-act races across `await` points.** Any "read shared state →
`await` something → mutate based on what was read" sequence can race, since
the `await` yields control and a different coroutine can run in between.
Concrete example: two peer-connection coroutines both run the piece picker,
both see "piece 42 still needed," both `await` sending a request for it —
piece 42 gets double-requested. Not corruption, but a real bug (wasted
bandwidth).

**The fix is the exact pattern the DHT layer already uses correctly**
(`lookup.py:65`, `queried.update(...)` happens synchronously, *before* the
`await asyncio.gather(...)` that follows): the "mark as claimed" mutation
must happen in the same synchronous block as the "check if available" read,
with no `await` in between. Ordinary (non-`async`) function calls never
yield in Python, so implementing pick-and-mark as plain synchronous code
makes it race-free automatically — no lock needed. A lock is only needed if
that check-and-mark logic itself must `await` something, which it shouldn't
here.

Other concurrency points specific to this layer:

- **Multiple peers writing to the same file at different offsets** is safe
  by construction (disjoint ranges) — except duplicate writes in endgame
  mode, which is a duplicate, not a race (§9).
- **CPU-bound piece hashing blocks the event loop if done inline** — same
  "don't block the loop" problem as blocking I/O, just CPU-bound. SHA1 over
  a single piece (a few MB) is low-single-digit milliseconds, likely fine
  inline. A *full-file* re-verify pass at startup (§1 option A) over many GB
  is not fine inline — needs chunking with periodic yields
  (`await asyncio.sleep(0)`) or `loop.run_in_executor`.
- **Blocking file I/O must not run directly on the event loop** — plain
  `open()`/`write()` are blocking syscalls that stall every peer connection
  for their duration if called directly from a coroutine. Needs
  `loop.run_in_executor` or an async file-I/O library.

*Likely follow-up:* "Give an example of a race that *would* need an
explicit lock in this system." — Anything where the check-and-mark can't be
made purely synchronous — e.g., if piece selection needed to consult another
peer over the network *before* committing to a choice (it shouldn't, in this
design, but if a future refinement introduced that), that would reintroduce
an `await` between check and mark and need explicit protection.

---

## 3. Backpressure

Three distinct surfaces:

1. **Network → disk.** Incoming data can arrive faster than it's written
   (fast LAN peers, slower disk). Unbounded in-memory queuing risks OOM.
   **Recommendation:** a bounded `asyncio.Queue(maxsize=N)` between
   "received a block" and "write it" — `await queue.put(...)` naturally
   suspends the producer once full, which *is* backpressure, using
   asyncio's own primitive.
2. **Us → peer (uploading).** `await writer.drain()` after `writer.write()`
   when sending piece data — asyncio's built-in write-buffer backpressure,
   directly usable with no custom code.
3. **New-connection backpressure.** A DHT lookup can surface many candidate
   peers at once; connecting to all simultaneously spikes FD usage and
   handshake CPU. Bound concurrent in-flight connection attempts with a
   semaphore (ties to §4).

**Worth naming explicitly:** BitTorrent's choking mechanism *is* a
backpressure/fairness mechanism at the peer-relationship level — "I'm
choking you" means "stop asking, I won't answer" — the same idea as (2),
one level up. This connects backpressure, rate limiting (§6), and
scheduling (§7): they're not three independent mechanisms, choke/unchoke
implements all three at once.

*Likely follow-up:* "What happens if the bounded disk-write queue stays
full for a long time?" — Backpressure propagates upstream: the network-read
side stops pulling more data, which (via TCP's own flow control) eventually
causes the *sending* peer's writes to block too — the slowdown is felt
end-to-end, which is the correct behavior, not a bug to route around.

---

## 4. Resource limits & connection lifecycle management

**Limits worth naming as constants** (mirroring `kademlia/constants.py`'s
style):
- Max concurrent peer connections, global and per-torrent (once
  multi-torrent exists, §0.2).
- Max concurrent in-flight *outbound connection attempts*, separate from
  total connections.
- Max outstanding block requests per peer (pipelining depth — also a
  throughput knob, §7).
- OS file-descriptor awareness: one socket FD per connection; default
  ulimits (often 1024 on Linux) cap this. Catch `OSError`/`EMFILE` and back
  off rather than crash or spin-retry.

**Connection lifecycle as an explicit state machine**, per peer:
`CONNECTING → HANDSHAKING → ACTIVE → CLOSING → CLOSED`, with `ACTIVE`
tracking interested/choked state both directions. Validating incoming
messages against the *current* state (rejecting a `PIECE` before handshake
completes, say) avoids a class of "message arrived out of order" bugs and
doubles as basic protection against buggy/hostile peers (§8).

**Idle-peer eviction:** a connected-but-useless peer (not interested,
nothing we need) shouldn't permanently occupy a slot. Periodically
re-evaluate the connected set and evict the least useful fraction to make
room for newly discovered candidates, rather than treating connections as
first-come-first-served-forever.

**Cleanup on disconnect is correctness-critical, not just hygiene:** any
block requests still in flight to a peer that disconnects must be released
back into the "needed" pool immediately, or that block (and its piece)
stalls forever. One centralized "on peer disconnected" path, not scattered
ad hoc cleanup.

*Likely follow-up:* "How do you decide who to evict when the connection
table is full and a new, potentially-better peer shows up?" — A reasonable
answer: never evict to make room for an *unproven* new peer immediately;
only evict idle/unproductive peers on a periodic timer, and let new peers
compete for those freed slots rather than preempting active ones — avoids
connection churn from constantly replacing "good enough" peers with
"maybe better" ones.

---

## 5. Timeouts: when to retry, when to give up

The DHT layer's *pattern* is worth reusing: retry logic lives one layer
above the raw transport (`Node._send_with_retry` wraps
`protocol.send_query`), with a bounded count and a timeout. Recommend the
same layering for BT peer-wire requests — a `request_block()` wrapper owns
timeout/retry, not the raw socket-read code.

**But the retry philosophy is genuinely different, worth being deliberate
about:**
- DHT RPCs retry the *same* target — correct, since it's a point-query to a
  specific node with no alternative place to ask.
- **BT block requests should not retry the same peer.** A timeout usually
  means "busy/slow right now," not "unreachable" — the useful response is
  reassigning the block to a *different* already-connected peer (optionally
  `CANCEL`-ing first), not re-asking the same slow peer. Naively copying
  the DHT's same-target retry pattern here would be a real inefficiency, if
  not a bug.

**Timeout scales should reflect what's actually being waited for**, not one
global constant (unlike the DHT's single `RPC_TIMEOUT=5s`, reasonable there
since all 4 RPCs are similarly-shaped point-queries):
- TCP connect: a few seconds.
- Handshake completion: a few seconds.
- Individual block request: order 10–30s (transfer time for ~16KB even on a
  slow link, plus slack for the peer being legitimately busy elsewhere).
- "Peer connected but produced nothing useful" idle timeout: minutes.

**"Give up" has three different scopes:**
1. One block request → reassign to another peer.
2. A peer → after N consecutive timeouts/violations, disconnect and apply
   backoff before reconnecting (echoes the DHT routing table's
   ping-before-evict, don't-immediately-retrust instinct — same idea,
   different layer).
3. A piece with no current source → should essentially never be a permanent
   give-up. Keep periodically re-querying the DHT for more peers for that
   `info_hash`. This is a real philosophical difference from the DHT layer:
   Kademlia lookups are *bounded* (`MAX_LOOKUP_ROUNDS`, then return
   whatever's found) because they're point-in-time queries; a torrent
   download is long-running by nature and should default to "keep trying
   with backoff," not "fail after N attempts."

*Likely follow-up:* "How do you avoid hammering a peer with reconnect
attempts if it's flapping (connecting and immediately disconnecting)?" —
Exponential backoff per peer, tracked in RAM, same shape as the give-up-on-
a-peer case above; cap the backoff ceiling so a peer isn't permanently
blacklisted from one bad session.

---

## 6. Rate limiting & bandwidth management

Two directions: upload (protect the uplink, be fair among peers wanting
data from us) and download (less commonly critical, sometimes wanted to cap
local resource usage).

**Mechanism: token bucket.** A global bucket refilled at a configured
bytes/sec rate; every outgoing `PIECE` send acquires tokens (or waits)
first. Distinct from backpressure (§3): backpressure *reacts* to an
already-slow consumer; rate limiting *proactively* self-caps even when the
network could go faster.

**Fairness among peers wanting our upload bandwidth is exactly what
BitTorrent's real choke/unchoke algorithm solves**, and it's worth
implementing rather than inventing something new — it's a well-proven
answer to "allocate a scarce resource fairly with no central coordinator":
- Unchoke a small number of peers at a time (classically ~4), prioritized
  by *reciprocal* rate — peers giving us the most get upload slots back
  ("tit-for-tat").
- Plus one rotating **optimistic unchoke** slot, changed periodically
  regardless of reciprocity, so new/unproven peers get a chance to
  demonstrate they're worth reciprocating with — without this, a peer with
  nothing to offer yet could never bootstrap into anyone's good graces.

**Recommendation:** global aggregate byte-rate cap (protects the actual
uplink) + choke/unchoke for *allocation* within that cap, rather than hard
per-peer byte caps — real clients generally don't rate-limit individual
peers directly; slot allocation via choking is the per-peer fairness
mechanism instead.

*Likely follow-up:* "Why not just give every interested peer an equal
share of upload bandwidth?" — Equal-share-among-many is worse throughput
for everyone than concentrating upload on a few peers at a time (each
connection has overhead, and very thin slices are inefficient); tit-for-tat
also actively *deters* free-riding (peers who never upload get choked out),
which pure equal-sharing doesn't.

---

## 7. Scheduling (rarest-first, and its real-world refinements)

**Core algorithm:** for each needed piece, count how many currently-
connected peers advertise having it (`BITFIELD`/`HAVE` messages); prefer
requesting the lowest-count pieces. Needs a live, incrementally-maintained
availability count per piece (updated as peers connect/disconnect/send
`HAVE`), not a full recount on every pick — a small histogram/bucket
structure keyed by count, not a linear scan, for any non-trivial piece
count.

**Three standard refinements, since naive rarest-first has known rough
edges:**

1. **Randomize within the rarest tier.** Deterministically always picking
   *the* single rarest piece means many clients in the same swarm converge
   on requesting the exact same piece simultaneously. Pick randomly among
   ties instead.
2. **Random-first-piece exception.** At the very start (nothing downloaded
   yet), rarest-first can spend a long time chasing a genuinely rare piece
   while having *nothing* to offer other peers in return — bad for
   bootstrapping reciprocity (§6's tit-for-tat needs something to trade).
   Pick the first piece(s) randomly, then switch to rarest-first once
   there's something to offer.
3. **Endgame mode.** Near the end, when only a handful of pieces/blocks
   remain, request the *same* remaining blocks from multiple peers
   simultaneously — accepting wasted duplicate bandwidth to avoid stalling
   the whole download on one slow/unresponsive peer for the last few
   blocks. This is exactly where §1's write-to-final-offset policy and §9's
   idempotency (duplicate-block handling) pay off — endgame mode makes
   duplicate writes a normal, expected occurrence, not an edge case.

**Also worth naming as a tunable:** per-peer request pipelining depth — how
many outstanding block requests to keep in flight per peer (commonly ~5–10
in real clients) to keep throughput up despite per-request round-trip
latency.

*Likely follow-up:* "Why rarest-first instead of, say, sequential
(in-file-order)?" — Sequential is actually useful for a *different* goal
(streaming playback while downloading), but for maximizing overall swarm
health and download speed, rarest-first spreads replication of scarce
pieces across the swarm faster, reducing the risk that a piece disappears
entirely if its only holder(s) leave — a genuinely different optimization
target than raw personal download speed, worth being able to articulate
that tradeoff explicitly.

---

## 8. Data integrity

**Piece-level SHA1 verification is BitTorrent's core integrity mechanism**
— verify every piece against its hash (from immutable metadata) before
marking it "have" *or* serving it to other peers. Never relay unverified
data — both a correctness and a "don't help poison the swarm" concern.

**On a hash mismatch:** discard the piece's data; re-request its blocks,
preferably from *different* peers than whoever supplied the mismatching
data. Track a per-peer "bad piece" count; repeated offenses → disconnect
and apply the same backoff/blacklist treatment as a peer that times out
repeatedly (§5) — "gives bad data" and "never responds" both mean "not a
good source," deserving the same downstream handling. Attribution
limitation worth stating plainly: since multiple peers can contribute
blocks to one piece (definitely true in endgame mode), a hash failure
doesn't always identify *which* peer's block was bad — BEP3-style hashing
is only piece-granular. Practical mitigation: mildly penalize all
contributors to a failed piece rather than pretending exact attribution is
possible.

**Wire-message parsing needs the same hardening already done in the DHT
layer** — bounds-check length-prefixed fields before trusting them (reject
an absurd claimed message length before allocating a buffer for it — a
classic parser-level DoS vector), matching the `MalformedMessageError`/
`_require_bytes` hardening in `kademlia/rpc/messages.py`. Reusable
*pattern*, not code (wire format differs per §0.1), but worth citing as
direct precedent.

**Scope boundary worth stating rather than deciding by omission:** once a
piece is verified and written, it is not continuously re-verified against
external corruption (disk errors, a user editing the file) — too expensive
to do continuously, and real clients don't either. An optional on-demand
"full re-verify" (like `fsck`) is a reasonable offering; continuous
protection is not.

*Likely follow-up:* "How would a malicious peer try to waste your
resources without ever sending an outright-wrong piece hash?" — Slow-loris
style: accept connections/requests and never respond (mitigated by §5's
timeouts and idle-eviction); advertise a bitfield claiming to have pieces it
doesn't and never deliver on requests (mitigated by per-peer failure
counting and eviction, same as a bad-data peer); or send technically-valid
but maximally-unhelpful traffic to consume connection slots (mitigated by
§4's connection limits and idle-peer eviction).

---

## 9. Idempotency

**What's free vs. what needs explicit guarding:**

- **Pure state-setting operations are naturally idempotent** — setting an
  already-set "have piece" bit, or processing a duplicate `HAVE` for a
  piece already known, are harmless no-ops by construction.
- **Operations with side effects beyond simple state assignment need
  explicit dedup guards.** Receiving the same block twice (endgame mode, or
  a retried request whose original response merely arrived late) writes
  harmlessly to the same offset (§1 makes this true by construction) — but
  any accompanying "bytes received" counter or "check if piece now
  complete" trigger must check "did we already have this exact block"
  *before* acting, or a duplicate legitimate delivery double-counts stats
  (corrupting choke/unchoke's reciprocity math, §6) or double-triggers
  completion logic.
- **The DHT layer already provides two clean, reusable examples of both
  categories**, worth citing rather than re-deriving: `STORE` is naturally
  idempotent (storing the same key/value twice is a no-op difference —
  `DataStore.put` just overwrites with identical content), and the
  republish/TTL machinery is *already* built to be called repeatedly and
  safely. Concretely: `ANNOUNCE_PEER` (§0.3) reusing a `store()`-like
  pattern gets idempotency "for free" the same way, as long as the
  per-node announce table treats a re-announce from the same peer identity
  as a set-add (already true, already a no-op on repeat) rather than
  something that needs separate dedup logic.
- **General rule of thumb:** idempotency is free for pure state-assignment;
  it must be explicitly designed for wherever a duplicate would trigger a
  *side effect* (a counter, a downstream action, a state transition) rather
  than just re-asserting something already true.

*Likely follow-up:* "Give a concrete example of a non-idempotent operation
in this system that *needs* a guard, and show what breaks without one." —
Piece-completion triggering: if "received the last block of piece 7" fires
"verify and mark piece 7 complete" *without* checking whether piece 7 was
already marked complete, a duplicate final block (endgame mode) would
re-trigger a redundant hash verification and could double-announce
`HAVE 7` to every peer — wasted CPU and wasted messages, not corruption,
but a real, avoidable inefficiency stemming from a missing idempotency
check.

---

## 10. Synthesis: crash-and-restart, walked through end to end

Pulling §1, §8, and §9 together into one concrete scenario, since it's the
best test of whether the design actually holds together:

1. Process is killed mid-download.
2. On restart: metadata is re-read (§1 — cheap, always re-parsed, never
   trusted as possibly-stale). For each piece fully present on disk by
   byte-range, re-hash and verify (§1 option A); anything that doesn't
   verify — including a piece only partially written when the process died
   — is simply treated as not-yet-had.
3. Any block that had arrived but whose piece was never hash-verified
   before the crash is **not** specially remembered or fast-pathed — it's
   already durably on disk (§1's write-to-final-offset-immediately policy),
   so the piece either re-verifies as complete on restart, or the *whole*
   piece (not just the missing blocks) is simply re-requested. No separate
   persisted "partial piece" bookkeeping — the file itself, verified by
   hash, *is* the bookkeeping.
4. Peer connections and all in-RAM scheduling/rate-limit/choke state are
   gone and don't need to survive — rebuilt from a fresh DHT lookup for the
   torrent's `info_hash` (§0.3), reusing the same `find_node`/announce
   machinery.
5. Nothing about this restart path needs BT-layer-specific persistence code
   beyond "the data file(s) on disk" and "the immutable metadata" — the
   direct payoff of §1's guiding principle (persist only ground truth,
   rebuild everything else).

---

## Open questions to settle before coding starts

1. §0.1 — Spirit-compliant-not-byte-compliant: agreed, or is real-client
   interop actually wanted?
2. §0.2 — Design per-torrent objects now, build/test single-torrent first:
   agreed?
3. §0.3 — `ANNOUNCE_PEER`/`GET_PEERS` as new RPCs with a separate per-node
   announce table (full replication via fan-out, short lease-style TTL,
   peer-driven re-announce): agreed as the mechanism?
4. §1 — Defer bitfield persistence (option B), re-hash on startup (option
   A) for v1: agreed, or is instant-resume a hard requirement now?
5. Anything else you want added as its own topic before this is considered
   settled?
