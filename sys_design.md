# BitTorrent Layer — System Design Discussion

## Purpose

The Kademlia DHT layer is done and tested (`ARCHITECTURE.md`, `test_dht.md`).
Per `claude.md`, the next step is the BitTorrent layer on top of it. This
document is where we think through the system-design questions before
writing any code — persistence, concurrency, backpressure, resource limits,
timeouts/retries, rate limiting, scheduling, data integrity, and idempotency,
plus a few foundational questions that came up while thinking it through.

This is a discussion document, not a finished spec. Each section gives a
recommendation and the reasoning behind it, but the open questions at the end
are genuinely open — nothing here is final until we've talked through it.
Code comes only after we converge.

---

## 0. Foundational open questions (answer these first — they shape everything below)

Before the 9 topics, three decisions are foundational enough that getting
them wrong means reworking most of the rest of this document. These are
proposed defaults with reasoning, not settled — flag disagreement early.

### 0.1 Protocol fidelity: real BEP-compliant BitTorrent, or our own simplified protocol inspired by it?

- **Strict BEP compliance** (BEP3 peer wire protocol, BEP9 metadata exchange,
  BEP5-style `announce_peer`/`get_peers` semantics) would let this client
  interoperate with real BitTorrent clients (Transmission, qBittorrent,
  libtorrent-based ones) on the same swarm.
- **Our own simplified protocol**, inspired by BitTorrent's design but not
  byte-compatible, is less work and easier to reason about/debug end-to-end
  since we control both sides.
- **Recommendation:** stay *spirit-compliant, not byte-compliant* — reuse
  BitTorrent's core mechanisms (piece/block splitting, rarest-first,
  choke/unchoke, SHA1 piece hashes) because they're well-proven answers to
  exactly the questions below, and keep using bencode for anything
  metadata-shaped (which is exactly why bencode was picked for the DHT layer
  — see `ARCHITECTURE.md`'s wire-protocol section). But don't aim for literal
  interop with real BitTorrent clients — that adds a lot of spec-compliance
  work (extension protocol negotiation, exact reserved-byte handling, etc.)
  that doesn't serve this project's goal of *understanding* the system. The
  peer wire protocol itself (piece/block transfer) should still use a
  length-prefixed binary framing like real BEP3, *not* bencode — bencode is
  fine for metadata-ish messages but is the wrong tool for a hot-path binary
  data-transfer protocol (encoding overhead, no fixed-size framing).

### 0.2 Single torrent per process, or multi-torrent?

`claude.md` doesn't say. This affects nearly every section below: resource
limits become per-torrent *and* global, scheduling needs a torrent-level
layer above the piece-level layer, persistence needs a torrent_id namespace,
etc.

- **Recommendation:** design the core piece-manager/peer-connection
  abstractions to be *per-torrent* objects from day one (so multi-torrent is
  just "run more than one instance of that object"), but build and test with
  **one torrent per process first**. Trying to get multi-torrent resource
  arbitration right *before* the single-torrent mechanics work would be
  premature — but naming this now avoids baking in single-torrent
  assumptions (like global-not-namespaced state) that would be painful to
  unwind later.

### 0.3 Where does peer discovery actually plug into the DHT layer?

This is worth being explicit about since it's the literal connection point
between the two layers: a torrent's `info_hash` (20 bytes — same shape as a
Kademlia key) becomes the DHT key. To announce "I have this torrent," we
`store(info_hash, our_contact_info)`. To find peers, we `find_value(info_hash)`
and treat the returned value(s) as a peer list. **This reuses the existing
Kademlia `Node.store`/`find_value` API as-is** (`kademlia/node.py`) — no
changes needed to the DHT layer. One real design wrinkle: Kademlia's
`STORE` as built stores a single opaque value per key, overwriting on
restore (`DataStore.put` replaces the entry). A torrent swarm needs *many*
peers to be discoverable under the same `info_hash`, not one. This needs
either (a) the value stored at each key being a *list* of peer contacts that
peers append themselves to, or (b) reusing the DHT's existing
K-closest-nodes replication (up to K=20 different nodes each store *one*
peer's announcement under the same key) and have `find_value`-callers treat
"ask several of the K closest nodes and union their answers" as the peer-list
lookup — closer to how real BEP5 `get_peers` actually works. **(b)** is
closer to the existing DHT semantics and avoids turning `DataStore` into
something that needs read-modify-write list semantics (which would also
reopen concurrency questions in the DHT layer itself). **Recommendation:
(b)**, implemented entirely in the BitTorrent layer as "call `find_node`
near `info_hash`, then query several of them for their local announce
records" rather than relying on the single-value `find_value` at all — this
needs a small new BT-layer-only RPC/store convention (still bencoded, still
using the same transport patterns), not a change to `kademlia/`.

---

## 1. Persistent state vs RAM

**Guiding principle:** persist only what represents *ground-truth work
product* or is *prohibitively expensive to recompute*; everything else
(session/network/scheduling state) lives in RAM and is treated as disposable
— rebuilt from the network or from persisted ground truth after a restart.
This is the same principle the DHT layer already follows implicitly (routing
table and in-flight RPC state are pure RAM; nothing there needs to survive a
restart because it's cheaply rediscoverable).

| State | Persist? | Why / where |
|---|---|---|
| Torrent metadata (info_hash, piece hashes, piece length, file list) | Yes | Immutable, needed to resume; either keep the original `.torrent`-equivalent file and re-parse, or cache a small parsed manifest — re-parsing is cheap, so don't bother with a derived copy |
| Downloaded piece **bytes** | Yes | This is the actual deliverable — written to the destination file(s) at piece/block granularity as data arrives |
| "Which pieces do we have" bitfield | **Recommend: derive from disk on startup, don't persist separately** (see below) | — |
| In-flight block-level request state (which blocks of an incomplete piece have arrived) | No — RAM only | Ephemeral scheduling state; treat any not-fully-verified piece as entirely absent after a crash |
| Known peers for this swarm | No (or soft-persist as a hint only) | Cheaply rediscoverable via the DHT; persisting is a minor optimization, not a correctness need |
| Rate limiter / bandwidth accounting | No — RAM only | Resets on restart, that's fine |
| Choke/unchoke state, per-peer stats | No — RAM only | Rebuilt as connections re-form |

**The interesting tradeoff is the bitfield.** Two real options:

- **(A) Re-derive on startup**: on process start, for each piece already
  fully present on disk (by file size/offset), re-hash it and mark "have"
  only if it matches. Simple, self-healing (silently fixes any
  disk-vs-bitfield divergence), zero extra persisted state, zero risk of a
  stale/corrupt resume file lying to you. Cost: an O(file size) SHA1 pass at
  every startup — for a multi-GB file this is real wall-clock time (though
  SHA1 throughput is roughly GB/s on modern hardware, so a few GB is a few
  seconds, not minutes).
- **(B) Persist the bitfield** alongside the data (a `.resume` file, the way
  Transmission/libtorrent do it): instant resume, no re-hash cost. Cost: a
  new failure mode — if the bitfield write and the data write aren't
  ordered correctly (must durably write piece data *before* marking it
  "have" in the persisted bitfield, or a crash between the two leaves the
  bitfield lying about a piece that isn't actually complete/verified on
  disk), you can silently serve corrupt data to other peers, or think you're
  done when you're not.

**Recommendation: (A) for a first implementation.** This project's whole
point is understanding the system correctly, and (A) has no
write-ordering/fsync subtlety to get wrong — it's correct by construction.
(B) is a legitimate, valuable optimization to layer in later once (A) works,
specifically *because* at that point we'll already have a hash-verification
code path to double-check against, so (B) becomes "an optimization with a
fallback," not "the only source of truth." Open question: agree with
deferring (B), or is instant-resume-on-restart a hard requirement now?

**Write-back policy (when to actually write bytes to disk):** write each
*block* directly to its final on-disk offset as it arrives (not buffered in
RAM until the whole piece completes) — piece sizes (256 KB–4 MB typically)
are large enough that buffering doesn't meaningfully reduce I/O syscalls, and
writing at the final offset immediately means duplicate blocks (endgame
mode, see §7) are naturally idempotent (same bytes, same offset, second
write is a harmless no-op). Only the "have piece" bit flips — and only after
hash verification — once all of a piece's blocks are in.

---

## 2. Concurrency & race conditions

Same execution model as the DHT layer: single-threaded `asyncio`, one event
loop, cooperative concurrency. Classic multi-threaded data races (two
threads mutating the same memory simultaneously) don't apply — but a
different, very real race class does, and it's worth naming precisely:

**Check-then-act races across `await` points.** Even single-threaded, any
sequence of "read shared state → `await` something → mutate shared state
based on what you read" can race, because the `await` yields control and a
*different* coroutine can run and mutate that same state in between. Concrete
example: two peer-connection coroutines both run the piece-picker, both see
"piece 42 is still needed," both `await` sending a REQUEST for it to their
respective peer, and now piece 42 has been double-requested — wasted
bandwidth, not corruption, but still a bug.

**The fix, and it's the exact pattern the DHT layer already uses correctly**
(`lookup.py`'s `queried.update(...)` happens synchronously, before the
`await asyncio.gather(...)` that follows): *the "mark as claimed" mutation
must happen in the same synchronous block as the "check if available" read,
with no `await` in between.* Ordinary (non-`async`) function calls never
yield control in Python, so as long as picking-a-piece-and-marking-it-
requested is implemented as plain synchronous code, it's automatically race-
free — no lock needed. A lock (`asyncio.Lock`) is only needed if that
check-and-mark logic itself has to `await` something (it shouldn't, for the
piece picker).

Other concurrency points specific to this layer:

- **Multiple peers writing to the same file, different offsets** — safe by
  construction (disjoint byte ranges, positional writes), *except* in
  endgame mode where the same block might be in flight from two peers at
  once — that's a duplicate-write, not a race (see §1's write-back policy
  and §9 idempotency), not something that needs locking.
- **CPU-bound piece hashing blocks the event loop** if done inline — this is
  the same "don't block the loop" problem as blocking file I/O, just
  CPU-bound instead of I/O-bound. SHA1 over even a 4 MB piece is low
  single-digit milliseconds on modern hardware, so doing it inline (blocking
  the loop briefly) is probably fine at *piece* granularity. It would *not*
  be fine for a full-file re-verify pass at startup (§1's option A) over
  many GB — that should be chunked with periodic `await asyncio.sleep(0)`
  yields, or offloaded to `loop.run_in_executor`, so it doesn't stall every
  peer connection for seconds at a time.
- **Blocking file I/O must not run directly on the event loop** — Python's
  plain `open()`/`write()` are blocking syscalls; calling them directly from
  a coroutine stalls the *entire* process (every peer connection) for the
  syscall's duration. Needs either `loop.run_in_executor` (a thread pool) or
  a library like `aiofiles`. Worth deciding explicitly rather than
  discovering it as a performance bug later.

---

## 3. Backpressure

Three distinct backpressure surfaces in this system:

1. **Network → disk.** If incoming data arrives faster than it can be
   written (slow disk vs. fast LAN peers), an unbounded in-memory queue of
   "blocks waiting to be written" risks unbounded memory growth.
   **Recommendation:** a bounded `asyncio.Queue(maxsize=N)` between
   "received a block" and "write it to disk" — `await queue.put(...)`
   naturally suspends the producer once the queue is full, which is exactly
   backpressure, for free, using asyncio's own primitive rather than custom
   flow-control code.
2. **Us → peer (uploading).** When sending piece data to a peer, use
   `await writer.drain()` after `writer.write(...)` — this is asyncio's
   built-in write-buffer backpressure (it suspends until the transport's
   buffered bytes drop below a low-water mark), directly applicable here
   with no custom code needed.
3. **New-connection backpressure.** A DHT lookup can surface many candidate
   peers at once; connecting to all of them simultaneously spikes FD usage
   and handshake CPU. Bound concurrent *in-flight connection attempts* with
   a semaphore (ties into §4's resource limits).

Worth calling out explicitly: **choking, in real BitTorrent, is fundamentally
a backpressure/fairness mechanism** — "I'm choking you" means "stop asking me
for data, I won't answer" — this is the same idea as (2) applied at the
peer-relationship level rather than the socket-buffer level. Naming this
connects §3 (backpressure), §6 (rate limiting), and §7 (scheduling) — they're
not three unrelated topics, choke/unchoke is the mechanism that implements
all three at once. More in §6.

---

## 4. Resource limits & connection lifecycle management

**Limits worth setting as named constants** (mirroring `kademlia/constants.py`'s
style — a `bt_constants.py` or similar):
- Max concurrent peer connections, both global and per-torrent (once
  multi-torrent exists — see §0.2).
- Max concurrent in-flight *outbound connection attempts* (separate from
  total connections — connection setup has its own CPU/FD-churn cost).
- Max outstanding block requests per peer (pipelining depth — also a
  scheduling/throughput parameter, see §7).
- OS file-descriptor ceiling awareness: each peer connection is one socket
  FD; default Linux ulimits (often 1024) cap how many can realistically be
  held open. Catch `OSError`/`EMFILE` on connection attempts and back off
  rather than crash or spin-retry.

**Connection lifecycle as an explicit state machine**, per peer:
`CONNECTING → HANDSHAKING → ACTIVE → CLOSING → CLOSED`, with `ACTIVE`
sub-tracking interested/choked state in both directions. Validating incoming
messages against the *current* state (e.g., rejecting a `PIECE` message
before a handshake completed) avoids a whole class of "what if a message
arrives out of order" bugs, and doubles as basic protection against
buggy/hostile peers (see also §8).

**Timeouts tied to lifecycle stages** (§5 goes deeper): a peer stuck in
`CONNECTING`/`HANDSHAKING` past its timeout is dropped — half-open
connections that never get cleaned up are a slow resource leak.

**Idle-peer eviction:** a connected-but-useless peer (not interested, has
nothing we need) shouldn't occupy a connection slot forever if better
candidates exist. Recommendation: periodically re-evaluate the connected
peer set and evict the least-useful fraction to make room for newly
DHT-discovered candidates, rather than treating the connection set as
first-come-first-served-permanent.

**Cleanup on disconnect is correctness-critical, not just hygiene:** when a
peer connection closes, any block requests still in flight *to that peer*
must be released back into the "needed" pool immediately, or that block (and
therefore its piece) stalls forever waiting for a peer that's gone. This
should be one centralized "on peer disconnected" path, not scattered
ad hoc cleanup at each call site that happens to talk to a peer.

---

## 5. Timeouts: when to retry, when to give up

The DHT layer already has a working, reusable *pattern* here — worth
explicitly carrying forward rather than reinventing: retry logic lives one
layer above the raw transport (`Node._send_with_retry` wraps
`protocol.send_query`, not the other way around), with a bounded retry count
and a timeout. **Recommendation: same layering for the BT peer-wire
protocol** — a `PeerConnection.request_block()` wrapper owns timeout/retry,
not the raw socket-read code.

**But the retry *philosophy* is genuinely different here, and this is worth
being deliberate about:**

- **DHT RPCs** retry the *same* target (the contact we already dialed) —
  correct, because a Kademlia RPC is a point-query to a specific node; there's
  no "other place" to ask the same question.
- **BT block requests should *not* retry the same peer.** If a peer hasn't
  answered a block request in time, they're probably just slow/busy right
  now — the useful response is to reassign that block to a *different*
  already-connected peer (optionally sending a `CANCEL` first), not to nag
  the same slow peer again. This is a meaningfully different retry shape
  from the DHT layer's, and naively copying the DHT's "same-target retry"
  pattern here would be a real bug (or at least a real inefficiency).

**Timeout scales should be derived from what's actually being waited for,
not one global constant** (unlike the DHT layer's single `RPC_TIMEOUT`,
which is reasonable there because all 4 RPCs are similarly-shaped
point-queries):
- TCP connect: a few seconds.
- Handshake completion: a few seconds.
- Individual block request: should reflect expected transfer time for a
  16 KB-ish block even on a slow link, with slack for the peer being
  legitimately busy elsewhere — order 10–30s, not the DHT's 5s.
- "Peer connected but produced nothing useful" idle timeout: minutes, not
  seconds.

**"Give up" has three different scopes, worth distinguishing explicitly:**
1. Give up on *one block request* → reassign to another peer (above).
2. Give up on *a peer* → after N consecutive timeouts/protocol violations,
   disconnect, and apply backoff before reconnecting to them (echoes the
   DHT routing table's existing "ping-before-evict, don't immediately trust
   a previously-bad contact again" instinct — same idea, different layer).
3. Give up on *a piece having no source at all* → this should essentially
   **never** happen by giving up permanently. If no currently-connected peer
   has a piece, keep periodically re-querying the DHT for more peers for
   that `info_hash` rather than failing the download. This is a real
   philosophical difference from the DHT layer: Kademlia lookups are
   *bounded* operations (`MAX_LOOKUP_ROUNDS`, then return whatever was
   found) because they're single point-in-time queries; a torrent download
   is long-running by nature and should default to "keep trying with
   backoff," not "fail after N attempts."

---

## 6. Rate limiting & bandwidth management

Two directions: **upload** (protect the user's uplink, and be fair among
peers wanting data from us) and **download** (less commonly critical, but
sometimes wanted to cap local resource/network usage).

**Mechanism: token bucket.** A global bucket refilled at a configured
bytes/sec rate; every outgoing `PIECE` send must acquire enough tokens (or
wait) first. Related to, but distinct from, backpressure (§3): backpressure
*reacts* to an already-slow consumer; rate limiting *proactively* self-caps
even when the network could go faster.

**Fairness among peers wanting our upload bandwidth is exactly what real
BitTorrent's choke/unchoke algorithm solves**, and it's worth implementing
the real one rather than inventing something new, because it's a genuinely
well-proven answer to "how do you allocate a scarce upload slot fairly
without a central coordinator":
- Only unchoke a small number of peers at a time (classically ~4) —
  prioritized by *reciprocal* rate (peers giving us the most, get upload
  slots back — "tit-for-tat").
- Plus one rotating **optimistic unchoke** slot, changed periodically
  regardless of reciprocity, specifically so new/unproven peers get a chance
  to demonstrate they're worth reciprocating with — without this, a new
  peer with nothing to offer yet could never bootstrap into the swarm's good
  graces.

**Recommendation:** global aggregate byte-rate cap (protects the actual
uplink) + choke/unchoke for *allocation* within that cap, rather than hard
per-peer byte caps — real clients generally don't rate-limit individual
peers directly; they use slot allocation (chosen via choke/unchoke) as the
per-peer fairness mechanism instead.

---

## 7. Scheduling (rarest-first, and its real-world refinements)

**Core algorithm, as specified:** for each piece we still need, count how
many currently-connected peers advertise having it (via their `BITFIELD`/
`HAVE` messages); prefer requesting pieces with the *lowest* count. This
needs a live, incrementally-maintained availability count per piece index
(updated as peers connect/disconnect/send `HAVE`), not a full O(pieces ×
peers) recount on every pick — for any non-trivial piece count this should
be a small histogram/bucket structure keyed by availability count, not a
linear scan.

**Three standard refinements worth including, since "rarest-first" alone has
known rough edges:**

1. **Randomize within the rarest tier.** Always picking *the* single rarest
   piece deterministically means many clients in the same swarm converge on
   requesting the exact same piece simultaneously (thundering herd on
   whoever has it). Pick randomly among the pieces tied for rarest instead.
2. **Random-first-piece exception.** At the very start of a download (we
   have nothing yet), rarest-first can spend a long time chasing a
   genuinely rare piece while we have *nothing* to offer other peers in
   return — bad for bootstrapping reciprocity (§6's tit-for-tat needs
   something to reciprocate *with*). Real clients pick the first piece(s)
   randomly, then switch to rarest-first once there's something to trade.
3. **Endgame mode.** Near the end of a download, when only a handful of
   pieces/blocks remain, request the *same* remaining blocks from multiple
   peers simultaneously — accepting some wasted duplicate bandwidth in
   exchange for not stalling the whole download on one slow/unresponsive
   peer for the last few blocks. This is where §1's "write directly to
   final offset" and §9's idempotency (duplicate-block handling) both pay
   off — endgame mode is *exactly* the scenario that makes duplicate writes
   a normal, expected occurrence rather than an edge case.

**Also worth naming as an explicit tunable:** per-peer request pipelining
depth — how many outstanding block requests to keep in flight per peer
(commonly ~5–10 in real clients) to keep the connection's throughput up
despite per-request round-trip latency. This is a genuine throughput knob,
not just a resource limit (though it's also that — see §4).

---

## 8. Data integrity

**Piece-level SHA1 verification is BitTorrent's core integrity mechanism**
— verify every piece against its hash (from the immutable metadata) before
marking it "have" *or* making it available to other peers. Never relay
unverified data — both a correctness concern and a "don't help poison the
swarm" concern.

**On a hash mismatch:** don't just silently drop and retry blindly.
- Discard the piece's data; re-request its blocks, preferably from
  *different* peers than whoever supplied the mismatching data, to isolate
  a possible bad actor.
- Track a per-peer "bad piece" count; repeated offenses → disconnect and
  apply the same backoff/blacklist treatment as a peer that times out
  repeatedly (§5) — from the system's perspective, "gives us bad data" and
  "never responds" are both "this peer isn't a good source," deserving the
  same downstream handling.
- Attribution limitation worth stating plainly: since multiple peers can in
  principle contribute blocks to the same piece (definitely true in endgame
  mode), a hash failure doesn't always tell you *which* peer's block was
  bad. BEP3 only gives piece-level granularity. Practical mitigation: mildly
  penalize/deprioritize *all* contributors to a failed piece rather than
  pretending you can pinpoint the culprit — an accepted, named limitation,
  not something to over-engineer around.

**Wire-message parsing needs the same hardening already done for the DHT
layer** — bounds-check length-prefixed fields *before* trusting them (e.g.,
reject an absurd claimed message length before allocating a buffer for it —
a classic parser-level DoS vector), matching the `MalformedMessageError`/
`_require_bytes` hardening already in `kademlia/rpc/messages.py`. Directly
reusable *pattern* (not code, since the wire format differs per §0.1), worth
citing explicitly as precedent.

**Scope boundary worth stating rather than silently deciding by omission:**
once a piece is verified and written, we do *not* continuously re-verify it
against external corruption (disk errors, a user editing the file). Real
clients don't either — it's prohibitively expensive to do continuously.
Offer an optional on-demand "full re-verify" (like `fsck`) rather than
building continuous protection.

---

## 9. Idempotency

**Where it matters, and what's true "for free" vs. what needs explicit
guarding:**

- **Pure state-setting operations are naturally idempotent** — setting an
  already-set "have piece" bit, or processing a duplicate `HAVE` message for
  a piece a peer already told us about, are harmless no-ops by construction.
  No special-casing needed.
- **Operations with side effects beyond simple state assignment need
  explicit dedup guards.** Receiving the same block twice (endgame mode, or
  a retried request whose original response merely arrived late rather than
  being lost) writes harmlessly to the same file offset (§1's write-back
  policy makes this true by construction) — *but* any accompanying
  "bytes-received" counter, or "check if piece is now complete" trigger,
  must check "did we already have this exact block" *before* acting, or a
  duplicate legitimate delivery double-counts stats (which would corrupt
  choke/unchoke's reciprocity calculations, §6) or double-triggers
  piece-completion logic.
- **The DHT layer already gives us a clean, reusable example of both
  categories**, worth citing directly rather than re-deriving: `STORE` is
  naturally idempotent (storing the same key/value twice is a no-op
  difference — `kademlia/storage.py`'s `DataStore.put` just overwrites with
  the same content), and the republish/TTL machinery
  (`Node._expiry_and_republish_loop`) is *already* built to be called
  repeatedly and safely. **Concretely: "announce ourselves as a peer for
  this `info_hash`" (§0.3) can piggyback directly on the existing `store()`
  API and its existing republish cadence** — no new idempotency mechanism
  needed there at all, just reuse what's already built and already tested.
- **General rule of thumb to close on:** idempotency is free for pure
  state-assignment; it must be explicitly designed for anywhere a duplicate
  triggers a *side effect* (a counter, a downstream action, a state
  transition) rather than just re-asserting a fact that was already true.

---

## 10. Synthesis: crash-and-restart, walked through end to end

Pulling §1, §8, and §9 together into one concrete scenario, since it's the
best test of whether the design actually holds together:

1. Process is killed mid-download (`kill -9`, power loss, whatever).
2. On restart: metadata is re-read (§1 — cheap, always re-parsed, never
   trusted as "maybe stale"). For each piece that's fully present on disk by
   byte-range, re-hash and verify (§1 option A) — anything that doesn't
   verify (including a piece that was only *partially* written when the
   process died) is simply treated as not-yet-had.
3. Any block that had arrived but whose piece never got hash-verified
   before the crash is **not** specially remembered or fast-pathed — it's
   already durably on disk (§1's write-to-final-offset-immediately policy),
   so if the piece re-verifies as complete on restart great, and if not, the
   *whole* piece (not just the missing blocks) is simply re-requested. No
   separate persisted "partial piece" bookkeeping needed — the file itself
   *is* the bookkeeping, verified by hash.
4. Peer connections and all in-RAM scheduling/rate-limit/choke state (§1's
   RAM column) are gone and don't need to be — they're rebuilt from a fresh
   DHT lookup for the torrent's `info_hash` (§0.3), reusing the exact same
   `store`/`find_node` machinery that's already there.
5. Nothing about this restart path requires any BT-layer-specific
   persistence code beyond "the data file(s) on disk" and "the immutable
   metadata" — which is the direct payoff of the §1 guiding principle
   (persist only ground truth, rebuild everything else).

---

## Open questions, gathered in one place for easy reply

1. §0.1 — Agree with "spirit-compliant, not byte-compliant" (reuse
   BitTorrent's mechanisms, skip literal interop with real clients)?
2. §0.2 — Agree with designing per-torrent objects now but building/testing
   single-torrent first?
3. §0.3 — Agree with implementing swarm peer discovery as "query several of
   the K closest DHT nodes and union their announce records" (BEP5-`get_peers`-
   flavored) rather than trying to force it through the existing single-value
   `find_value`?
4. §1 — Agree with deferring bitfield persistence (option B) and just
   re-hashing on startup (option A) for the first version?
5. Anything from your own experience/reading that you want added as a 10th+
   topic before we lock this in?
