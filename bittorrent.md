# BitTorrent Layer — Design

The upper layer of this project: file transfer between peers, using the
Kademlia DHT underneath purely for finding who else has the file. No tracker
anywhere. Read `ARCHITECTURE.md` first for the DHT layer, and `sys_design.md`
for the reasoning behind the tradeoffs this document implements.

## Decisions taken before writing any code

| Question | Choice | Why |
|---|---|---|
| Protocol fidelity | Spirit-compliant, not byte-compliant | Reuses BitTorrent's proven mechanisms (piece/block split, rarest-first, choke/unchoke, SHA1 piece hashes, BEP3-shaped wire framing) without chasing literal interop with real clients, which is compliance busywork that teaches nothing about the system |
| Metadata | Real BEP3 `.torrent` files | We already had a bencode codec, so parsing real ones costs almost nothing extra, and `info_hash` is then genuinely `SHA1(bencoded info dict)` rather than something invented |
| Torrent scope | Per-torrent objects, single torrent per process for now | A `TorrentSession` owns everything torrent-specific, so multi-torrent later is "run more than one" rather than a refactor |
| New DHT RPCs | Pluggable registry in `kademlia/` | Keeps the DHT a generic arbitrary-key/value store per `claude.md`, with no BitTorrent concepts leaking into it |
| Upload allocation | Tit-for-tat + optimistic unchoke | Rank peers by what they're feeding *us*; one rotating slot so newcomers can break in |

## Module map

```
bittorrent/
  constants.py     tunables (block size, pipelining depth, timeouts, unchoke params)
  metadata.py      .torrent parsing; info_hash; piece hashes and sizes
  storage.py       PieceStore: block-level disk I/O, hash verification, bitfield
  picker.py        PiecePicker: rarest-first scheduling and block claiming
  choking.py       ChokeManager: tit-for-tat + optimistic unchoke
  discovery.py     ANNOUNCE_PEER / GET_PEERS over the DHT; the per-node peer table
  session.py       TorrentSession: owns the above, drives everything
  wire/
    messages.py    peer wire protocol: handshake + length-prefixed binary messages
    connection.py  PeerConnection: one TCP peer, its state machine and read loop
```

Roughly 1,700 lines. Entry point for reading: `session.py`, then follow the
callbacks into `connection.py` and `picker.py`.

## How the two layers connect

A torrent's `info_hash` is 20 bytes, exactly like a `NodeID`, so "who is in
this swarm" becomes "which nodes are closest to this key" — answered by the
DHT's existing iterative lookup, unchanged.

**What the DHT's own `STORE` could not do** is hold a peer list.
`DataStore.put` replaces a single value per key, so the second peer to
announce would erase the first. Swarm membership needs *additive* semantics,
so `discovery.py` registers two extension RPCs with their own per-node table:

```
ANNOUNCE_PEER(info_hash, port) -> add me to the set for this info_hash
GET_PEERS(info_hash)           -> who is in that set?
```

```python
peer_announcements: Dict[info_hash, Set[(ip, port)]]   # adds, never overwrites
```

**Replication, not partitioning.** Every announcing peer sends
`ANNOUNCE_PEER` to *all* of the K closest nodes it finds — not one each.
Since closeness to a fixed key doesn't depend on who is asking, every peer's
announcement converges on the same K nodes, so each of those nodes
independently accumulates the whole peer list. Querying several on lookup is
redundancy against a missed announcement, not reassembly of fragments.

**The IP is never self-reported.** `ANNOUNCE_PEER` carries only a port (the
peer's TCP listener differs from its DHT socket); the address is taken from
the datagram's own source. A peer can therefore announce itself, but not
someone else.

**Announcements are a liveness lease, not storage.** A node never republishes
someone else's announcement — it has no way to know that peer is still up.
Entries expire on a short TTL (30 min, as in real BEP5) and each peer renews
its own. A peer that leaves simply stops renewing and falls out. This is
deliberately *unlike* the DHT's own republish, where any holder re-propagates
inert data that needs nobody's ongoing consent.

### Extending the DHT without polluting it

`kademlia/rpc/messages.py` gained a small registry: `register_query` and
`register_response` let a layer above add RPCs, supplying only the
payload-specific encode/decode while the envelope framing stays owned by the
DHT. Built-ins are tried first, so registering an extension cannot change
existing behaviour, and extension responses carry an explicit `rt`
discriminator that the four built-in responses never emit. `Node` gained
`register_query_handler` and `send_extension_query` to match. Nothing in
`kademlia/` knows what a torrent is.

**Every node must install the discovery handlers**, including for torrents it
isn't downloading — any node can be among the K closest to some info_hash.
That "infrastructure role" is separate from being a participant in a swarm.
A node without the handlers answers `204 Method Unknown`, so callers fail
immediately rather than waiting out a timeout.

## Data path

### Metadata (`metadata.py`)

Parses a bencoded `.torrent`: `piece length`, `pieces` (concatenated 20-byte
SHA1s), `name`, `length`. `info_hash = SHA1(bencode(info))` — re-encoding is
safe because bencode is canonical (sorted keys), so a well-formed file
round-trips to identical bytes. `piece_size(i)` accounts for the short final
piece. Multi-file torrents are rejected outright rather than mis-read.

### Storage (`storage.py`)

Blocks are written **straight to their final offset** as they arrive, never
buffered until a piece completes. That makes duplicate deliveries (endgame
mode, late retries) harmless by construction — same bytes, same offset — and
means the file itself is the bookkeeping.

**No persisted bitfield.** On startup the file is re-hashed and only what
verifies counts as held. Slower to resume than a `.resume` file, but it has
no write-ordering hazard: a half-written piece fails its hash and is simply
treated as absent. That is the entire crash-recovery story.

All disk I/O hops to a thread via `run_in_executor` — a blocking `write()` on
the event loop would stall every peer connection for the syscall's duration.

### Rarest-first scheduling (`picker.py`)

The core algorithm, with the three refinements plain rarest-first needs:

1. **Rarest first.** Availability per piece is maintained incrementally from
   `BITFIELD`/`HAVE` messages and peer departures — never recounted per pick.
2. **Ties broken randomly**, so every client in a swarm doesn't converge on
   the same piece at the same instant.
3. **Random first pieces.** Until we hold `RANDOM_FIRST_PIECES`, selection is
   random rather than rarest-first: early on we have nothing to trade, and
   chasing a genuinely rare piece keeps it that way, starving tit-for-tat.
4. **Endgame mode.** Once `ENDGAME_THRESHOLD` blocks remain, the stragglers
   are requested from several peers at once so the download doesn't stall
   behind one slow peer. Never duplicated to the *same* peer.

`pick()` is deliberately a plain synchronous method. Its "find an unclaimed
block" read and "mark it claimed" write must happen with no `await` between
them — otherwise two peer coroutines both see the same block as free and both
request it (`sys_design.md` §2). Keeping it synchronous makes that race
impossible without a lock.

A departing peer's claims are released (`release_peer`) so its blocks become
pickable again; a single timed-out request is released individually
(`release_block`) and reassigned to a **different** peer — a timeout means
"busy", not "gone", so re-asking the same peer is the wrong move.

### Wire protocol (`wire/`)

Deliberately *not* bencode — this is the hot path carrying file data, so a
fixed-size binary header is cheaper and frames cleanly on a TCP stream.

```
handshake: <19><"BitTorrent protocol"><8 reserved><info_hash><peer_id>   (68 bytes)
message:   <4-byte big-endian length><1-byte id><payload>                (length 0 = keep-alive)
ids: 0 choke  1 unchoke  2 interested  3 not_interested
     4 have   5 bitfield 6 request     7 piece  8 cancel
```

Every length is bounds-checked *before* anything is allocated for it — a peer
claiming a 4GB message gets disconnected, not handed a 4GB buffer. Sends use
`await writer.drain()`, which is asyncio's built-in backpressure for a peer
that's slow to consume.

`PeerConnection` runs an explicit state machine (`CONNECTING → HANDSHAKING →
ACTIVE → CLOSED`) so out-of-order messages are rejected rather than
half-processed, and a handshake for a different `info_hash` is refused.

### Choking (`choking.py`)

Every `UNCHOKE_INTERVAL`, peers that want data from us are ranked by the
download rate *we* got from them since the last round, and the top
`UNCHOKE_SLOTS` (4) are unchoked. That's tit-for-tat, and it's what deters
free-riding.

Ranking alone deadlocks newcomers, though: a peer that has never sent us
anything ranks last forever, so it never gets unchoked, so it can never
obtain a piece to trade back. One **optimistic unchoke** slot, rotated every
`OPTIMISTIC_UNCHOKE_INTERVAL` to a random choked peer regardless of rank, is
what lets new peers in — including us when we join a swarm holding nothing.

## Session lifecycle

`TorrentSession.start()` opens the store (re-hashing existing data), binds a
TCP listener, and starts four background loops: announce, peer discovery,
choke rounds, and a request-timeout sweep.

Requesting is event-driven rather than polled — `_maybe_request` fires on
unchoke, on a new bitfield/have, and after each block arrives, topping the
peer's pipeline back up to `MAX_PIPELINED_REQUESTS`.

When a piece completes it's verified against its hash. On success it's marked
held and `HAVE` goes out to every peer. On failure the whole piece is
discarded and re-requested — piece hashes are piece-granular, so when several
peers contributed blocks there's no way to identify the culprit; the peer
that finished it takes the blame, and repeat offenders are dropped and backed
off. The same failure counter covers timeouts, since "gives bad data" and
"never answers" both mean "not a good source".

### Bugs this design had to fix during implementation

Worth recording, since two were real design flaws rather than typos:

- **Extension responses were dispatched as queries.** The transport told
  replies from queries with a hardcoded isinstance tuple of the four built-in
  response types, so registered ones fell through to the query path. Fixed
  with `messages.is_response()`, which consults the registry.
- **A node with no discovery handler answered with silence**, so every
  announce to it burned a full timeout × retries. Now it returns
  `204 Method Unknown`.
- **`get_peers` ignored the node's own table.** A lookup never returns the
  searcher itself, so in a small swarm the announcement made *directly to us*
  sat locally unread while we asked everyone else. This made a real
  two-process transfer fail even though in-process tests passed.
- **Announcing while alone in the DHT reached nobody**, and the next attempt
  was a full `ANNOUNCE_INTERVAL` away. The loop now retries quickly until an
  announcement actually lands, then settles to the slow cadence.
- **`asyncio.IncompleteReadError` escaped handshake error handling** — it
  derives from `EOFError`, not `OSError`, so a peer that rejects our
  handshake by closing the socket raised an uncaught exception.
- **Peers connected to each other twice.** Spotted in a Docker run reporting
  6 peers in a 5-container swarm. Outbound connections are keyed by the
  peer's *listening* port, but an inbound one only reveals the remote's
  *ephemeral source* port, so address-based dedup never matched and a mutual
  dial left two live connections per pair. Now deduplicated on the `peer_id`
  from the handshake, with a tie-break both sides compute identically (the
  larger peer_id keeps the connection it dialed) so they agree on which one
  survives rather than each dropping a different one and killing both.

## Known limitations

- Single-file torrents only; multi-file is rejected, not mis-parsed.
- One torrent per process (the objects are per-torrent, but nothing
  multiplexes them yet).
- No `.resume` file, so a restart re-hashes the whole file.
- IPv4 only, no NAT traversal — same-LAN by project scope.
- No encryption, no peer authentication, no Sybil resistance.
- Piece-granular hashing means bad-block attribution is a heuristic.
