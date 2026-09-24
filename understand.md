# Understanding the BitTorrent Layer — a reading order

A path through the code that builds up one concept at a time, so nothing
references something you haven't seen yet. Roughly 1,700 lines of
implementation across 9 files; expect a few focused hours to genuinely
follow it, not one sitting.

The ordering principle: **pure logic first, I/O second, orchestration last.**
The files with no network and no async are where the actual ideas live; the
async machinery is mostly plumbing once you know what it's plumbing.

Each stop lists what to read, the questions it should answer, and the test
file that demonstrates it. **Read the tests alongside the code** — they're
written as executable descriptions of the behaviour, and several encode
reasoning that's hard to see from the implementation alone.

---

## Stop 0 — Orientation (~20 min)

| Read | Why |
|---|---|
| `claude.md` | The whole project in 5 lines. Note what's explicitly out of scope (NAT). |
| `bittorrent.md` | The design and the decisions behind it. Skim now, return after Stop 7. |
| `ARCHITECTURE.md` | The DHT layer beneath. Skim the "mental model" section. |

**Question to hold onto:** why is this split into two layers at all, and what
does the lower one actually promise the upper one?

---

## Stop 1 — What is a torrent? (`bittorrent/metadata.py`, 127 lines)

The smallest, purest file. No async, no network, no state. It defines the
vocabulary everything else uses.

Read `TorrentMetadata`, then `from_bytes`, then `piece_size`.

**It should answer:**
- What exactly is the `info_hash`, and why is it 20 bytes? (Hint: compare to
  `kademlia/identifier.py`'s `NodeID`. This is the hinge the whole two-layer
  design turns on.)
- Why does `piece_size(i)` exist instead of just using `piece_length`?
- Why is re-encoding the info dict to compute its hash safe? (See the comment
  about bencode being canonical.)

**Tests:** `tests/test_bt_metadata.py` — especially
`test_info_hash_is_sha1_of_bencoded_info_dict` and `test_last_piece_is_short`.

---

## Stop 2 — Where do the bytes go? (`bittorrent/storage.py`, 148 lines)

Still no network. Maps (piece, offset) to file positions, verifies hashes,
produces the bitfield.

**It should answer:**
- Why are blocks written straight to their final offset instead of being
  buffered until the piece is complete? (This one decision makes duplicate
  deliveries harmless — it comes back at Stop 5 and Stop 8.)
- There is no saved "which pieces do I have" file. So how does a restarted
  download know where it left off? What does that buy, and what does it cost?
- Why does every disk method hop through `run_in_executor`?

**Tests:** `tests/test_bt_storage.py` —
`test_complete_download_then_rescan_recovers_state` and
`test_rescan_treats_corrupt_piece_as_absent` are the crash-recovery story.

---

## Stop 3 — Rarest-first (`bittorrent/picker.py`, 169 lines) ← the heart

The algorithm you asked for. Pure logic, fully synchronous, no I/O. **Read
this one slowly.** Everything else exists to feed it or act on its output.

Read the module docstring first, then `pick`, then `_candidate_pieces`, then
the `_is_available` / endgame logic.

**It should answer:**
- Why is plain "always pick the rarest" not good enough? Find the three
  refinements in the code and work out what each one prevents.
- Why does `pick()` *claim* blocks (mutate state) rather than just returning
  suggestions?
- **The important one:** why is `pick()` deliberately not `async`? What
  concretely breaks if you put an `await` between "find an unclaimed block"
  and "mark it claimed"? (`sys_design.md` §2 spells this out.)
- What's the difference between `release_block` and `release_peer`, and why
  does each exist?

**Tests:** `tests/test_bt_picker.py` maps almost 1:1 onto the refinements —
`test_ties_are_broken_randomly`, `test_random_first_pieces_ignores_rarity`,
`test_endgame_allows_duplicate_requests_to_different_peers`.

---

## Stop 4 — How peers talk (`bittorrent/wire/`, 454 lines)

Read `messages.py` first (pure encode/decode, no state), then
`connection.py` (state machine + async read loop).

**`messages.py` should answer:**
- Why is this binary framing rather than bencode, when the DHT layer uses
  bencode for everything?
- Why is the length prefix checked against a cap *before* any buffer is
  allocated? What attack does that stop?

**`connection.py` should answer:**
- What are the four boolean flags, and why four rather than two? (Choking and
  interest are each independent *per direction*.)
- What does `close()` do about the read task, and why the
  `is not asyncio.current_task()` check?
- Why is retry/timeout policy here rather than in the raw socket code?

**Tests:** `tests/test_bt_wire.py`, especially
`test_read_message_rejects_an_absurd_length_before_allocating`.

---

## Stop 5 — Who gets to download from us (`bittorrent/choking.py`, 77 lines)

Short and self-contained. Read the docstring, then `tick`.

**It should answer:**
- Rank peers by what they send *us*, then unchoke the best — what problem
  does that solve, and what behaviour does it deter?
- The optimistic unchoke slot: what deadlock exists without it? Work through
  what happens to a peer that joins holding nothing.
- Why is the rate computed as a delta between ticks rather than a running
  average?

---

## Stop 6 — Finding peers (`bittorrent/discovery.py`, 267 lines)

The bridge between the two layers. Needs the DHT in your head, so if
`ARCHITECTURE.md` has faded, re-skim it first.

Read the long module docstring carefully — it's the densest explanation in
the codebase — then `PeerTable`, then `PeerDiscovery`.

**It should answer:**
- Why can't the DHT's own `STORE`/`FIND_VALUE` hold a peer list? Be precise:
  the reason is about *semantics*, not about the value's type.
- Every peer announces to **all** K closest nodes, not one each. What does
  that make each of those K nodes hold — a fragment, or the whole list? Why
  does the answer matter when you go to query them?
- Why does `ANNOUNCE_PEER` carry a port but *not* an IP?
- Why does a node never republish *someone else's* announcement, when the
  DHT layer happily republishes stored values?
- `get_peers` also reads the node's own table. Why is that not redundant?
  (A lookup never returns the searcher itself.)

**Then read the extension mechanism it depends on:**
`kademlia/rpc/messages.py` → `register_query` / `register_response` /
`is_response`, and `kademlia/node.py` → `register_query_handler` /
`send_extension_query`. **Question:** why go to this trouble instead of just
adding two more message types to the DHT's existing `if/elif` chain?

**Tests:** `tests/test_bt_discovery.py` —
`test_get_peers_includes_announcements_made_to_us` documents a real bug this
caught.

---

## Stop 7 — The conductor (`bittorrent/session.py`, 414 lines)

Read this **last**. It references everything above, which is exactly why it's
incomprehensible first and straightforward now.

Read in this order rather than top to bottom:

1. `__init__` and `start` — what it owns, what background loops it starts
2. `_register` / `_find_duplicate` — how a peer joins, and the peer_id dedup
3. The `on_*` callbacks — the event-driven core, in this order:
   `on_bitfield` → `on_have` → `on_unchoked` → `on_piece` → `_piece_complete`
4. `_maybe_request` — the one place requests originate
5. `on_request` — the upload side
6. The four `_*_loop` methods — announce, discovery, choking, timeouts

**It should answer:**
- Requests are event-driven, not polled. Which events re-trigger
  `_maybe_request`, and why is that set sufficient to keep the pipeline full?
- In `on_piece`, why the early return when `self.store.has_piece(index)`
  already? What breaks without that guard? (Tie this back to Stop 2.)
- A piece fails its hash. Which peer gets blamed, and why can't we know for
  certain it was the right one?
- Why do the announce and discovery loops use a *shorter* retry interval when
  nothing has worked yet?

---

## Stop 8 — Trace these flows end-to-end

Reading files tells you what exists; tracing tells you how it *behaves*. Do
these with the code open, writing down the call chain.

1. **One block, request to disk.** Start at `_maybe_request`. Follow through
   `picker.pick` → `conn.request_block` → the wire → the peer's `on_request`
   → back through `_read_loop` → `on_piece` → `store.write_block`. How many
   times does the block change representation along the way?
2. **A piece completes.** From the last block arriving to `HAVE` going out to
   every peer. Where exactly does verification happen, and what's the state
   if it fails?
3. **A new peer joins the swarm.** From `_discovery_loop` calling
   `get_peers`, through `add_peer`, the handshake, `_register`, the bitfield
   exchange, to the first request. Then trace the *inbound* version
   (`_on_incoming`) and note where the two paths converge.
4. **A peer dies mid-download.** `_read_loop` raises → `close()` →
   `on_peer_disconnected`. Follow what happens to the blocks it owed us.
   Why would the download stall forever without this path?
5. **Both peers dial each other simultaneously.** Two connections form for
   one pair. Follow `_find_duplicate` and work out why the tie-break makes
   both sides keep the *same* one.

---

## Stop 9 — Learn by breaking it

The fastest way to confirm you understand something is to break it and
predict the failure before you run it. Edit `bittorrent/constants.py`,
predict, then run `pytest tests/ -q` or the harness.

| Change | Predict first, then check |
|---|---|
| `RANDOM_FIRST_PIECES = 0` | Which test fails, and what does its failure tell you about bootstrapping? |
| `UNCHOKE_SLOTS = 1` | Does a 4-leecher Docker run still finish? Faster or slower? Why? |
| `ENDGAME_THRESHOLD = 0` | What gets slower, and specifically at what point in the download? |
| `MAX_PIPELINED_REQUESTS = 1` | Why does throughput drop even though nothing is broken? |
| Comment out the `release_peer` call in `on_peer_disconnected` | Which test catches it? Why is this a *stall* rather than a crash? |
| Make `pick()` `async` and `await asyncio.sleep(0)` mid-way | Does any current test catch it? What does that tell you about testing races? |

Also worth doing: disable a fix and confirm its test actually fails — the
technique used on the duplicate-connection bug. A test that passes both with
and without the fix is proving nothing.

---

## Stop 10 — The design reasoning

Now that the code is concrete, these read completely differently:

- `bittorrent.md` — re-read in full, especially **"Bugs this design had to fix
  during implementation"**. Five real bugs, two of which only appeared in a
  real two-process run and not in-process. Ask yourself for each: what class
  of test would have caught it earlier?
- `sys_design.md` — the tradeoff analysis written *before* any code. Compare
  what was predicted against what the code actually does. The §2 concurrency
  section and §9 idempotency section are the ones that paid off most directly.

---

## If you only have an hour

`metadata.py` → `picker.py` → `bittorrent.md`. That's the data model, the
algorithm, and the reasoning — enough to discuss the system intelligently,
though not enough to modify it safely.

## Order summary

```
claude.md, bittorrent.md (skim)
  └─ metadata.py        what a torrent is
      └─ storage.py     where bytes live
          └─ picker.py  ← rarest-first, the heart
              └─ wire/messages.py, wire/connection.py   how peers talk
                  └─ choking.py      who we upload to
                      └─ discovery.py + kademlia registry hooks   finding peers
                          └─ session.py   ← everything, tied together
                              └─ trace flows, then break things
```
