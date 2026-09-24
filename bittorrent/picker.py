"""Rarest-first block scheduling.

Plain rarest-first has three well-known rough edges, all handled here:

* **Ties are broken randomly.** Always deterministically picking "the" rarest
  piece makes every client in a swarm converge on the same piece at the same
  moment.
* **The first few pieces are picked at random instead.** Early on we have
  nothing to offer, and chasing a genuinely rare piece keeps it that way --
  tit-for-tat (see choking.py) needs us to have *something* to trade.
* **Endgame mode.** Near the end, the last few blocks are requested from
  several peers at once, so the whole download doesn't stall behind one slow
  peer. Duplicate arrivals are harmless (storage.py writes to a fixed offset).

`pick()` is deliberately a plain synchronous method. Its "find an unclaimed
block" read and its "mark it claimed" write must happen with no `await`
between them, or two peer coroutines can both claim the same block in the gap
(sys_design.md §2). Keeping it synchronous makes that race impossible without
needing a lock.
"""

from __future__ import annotations

import random
from typing import Dict, List, Optional, Set, Tuple

from bittorrent.constants import BLOCK_SIZE, ENDGAME_THRESHOLD, RANDOM_FIRST_PIECES
from bittorrent.metadata import TorrentMetadata

Block = Tuple[int, int, int]  # (piece index, begin offset, length)


class PiecePicker:
    def __init__(self, metadata: TorrentMetadata, have: Optional[Set[int]] = None):
        self.metadata = metadata
        self.have: Set[int] = set(have or ())
        self.availability: List[int] = [0] * metadata.piece_count
        # piece -> begin -> peers with that block outstanding
        self._requested: Dict[int, Dict[int, Set[str]]] = {}
        # piece -> begins that have arrived but whose piece isn't verified yet
        self._received: Dict[int, Set[int]] = {}

    # -- availability bookkeeping (driven by BITFIELD / HAVE messages) --

    def add_peer(self, pieces: Set[int]) -> None:
        for index in pieces:
            if 0 <= index < self.metadata.piece_count:
                self.availability[index] += 1

    def remove_peer(self, pieces: Set[int]) -> None:
        for index in pieces:
            if 0 <= index < self.metadata.piece_count and self.availability[index] > 0:
                self.availability[index] -= 1

    def peer_got_piece(self, index: int) -> None:
        if 0 <= index < self.metadata.piece_count:
            self.availability[index] += 1

    # -- picking --

    def pick(self, peer_pieces: Set[int], peer_key: str, count: int) -> List[Block]:
        """Up to `count` blocks for this peer, claimed atomically as we go."""
        picked: List[Block] = []
        endgame = self._in_endgame()

        for index in self._candidate_pieces(peer_pieces):
            for block in self._blocks_of(index):
                if len(picked) >= count:
                    return picked
                _, begin, _ = block
                if self._is_available(index, begin, peer_key, endgame):
                    # Claim before returning: the caller may await on the way to
                    # actually sending the request, and another coroutine must
                    # not be able to pick this same block in the meantime.
                    self._requested.setdefault(index, {}).setdefault(begin, set()).add(peer_key)
                    picked.append(block)
        return picked

    def _candidate_pieces(self, peer_pieces: Set[int]) -> List[int]:
        wanted = [i for i in peer_pieces if i not in self.have and self._piece_has_gaps(i)]
        if not wanted:
            return []
        if len(self.have) < RANDOM_FIRST_PIECES:
            random.shuffle(wanted)
            return wanted
        # Rarest first, ties broken randomly.
        return sorted(wanted, key=lambda i: (self.availability[i], random.random()))

    def _piece_has_gaps(self, index: int) -> bool:
        received = self._received.get(index, ())
        return len(received) < self.metadata.block_count(index, BLOCK_SIZE)

    def _blocks_of(self, index: int) -> List[Block]:
        size = self.metadata.piece_size(index)
        blocks = []
        for begin in range(0, size, BLOCK_SIZE):
            blocks.append((index, begin, min(BLOCK_SIZE, size - begin)))
        return blocks

    def _is_available(self, index: int, begin: int, peer_key: str, endgame: bool) -> bool:
        if begin in self._received.get(index, ()):
            return False
        holders = self._requested.get(index, {}).get(begin)
        if not holders:
            return True
        # Outstanding elsewhere: only re-request in endgame, and never duplicate
        # a request to the peer that already owes us this block.
        return endgame and peer_key not in holders

    def _in_endgame(self) -> bool:
        return 0 < self._blocks_remaining() <= ENDGAME_THRESHOLD

    def _blocks_remaining(self) -> int:
        remaining = 0
        for index in range(self.metadata.piece_count):
            if index in self.have:
                continue
            total = self.metadata.block_count(index, BLOCK_SIZE)
            remaining += total - len(self._received.get(index, ()))
        return remaining

    # -- results coming back --

    def block_received(self, index: int, begin: int) -> bool:
        """Record an arrived block. True if that completes the piece."""
        self._requested.get(index, {}).pop(begin, None)
        self._received.setdefault(index, set()).add(begin)
        return len(self._received[index]) == self.metadata.block_count(index, BLOCK_SIZE)

    def piece_verified(self, index: int) -> None:
        self.have.add(index)
        self._received.pop(index, None)
        self._requested.pop(index, None)

    def piece_failed(self, index: int) -> None:
        """Hash check failed -- discard the whole piece and re-request it."""
        self._received.pop(index, None)
        self._requested.pop(index, None)

    def release_block(self, index: int, begin: int, peer_key: str) -> None:
        """Drop one peer's claim on one block (e.g. its request timed out), so
        it can be reassigned -- to a *different* peer, per sys_design.md §5."""
        holders = self._requested.get(index, {}).get(begin)
        if not holders:
            return
        holders.discard(peer_key)
        if not holders:
            del self._requested[index][begin]
            if not self._requested[index]:
                del self._requested[index]

    def release_peer(self, peer_key: str) -> None:
        """Drop a departed peer's claims so its blocks become pickable again."""
        for piece, blocks in list(self._requested.items()):
            for begin, holders in list(blocks.items()):
                holders.discard(peer_key)
                if not holders:
                    del blocks[begin]
            if not blocks:
                del self._requested[piece]

    # -- progress --

    @property
    def is_complete(self) -> bool:
        return len(self.have) == self.metadata.piece_count

    def missing_pieces(self) -> Set[int]:
        return {i for i in range(self.metadata.piece_count) if i not in self.have}
