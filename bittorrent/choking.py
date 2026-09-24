"""Choke/unchoke: deciding which peers get to download from us.

Upload capacity is scarce, so it's allocated by reciprocity ("tit-for-tat"):
rank the peers that want data from us by how fast *they* have been feeding
*us*, and unchoke the best few. Peers that give nothing get choked, which is
what deters free-riding.

Ranking alone has a bootstrap problem, though: a peer that has never sent us
anything ranks last forever, so it never gets unchoked, so it can never obtain
a piece to trade back -- a deadlock for every newcomer, including us when we
join. The fix is one **optimistic unchoke** slot, rotated periodically to a
random choked peer regardless of rank. That's how new peers get their first
foot in the door, and how we discover peers that would out-perform our current
picks if given the chance.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Dict, List, Optional

from bittorrent import constants

logger = logging.getLogger(__name__)


class ChokeManager:
    def __init__(self, session):
        self.session = session
        self._last_bytes: Dict[str, int] = {}
        self._last_tick = time.monotonic()
        self._optimistic_key: Optional[str] = None
        self._optimistic_since = 0.0

    def download_rates(self, now: float) -> Dict[str, float]:
        """Bytes/sec received from each peer since the previous tick."""
        elapsed = max(now - self._last_tick, 1e-6)
        rates = {}
        for conn in self.session.active_peers():
            previous = self._last_bytes.get(conn.key, 0)
            rates[conn.key] = max(0, conn.bytes_downloaded - previous) / elapsed
            self._last_bytes[conn.key] = conn.bytes_downloaded
        return rates

    async def tick(self) -> None:
        now = time.monotonic()
        rates = self.download_rates(now)
        self._last_tick = now

        # Only peers that actually want something from us are worth a slot.
        interested = [c for c in self.session.active_peers() if c.peer_interested]
        ranked: List = sorted(interested, key=lambda c: rates.get(c.key, 0.0), reverse=True)

        winners = {c.key for c in ranked[: constants.UNCHOKE_SLOTS]}

        # Rotate the optimistic slot on its own, slower schedule.
        if now - self._optimistic_since >= constants.OPTIMISTIC_UNCHOKE_INTERVAL:
            hopefuls = [c for c in interested if c.key not in winners]
            self._optimistic_key = random.choice(hopefuls).key if hopefuls else None
            self._optimistic_since = now
        if self._optimistic_key is not None:
            winners.add(self._optimistic_key)

        for conn in self.session.active_peers():
            await conn.set_choking(conn.key not in winners)

        if winners:
            logger.debug(
                "unchoked %s (optimistic=%s)", sorted(winners), self._optimistic_key
            )

    def forget(self, key: str) -> None:
        self._last_bytes.pop(key, None)
        if self._optimistic_key == key:
            self._optimistic_key = None
