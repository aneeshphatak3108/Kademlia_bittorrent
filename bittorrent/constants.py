"""Tunable constants for the BitTorrent layer.

Deliberately separate from kademlia/constants.py: the DHT's values are tuned
for durable arbitrary key/value data (24h TTLs, hourly republish), which is
the wrong scale for peer announcements (a liveness lease) and for
piece-transfer timing.
"""

BLOCK_SIZE = 16 * 1024  # 16 KiB -- the standard BitTorrent request granularity

# How many block requests to keep in flight per peer. Deep enough to keep the
# connection busy across round-trip latency, shallow enough that a slow peer
# isn't sitting on a huge share of the outstanding request budget.
MAX_PIPELINED_REQUESTS = 8

# Peer connection management
MAX_PEER_CONNECTIONS = 50
MAX_CONCURRENT_CONNECT_ATTEMPTS = 8
HANDSHAKE_TIMEOUT = 10.0
CONNECT_TIMEOUT = 10.0
BLOCK_REQUEST_TIMEOUT = 30.0  # much longer than a DHT RPC: a peer may be busy, not dead
KEEPALIVE_INTERVAL = 120.0
PEER_IDLE_TIMEOUT = 300.0

# A peer that repeatedly times out or serves data failing its piece hash is
# dropped, then not retried until the backoff expires.
MAX_PEER_FAILURES = 3
PEER_RETRY_BACKOFF = 60.0

# Choke/unchoke (tit-for-tat). Reciprocity is recalculated every
# UNCHOKE_INTERVAL; one rotating optimistic slot gives unproven peers a way in.
UNCHOKE_SLOTS = 4
UNCHOKE_INTERVAL = 10.0
OPTIMISTIC_UNCHOKE_INTERVAL = 30.0
RATE_WINDOW = 20.0  # seconds of history used for the download-rate ranking

# DHT peer announcements are a liveness lease, not durable storage -- short TTL
# so departed peers fall out quickly, with re-announce well inside it.
ANNOUNCE_TTL = 1800.0  # 30 minutes, matching real BEP5 practice
ANNOUNCE_INTERVAL = 600.0  # re-announce every 10 minutes once it's landing
PEER_DISCOVERY_INTERVAL = 60.0  # how often to look for new peers while incomplete

# Announcing while still alone in the DHT reaches nobody, and waiting a full
# ANNOUNCE_INTERVAL to try again would leave us undiscoverable for minutes.
# Back off to the slow cadence only once an announcement actually lands.
ANNOUNCE_RETRY_INTERVAL = 5.0
# Likewise, poll harder while we have no peers and still want data.
PEER_DISCOVERY_RETRY_INTERVAL = 3.0

# Switch to endgame mode (request remaining blocks from several peers at once)
# when this few blocks are left outstanding.
ENDGAME_THRESHOLD = 8

# Pieces picked at random rather than rarest-first at the very start, so we
# have something to trade before chasing genuinely rare pieces.
RANDOM_FIRST_PIECES = 2
