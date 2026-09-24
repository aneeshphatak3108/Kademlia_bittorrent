"""Tunable constants for the Kademlia DHT layer, per the original paper / BEP 5 conventions."""

ID_BITS = 160  # SHA-1 keyspace
K = 3
  # bucket size / replication factor
ALPHA = 3  # lookup concurrency

RPC_TIMEOUT = 5.0  # seconds, per query attempt
RPC_RETRIES = 2  # additional attempts after the first

BUCKET_REFRESH_INTERVAL = 3600  # 1 hour
REPUBLISH_INTERVAL = 3600  # 1 hour
TTL_DEFAULT = 86400  # 24 hours
EXPIRY_SWEEP_INTERVAL = 300  # 5 minutes

MAINTENANCE_TICK_INTERVAL = 60  # how often background loops wake to check due work

MAX_LOOKUP_ROUNDS = 20  # safety cap on iterative lookup rounds
