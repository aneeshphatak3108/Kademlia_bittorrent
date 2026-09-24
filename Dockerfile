# Minimal image for running Kademlia DHT nodes in containers (Tier 3 testing,
# see scripts/docker_harness.py and test_dht.md). No source is baked in --
# the repo is bind-mounted to /app at `docker run` time, so code edits on the
# host are live in every container without rebuilding this image.
FROM python:3.8-slim

RUN pip install --no-cache-dir pytest pytest-asyncio

WORKDIR /app
