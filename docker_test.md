# Docker DHT Testing — Setup and Steps

Straightforward, no side quests. This machine has Docker installed via the
**Canonical snap package**, not the usual apt package — that matters for the
permission fix below, verified against this exact system (confirmed by
reading the running `dockerd` process's actual command line).

## 1. One-time permission fix

The snap's `dockerd` is launched with `--group docker`, expecting a group
called `docker` to exist and own the socket — but on this machine that group
was never created, so the daemon falls back to `root:root`, which is why you
got permission denied and why `usermod -aG docker` failed (can't add
yourself to a group that doesn't exist).

Run these three commands:

```bash
sudo groupadd docker
sudo usermod -aG docker $USER
sudo snap restart docker
```

Then either log out and back in, or run `newgrp docker` in your current
terminal (group membership doesn't apply to already-open sessions
otherwise).

Verify it worked:

```bash
docker ps
```

No output rows and no error = success (empty list, since nothing's running
yet). If you still get a permission error, close *every* terminal and open a
fresh one (some terminal multiplexers cache group membership per-session).

## 2. Run the DHT container test

```bash
cd /home/aneesh/Desktop/Bit_torrent_grind
python scripts/docker_harness.py
```

That's it — one command. No venv activation needed (it only shells out to
the `docker` CLI). Optionally add `--nodes N` to change the mesh size
(default 8).

It will, automatically, in order:
1. Build a small Docker image (`kademlia-dht-node:latest`) from the
   `Dockerfile` — pulls `python:3.8-slim` the first time (a bit slow),
   cached and fast after that.
2. Create a Docker network and start N containers on it, each a real
   Kademlia DHT node with its own container IP.
3. Bootstrap them into a mesh (node0 = seed, the rest join via the previous
   one).
4. Store a value on node0, then figure out (via each node's own
   `dump_store`) which containers *don't* have a local copy from the
   store's replication fan-out, and query one of those — this forces a
   genuine network lookup (`find_value` actually traversing the DHT),
   not just a trivial local hit.
5. `docker kill` two of the *other* non-holder containers, then query yet
   another non-holder — proves a node with no local copy can still find
   the value after real container churn, not just right after the store.
6. Tear everything down (containers + network), whether it passed or failed.

Expect output ending in:
```
All Tier 3 scenarios passed.
```

## 3. Cleanup

Normal runs (pass or fail) tear down everything themselves — containers and
the network are removed in a `finally` block regardless of outcome. These
are for the abnormal case: the script itself got killed (Ctrl-C mid-run,
terminal closed, machine slept), so its own cleanup never got to run.

```bash
# Remove any leftover node containers (node0, node1, ... whatever N was)
docker ps -aq --filter "name=^node[0-9]+$" | xargs -r docker rm -f

# Remove the test network
docker network rm kademlia-test-net 2>/dev/null

# Confirm both are gone
docker ps -a --filter "name=^node[0-9]+$"
docker network ls
```

`xargs -r` skips the removal when nothing matches, rather than calling
`docker rm` with no arguments and erroring.

**If you still need `sudo` for Docker**, wrap the whole pipeline in one root
shell rather than just prefixing `sudo`:

```bash
sudo sh -c 'docker ps -aq --filter "name=^node[0-9]+$" | xargs -r docker rm -f'
```

Prefixing alone (`sudo docker rm -f $(docker ps -aq ...)`) fails: the shell
expands `$(...)` before sudo takes effect, so the inner `docker ps` runs as
your normal user and hits permission denied, leaving the outer command with
nothing to remove.

Optional, only if you want a fully clean slate (next run will just rebuild
it, slower one time):

```bash
docker rmi kademlia-dht-node:latest 2>/dev/null
```

## 4. If something still doesn't work

```bash
docker info                    # confirms the daemon is reachable at all
docker network ls              # check for a leftover "kademlia-test-net" from a crashed run
cat logs/node0.log             # per-node logs, same logs/ dir as the other test tiers
```
