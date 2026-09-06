# Operations Runbook

This is day-2 operational guidance: how to run, diagnose, and recover a
TollMeshCache cluster. It assumes you've read [Architecture](architecture.md)
for how the system actually works; this document is about what to *do*
when something needs attention.

Everything in this document describes behavior that has been verified
live against real running processes (not just unit-tested), and says so
explicitly where that matters. Where something is a recommendation rather
than a verified fact, it's marked as such.

## Starting a cluster

```
tollmeshcache -node node1 -bind-addr 0.0.0.0 -bind-port 7946 -http-addr :8080 \
  -api-key "$TOLLMESH_API_KEY" -cluster-secret "$TOLLMESH_CLUSTER_SECRET"

tollmeshcache -node node2 -bind-addr 0.0.0.0 -bind-port 7946 -http-addr :8080 \
  -api-key "$TOLLMESH_API_KEY" -cluster-secret "$TOLLMESH_CLUSTER_SECRET" \
  -join node1.internal:8080
```

- `-join` only needs to name **one** existing member. That node's response
  to the join request includes its own peer list, so the new node
  transitively discovers everyone already in the cluster (see
  `joinCluster` in `cmd/tollmeshcache/main.go`).
- Prefer `TOLLMESH_API_KEY`/`TOLLMESH_CLUSTER_SECRET` environment
  variables over the `-api-key`/`-cluster-secret` flags in any
  environment where `ps`/`/proc` is visible to anyone other than the
  process owner — a flag value is visible to any local user who can list
  processes; an env var is not. The code already prefers the env var
  automatically when the flag is left unset.
- Every node needs the *same* `-cluster-secret` and (if you're running
  authenticated) the same `-api-key`. There's no per-node secret
  rotation across the fleet — see [TLS and certificates](#tls-and-certificates)
  below for the one thing that *does* rotate live.

## Adding a node to a live cluster

Same as startup: start the new node with `-join <any-existing-node>`. It
announces itself via `POST /internal/peers/join`, discovers the rest of
the cluster transitively, and begins converging via gossip within one
sync interval (5s by default). There is no special "draining" or
rebalancing step to run — every node already holds full replicated state
for the 13 primitives that replicate (see
[Architecture: Gossip and state sync](architecture.md#gossip-and-state-sync)),
so a new node is a full peer as soon as gossip has had a few rounds to
run, not a partial one that needs to "catch up" before serving reads
correctly for replicated features.

**Caveat, stated plainly:** Job Queues, Pub/Sub message delivery to a
specific subscriber, WASM script compilation, and a few other things have
narrower guarantees than full replication — see each feature's own
section in [Architecture](architecture.md) for exactly what does and
doesn't follow a new node immediately.

## Removing a node

There is no formal "decommission" procedure or API call. Stopping a node
(`SIGTERM`/`SIGINT`, both handled for graceful shutdown) simply removes
it from active gossip; peers will start marking it unhealthy once its
health checks and gossip rounds start failing (see
[Interpreting health signals](#interpreting-health-signals)). Nothing
automatically prunes a stopped node from other nodes' peer lists — if you
want a peer list to stop including a permanently-retired node, you
currently have to restart the remaining nodes without that node in their
`-join` target, or accept that `/peers`/`/peers/health` will keep listing
it as unhealthy indefinitely. This is a known gap, not a hidden one.

## Interpreting health signals

Three different endpoints answer three different questions. Confusing
them is the most common way to misdiagnose a real incident:

| Endpoint | Question it answers | Auth |
|---|---|---|
| `GET /livez` | Is this process itself alive and responding at all? | none |
| `GET /readyz` | Is this node connected to at least one peer (or standalone with none configured)? | none |
| `GET /peers/health` | Per-peer detail: is *each specific* configured peer currently reachable? | cluster secret |

**`/livez` deliberately does not reflect peer connectivity.** A node cut
off by a network partition is still alive and still correctly serving
local reads/writes — killing and restarting it (which is what a liveness
probe failure typically triggers in an orchestrator) would not fix a
partition and would turn it into a much worse, self-inflicted outage by
cycling every partitioned node. If you're using Kubernetes or similar,
wire `/livez` to the liveness probe and `/readyz` to the readiness probe,
not the other way around, and not the same endpoint for both.

**`/readyz` returns 503 when a node with configured peers has zero
currently-healthy peers.** This means the node is isolated from the rest
of the cluster — it can still serve reads/writes against its own local
state, but that state may be stale relative to the rest of the cluster,
and any writes it accepts won't be visible elsewhere until connectivity
returns. A load balancer routing away from a `/readyz`-failing node is
reasonable; an orchestrator restarting it is not (same reasoning as
`/livez`).

**Verified live** (see [Architecture: Cluster membership and failure
detection](architecture.md#cluster-membership-and-failure-detection)):
killing a peer causes the survivor to mark it unhealthy and `/readyz` to
flip to 503 within a few health-check cycles; restarting the peer causes
recovery to be detected and `/readyz` to return to 200 — without needing
gossip to happen to pick that peer, since `PeerManager` runs an
independent periodic health check.

## Common incidents

**A node's `/readyz` is failing but `/livez` is fine.**
The node is isolated (all peers unreachable) but otherwise healthy.
Check `/peers/health` on that node (needs the cluster secret) to see
which peers it thinks are down, and check connectivity/firewall rules
between it and them. If the peers themselves are fine and reachable from
elsewhere, suspect a one-way network issue specific to this node, or a
TLS/cluster-secret mismatch (see below) rather than the peers actually
being down.

**Two nodes disagree on a value.**
This is expected, briefly, after any write — TollMeshCache is AP, not CP,
and gossip converges within roughly one sync interval (default 5s) under
normal conditions, faster for Job Queue claims and Transaction commits
specifically (see [Architecture: bounded mitigation for the Job
Queue/Transaction race window](architecture.md#cluster-membership-and-failure-detection),
~45ms measured live rather than the full interval). If two nodes still
disagree well past that window with no partition or health-check failure
visible, that's worth investigating as a potential real bug, not
dismissing as "eventual consistency."

**A node just returned from being down for a while and disagrees with
everyone else.**
This is exactly what gossip is for. `/readyz` will show it as isolated
until it re-establishes healthy peer connections, at which point it
starts converging. **Verified live**: a genuine split-brain scenario (two
independent 2-node clusters, each accepting a conflicting write to the
same key, later merged into one mesh) converged correctly to the
deterministically-later write across all four nodes with zero corruption
or crashes — this is the CRDT/LWW-register guarantee working as
designed, not a special case. It can take a few gossip rounds longer
than the sync interval if the reconnecting node has multiple peers and
happens to pick the "wrong" one on early rounds (peer selection is
random among healthy peers each round) — this is normal, not a bug, and
resolves itself.

**The WAL directory is growing.**
Expected under any real write throughput; `MeshStore.autoSnapshotLoop`
compacts it automatically every 5 minutes by default (see
[Storage layout](architecture.md#storage-layout)). If it's growing
*without* periodically shrinking back down (check for new files
appearing in the `snapshots/` directory alongside `wal.log` resetting to
a small size every ~5 minutes), that's a real problem worth investigating
directly — check the process's logs for `auto-snapshot failed` messages.

**Memory is growing under sustained load.**
Some growth is expected and legitimate: an unclaimed Job Queue backlog,
for instance, holds every pending job in memory until it's claimed —
that's not a leak, it's a real backlog (see the sustained-load-test
paragraph in [Storage layout](architecture.md#storage-layout) for the
methodology used to distinguish the two). Completed/failed jobs *do* get
evicted automatically after `maxAge` (24h default). Use `/debug/pprof/heap`
and `/debug/pprof/goroutine` (cluster-secret gated — see
[TLS and certificates](#tls-and-certificates) for why this isn't the API
key) to distinguish "a real backlog of live data" from "goroutines or
allocations that shouldn't exist," rather than guessing from RSS alone.

## Disaster recovery

**Verified live**: killing every node in a cluster, deleting all WAL
files (simulating total loss where only backed-up snapshot files
survive), and restarting each node from nothing but its own last
snapshot correctly recovered every one of the ten snapshot-covered
feature groups (cache, sorted sets, streams, job queues, pub/sub
history, transactions, WASM scripts, ranking configs, metrics
high-water marks) with no data loss for anything that had been
snapshotted, and no cross-node re-join was needed for each individual
node's own local recovery to succeed.

**Recommended backup procedure** (this is a recommendation, not something
this project automates for you): periodically copy each node's
`snapshots/` directory to durable, off-host storage. `POST
/persistence/snapshot` (API-key gated, like other SDK-facing operational
endpoints — it's not under `/internal/`) can be called
on a schedule to force a fresh snapshot before each backup copy, bounding
how much WAL replay (if any) would be needed on top of the backed-up
snapshot during a real restore. There is no built-in backup scheduler or
off-host upload — wire this into whatever backup/cron infrastructure your
environment already uses.

**Restore procedure**: start a fresh node pointed at a data directory
containing a restored `snapshots/` folder (WAL absent or empty is fine —
recovery naturally falls back to snapshot-only). The node loads the
latest snapshot automatically on startup; no explicit "restore" command
is needed. If restoring an entire cluster from backups taken at
different times across nodes, expect brief post-restore divergence that
resolves via normal gossip, the same as the split-brain-healing case
above — this has not been separately verified with backups taken at
different wall-clock times across nodes, so treat "probably fine, same
mechanism as verified split-brain healing" as a reasoned inference, not
an independently confirmed fact, until it's been tested that way.

**Snapshot format is versioned** (`Snapshot.FormatVersion`). Restoring a
snapshot taken by a *newer* build than the one doing the restoring is
refused with a clear error rather than silently misinterpreted — this
matters specifically for a rollback-after-upgrade scenario. Restoring an
older snapshot with an *older* build (or a very old, pre-versioning
snapshot) is supported.

## TLS and certificates

TLS is opt-in via `-tls-cert`/`-tls-key`/`-tls-ca` — see
[Architecture: HTTP as the transport](architecture.md#http-as-the-transport)
for what each flag does and how mutual TLS activates when all three are
set. Two operational notes beyond the architecture description:

- **Certificates hot-reload.** Replace the files at the same paths (an
  atomic rename from your PKI/rotation tooling is safer than an in-place
  overwrite, to avoid a `CertReloader` observing a half-written file
  mid-swap) and the running process picks up the new certificate within
  one health-check/handshake cycle — no restart needed. Verified live by
  rotating a running node's certificate and confirming the new one was
  presented within seconds.
- **This project does not issue certificates for you.** `scripts/generate-cluster-ca.sh`
  (in this repository) is a minimal bootstrap script for standing up a
  single self-signed cluster CA and per-node leaf certificates suitable
  for a small deployment or getting started; it is explicitly not a
  substitute for a real PKI (Vault, cert-manager, a proper internal CA)
  in an environment that needs certificate lifecycle management,
  revocation, or audit trails.
- `/debug/pprof/*` and `/peers/health` require the **cluster secret**,
  not the API key — a deliberate, security-review-driven choice (see
  [Architecture](architecture.md#a-broader-security-review-pass-across-everything-built-this-session)),
  since pprof's `cmdline` endpoint can reveal the cluster secret itself
  if it was ever passed as a flag rather than an env var. Don't assume
  your API-key-holding tooling can reach these; it needs the cluster
  secret specifically.

## Monitoring

`GET /metrics` (Prometheus text format, API-key gated) exposes this
node's own local counters. `GET /metrics/cluster` (API-key gated) exposes
a real cluster-wide aggregation (sum across every known node) for the
same monotonic counters, converged via gossip's GCounter-shaped merge —
see [Architecture: Cluster membership](architecture.md#cluster-membership-and-failure-detection)
for `/livez`/`/readyz`/`/peers/health` beyond raw metrics.

`deploy/prometheus.yml` and `deploy/alerts.yml` in this repository are a
starting-point scrape config and alerting-rule set — again, a starting
point to adapt, not a finished, battle-tested alerting policy for your
specific traffic and SLOs.
