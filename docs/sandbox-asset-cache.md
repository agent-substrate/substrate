# Sandbox Asset Cache (atelet)

## Summary

Every actor runs on sandbox binaries that come from a cluster-scoped
`SandboxConfig`: a gVisor release, or a micro-VM kernel, VMM and guest image.
`atelet` keeps **one node-local, content-addressed cache** of these assets and
gives each actor **hard links** into it. Nothing is copied per actor, an asset
is downloaded once per node however many actors and workers use it, and the
cache evicts to a byte budget without ever taking an asset something still
uses.

This page describes what is on disk, how an actor gets its assets, how
`atelet` decides which assets a node needs, and how eviction and its metrics
behave. For the design history see the node-local artifact cache proposal in
issue #690.

## What is on disk

Everything lives under `/var/lib/ate`, on one filesystem, because hard links
cannot cross mounts.

| Path | What it holds |
|---|---|
| `sandbox-asset-cache/` | The cache (`--sandbox-asset-cache-dir`). `entries/<sha256 of key>/data` is a cached file or extracted tree, published read-only with execute bits kept; `tmp/` holds in-flight downloads; `.rm-*` are retired entries awaiting removal. An entry directory's mtime is its last-use clock. |
| `actors/<actor uid>/sandbox-assets/` | The actor's assets, each a hard link into the cache, named by asset name. The only location a worker runs a sandbox binary from; the worker checks that a request names the actor's own directory. `sandbox-assets.staging/` beside it exists only while a Run fetches. |
| `sandbox-asset-pins/<runtime hash>/` | One directory per runtime some local worker advertises as its default, with the runtime's assets linked in by the prewarmer. A pin is what keeps a default runtime cached while no actor runs it. |
| `sandbox-asset-scratch/` | Short-lived destinations for the one fetch that has neither an actor nor a pin: the `runsc` binary the worker sandbox syncer probes for CPU features. Wiped at startup. |
| `sandbox-runtime-index.json` | The runtime index serialized: every runtime a worker on this node has advertised, by hash, with the asset record the hash was computed from. |
| `workers/<pod uid>/sandbox-runtimes.json` | What each worker advertises to the control plane, written by `atelet`. |

A cache entry is keyed by the asset's sha256, so the same binaries under
another name or from another bucket are one entry. The store never
invalidates: changed content arrives under a new key.

## How an actor gets its assets

1. ate-api-server places the actor on a worker and names, in `Run` or
   `Restore`, the **sandbox runtime** that worker registered. Its
   `sandbox_bundle_hash` attribute is the hash of one `SandboxConfig`
   version's assets and pause image.
2. `atelet` resolves that hash in its **runtime index** to the asset record
   the hash was computed from. A hash the index does not hold fails the
   request with `FailedPrecondition`: the actor reached a node whose workers
   never advertised that runtime, which is a placement error to surface, not
   something to serve from whatever the request describes.
3. Each asset is looked up in the cache by its sha256. A miss downloads it
   through the object-store plugin's asset provider (anonymously from a
   public `gs://` bucket when the plugin cannot serve it), verifies the
   sha256 twice, extracts a gVisor release, and publishes the entry
   atomically. Concurrent callers for one asset share one download.
4. The asset is **hard-linked** into the actor's `sandbox-assets/`
   directory. `Run` stages into `sandbox-assets.staging/` first and swaps the
   set in only after the actor's previous state is reset, so a failed fetch
   leaves a live activation as it was. `Restore` resets the directory and
   stages into it. `Checkpoint` leaves the links in place: the actor stays
   resumable on the node.

The link is also the cache's only signal that an entry is in use.

## How a node knows which runtimes it needs

The **worker sandbox syncer** watches this node's worker pods, their
`WorkerPool`s and `SandboxConfig`s. For each worker it plans the config its
pool runs, hashes every enabled version, and:

- **registers** every hash in the runtime index, with its record, and writes
  the index file durably, *before* anything else, so a hash is resolvable
  before the worker can register it with the control plane;
- hands the worker's **default** runtime to the **prewarmer**, which fetches
  its assets into the cache, links them into the runtime's pin, and pulls its
  pause image, so the first actor on the node finds a warm cache;
- probes what a snapshot taken on each runtime must find again (CPU features
  for gVisor, host CPU identity for micro-VM) and writes the worker's
  `sandbox-runtimes.json`, from which the worker registers;
- records what the worker now advertises, which is what the written file
  says, and asks the prewarmer to prune pins of runtimes no worker advertises
  as its default any more.

A `SandboxConfig` or `WorkerPool` change registers the affected pools'
runtimes at once and rewrites their files after a jitter of up to thirty
seconds, so the fleet does not re-register in one burst. After an `atelet`
restart the index loads from its file before the port opens, so a request for
a runtime a worker registered earlier resolves with no wait on the syncer. The
in-memory map is the read path; the file is write-through persistence.

Only default runtimes are prewarmed and pinned. The first actor restored on
another enabled version a worker advertises pays that version's download.

## Eviction

With a budget set, an eviction pass runs every
`--sandbox-asset-cache-gc-period`. It sizes the cache, and when that exceeds
`--sandbox-asset-cache-max-bytes` it asks the store to free down to 90% of
the budget. The store applies three rules to
each entry, in order:

1. **Linked, never evicted.** Any hard link from an actor's directory or a pin
   keeps the entry. Retiring it would free nothing while the link lasts and
   would make the next caller download a second copy beside it.
2. **Younger than `--sandbox-asset-cache-min-age`, skipped.** This covers the
   moment between an entry's publication and its first link.
3. **Otherwise eligible, oldest first**, until the freed bytes reach the
   target.

Removal happens after all retires, outside the cache's locks: an entry is
renamed to `.rm-*` and deleted. A hit racing the pass vetoes its entry.

| Flag | Default | Meaning |
|---|---|---|
| `--sandbox-asset-cache-dir` | `/var/lib/ate/sandbox-asset-cache` | Where the cache lives. Must share a mount with the actor directories. |
| `--sandbox-asset-cache-min-age` | `1h` | Entries younger than this are never evicted. |
| `--sandbox-asset-fetch-timeout` | `5m` | Bound on one download, and for a gVisor release its extraction. |
| `--sandbox-asset-cache-gc-period` | `5m` | How often a pass runs. `0` disables passes. |
| `--sandbox-asset-cache-max-bytes` | `0` | The budget. `0` means no budget: the cache only grows, and `atelet` warns so at startup. |
| `--sandbox-asset-cache-gc-dry-run` | `false` | Log what a pass would evict without deleting. |

## Metrics

| Instrument | Type | What it says |
|---|---|---|
| `ate.sandboxassetcache.size` | up/down counter, bytes | Disk the cache occupies, read on every collection. Compare with the budget. |
| `ate.sandboxassetcache.evictions` | counter, entries | Entries passes removed. A steady rate with the size at the budget means the cache thrashes: a runtime leaves and returns on each pass, and each return pays for a download. |
| `ate.sandboxassetcache.gc.passes` | counter, by `ate.sandboxassetcache.gc.outcome` | Passes that had work to do. `complete`: the pass reached its target. `shortfall`: it freed less than asked. `skipped`: it could not size the cache and did nothing. A pass within budget is not counted. |

A **shortfall** is not a fault of the pass. Everything left is linked by an
actor, pinned for a worker's default, younger than the min age, or busy. The
size then measures what the actors and defaults on this node need, not
waste; the answer is a larger budget or fewer runtimes on the node. The pass
warns on the first few shortfalls and then about hourly.

The eviction and pass counters only appear once a pass had work to do, so on
an install without a budget only the size instrument has a series. The e2e
metrics suite asserts that one.

## Runbook: a node is over budget

1. Read `ate.sandboxassetcache.size` against `--sandbox-asset-cache-max-bytes`
   and `ate.sandboxassetcache.gc.passes` by outcome for the node.
2. `shortfall`: list `/var/lib/ate/sandbox-asset-pins/` for the defaults held,
   and `ls -l /var/lib/ate/actors/*/sandbox-assets/` for what actors hold. If
   those account for the size, raise the budget or reduce the runtimes the
   node's pools run. Do not delete pins by hand: the prewarmer rebuilds
   them, and a running actor's links are its runtime.
3. `skipped`: `atelet`'s log names the error from sizing the cache directory.
4. No passes at all while over budget: the period is `0`, or `atelet` is
   older than the budget flag. The startup log line `Sandbox asset cache has
   no budget` says the budget is unset.
5. To see what a pass would take without taking it, restart `atelet` with
   `--sandbox-asset-cache-gc-dry-run` and read the pass log lines.

## Not covered here, deliberately

- **Snapshots.** The same `filecache` store is designed to serve shared
  snapshot restores, the golden and clone images many actors start from.
  That milestone is separate and not yet merged.
- **One GC driver for both caches.** The image layer cache has its own
  eviction loop with the same shape but no metrics. Sharing the driver would
  give it the pass and eviction instruments; it is a follow-up.
- **Which runtime an actor gets.** The `WorkerPool` resolves the
  `SandboxConfig`, the worker registers the config's versions, and
  ate-api-server sends the runtime the assigned worker registered. An
  `ActorTemplate` has no say in it. The cache serves whatever runtime the
  request names and has no part in choosing it.
- **Releasing index entries.** The runtime index never forgets a hash; a node
  sees a handful of configs in its lifetime, and the file stays small.
