# imagecache — node-local OCI image layer cache

`internal/imagecache` implements substrate's node-local OCI image cache: a
content-addressed pool of **unpacked image layers**, stored once per node and
shared by every actor on it, plus the machinery that composes an actor's
rootfs from those layers as an overlayfs mount instead of extracting the
image on every run.

It replaces the previous design (an in-memory LRU of flattened image
tarballs in `atelet`, re-untarred into every bundle on every actor
start/resume) and is the Phase 1 implementation of
[#463](https://github.com/agent-substrate/substrate/issues/463), addressing
[#120](https://github.com/agent-substrate/substrate/issues/120),
[#166](https://github.com/agent-substrate/substrate/issues/166),
[#228](https://github.com/agent-substrate/substrate/issues/228) and
[#437](https://github.com/agent-substrate/substrate/issues/437).

What it buys, concretely:

- Actor start/resume composes the rootfs with **one overlay mount
  (milliseconds)** instead of a full image extraction (tens of seconds for
  GB-scale images). `Restore timing breakdown` logs show `oci_unpack`
  dropping from ~15–20 s to single-digit milliseconds on warm nodes.
- Layers shared between images are **downloaded and unpacked once per
  node**, not once per image; actors sharing layers also share page cache.
- The cache is **on disk and survives atelet restarts and node reboots**
  (the old in-memory cache was lost on every restart, and its unbounded heap
  retention could OOM atelet — #437).
- **Tag refs are cacheable**: a tag costs one `HEAD` request to resolve to a
  manifest digest (the only safe cache key for mutable tags); digest refs
  hit the cache with zero network I/O.
- Memory use during pulls is O(stream buffers), independent of image size
  (the old `mutate.Extract` path buffered entire flattened images — #120).

## The privilege split

The design is shaped by an existing substrate boundary: **atelet runs as
plain root with every Linux capability dropped** ("atelet does no mounts" —
see `manifests/ate-install/atelet.yaml`), while the **ateom worker pods are
privileged** and own all mounts on the node. The module is split accordingly:

| Half | Runs in | Files | Needs |
|---|---|---|---|
| Store: pull, parse, unpack, record | atelet | `imagecache.go`, `unpack.go`, `spec.go` (portable) | nothing but file I/O |
| Consumer: finalize, mount, unmount | ateom-gvisor / ateom-microvm | `bundle_linux.go` (`//go:build linux`) | `CAP_MKNOD`, `CAP_SYS_ADMIN` |

The two halves communicate through the filesystem only: the shared cache
directory (on the `/var/lib/ateom-gvisor` hostPath, so the same absolute
paths resolve in every pod) and a small per-bundle spec file.

Because the consumer mounts the overlay **in its own mount namespace** —
exactly where the workload resolves it (runsc's gofer for gVisor, virtiofsd
for the micro-VM) — no Kubernetes mount-propagation configuration is needed
anywhere.

## On-disk layout

```
<cache-root>/                        default: /var/lib/ateom-gvisor/image-cache
  version                            layout version marker ("1")
  layers/sha256/<diffid-hex>/
      fs/                            the unpacked layer tree (an overlay lowerdir)
      whiteouts.json                 whiteout state recorded at unpack time
      finalized                      marker written by FinalizeLayer (consumer side)
      size                           byte count recorded at unpack (lazily
                                     backfilled for older layers), so sizing
                                     the pool never walks trees
  layers/sha256/.tmp-*/              in-flight unpack (swept at startup)
  layers/sha256/.rm-*/               retired by eviction, awaiting async
                                     removal (swept at startup)
  manifests/sha256/<digest-hex>.json image config + ordered diffID list; the
                                     file's mtime doubles as the image's
                                     last-use timestamp
```

A layer directory that exists is always complete: unpack streams into a
`.tmp-*` sibling and moves it into place with a single atomic rename.
Startup recovery (`New`) sweeps leftover `.tmp-*` and `.rm-*` dirs,
verifies the layout version, and reclaims orphaned layers (see Garbage
collection below). An "image" is nothing but a manifest record listing
layer diffIDs in order — layers shared by N images exist once.

## Pull path (atelet: `Store.EnsureImage`)

1. **Resolve** the ref. Digest refs are parsed directly; tag refs cost one
   `remote.Head`. Localhost/loopback registries are rewritten for kind
   (`--localhost-registry-replacement`) and pulled over plain HTTP; gcr.io /
   pkg.dev registries get the configured GCP authenticator.
2. **Cache check**: if the manifest record exists and every layer dir is
   present, return with no network I/O. Missing layers (only) are re-pulled.
3. **Pull** by resolved digest: layers download in parallel (bounded at 4),
   each streamed download → decompress → untar directly into the pool.
   Concurrent pulls of the same image or layer are collapsed with
   singleflight, so simultaneous actor starts never duplicate work — and
   each completed layer lands individually, so an interrupted pull makes
   incremental progress across retries.
4. **Unpack** (`unpackLayer`) is the repo's hardened untar: `os.Root`
   confinement (path traversal and symlink/hardlink escapes are refused),
   "later entry wins" within a layer, read-only-dir handling that works
   without `CAP_DAC_OVERRIDE`, and creation of parent directories that the
   layer tar omits (they may exist only in lower layers). Whiteout entries
   (`.wh.*`) are **not** written into the tree — overlayfs whiteouts are
   char devices atelet cannot create — they are recorded in
   `whiteouts.json` for the consumer to materialize.
5. **Record**: the image config + diffID list is written under the
   requested digest (and the per-platform child digest for multi-arch refs).

`prepareOCIDirectory` in atelet then writes `rootfs-overlay.json`
(`OverlaySpec`) into the bundle next to `config.json`, listing the layer
directories bottom-first plus any `ExtraDirs` (in-rootfs bind-mount targets,
e.g. the actor identity mount at `/run/ate`), and creates the empty
bundle-local `rootfs/`, `upper/`, and `work/` directories.

## Compose path (ateom: `SetupBundleRootfs`)

Called immediately before `runsc create`/`runsc restore` (gVisor) and before
staging the virtio-fs lower (micro-VM):

1. **`FinalizeLayer`** for each referenced layer — materializes the recorded
   whiteouts as 0:0 char devices (`mknod`) and opaque dirs as
   `trusted.overlay.opaque=y` xattrs. Once per layer node-wide; idempotent
   and safe under concurrent ateom pods (`EEXIST` tolerated, marker written
   last). Paths from `whiteouts.json` are re-validated, so a crafted file
   cannot escape the layer tree.
2. **Mount** an overlay at `<bundle>/rootfs`: `lowerdir` is the layer chain
   reversed into overlayfs's top-first order (duplicate layers — images can
   legitimately list the same diffID twice — are collapsed to the topmost
   occurrence, which overlayfs otherwise rejects with `ELOOP`), `upperdir` /
   `workdir` are the bundle-local dirs, holding this actor's private writes.
   The mount uses the new mount API (`fsopen` + one `fsconfig` `lowerdir+`
   append per layer) rather than `mount(2)`, whose single-page option-string
   cap the digest-derived layer paths would hit at ~34 layers. **Minimum
   supported kernel: Linux 6.5** (`lowerdir+`); every current GKE channel
   ships ≥ 6.6 (Stable: COS 121 LTS).
3. **ExtraDirs** are created through the mount (landing in the upper), again
   under `os.Root` confinement.
4. **Implicit-parent metadata repair.** A layer tar routinely omits entries
   for parent directories that exist only in lower layers; unpack fabricates
   them (root:root 0755) and records them as `implicitDirs` in the layer
   metadata. Because overlayfs takes a merged directory's attributes from
   the **top-most** layer containing it, such a fabricated dir would shadow
   the real metadata a lower layer declared (`/tmp` losing its 1777 sticky
   bit, `/root` opening from 0700 to 0755). At compose time the consumer
   resolves each shadowed dir's true mode/ownership from the top-most
   *non-implicit* layer in this image's chain and applies it through the
   mount — the copy-up lands in the actor's private upper; the shared pool
   is never modified. Residual gaps: directory mtimes and xattrs are not
   repaired, and a dir implicit in *every* layer of the chain keeps the
   fabricated attrs.

A bundle without a spec file is left untouched (compatibility with bundles
prepared by a pre-imagecache atelet). A zero-layer spec composes an empty
rootfs with ExtraDirs and no mount.

Actor semantics are unchanged from the untar era: the upper is wiped by
atelet's `resetActorDirs` between runs, so every run still starts from a
bit-exact, pristine image rootfs — it just costs a mount instead of an
extraction. The micro-VM path is nearly untouched: it bind-mounts the (now
overlay-composed) bundle rootfs into virtiofsd's shared dir and the guest
keeps building its own tmpfs upper, as before.

**Teardown**: `UnmountAllUnder(bundleDir)` lazily detaches every mount below
an actor's bundle directory (via `/proc/self/mountinfo`) before atelet wipes
it — called from the checkpoint cleanup path in ateom-gvisor and
`teardownActor` in ateom-microvm.

## Garbage collection (engine; the periodic loop lands next)

`gc.go` holds the eviction engine. Nothing in production calls
`Store.EvictUnused` yet — the watermark-driven periodic loop and its
flags (period, high/low watermarks, max-bytes cap, min-age, dry-run)
arrive with the next change — so cached data still only grows for now.
The one behavior `New` gains today is the startup orphan scan below.

**One pass** (`Store.EvictUnused(ctx, targetBytes, dryRun)`):

1. **Root set** (`Store.InUse`): scan every bundle's
   `rootfs-overlay.json` under the actors dir (`WithActorsDir`).
   Overlay mounts live in the ateom pods' mount namespaces, so atelet
   cannot see them in its own `/proc/mounts`; the bundle specs are
   written by atelet itself before any ateom is asked to mount and
   removed only after unmount, so they are the authoritative "actively
   mounted" set. A spec roots its image digest, each layer dir it
   names, and its *exact* layer set — the last also roots the
   multi-arch twin record and records of digestless (pre-`imageDigest`)
   specs.
2. **Refcount** layers across *all* image records, and list unrooted
   records older than min-age as eviction candidates, LRU-ordered by
   last use (the record's mtime — refreshed on every cache hit and on
   every completed layer of an in-flight pull).
3. **Evict** candidates until ~targetBytes is freed: delete the record
   (after a freshness re-check under the same lock the cache-hit path
   holds), then retire each layer the removal left unreferenced. If any
   layer must be kept — still referenced by another record, rooted by a
   spec, younger than min-age, or its retirement failed — the record is
   restored byte-exact and the image simply is not evicted this pass: a
   layer is never left on disk without a record explaining it.

The pass reaches layers **only through records**: a layer is deleted
exactly when its last referencing record goes, never by an independent
scan of the pool.

**Everything fails toward retention.** If the image records or the
bundle specs cannot be fully enumerated (an unreadable file or
directory), the pass does nothing and logs at ERROR naming the culprit:
refcounts and roots computed from partial data would retire layers that
unread records still reference or running actors still mount. Dry-run
mutates nothing at all — not even the lazy size-file backfill.

**In-flight pulls need no separate protection.** `EnsureImage` writes
the image record **before** unpacking (the way Go allocates black during
GC and containerd creates its ingest record before the bytes land), so
every layer a pull produces is referenced — and kept fresh by a
per-layer progress touch — from before it exists on disk. An interrupted
pull's record is resumable progress, not garbage: the next pull of that
digest re-fetches only the missing layers, and a pull that never resumes
ages out through ordinary LRU.

**Startup recovery.** A layer no record references can only be crash
debris (eviction interrupted between record-delete and layer-rename) or
operator damage; `New` reclaims such orphans once, at startup
(`Store.RecoverOrphans`), when no pull can be racing the scan — and
skips the scan entirely, conservatively, if any record or bundle spec
fails to read. There is no online whole-pool scan (ext4's split: bounded
recovery at mount, fsck offline).

**Deletion is two-phase.** A layer is atomically renamed to `.rm-*`
inside the layer's singleflight (one `rename(2)` — eviction can never
stall a pull), then removed asynchronously; a crash in between leaves
the dir for the startup sweep. This matters because the kernel offers no
protection here: deleting a directory that is a live overlay lowerdir in
another mount namespace succeeds silently, leaves the overlay's behavior
undefined, and doesn't even free the space until the mount goes away.

Deleting the cache root by hand (while no actors are starting) remains
safe — the store re-pulls whatever is missing.

This is Phase 2 of [#463](https://github.com/agent-substrate/substrate/issues/463);
the watermark loop, flags, and cache metrics complete it. Phase 3 adds
the control-plane surface (reporting cached digests for scheduling
affinity, and a `PreloadImage` API with expiring pins). The
layer-materializer seam is also designed so a lazy-pull backend
(eStargz/SOCI-style FUSE) can replace the untar backend later without
restructuring.

## Testing

- Portable unit tests (run everywhere, including macOS): the unpack security
  suite (traversal, symlink/hardlink escape, whiteout capture, later-entry-
  wins, read-only dirs, missing parents), spec round-trips, overlay option
  assembly (including duplicate-layer dedup), mountinfo parsing, ref
  rewriting, options. End-to-end pull tests run against an in-memory
  registry (`pkg/registry`).
- Linux-tagged tests (`bundle_linux_test.go`): unprivileged ones cover
  escape rejection and specless/zero-layer compose; root-gated ones execute
  the real `mknod`/xattr materialization and a full mount → write-isolation
  → unmount round trip (the write-isolation assertion — actor writes land in
  the bundle upper, never in the shared pool — is the key safety property).
  The root-gated ones self-skip via `roottest.Require`; CI (and
  `hack/run-root-tests.sh` locally) reruns the package under `sudo` so they
  execute.
- `tools/validate-image-cache` batch-validates that arbitrary registry
  images can be pulled, parsed, and unpacked by the store half — useful for
  sweeping large image corpora before relying on them in production.
