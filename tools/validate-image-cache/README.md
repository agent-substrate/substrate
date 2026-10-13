# validate-image-cache

Batch-validates that OCI images can be **pulled, parsed, and unpacked** by
`internal/imagecache` — the atelet-side half of substrate's node-local image
cache. For every ref it exercises the exact production pull path
(`Store.EnsureImage`: registry pull, parallel per-layer streaming unpack,
whiteout capture, record write) and reports per-image results as CSV.

It does **not** mount overlays or run workloads: the consumer half is
Linux-only and privileged, and is covered by the `bundle_linux_test.go`
unit tests and the e2e suites instead. This tool answers one question at
corpus scale: *"can every image in this repository be loaded into the
cache?"* — the class of failure that only shows up on real images
(layer tars without parent-dir entries, duplicated identical layers, exotic
whiteouts, oversized layer counts, ...).

## Prerequisites

- Application-default credentials with read access to the registry
  (`gcloud auth application-default login`). gcr.io / pkg.dev registries are
  authenticated with them; other registries are pulled anonymously. This is
  *not* how atelet authenticates — it borrows its node's kubelet credential
  provider plugin, which does not exist on a developer's machine — so a
  corpus that validates here can still fail to pull on a node. Note also
  that a repo readable by *your user* (e.g. via a `domain:google.com` grant)
  is not necessarily readable by a *service account* — validate with the
  identity that production will use.
- Disk: unpacked layers are 2–3× their compressed size. The tool bounds
  usage by asking the cache's own eviction engine to reclaim space when the
  cache volume's free space drops below `--min-free-gb`, but give it room
  to breathe (tens of GB minimum).
- Runs anywhere Go runs, including macOS (unpack is pure file I/O). Caveat:
  a case-insensitive filesystem (default macOS APFS) unpacks case-colliding
  paths slightly differently than Linux — silently, not as an error. Linux
  is the reference environment for a final sign-off run.

## Generating a refs file

One image ref per line; digest refs are recommended (they validate cache
hits offline and are immune to tag moves):

```bash
gcloud artifacts docker images list REPOSITORY \
  --format="value[separator='@'](package,version)" > refs.txt
```

## Running

Random sample of 500 images (reproducible via the seed):

```bash
go run ./tools/validate-image-cache \
  --refs-file refs.txt \
  --sample 500 --seed 1 \
  --cache-dir "$HOME/imagecache-validate" \
  --out results.csv
```

Full corpus, pulled for the platform your nodes actually run:

```bash
go run ./tools/validate-image-cache \
  --refs-file refs.txt \
  --cache-dir /cache/imagecache \
  --platform linux/amd64 \
  --parallel 4 --min-free-gb 40 \
  --out results.csv
```

### Flags

| Flag | Default | Meaning |
|---|---|---|
| `--refs-file` | (required unless `--evict-all`) | file with one image ref per line |
| `--cache-dir` | (required) | cache root; reused (and cache-hit) across runs |
| `--sample` | 0 (= all) | validate a random sample of N refs |
| `--seed` | 1 | sampling seed; same seed + file ⇒ same sample |
| `--out` | `validate-results.csv` | results CSV, written incrementally |
| `--parallel` | 3 | images validated concurrently (each pulls up to 4 layers in parallel) |
| `--timeout` | 20m | per-image timeout |
| `--min-free-gb` | 150 | reclaim the shortfall below this free-space floor via the eviction engine |
| `--evict-idle` | 10m | eviction min-age: layers and records younger than this are never evicted (minimum 1m on a node with an actors dir); must be far below disk-fill time on small disks |
| `--evict-all` | false | evict everything evictable and exit (mutually exclusive with `--refs-file`); rooted images and anything younger than `--evict-idle` survive |
| `--force` | false | allow eviction on a node with an actors dir (required for both modes there) |
| `--platform` | `linux/amd64` | image platform to pull |

## Output and rerunning

`results.csv` columns: `ref, digest, layers, seconds, error` (empty error =
pass). Progress logs every 10 images with an ETA; failures are logged
immediately as `FAIL [n/total]` and never stop the run. Exit code is
non-zero if any image failed.

Reruns are cheap by design: completed layers stay in the cache (and even an
interrupted pull keeps every layer that finished), so re-running the same
sample — or just the failed refs — mostly re-validates from local disk.
Eviction never removes layers or records younger than `--evict-idle`, so
this process's own in-flight images are not raced; on a small disk with
high throughput, set it well below the time the corpus needs to fill the
disk, or nothing will be evictable while it fills.

## Live nodes

On a node with an actors dir, **both modes require `--force`** and
`--evict-idle` is floored at 1m. The engine's locks are per-process, so a
run there is not synchronized with the node agent's own GC and pulls.
Bundle-spec rooting protects placed actors' images and min-age protects
fresh pulls, but a layer that ages past `--evict-idle` and is reused
mid-pass can be evicted out from under an actor: a not-yet-mounted start
fails once and heals on re-pull; an already-mounted actor can take I/O
errors from a removed lowerdir. Note that refs mode evicts too — below
`--min-free-gb` (default 150) it flushes repeatedly, not once.

## Scaling out

The tool is embarrassingly parallel across a refs file: to sweep a large
corpus quickly, split the refs into shards (sorted by image name, so image
families keep their shared base layers on one machine) and run one process
per VM near the registry region, e.g. via a GCE startup script that fetches
a shard from GCS, runs the tool with `--cache-dir` on a local SSD, uploads
its CSV, and powers off. In-region, a c3-standard-4 with a local NVMe SSD
sustains roughly 10–15 images/min at `--parallel 4`.
