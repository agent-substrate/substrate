# Object-store plugin upload memory

The node plugin shares one upload memory budget across all snapshot requests.
`--upload-memory-budget` accepts a Kubernetes quantity and defaults to `512Mi`.
Uploads wait before allocating compressor or backend buffers. Waiting requests
cancel with their RPC, and failed uploads release their reservations after their
compression and upload workers stop.

Reservations cover parallel compression input buffers, queued compressed frames,
zstd state, and the storage client's upload buffers. GCS range uploads reserve
serial encoders and resumable-upload buffers together. Large files reduce their
compression or range parallelism to fit the budget; file size does not need to fit
in memory. The sparse format and download readers are unchanged.

With the current buffer sizes, the minimum budget is `384Mi` for GCS and `224Mi`
for S3. Startup rejects smaller values. The accounting is conservative: GCS
reserves for a composite upload even when an object ultimately fits in one
request. Raise the budget to trade memory for more simultaneous uploads.

The budget is **not a process memory limit**. The Go runtime, garbage awaiting
collection, connections, downloads, and asset transfers need additional memory.
Size the sidecar's Kubernetes memory request and limit for those costs as well;
the existing `128Mi` request is not a bound on its usage. If setting `GOMEMLIMIT`,
leave headroom below the container limit for memory outside the Go heap.

A manifest included with data files is uploaded only after all those files
succeed. The existing two-RPC flow (data, then manifest) also uses the same budget.

## Validation

```sh
go test -race ./pkg/objectstorage ./internal/objectstoreplugin ./cmd/objectstore-plugin -count=2
go test ./pkg/objectstorage -run '^$' -bench '^BenchmarkConcurrentSnapshotUploads$' -benchtime=1x -count=1
```

The tests cover ten overlapping snapshots with eight data files each, canceled
waiters, source and destination failures, retry after failure, manifest ordering,
large sparse sources, and slow uploads through the real S3 upload manager and GCS
writers. Their network peers are simulated; they do not require cloud credentials.

The benchmark compares admitting all 80 tiny-file uploads with sharing the
budget, using a destination that stalls for 20 ms. It samples peak heap growth
and reports files per second. It isolates compression backpressure; it is not a
measurement of cloud throughput or the sidecar's total resident memory.
