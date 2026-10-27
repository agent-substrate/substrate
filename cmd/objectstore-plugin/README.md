# objectstore-plugin

The in-tree object-store plugin. It serves the `objectstore.v1` API over a Unix
socket as a sidecar: `node` next to atelet, moving snapshot files and sandbox
assets, and `control` next to ate-api-server, copying and deleting whole
snapshots. `ATE_STORAGE_BACKEND` picks GCS, the default, or S3. See
`docs/architecture.md` for where it sits.

## Per-actor snapshot credentials on GCS

On GCS, `control` mode also serves a credential exchange, when
`--exchange-listen` is set: the node plugin presents the JWT atelet handed it
with a transfer and receives a GCS access token limited to that actor's
snapshot prefixes, read-write on its own and read-only on the tag it was
created from. The exchange is private to this plugin, defined in
`internal/proto/objectstoreexchangepb`.

On the node side, `node` mode on GCS dials that exchange, at
`--exchange-address` through the ate-api-server Service, verified with
`--exchange-ca-file` and `--exchange-server-name`. Each `FetchSnapshot` or
`UploadSnapshot` runs on a storage client built with the actor's token. The
token and the client are cached under the JWT they were obtained for, never
under a claim read out of it, while at least twenty minutes remain, so the
transfers that follow one another for an actor share connections. A client
that leaves the cache is not closed, its connections close once idle. A request
GCS refuses with a cached token is retried once with a token the exchange
re-derives, and a second refusal is `PERMISSION_DENIED`. A transfer without
`actor_jwt` is `INVALID_ARGUMENT` on GCS. S3 ignores the field and transfers
with the pod's own credentials.

The JWT is the one ate-api-server mints for atelet through
`WorkerService.MintWorkerActorJwt`, with the audience named on the
`actor_jwt` proto field. Its shape is defined in `internal/actoridjwt`, and
the exchange accepts only the worker-for-actor kind, never an actor's own
token. Its public keys come from the `jwks` key of the `actor-id-jwt-pool`
Secret, read through `oidcjwt.FileKeySource`.

Three packages behind the exchange know no storage provider and are what a
provider's own plugin binary reuses. The rest is this binary's.

Reusable by every provider:

- `internal/oidcjwt`: verifies the JWT against the `jwks` file.
- `internal/objectstoreplugin/actorlookup`: fetches the actor over the
  loopback, refuses a recreated one.
- `internal/objectstoreplugin/actorscope`: turns the actor's status into
  grants, read-write on its own prefix, read-only on its tag.

This binary's, GCS and S3:

- `internal/objectstoreplugin`: the node, asset and control plugins behind the
  socket, and the `Credentials` seam a transfer gets its storage client from,
  the pod's own on S3.
- `internal/proto/objectstoreexchangepb`: the private wire format between the
  node and control sidecars.
- `internal/objectstoreplugin/exchange`: the handler, and the `Issuer` interface
  that is the seam between its backends.
- `internal/objectstoreplugin/gcsboundary`: grants to Credential Access Boundary
  rules.
- `internal/objectstoreplugin/gcsissuer`: the GCS `Issuer`, STS exchange and
  token cache.
- `internal/objectstoreplugin/exchangeclient`: the node side on GCS, the
  exchange call and the cache of tokens and storage clients per JWT. The
  `Credentials` interface in `internal/objectstoreplugin` is its seam.
- `cmd/objectstore-plugin`: the binary and its wiring.

S3 has no issuer: its node plugin ignores the JWT and transfers with the
pod's own credentials, and the exchange answers `UNIMPLEMENTED` there. A
provider other than these two brings its own binary in this repository, with
its own exchange handler and proto shaped for its credential, its own issuer
and storage client, and imports the three reusable packages. A binary outside
the repository cannot, since they are under `internal/`.

## Upload memory

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
for S3. Startup rejects smaller values. Each upload reserves for the path it
takes: on GCS, a file split into range parts reserves only those parts, sparse
files with less than `8Mi` of populated data scale their compression chunk
buffers down to `1Mi`, and an object whose stream is bounded below one request
(`64Mi`) sizes both its lookahead and single-request chunk buffer to that
bound instead of the composite-upload buffers. Unbounded streamed objects still
reserve for a composite upload, even when they ultimately fit in one request.
Raise the budget to trade memory for more simultaneous uploads.

The budget is **not a process memory limit**. The Go runtime, garbage awaiting
collection, connections, downloads, and asset transfers need additional memory.
Size the sidecar's Kubernetes memory request and limit for those costs as well;
the existing `128Mi` request is not a bound on its usage. If setting `GOMEMLIMIT`,
leave headroom below the container limit for memory outside the Go heap.

A manifest included with data files is uploaded only after all those files
succeed. The existing two-RPC flow (data, then manifest) also uses the same
budget; a manifest small enough for one request reserves only that request's
buffers.

### Validation

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
