# Pluggable store backends: a generic key-value protocol with an in-memory reference implementation

Status: proposal. Discussion happens on the tracking issue; this document is
updated as the design settles and becomes the reference once implemented.

## Summary

`ateapi` has exactly one store backend, PostgreSQL
([`cmd/ateapi/internal/store/atepg`](../../cmd/ateapi/internal/store/atepg)),
and the wiring assumes it: [`connectStore`](../../cmd/ateapi/main.go) returns
`*atepg.Persistence`, `main.go` uses its concrete `Pool()`,
`SetPolicyManager`, `Close` and `Ping`, and the OpenFGA authorization
datastore shares its connection pool.

This proposal adds a second backend behind the unchanged
[`store.Interface`](../../cmd/ateapi/internal/store/store.go): a thin client
(`store/storekv`) that speaks a small gRPC protocol,
[`KVStore`](../../api/storekv/v1/storekv.proto), to a versioned key-value
server. The protocol has eight RPCs and knows nothing about actors, workers
or tags; the resource model stays in `ateapi`. A **purely in-memory reference
server** (`storekv/memserver`) ships with it. It is the normative
implementation of the protocol, the backend the store contract suite runs
against in CI without Docker, and a zero-dependency `ateapi` mode for local
development.

Nothing changes for existing installations: PostgreSQL stays the default and
its migration rules stay as they are.

## Motivation

**Testing and local development.** The store contract suite
([`storecontract.RunContractTests`](../../cmd/ateapi/internal/store/storecontract/contract.go))
is the only thing that defines what a backend must do, and today it can only
run against a PostgreSQL container started by
[`storetest`](../../cmd/ateapi/internal/store/storetest/storetest.go). That
needs Docker, is slow to start, and is unavailable in some developer
environments and sandboxes. An in-memory backend lets the suite run
everywhere in seconds and gives `ateapi --store-backend=memory` a mode with
no external process at all, for `kind` end-to-end tests and quick iteration.

**Scale paths beyond one PostgreSQL instance.** The roadmap lists "State
Store Scale: PostgreSQL scaling and partitioning support to enable management
of 1M+ concurrent actors", and
[`TestActorsTablePartitionable`](../../cmd/ateapi/internal/store/atepg/partition_test.go)
guards that option. This proposal is complementary: it lets an operator back
`ateapi` with a horizontally scaled store (a distributed key-value store or
database behind a thin adapter) without that store having to
re-implement 46 resource-specific methods or Substrate's PostgreSQL schema.
Partitioning `atepg` and offering a KV backend are not exclusive.

**Keeping the domain model in one place.** `store.Interface` has 46 methods
at the time of writing, and some domain logic lives inside the PostgreSQL
backend rather than above it: default backfill on read
(`defaults.Apply` in `atepg.unmarshalStored`), the immutable-field checks in
`validateUpdateTagMutation` and `validateUpdateActorTemplateMutation`, worker
allocation arithmetic in `BindActorToWorker`, and metadata stamping. The
project already removed a second backend once (`ateredis`, #940) partly
because two copies of this logic drifted. A protocol that only moves opaque
payloads under keys confines engine-specific code to key layout, CAS,
transactions, watches and leases, and makes the shared logic a prerequisite
rather than an afterthought.

**Third-party implementations.** The store package is Go `internal/`, so a
different storage engine today means a fork of the repository. A documented
protocol with a reference server and a conformance suite lets an operator or
a vendor implement `KVStore` outside the tree and run an unmodified `ateapi`
against it.

### Goals

* Serve `store.Interface` from a remote key-value server through a protocol
  small enough to implement in an afternoon against the reference server.
* Run the full store contract suite without Docker or external services, in
  CI and on a developer machine.
* Make the semantics every backend must share (defaults, immutable fields,
  allocation, metadata) live in package `store`, used by `atepg` and
  `storekv` alike.
* Keep `postgres` the default backend, with no behavior change.

### Non-goals

* Replacing or deprecating `atepg`.
* Changing `store.Interface` semantics or the public `ateapi` API.
* Sharing one store between several `ateapi` installations.
* Defining how a particular `KVStore` server is deployed or authenticated.
  The protocol is authentication-agnostic; dial options are the operator's.

## Design

### Backend selection

`ateapi` gains a `--store-backend` flag:

| Value | Backend | Durable | Use |
| --- | --- | --- | --- |
| `postgres` (default) | `atepg`, unchanged | yes | production today |
| `kv` | `storekv` client to a `KVStore` server at `--store-kv-endpoint`, store name `--store-kv-store` | as durable as the server | out-of-tree stores |
| `memory` | `storekv` client to an in-process `memserver` | no | development, `kind` E2E, CI, disposable installs |

`kv` takes explicit flags in the style of the existing Postgres and atelet
flags, with no provider-specific code in `ateapi`: `--store-kv-endpoint` is
any gRPC target (`dns:///host:port`, `unix:/path`, `passthrough:///...`),
`--store-kv-store` names the store, `--store-kv-tls-ca-file` pins the server
CA, `--store-kv-client-cred-bundle` enables mTLS through the same
`credbundle` loader the atelet dialer uses, and `--store-kv-token-file`
sends a bearer token re-read from a file (a projected Kubernetes
ServiceAccount token covers workload identity on any provider). Because the
endpoint can be a Unix socket, an operator who needs another authentication
scheme can run a local proxy sidecar, as `ateapi` already does for the
object-store plugin, without any change here.

`memory` is not durable: a restart loses every actor, worker and assignment
while snapshots and volumes persist, and two replicas would each have their
own store. It therefore requires `--store-memory-acknowledge-data-loss`,
logs a warning at startup, and `ate-setup` emits it only for its development
profile with a single replica. It is not hidden behind a build tag because a
documented, loud flag is more honest than an undocumented one.

`main.go` stops depending on `*atepg.Persistence`. `connectStore` returns
`store.Interface` plus a small lifecycle interface:

```go
// Lifecycle is what main.go needs from a backend beyond store.Interface.
type Lifecycle interface {
    // Ping is the readiness check behind /readyz.
    Ping(ctx context.Context) error
    // Close stops background work and releases connections.
    Close() error
}
```

The OpenFGA integration stops taking `*pgxpool.Pool` (see
[Authorization](#authorization)).

### Shared store helpers

Before `storekv` exists, the logic that any backend must apply moves out of
`atepg` into package `store` as exported helpers, and `atepg` is changed to
call them. This is a behavior-preserving refactor covered by the existing
tests:

* decoding with `DiscardUnknown` followed by default backfill
  (`store.DecodeStored`),
* immutable-field validation for `Tag` and `ActorTemplate` updates
  (`store.ValidateTagUpdate`, `store.ValidateActorTemplateUpdate`),
* create and update metadata stamping (`store.StampCreate`,
  `store.StampUpdate`),
* worker allocation arithmetic around `resources.AddToAllocated` for bind,
  rebind and release.

After this step the contract suite is the only coupling between a backend
and the rest of `ateapi`.

### The `KVStore` protocol

[`api/storekv/v1/storekv.proto`](../../api/storekv/v1/storekv.proto) defines
eight RPCs. The data model is one table:

```
(collection, namespace, name) -> (uid, version, secondary_key, payload)
```

* `collection` is a short string the client chooses (`actors`, `workers`,
  ...). The server treats it as an opaque partition of the key space.
* `namespace` is the atespace for scoped resources and empty for global
  ones.
* `uid` is server-assigned on create and never reused for the same name.
  `version` starts at 1 and increases by exactly 1 per write. Together they
  are the precondition every update and delete carries today. The entry,
  not the payload, carries identity: the client copies `uid` and `version`
  into `ResourceMetadata` when it decodes a payload, so whatever a payload's
  own metadata says is never authoritative.
* `secondary_key` is an optional client-chosen index key. It gives the
  server one extra lookup and listing path per entry without knowing what
  the key means.
* `payload` is the wire-encoded resource. The server never decodes it.

The RPCs:

| RPC | What it gives `store.Interface` |
| --- | --- |
| `Handshake` | Protocol and layout version check at startup, before `/readyz`. |
| `GetEntry` | Point read by key, by uid within a collection, or by secondary key. |
| `ListEntries` | Paged listing of one namespace, of all namespaces, or of entries with a secondary key, in `(namespace, name)` order. |
| `PutEntry` | Create (`must_not_exist`) or compare-and-swap update (`expected_uid`, `expected_version`), with an optional parent-exists guard and an optional change event. |
| `DeleteEntry` | Guarded delete with an optional "namespace still has children in these collections" refusal and optional cascades by key or by secondary key. |
| `Transact` | Several puts and deletes committed atomically under their preconditions. |
| `Watch` | Change events for one collection, in commit order, from now. |
| `ManageLease` | Acquire, renew and release a TTL lease. |

A failed compare-and-swap returns `ABORTED` with the current entry attached
to the status, so the client can re-run its `mutate` closure without a second
read.

### How the 46 methods map

| `store.Interface` | collection | namespace | name | secondary_key | RPCs |
| --- | --- | --- | --- | --- | --- |
| `Atespace` CRUD | `atespaces` | `""` | atespace | | `PutEntry`, `GetEntry`, `ListEntries`, `DeleteEntry` with `reject_if_namespace_non_empty_in = [actors, actor_templates, tags]` and `cascade_delete = [atespace_access_policies//<atespace>]` |
| `Actor` CRUD, `MarkActorForDeletion` | `actors` | atespace | actor | | `PutEntry` with `require_parent = atespaces//<atespace>`; `DeleteEntry` cascades to `egress_policies/<atespace>/<actor>` |
| `EgressPolicy` CRUD | `egress_policies` | atespace | actor | | `PutEntry` with `require_parent = actors/<atespace>/<actor>` |
| `ActorTemplate` CRUD, `MarkActorTemplateForDeletion` | `actor_templates` | atespace | template | | as actors |
| `Tag` CRUD, `MarkTagForDeletion` | `tags` | atespace | tag | | as actors |
| `GlobalAccessPolicy` | `global_access_policy` | `""` | `global` | | `PutEntry`, `GetEntry` |
| `AtespaceAccessPolicy` CRUD | `atespace_access_policies` | `""` | atespace | | `PutEntry` with `require_parent` |
| `Worker` CRUD, `MarkWorkerForDeletion` | `workers` | `""` | worker | | `PutEntry(emit_event)`, `DeleteEntry` with `cascade_delete_by_secondary_key_in = [worker_assignments]` |
| `BindActorToWorker`, `ReleaseActorFromWorker`, `GetWorkerAssignment`, `ListWorkerAssignments`, `FindWorkerHostingActor` | `worker_assignments` | `""` | actor uid | worker | `Transact` over the worker entry and the assignment entry; `GetEntry` by key; `ListEntries` by secondary key |
| `WatchWorkers` | `workers` | | | | `Watch` |
| `AcquireLease` | | | lease key | | `ManageLease` |

Two choices in this table are deliberate:

* An assignment is keyed by **actor uid**, with the worker as its secondary
  key. That keeps the invariant `atepg` enforces with `actor_uid PRIMARY KEY`
  in `worker_assignments`: an actor has at most one worker, and two
  concurrent binds to different workers cannot both commit.
* `Watch` does not replay existing entries. That matches `WatchWorkers`
  today: `workercache` opens the watch and then lists, and relists whenever
  the watch closes. The server sends response headers once the stream's
  cursor is fixed and the client waits for them before `WatchWorkers`
  returns, so a write that follows the call is never missed. Events reach
  a watcher once, through the stream only. The client does not re-publish
  its own committed writes to local watchers as `atepg` does since #1583:
  that compensated for a 50 ms poll, and the stream is push. It can be
  added later without a protocol change if measured latency warrants it.

### Concurrency

`atepg` runs `mutate` once under `SELECT ... FOR UPDATE`. `storekv` reads,
runs `mutate`, and writes under a compare-and-swap, retrying on `ABORTED`
with the entry the server returned. `store.Interface` already allows this
("mutate may run more than once"). The client keeps a bounded retry budget
and surfaces `ErrVersionConflict` when it is exhausted, exactly as `atepg`
does. `BindActorToWorker` is the hot path (many binds against one worker);
the follow-up proposal to pass declarative admission constraints instead of
an `admit` closure lets a server evaluate admission inside `Transact` and
removes the retries.

### Watch semantics

A conforming server keeps change events for a bounded retention and reports
it in `Handshake`. A client that falls behind is disconnected with `ABORTED`
and relists, which is the behavior `TestWatchWorkers_ClosesWhenTrimmedPastCursor`
pins for `atepg` today. Events for one collection are delivered in commit
order; events of different collections are independent. A transaction that
emits several events appends them atomically, so a watcher sees either none
or all of them, and a server trims by event count or age, never through
the middle of a commit.

### Leases

`ManageLease` carries the TTL and a holder token. The server's clock decides
expiry. The client renews at a third of the TTL and cancels the lease
context when a renewal fails, as `atepg`'s `renewLeaseLoop` does. Expiry is
enforced by the server at acquire time, not by background cleanup, so a
server that only reclaims storage lazily is still correct.

### Authorization

OpenFGA tuples are written today inside the same PostgreSQL transaction as
the `AccessPolicy` row, through a datastore adapter in
[`cmd/ateapi/internal/authz/datastore.go`](../../cmd/ateapi/internal/authz/datastore.go)
that overrides parts of OpenFGA's PostgreSQL store, and the only tuples ever
stored are the `(role, member)` pairs from `GlobalAccessPolicy` and
`AtespaceAccessPolicy`. At runtime `authz` calls exactly one OpenFGA method, `Check`; `Read` and
`Write` are used only to reconcile tuples after a policy write. Every
structural edge (`parent_global`, `parent_atespace`) and every system role
is already supplied per `Check` as a contextual tuple.

This proposal **derives the remaining tuples from the policies** instead of
storing them. On `Check(user, relation, object)` the authorizer reads
`GlobalAccessPolicy` and, for an atespace-scoped object, that atespace's
`AtespaceAccessPolicy` (one or two point reads, cacheable per request),
turns the bindings **for the calling user only** into contextual tuples,
and runs `Check` against an OpenFGA server that holds just the model in its
in-memory datastore. Filtering to the caller is exact because members are
plain `user:<id>` values, and it keeps the request far below OpenFGA's cap
on contextual tuples per request.

What this removes: the tuple tables and the model bootstrap from the store,
the datastore adapter that joins tuple writes to the policy row's
transaction, and the "policy row and tuples must commit together"
invariant, because there is one source of truth. A policy write is one
entry write on any backend; revocation takes effect on the next `Check`.
Relations the model defines but nobody writes today (`scheduled_worker`,
`host_node`) can be derived the same way from the assignment and worker
entries when they are needed.

The alternative considered is an OpenFGA datastore implemented over
`store.Interface`, with tuples, model and changelog as collections and
`Transact` for atomicity. It preserves today's structure but is several
hundred lines, needs more round trips per reconcile, and wants two index
paths per tuple (by object for `ReadUsersetTuples`, by user for
`ReadStartingWithUser`) where the generic entry has one secondary key. A
cache refreshed from a watch was also considered and rejected because it
makes revocation eventually consistent across replicas.

### Layout version

A relational schema has migrations; a key-value layout has none, yet
resources have been renamed and removed before (`ActorSnapshotTag` to `Tag`
in #1491, snapshots removed in #1417). Each store carries its layout
version as an ordinary entry in a reserved `_meta` collection, so it is
data-local: it travels with backups, restores and copies, and any `KVStore`
server supports it without knowing what it means. `Handshake` reports
`current_layout_version` by reading that reserved key, which any server can
do, so the client learns protocol and layout compatibility in one round
trip; a server that wants to enforce the boundary compares an
`x-substrate-layout-version` request header against the same key. A client
that is behind refuses to serve (which halts a rolling update on its first
new pod, with the old pods still serving). A client that is ahead runs an
ordered layout migration under `ManageLease`, writing each step with a
compare-and-swap on the `_meta` entry, before reporting ready: the
key-value equivalent of the PostgreSQL migration step in
[version-skew-policy.md](../version-skew-policy.md).

### The reference server

`pkg/storekv/memserver` is a Go package with no dependencies beyond the
generated protocol: one mutex, maps keyed by `(collection, namespace,
name)`, per-collection uid and secondary-key indexes, a per-collection event
ring with a retention bound and a trim watermark, leases with an injectable
clock. It can be served over gRPC (loopback or `bufconn`) or called
in-process. It is deliberately simple; its job is to be obviously correct and
to define the semantics that `storekv` relies on.

It lives under `pkg/`, with the differential harness
`pkg/storekv/conformance`, so that anyone implementing `KVStore` outside
this repository can import both. The `storekv` client and `storecontract`
stay under `cmd/ateapi/internal` because they depend on `store.Interface`
and the `ateapi` types; to give outside implementers the full suite anyway,
the in-tree contract test runs against the endpoint named by
`STOREKV_CONFORMANCE_ENDPOINT` when it is set, and against `memserver`
otherwise. A server is conformant when:

1. `storecontract.RunContractTests` passes through `storekv` against it, and
2. the differential suite (recorded request sequences replayed against both
   servers, results compared) agrees with `memserver`.

### Testing and CI

* `storecontract.RunContractTests` runs against `storekv` + `memserver` in
  the normal `go test ./...` with no Docker.
* The suite grows to cover what only `atepg`'s own tests cover today and
  every backend must honor: `AccessPolicy` lifecycle and cascade, concurrent
  lease takeover and expiry, and watch closure past the trim point.
* `memserver` gets unit tests for retention, trim, lease expiry and
  transaction atomicity under concurrent clients.
* The PostgreSQL contract test keeps running as it does now.

### What a prototype showed

A prototype of the client and `memserver` passes `RunContractTests` (127
subtests, under the race detector) over an in-process connection. It
settled the points above about identity and the watch cursor, and found two
things the suite had not made explicit:

* Subresources without defaults. `defaults.Apply` is defined for the seven
  top-level resources only, so a backend decodes `ActorAssignment` without
  it. The shared decode helper takes that into account.
* `delete_time` on a plain delete. The suite requires it on a deleted
  `Atespace` and `EgressPolicy` (set at delete time) but expects a deleted
  `Worker` or `Tag` to carry only what a prior mark wrote. The prototype
  mirrors `atepg` kind by kind; a follow-up can make the contract uniform.

### Rollout

The change is a sequence of small PRs, each useful on its own and each
leaving `postgres` untouched:

1. Backend seam: `--store-backend`, `Lifecycle`, `main.go` off
   `*atepg.Persistence`; authorization derived from the policies as above.
2. Shared helpers moved into package `store`; `atepg` calls them.
3. `KVStore` proto, `storekv` client, `memserver`, contract suite in CI.
4. Layout version, contract-suite additions, `ate-setup` support, docs
   (`upgrade.md`, `version-skew-policy.md`).

## Alternatives considered

**Another SQL backend (MySQL, SQLite).** Each one re-implements 46 methods,
a transactional outbox and leases, and carries its own migrations. That is
the `ateredis` experience again. A key-value protocol moves that work into
one client and leaves engine-specific code to key layout and CAS.

**Making `atepg`'s SQL portable.** The backend relies on PostgreSQL
features on purpose: `xid8` ordering for the outbox, partitioned unlogged
tables, `FOR UPDATE`, advisory locks. Removing them would cost the features
that make it good.

**A typed gRPC protocol mirroring `store.Interface`.** Forty-six RPCs with
resource-specific messages. Every new resource or lookup is a protocol
change on both sides, and every server re-implements the domain rules. The
generic protocol absorbs new resource kinds, fields and single-key lookups
with a client-only change.

**Do nothing; partition PostgreSQL.** Still worth doing for `atepg`. It
does not give the project a Docker-free contract suite, a zero-dependency
development mode, or a path to stores that are not PostgreSQL.

## What the protocol does not absorb

So that expectations are right: adding a field, a resource kind, a
single-key lookup or a watched collection is a client-only change. A new
query shape (filter or sort by a non-key field), a new uniqueness constraint
beyond uid, or a payload larger than a server's value limit needs a protocol
or server change. A rename or reshape needs a layout migration. The
maintenance moves from server schema to the `storekv` adapter; it does not
disappear.

## Questions for review

These are the positions this document takes where another choice was
reasonable. Each is open to discussion on the tracking issue.

1. **Authorization: derive tuples from the policies** rather than an OpenFGA
   datastore over the store. Smaller, backend-neutral, one source of truth;
   it does move tuples out of storage, which upstream chose to have.
2. **`kv` credentials: explicit flags** (CA file, mTLS credential bundle,
   bearer token file) plus Unix-socket endpoints for proxy sidecars, rather
   than a pluggable credential-provider registry. Go has no plugins, so a
   registry would still mean compiling providers in.
3. **`memory` is allowed outside development** behind an explicit
   data-loss acknowledgement flag and an installer that only emits it for
   single-replica development profiles, rather than hidden behind a build
   tag.
4. **Layout version lives in a reserved `_meta` entry**, and `Handshake`
   reports it by reading that key, rather than server-owned state behind an
   additional RPC.
5. **`memserver` and the conformance harness live under `pkg/`** so other
   implementations can import them; the client and the contract suite stay
   internal, with an environment variable to point the contract suite at an
   external endpoint.
