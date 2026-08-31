# Rust dynamic modules in Agent Substrate: where they pay, where they don't

Grounded on commit `69828945` ("Bump Go to 1.27"). Every path below was read on disk at that commit. Envoy is pinned to `envoyproxy/envoy:v1.39-latest` (`manifests/ate-install/atenet-router.yaml:270`, `manifests/ate-install/atenet-egress.yaml:229`); the Rust SDK rev that matches it is `envoyproxy/envoy` `b579d07d3ad7ee11d32b105e91a5a39ad24718d7` (= v1.39.1), already pinned by the untracked prototype at `demos/envoy-rust-dynamic-module/rust-module/Cargo.toml`.

**One-line answer to "ext_proc, and what else?"** The ext_proc hop is real but it is the *smaller* half of the ingress win. The larger half is that `cmd/atenet/internal/router/ingress/resumer.go` has no cache, so every routed request becomes an mTLS gRPC round trip **and a PostgreSQL `SELECT`** on a single-instance database. The same shape repeats on egress, where it is worse: `cmd/atenet/internal/router/egress/egress.go:168` calls `GetActor` on every CONNECT with no cache, singleflight, or TTL, and the code carries its own TODO admitting it (`egress.go:167`). Rust modules are the right vehicle for both, but the cache is the win and Rust is the delivery mechanism — not the other way round.

---

## 1. Ranked opportunities

Ranked by impact × confidence. "Est." figures are labeled; measured figures cite the harness.

### 1a. Replaces existing work

| # | Opportunity | Envoy extension point | Code it replaces | Est. saving | Conf |
|---|---|---|---|---|---|
| 1 | **Ingress: actor→worker TTL cache in-module, ext_proc hop deleted** | HTTP filter, replacing `envoy.filters.http.ext_proc` on `ingress_http`/`ingress_https`/`main_internal` | `xds.go:1045-1051` (filter), `extproc/extproc.go:88-121` (stream), `ingress/ingress.go:86-193`, `ingress/resumer.go:166-279` | Measured Go ladder: 291-322 µs → 0.57-0.61 µs warm lookup. Honest apples-to-apples for *hop removal alone*: 291-322 µs → 127-157 µs (~2×). The 300× belongs to the **cache**, not to Rust. Control-plane: `N`→`M/T` ResumeActor RPCs (50× at N=1000, M=200, T=10s) | High |
| 2 | **Egress CONNECT: cache the `GetActor` verdict + delete the ext_proc hop** | HTTP filter on the egress HCM, replacing `atenet-egress.yaml:120-147` | `egress/egress.go:94-141`, `:160-193` (GetActor per CONNECT), `:198-215` + `:229-284` (XFCC → PEM → chain verify) | Removes one cross-pod mTLS gRPC + one Postgres `SELECT` **per outbound TCP connection**, plus ~35-40 µs XFCC/protobuf and ~62-99 µs chain verify per CONNECT | High |
| 3 | **CONNECT tunnel: same module on `main_internal`** | HTTP filter on the `main_internal` HCM (`xds.go:851`) | `xds.go:1045-1051` via `buildHcm("main_internal", false)` | Same per-request saving as #1, applied to every request *inside* a tunnel (`docs/architecture.md:355-357`: "each request inside a long-lived tunnel still resumes the Actor") | High |
| 4 | **Per-request observability tax in the Go sidecar** | n/a — deleted with the hop; Envoy-native stats replace it | 4× `slog.InfoContext` (`ingress.go:87,129,145,159`), 2 OTel spans (`ingress.go:94`, `resumer.go:167`), otelgrpc server handler (`extproc.go:82`), histogram (`extproc/metrics.go:63-73`), QueryRecorder (`extproc/record.go:50-64`) | Measured ~4.3-6.4 µs, ~19-22 allocs, ~1.5 KB/req; +2.9-3.2 KB and +39 allocs from otelgrpc; ~1049-1165 B of JSON per request (~11 MB/s at 10k rps) | High |
| 5 | **Header flatten + protobuf wire + CEL filter-state transport** | HTTP filter (`get_header_value`, `get_filter_state_bytes`) | `extproc/metadata.go:48-79`, `xds.go:1026` (`RequestAttributes`), `xds.go:876-906` (`set_filter_state` on the two plain-ingress listeners only) | Measured: 1407 B wire, 6.5-8.3 µs / 4.6 KB / 78 allocs unmarshal, +2.4 µs / 3.1 KB / 26 allocs map flatten. Subsumed by #1 | High |
| 6 | **Parking lot / circuit-breaker coupling dissolved** | HTTP filter `StopIteration` + a module-owned timer | `ingress/parking.go:29-43`, `xds.go:529-534` + `config.go:182-190` (breaker = 2× lot, 1024 floor), `dataplane.go:72-76` (MessageTimeout = budget+5s) | Removes 1024 grpc-go stream goroutines and the two coupled knobs. Does **not** remove the decode-stopped filter chains or the coalescing map | High |
| 7 | **Aggregating access logger** | `envoy.access_loggers.dynamic_modules` (compiled into stock 1.39.1; xDS msg already vendored) | `xds.go:1039`, `:1067-1074`, `:914-923` — 5 bare `StdoutAccessLog`s, no `filter:` anywhere in the repo | O(1) log volume per hot actor instead of O(requests). Note Tier 1 (below) is 90% of this and needs no Rust | Medium |

### 1b. Enables new capability

| # | Opportunity | Envoy extension point | Why it's new | Conf |
|---|---|---|---|---|
| 8 | **Parse `ActorIdentity` at the TLS handshake, publish to filter state** | `envoy.tls.cert_validator` dynamic module (`transport_sockets/tls/cert_validator/dynamic_modules`; SDK `cert_validator.rs`; `do_verify_cert_chain` gets `certs: &[&[u8]]`) | Deletes XFCC, the percent-encoding, the PEM round trip, and the duplicated verify — the exact capability gap that forced ext_proc (`egress.go:60-63`). Runs once per connection | High |
| 9 | **Per-request MITM egress policy in-process** | HTTP filter substituted for `#ATE_MITM_EXTPROC_FILTER` (`atenet-egress-with-sdsmint.yaml:356`, `:452`) | The slot is pre-cut and inert today; filling it with ext_proc adds a gRPC round trip **per tunnelled HTTP request** (the hottest amplification in the stack). A module fills it at zero hops | Medium |
| 10 | **Per-actor egress destination allowlist / quotas / byte accounting** | HTTP filter on the egress HCM | `egress.go:135-140` lets every authenticated CONNECT proceed with **no destination check at all**. `GetActorEgressPolicy` exists (`controlapi/egress_policy.go:57`) with **zero enforcement consumers** in-tree | Medium |
| 11 | **Speculative resume from the ClientHello SNI** | Network filter + `envoy.filters.listener.tls_inspector` on `ingress_https` only | Overlaps the resume with the handshake. `get_requested_server_name` is backed by `requestedServerName()`, filled pre-handshake; `initializeReadFilters()` runs `onNewConnection()` before `onConnected()` | Medium |

### 1c. Bank these first — no Rust required

| Change | Where | Why it comes first |
|---|---|---|
| Drop `--component-log-level upstream:debug,router:debug,ext_proc:debug` | `atenet-router.yaml:275-276` | The only such flag in the repo. The benchmark harness already strips it (`benchmarking/automation/testtypes/nighthawk_ingress.py`, docstring `:118-122`), i.e. **the benchmarked config is not the shipped config**. Cheapest latency fix in the tree |
| TTL cache in front of `apiClient.ResumeActor` / `GetActor`, in Go | `ingress/resumer.go:166`, `egress/egress.go:168` | Captures nearly all of the control-plane win with none of the unsandboxed risk. Lets the cache-correctness review happen *before* the Rust review |
| `ext_proc forward_rules.allowed_headers` at both sites | `xds.go:998-1035`, `atenet-egress.yaml:120-147` | 1407 B → 329 B, 78 → 33 allocs. Must list pseudo-headers, `traceparent`, `tracestate`, and `x-forwarded-client-cert` — dropping xfcc fails closed (403), dropping traceparent fails **open** and silently breaks traces |
| `GetCertificate: credbundle.Loader(...)` | `internal/atunnel/ingress.go:159-161`, deleting `:243-253` | 67-79 µs / 105 allocs → 2.6-2.9 µs / 2 allocs per worker TLS handshake. `credbundle.Loader` (`internal/credbundle/credbundle.go:38-43`) already implements exactly the inode+mtime cache; atunnel is the *one* server-side `GetCertificate` in the repo that opted out |
| Delete the re-parse at `sdsmint/minter.go:137`; replace the `RefreshingPool` mutex (`localca.go:129`) with an atomic/ArcSwap handle | `cmd/atenet/internal/sdsmint/`, `internal/localca/` | 10.3 µs / 3256 B / 38 allocs per mint is pure waste on a handshake-blocking path; the mutex holds an `os.ReadFile` + Unmarshal once a minute while every concurrent handshake waits |
| Add a `RetryPolicy` / consume `X-Ate-Assignment-Stale` | `xds.go` (no `RetryPolicy` exists anywhere), `internal/atunnel/ingress.go:44-46` | The signal is produced and thrown away today. It is the prerequisite for any cache |

### 1d. Refuted — do not pursue

- **sdsmint as a dynamic module.** No cert-selector or secret-provider module point exists. Under `extensions/transport_sockets/tls/cert_selectors/` the only implementation is `on_demand_secret`, driven solely by an xDS `config_source`; the TLS module hook that exists validates *peer* certs. An HTTP filter runs after the handshake the leaf is parked on. The remaining wins there (drop the re-parse, kill the mutex, rewrite the sidecar's per-secret goroutine trio) need no module.
- **A network filter for CONNECT authority or for reading the peer certificate.** `envoy.filters.network.dynamic_modules` exists but operates on raw TCP; its SSL callbacks are the same CEL subset (`abi.h:318-330`: subject, DNS SAN, URI SAN, SHA-256 digest) — no raw DER. It cannot see the CONNECT's `:authority` and cannot replace `set_filter_state`.
- **The `dns_gateway` example mapping onto substrate.** Envoy is nowhere in actor-name resolution (`grep` for dns_filter / UDP listeners in `xds.go` and `atenet-router.yaml`: zero hits), and on egress `atunnel` always sends an IP:port, which the manifest itself states: "atunnel always sends an IP:port, so DNS resolution is effectively a passthrough" (`atenet-egress.yaml:179-180`). The `egress_dns_cache` never resolves a hostname.
- **A worker-side Envoy.** There is no Envoy in a worker pod (`workerpool_apply.go:180` builds exactly one container). Adding one per worker works directly against the premise of the pool. The code's own plan — route worker ingress through the already-shipped mTLS CONNECT listener at `:8443` (`internal/atunnel/ingress.go:302-338`) and move protocol selection into the router's route config — is cheaper, needs no Rust, and the missing piece is router-side only (`ingress/ingress.go:157` hardcodes `workerIP:443`).
- **`upstream_http_filters` for per-attempt re-resolution.** The only `on_upstream_*` symbols in the built SDK are the HTTP-to-TCP bridge. Unverified; do not promise it.

---

## 2. #1 in detail: replacing the ingress ext_proc hop

### 2.1 The old path, step by step

A request to `agent-1.team-a.actors.resources.substrate.ate.dev` on `ingress_http` (`xds.go:1145`) or `ingress_https` (`xds.go:1208`):

| # | Step | Where | Cost |
|---|---|---|---|
| 1 | HCM decodes headers | `xds.go:1060-1097` | C++, not measured |
| 2 | `set_filter_state` evaluates `%REQ(:AUTHORITY)%` into `dev.ate.authority`, `SharedWithUpstream: ONCE` | `xds.go:876-906`, prepended at `:1042-1044` | 1 formatter eval + 1 `StringAccessor` held for the stream — est. low single-digit µs |
| 3 | ext_proc opens a **fresh HTTP/2 bidi stream** to `ate-cluster` (STATIC, 127.0.0.1:50051, 250 ms connect timeout) | `xds.go:1045-1051`, cluster `xds.go:521-570`, `:216` | ~11.6 KB and ~150 allocs per request are pure stream setup/teardown (measured by `go test -overlay` A/B vs. a reused stream) |
| 4 | Envoy evaluates the CEL attribute `filter_state['dev.ate.authority']`, builds a `Struct`, marshals a `ProcessingRequest` with **every** header (no `forward_rules`) | `xds.go:1026`, `:1014-1021` | 1407 B wire for an 18-header request; Go-side unmarshal 6.5-8.3 µs / 4.6 KB / 78 allocs |
| 5 | `Server.Process` Recv/Send loop — one iteration per stream; otelgrpc server span | `extproc/extproc.go:88-121`, `:82` | +2.9-3.2 KB, +39 allocs |
| 6 | `NewRequestMetadata` flattens every header into a lowercased `map[string]string` | `extproc/metadata.go:48-79` | 2.38-2.49 µs / 3136 B / 26 allocs (18 headers) |
| 7 | Handler: log line, OTel Extract + 2 spans, parking admission | `ingress.go:87`, `:93-95`, `:124`, `resumer.go:167` | ~0.95 µs / 712 B unsampled (3.2 µs sampled); parking is unconditional |
| 8 | `ResumeActor`: `actorRef.String()`, singleflight `DoChan` (+1 goroutine), `WithTimeout`+`WithoutCancel` (+1 runtime timer), `ExponentialBackoffWithContext` | `resumer.go:171-243` | ~670-920 ns + 568 B + 1 goroutine |
| 9 | **Cross-pod mTLS unary gRPC to ate-apiserver** | `router.go:195-213`, `internal/ateapiauth/client.go:58-85` | Loopback-insecure leg measured at ~132 µs; production = real RTT |
| 10 | ate-apiserver interceptor chain: `proto.Clone` ×2 + protoreflect walk + a ~1 KB JSON "Handle RPC" line | `main.go:224-229`, `ateinterceptors.go:37-59`, `:116-125` | ~7-12 µs / 3.2 KB / 56 allocs |
| 11 | `workflow_resume.go:89` → `store.GetActor` → `SELECT proto FROM actors WHERE atespace=$1 AND name=$2` + `proto.Unmarshal`, then early return at `:93-95` | `atepg.go:615-628`, PK at `schema.go:37-45` | Est. 0.3-2 ms on a **single-instance** Postgres (`postgres.yaml:95`) — not measured here |
| 12 | Response: dynamic metadata `{envoy.filters.listener.original_dst: {local, port}}` + header mutation | `ingress.go:51-58`, `:162-183` | 120-124 B `ProcessingResponse` |
| 13 | 3 more slog lines, route-duration histogram (4 string attrs), QueryRecorder ring write | `ingress.go:129,145,159`, `extproc/metrics.go:63-73`, `record.go:50-64` | ~4-6 µs, ~1049-1165 B of JSON |
| 14 | ORIGINAL_DST cluster reads `MetadataKey` and dials `workerIP:443` | `xds.go:745-786`, `:753-759` | — |

**Measured totals** (Apple M5 Pro, go1.27, loopback, trivial fake ateapi; repo-untracked scratch benchmark):

| Configuration | ns/op | B/op | allocs/op |
|---|---|---|---|
| Full path (ext_proc stream + real gRPC ateapi) | 291-322 µs | 33.0-34.6 KB | 496-498 |
| ext_proc hop only (ateapi in-process) | 159-196 µs | 22.8-23.0 KB | 336 |
| ext_proc hop with a **reused** stream | 184-234 µs | 11.0-11.5 KB | 186 |
| Handler + real gRPC ateapi, **no ext_proc transport** | 127-157 µs | 15.5-16.1 KB | 226 |
| Handler body alone | 6.0-6.8 µs | 4.6 KB | 68 |
| Warm map lookup (parse + probe) | 0.57-0.61 µs | 221 B | 3 |

Read those as **allocation ratios and round-trip counts, not production latency**. Note in particular that stream reuse buys ~11.6 KB and ~150 allocs but **no latency** — the round trip dominates. Roughly 60% of the ~34 KB is the router pod; the rest is the benchmark's own gRPC client (82 of ~476 profiled allocs) and ate-apiserver. Conversely the harness *under*-counts production: it suppresses all four log lines, passes a nil histogram (`extproc/metrics.go:64-66` early-returns), leaves `ParkedRequestConfig{}` so `Max=0` skips admission and all three parking metrics (`parking.go:95`, `:164-166`), and dials ateapi insecure.

### 2.2 The new path

**Cache hit:**

| # | Step | Cost |
|---|---|---|
| 1 | HCM decodes headers | unchanged |
| 2 | `on_request_headers`: `get_filter_state_bytes(b"dev.ate.authority")` — borrowed slice, no copy, no CEL, no `Struct` | est. ~100 ns |
| 3 | `ParseActorDNSName` equivalent → `(atespace, name)`; port from the authority, **not** from cache | est. ~100 ns |
| 4 | `DashMap` probe on the process-global cache | est. ~100-300 ns |
| 5 | `set_dynamic_metadata_string("envoy.filters.listener.original_dst", "local", "<ip>:443")` and `(..., "port", "<port>")` | est. ~100 ns |
| 6 | Return `Continue` | — |
| 7 | Route adds `X-Ate-Target-Port` declaratively from `%DYNAMIC_METADATA(...)%` — **already exists**, no module work | `xds.go:103`, `:820-828` |
| 8 | ORIGINAL_DST dials | unchanged |

**Estimated hit cost: ≤1 µs, single-digit allocations.** The Go warm-lookup figure (0.58 µs / 3 allocs) is the reference point; **no Rust number has been measured anywhere** — treat sub-microsecond as an estimate justified by the operations involved, not a benchmark.

The only end-to-end A/B in the tree is the prototype's own README (`demos/envoy-rust-dynamic-module/`, untracked): p50 **5.56 ms → 3.03 ms** (hop removed) **→ 0.35 ms** (hop removed *and* cached), on a 4-CPU Colima VM against a 1 ms fake control plane. That decomposition is the honest shape of the win: hop removal ≈ 1.8×, cache ≈ another 8.7×.

**Two mandatory implementation details:**

1. `port` must be written as a **string**. `ingress.go:53-58` and `:163-167` use `strconv.Itoa`; a numeric metadata setter breaks the ORIGINAL_DST cluster's `MetadataKey` read (`xds.go:753-759`).
2. `local` is `net.JoinHostPort(workerIP, "443")` — the module must not cache the target port. It is parsed per request from the authority (`ingress.go:110-117`, default `defaultActorPort`), and CONNECT traffic legitimately names a different port for the same actor. Caching it would misroute CONNECT.

**Cacheable value:** `{worker_pod_ip, worker_pod_uid, actor_uid, template_ns, template_name, inserted_at}`. Those are the only `Actor` fields the handler consumes (`ingress.go:139-140`, `:144`; `:147` is a log line only). `template_ns`/`template_name` are the low-cardinality histogram attributes and are immutable (`ateapi.proto:273`, `:277`) — but both are `optional` with a TODO to replace them by the `actor_template` ObjectRef (`:266-284`), so cache defensively.

**Admission predicate, on every resolve, before insert:**
- state **must** be `ACTOR_STATE_RUNNING`. "Assignment is non-nil" is *not* sufficient: `RESUMING` carries an assignment before the restore finishes (`workflow_resume.go:546-547` sets RESUMING + assignment, `:803-806` promotes to RUNNING), and `SUSPENDING`/`PAUSING` keep theirs until the terminal write (`workflow_suspend.go:151` then `:406`; `workflow_pause.go:136` then `:294`).
- `worker_pod_ip` must parse as an IP, mirroring `ingress.go:150-153`.
- **Never cache negatives.** `NotFound`→404, `PermissionDenied`/`Unauthenticated`→403/401 (`docs/request-parking.md:105-113`). Caching those denies a legitimate actor for T seconds and would cache a would-be authz outcome the moment authz exists. One nuance: refusing to cache `NotFound` at all makes an attacker spraying random authorities a 1:1 amplifier onto ateapi — bounded in concurrency by the 1024 parking lot but not in rate. Prefer a **sub-second** negative dedup window for `NotFound` specifically.
- Key on the **full `(atespace, name)` tuple**, never the bare name. `ateapi.proto:183-189`: name is unique *within its atespace*. The handler's input is explicitly unauthenticated client input (`ingress.go:19-24`).
- Bound the map with an LRU — the key space is attacker-controlled.

### 2.3 Cache miss

Two designs, in increasing order of risk:

**(a) Fall through to the existing Go ext_proc handler.** Parking lot, singleflight, backoff, retry classification (`resumer.go:152-161`: `Aborted`/`ResourceExhausted`/`FailedPrecondition`/`Unavailable` park, everything else fails fast), and the detached-flight semantics all stay untouched. Strictly additive and revertable.

⚠️ **`Continue` does not skip ext_proc.** A module returning `Continue` falls through to the *next* filter, which is ext_proc at `xds.go:1045-1051` — the stream still opens, so a naive "front cache" is latency-neutral. Two mechanisms exist in the vendored API to actually skip it:
- the module writes a hit marker into dynamic metadata; a `RouteMatch.dynamic_metadata` matcher (`vendor/.../config/route/v3/route_components.pb.go:1628`) selects a route whose `typed_per_filter_config` carries `ExtProcPerRoute{disabled: true}` (`vendor/.../ext_proc/v3/ext_proc.pb.go:783, 853`); or
- wrap ext_proc in `envoy.filters.http.match_delegate`.

**(b) The module owns the miss.** `StopIteration` + `send_http_callout` + `on_http_callout_done` + `continue_decoding`. This is what unlocks the second-order win — a cache hit never enters the parking lot (`ingress.go:124-127`) and never occupies an ext_proc circuit-breaker slot, so hot-actor traffic stops competing with cold-start traffic for the same 1024/2048 admission budget entirely.

But (b) must reimplement, in Rust, all of:
- bounded admission (`parking.go`, `DefaultParkedRequestMax=1024`),
- the retryable/fail-fast gRPC-code table (`resumer.go:152-161`) — note `FailedPrecondition` is retryable **only when parking is enabled** (`resumer.go:156-157`: `return r.parkEnabled`),
- singleflight coalescing *with the leader/joiner metric labels* (`resumer.go:69-76`, `:264-275`),
- and, critically, **the detached-flight invariant** at `resumer.go:185-193`: the budget bounds the retry loop but never cancels an in-flight `ResumeActor`, because ateapi durably claims the worker and marks the actor RESUMING before the snapshot restore and rolls back on neither cancellation nor reclaim. Cancelling strands the worker (issue #675). A DashMap in-flight marker dropped when the HTTP stream tears down reintroduces exactly that bug.

The SDK's `http_filter_scheduler_new/commit/delete` is a **cross-thread wakeup, not a timer** — no delay parameter (`abi.h:2533/2554/2565`; `EnvoyHttpFilterScheduler: Send + Sync { fn commit(&self, event_id: u64) }`). The 100 ms×1.1 backoff cadence needs the module's own thread, which the SDK doc warns "must join or quiesce ... before worker shutdown so a scheduled event cannot race the worker dispatcher teardown."

### 2.4 How the module reaches ate-apiserver

This is the load-bearing gap, and it is bigger than the ext_proc removal itself.

**What the Go process has that the module does not:**
- `internal/ateapiauth/client.go:58-85`: TLS 1.3 minimum, CA pool from file, and `credbundle.ClientLoader(cfg.ClientCredBundle)` wired to `GetClientCertificate` so the bundle is **re-read on every handshake** for in-place kubelet rotation.
- `client.go:79-81`: `grpc.WithResolvers(k8sresolver.NewBuilder)` — a Kubernetes EndpointSlice watch with `round_robin` across the 2 ate-apiserver replicas (`ate-api-server.yaml:63`), resolving `k8s:///api.ate-system.svc:443` (`atenet-router.yaml:196-198`).
- ateapi is **gRPC only**. `ateapi.proto` has zero `google.api.http` annotations and `cmd/ateapi` has no grpc-gateway; `main.go:214` serves gRPC on :443.

**What the module has:** `send_http_callout` to a *named Envoy cluster*, and `start_http_stream`/`stream_send_data`/`stream_send_trailers`. No gRPC client, no protobuf codec, no k8s client.

So the miss path requires **one** of:
1. **An HTTP/JSON resolve surface on ate-apiserver.** This is what the prototype does — `lib.rs:290-303` calls `GET /v1/resume?atespace=&actor=`, served only by `demos/.../fakeate/main.go:185-199`, a shim that runs alongside the real gRPC `Control` service. Simplest, but a new public API surface with its own authn story.
2. **Hand-framed gRPC over the callout** (5-byte length prefix + prost-encoded `ateapipb`, `content-type: application/grpc`). The unresolved question: `on_http_callout_done` surfaces response *headers and body*, not trailers — and unary gRPC carries its status in trailers. Go's trailers-only error responses would surface in headers; a *successful* call's status would not. **Verify this before scheduling the work.**
3. **Keep the Go sidecar as the miss path** (design (a) above). No ateapi change, no framing question.

For 1 and 2, the mTLS objection is answerable but not free: Envoy terminates the client mTLS on the callout cluster's transport socket, fed by SDS. `cmd/atenet/internal/sdsmint/` already mints for the dataplane, and the Envoy container already mounts the same podidentity bundle the router presents (`atenet-router.yaml:308-309` vs `:259-260`, same SPIFFE id `spiffe://cluster.local/ns/ate-system/sa/atenet-router`); only the `servicedns-ca` trust bundle (`:262-263`, router-only today) needs adding. The EndpointSlice resolver becomes an EDS or STRICT_DNS cluster the Go router programs — which it can, since it stays the xDS control plane. `sdsmint` does not mint that client identity today (`minter.go:93` mints serving leaves for hostnames).

**Note this is a threat-model change, not a config change:** it relocates the router's ateapi *client identity* into the dataplane process that terminates untrusted client traffic. That needs explicit review.

### 2.5 Shared state across Envoy worker threads

The documented pattern, and the one the prototype uses: a process-global `OnceLock<DashMap<String, Binding>>` (`demos/envoy-rust-dynamic-module/rust-module/src/lib.rs:143-146`), with `Binding { worker_ip, expires_at: Instant }` (`:132-137`, default TTL 5 s at `:113`). Envoy `dlopen()`s the `.so` once with `do_not_close`, so process-global state survives an ECDS config redelivery that re-runs `new_http_filter_config_fn`. The SDK also exposes `register_shared_data`/`get_shared_data`.

Key must be `format!("{}/{}", atespace, name)` (`lib.rs:176-178`) — atespace-qualified, matching `resources.ActorRef` (`internal/resources/resourceref.go:39-41`).

### 2.6 TTL and invalidation — the stale-worker-IP case

**The complete invalidation set is six events**, provable by exhaustive grep for `Status.WorkerAssignment` writes across `cmd`+`internal`+`pkg` (five clears, one assign):

1. `SuspendActor` → SUSPENDED: `workflow_suspend.go:406`
2. `PauseActor` → PAUSED/CRASHED: `workflow_pause.go:294`
3. `crashActor`: `crash.go:91`, gated by `ateerrors.ActorCrashRequested` at `crash.go:39`
4. Worker pod deleted: `syncer.go:384-390` → `DeleteWorker` → `ensureBoundActorReleased` (`workflow_worker_delete.go:84`), clearing at `:136`
5. `DeleteActor`: `workflow_delete.go:243`
6. Re-resume rebinds via `workflow_resume.go:547`, picking uniformly at random among free candidates (`scheduling.go:128`), so the IP almost always changes

**There is no idle-suspend controller.** No timer, ticker, or idle reaper suspends actors; `docs/roadmap.md:114` lists "Automated Garbage Collection ... based on configurable TTL" as a *future* idea. `docs/architecture.md:199-203` confirms the model is externally-driven. Non-test `SuspendActor`/`PauseActor` callers are: `kubectl-ate/internal/cmd/suspend_actor.go:42` (operator), `actortemplate_controller.go:157-160` and `template_reconciler.go:294` (golden-snapshot actors only), and the load harness.

Further, crash (#3) is not spontaneous: `maybeCrashActor` is reachable only from inside the suspend/pause/resume workflows (`workflow_suspend.go:265,317`; `workflow_pause.go:208`; `workflow_resume.go:715,755,782`), and there is **no worker→control-plane crash-report path at all** — no such RPC in `ateapi.proto`, and no `ControlClient` constructed anywhere in `cmd/atelet` or `cmd/ateom-*`. So exactly **one** invalidation event is truly exogenous-and-spontaneous: #4.

And #4 is slower than you'd guess, in a way that *helps*: a graceful pod delete only runs `markWorkerDraining` ("We deliberately do NOT touch the bound actor here ... Actor cleanup happens on the Pod Deleted event"), worker pods carry a hardcoded 3600-second termination grace period (`workerpool_apply.go:39`), and a draining worker keeps legitimately hosting its actor (`worker.go:228`: "status.assignment is deliberately left alone"). Bindings are far more stable than the query rate.

**Not invalidating:** `DrainWorker`; a change to `Actor.worker_selector` ("Changes take effect on the next ResumeActor call", `ateapi.proto:289`); and preemption, which does not exist — the scheduler only picks workers with `GetAssignment() == nil` (`scheduling.go:116`) and returns `ErrNoCapacity` otherwise.

**There is nothing to subscribe to.** `grep -n stream pkg/proto/ateapipb/ateapi.proto` returns nothing; all three generated ServiceDescs carry `Streams: []grpc.StreamDesc{}` (`ateapi_grpc.pb.go:1389, 1499, 1683`). There *is* an outbox (`atepg/outbox.go`) but it is **worker-only** — payload codec over `*ateapipb.Worker` (`:39`, `:48`), subscriber `WatchWorkers` (`:436, :445, :574`), store interface declares only `WatchWorkers` (`store.go:230-235`), and its sole consumer is an in-process cache inside ateapi (`workercache/workercache.go`). A `WatchActors` needs a new partitioned `actor_outbox` plus a new watcher — not just a new RPC.

**So: TTL + negative feedback. The negative feedback already exists and is thrown away.**

`internal/atunnel/ingress.go:46` defines `StaleAssignmentHeader = "X-Ate-Assignment-Stale"`; `reject()` (`:524-527`) sets it with a 421; `authorize()` (`:492-521`) re-derives `(atespace, name)` from the untouched `Host` and rejects anything that is not the actor this worker currently hosts (`:508`: `active == nil || active.ref != ref`). Both the plain path (`:470`) and the CONNECT path (`:307`) go through it. The comment at `:44-45` says its purpose is exactly "to distinguish an atunnel routing rejection from a 421 returned by the actor application itself."

`grep -rn StaleAssignmentHeader|Misdirected|421` outside `internal/atunnel` and its test: **no consumers**. The router cannot see it — `xds.go:1016` sets `ResponseHeaderMode: SKIP` — and there is no `RetryPolicy` anywhere.

**This is what makes the cache safe, and it is free to a module** (`on_response_headers` is a local function call) but expensive to Go ext_proc (flipping `ResponseHeaderMode` to `SEND` costs a second gRPC round trip per response).

**Required eviction triggers — all three:**
1. `status == 421 && x-ate-assignment-stale == "true"` → evict `(atespace, name)`, bump a counter.
2. **Upstream connect failure / reset.** If the worker pod is gone entirely there is no 421 — the ORIGINAL_DST cluster produces a local 503/UF. Read `ResponseFlags`/`ResponseCodeDetails` (both in the attribute enum) or evict from `on_http_filter_http_stream_complete`/`_reset`. Without this, a vanished worker burns `n × T` requests instead of one.
3. Hard TTL expiry.

**Prerequisite hardening (Go side, one line):** the header is currently **forgeable by the actor**. `atunnel`'s ReverseProxy (`ingress.go:130-150`) has no `ModifyResponse` and never strips `StaleAssignmentHeader` from the actor's own response. An actor can emit `421 + X-Ate-Assignment-Stale: true` itself. Blast radius is bounded to its own key, but it hands the actor a knob to force one `ResumeActor` RPC per response — amplification back onto exactly the load the cache removes. Add a `ModifyResponse` that deletes the header from proxied responses **before** the module trusts it.

**Known gap the cache widens:** `authorize()` compares `ActorRef` (atespace+name), **not** uid. `Actor.metadata.uid` exists (`ateapi.proto:191-200`) and `ActorAssignment.actor_uid` exists (`:1362-1372`), but atunnel doesn't check it. A delete-and-recreate of `foo/bar` landing on the same worker routes to the new incarnation without a 421. Same atespace, same name — **no cross-tenant exposure**, but cross-*incarnation*, and the cache widens the window from milliseconds to T. The fix is cheap: the worker already pins `ExpectedActorUID` on the credential broker (`internal/atunnel/credential.go:44, :61-63, :148, :178`), so it's threading that into `activation` (`ingress.go:92-97`) and comparing at `:508`. No proto change needed.

**The one genuine regression to design around:** a cache hit **skips `ResumeActor`**, and `ResumeActor` is what *wakes a suspended actor*. Today routing an actor that has been suspended resumes it. With a cache, a stale hit routes to the old worker and gets a 421 instead of a resume. So eviction must **re-run the slow path within the same request**, not evict-and-fail. Otherwise the TTL becomes a user-visible error window on exactly the request that should have triggered a cold resume.

**TTL sizing.** The TTL is a staleness/availability knob, not a correctness bound — correctness comes from atunnel failing closed. Start at **T = 1-5 s** with the eviction loop proven, then raise. Recommended shape: soft TTL (serve stale, refresh in background) + hard TTL (evict) + immediate eviction on both negative signals. If `roadmap.md:114`'s idle-TTL GC ever lands, the soft TTL must drop below its grace period or the GC must publish invalidations.

### 2.7 The arithmetic

Let `N` = ingress QPS, `M` = distinct hot actors, `n = N/M`, `L` = ResumeActor RTT, `T` = TTL, `C` = aggregate binding-change rate, `R` = independent caching processes.

Current: singleflight collapses only calls arriving while one is in flight, so
`R_current = N / (1 + (N/M)·L)`. With `L` in the low ms and `n = 5` rps, `n·L ≈ 0.01` — dedup buys ~1%, so **`R_current ≈ N`**.

Cached: `R_cached ≈ R·(M/T + C)`.

Reduction ≈ `N·T / (R·(M + C·T))` ≈ **`N·T/(R·M)`** when `C·T ≪ M`.

| N | M | T | C | R_cached | Factor |
|---|---|---|---|---|---|
| 1000 | 200 | 10 s | 0.1/s | 20.1 rps | **50×** |
| 1000 | 1000 | 10 s | 0.1/s | 100.1 rps | 10× |
| 200 | 200 | 60 s | 0.1/s | 3.43 rps | 58× |
| 1000 | 200 | 2 s | 0.1/s | 100.1 rps | 10× |

The factor degrades exactly when the workload is cold-ish (`M` approaching `N·T`) — which is when a cache shouldn't be expected to help.

`R = 1` today (`atenet-router.yaml:150` `replicas: 1`), so the cache is fleet-wide. Scaling the router without sharding multiplies `R_cached` by `R`; sticky routing on `:authority` would preserve the ratio.

Staleness cost: ≈ `C` failed requests/s (with eviction-and-retry, ≈ 0 user-visible), versus ~0 today. At `C=0.1/s`, `N=1000` that is 1 in 10,000, and only for actors whose binding changed under traffic.

---

## 3. Everything else, in priority order

### 3.1 Egress CONNECT authorization (#2, #8)

**What happens on every actor outbound TCP connection.** `internal/atunnel/egress.go:265` handles each accepted conn; `internal/atunnel/client.go:132-174` does a fresh TCP dial + fresh TLS handshake (`tlsConfig.Clone()` at `:140`, no `ClientSessionCache`) and writes one HTTP/1 CONNECT. `codec_type: HTTP1` (`atenet-egress.yaml:82`) means a CONNECT consumes its connection, so **connection == CONNECT == ext_proc stream == GetActor, 1:1**.

Per CONNECT the Go handler does:

1. Envoy serializes the whole validated chain as percent-encoded PEM into XFCC (`atenet-egress.yaml:86-88`, `SANITIZE_SET` + `set_current_client_cert_details.chain: true`) — because "the CEL request attributes Envoy exposes (subject, SANs, SHA-256 digest) cannot express the custom ActorIdentity X.509 extension" (`egress.go:60-63`).
2. ext_proc round trip to the loopback sidecar (`atenet-egress.yaml:120-147`, 2 s timeout, 5 s message_timeout, `failure_mode_allow: false`; cluster 127.0.0.1:50051 at `:161-178`). Measured: leaf 611 B DER / 883 B PEM / 931 B percent-encoded / 1066 B XFCC value → 1228 B `ProcessingRequest`; ~4.9 µs / 3.9 KB protobuf both sides.
3. `parseXFCCChain` (`egress.go:288-310`) with the hand-written quoted-string splitter `splitXFCCUnquoted` (`:340-410`, rune-by-rune through `strings.Builder`), `url.PathUnescape` (`:304` — deliberately not `QueryUnescape`, `+` would corrupt the DER), `pem.Decode`, `x509.ParseCertificate` (`:312-334`). Measured **~31 µs / 17 KB / 91 allocs**, of which the splitter alone is ~16 µs / 10.5 KB.
4. `verifyActorCertificate` (`:229-284`): validity window (`:237`), IsCA (`:244`), ClientAuth-EKU scan (`:250`), `leaf.Verify` against the actor-identity CA (`:253`), extension scan + `json.Unmarshal` of the `ActorIdentity` OID `1.3.6.1.4.1.11129.2.12.2` (`internal/substratex509/substratex509.go:34-43, :167-195`), purpose check (`:279`). Measured **62-99 µs**. The primitive is **Ed25519, not ECDSA P-256** — the CA pool is generated with `localca.KeyTypeED25519` (`cmd/ate-setup/internal/steps/create.go:62 → :215`), self-signed root, no intermediates.
5. `validateActor` (`:160-193`): **`h.apiClient.GetActor` on every CONNECT** (`:168`), with the verbatim TODO one line above at `:167`: *"this can cause heavy load on ate api server. Change it based on .../issues/592."* Server side: `RPCService.GetActor` (`controlapi/actor.go:177`) → `ServiceImpl.GetActor` (`:191`, literally `// TODO: implement this` + `return s.store.GetActor(...)`) → the same `SELECT proto FROM actors` (`atepg.go:615-629`). Cross-pod to `dns:///api.ate-system.svc:443` (`atenet-egress.yaml:313`).

There is **no cache of any kind** in the egress package — `Handler` holds only `apiClient` and `actorIdentityRoots` (`egress.go:71-77`), built once at `router.go:249`. Not even singleflight. And the actor certificate is valid for one hour (`actoridentity.go:85, :201`), renewed at 90% of remaining (`internal/atunnel/egress.go:183-186`), so an agent opening 100 outbound connections in an hour triggers 100 byte-identical verifications of the same certificate.

**Ranking correction:** per-unit this is the most expensive path in the system, but it fires once per outbound *TCP connection* while ingress ext_proc fires once per *HTTP request*. Ingress is very likely the bigger total-system win.

**The fix is two parts, not one.**

**Part A — cert-validator module (the better extension point, and the one the original analysis missed).** v1.39.1 ships `api/envoy/extensions/transport_sockets/tls/cert_validator/dynamic_modules/v3/dynamic_modules.proto` and SDK `source/extensions/dynamic_modules/sdk/rust/src/cert_validator.rs`, pluggable via `CertificateValidationContext.custom_validator_config` (field 12, category `envoy.tls.cert_validator`). `do_verify_cert_chain` receives `certs: &[&[u8]]` — the **raw DER chain** (`abi.h:11944-11953`) — at handshake time and can set connection-lifetime filter state. That eliminates XFCC, the percent-encoding, the header, and the PEM entirely, and runs once per connection. Given the DER, `x509-parser` + `serde_json` handle the custom OID trivially.

Its limitation is that it is **synchronous** — it cannot do the control-plane callout. Hence:

**Part B — HTTP filter with a UID-keyed TTL cache** reads the identity from filter state and serves `GetActor` from cache.

**What must NOT be dropped in the port.** The `IsCA`, ClientAuth-EKU, validity-window and `ActorIdentity` purpose checks have **no Envoy-side equivalent** — Envoy 1.39 enforces neither EKU nor a CA-flagged-leaf rejection. Omitting them is a privilege-escalation regression. Keep the validity-window comparison *per CONNECT* rather than folding it into a cached verdict: certs live exactly one hour, so an hour-TTL memo could outlive the cert it vouches for.

**What the cache changes semantically — argue it as a security decision, not an optimization.** The TTL is the revocation lag for three distinct denials: deleted actor (`NotFound` → 403 via `mapEgressIdentityError:418-420`), recreated actor with a new UID (`:177-185`), and **not-running** actor (`:188-191`). The third matters most: suspend is the core mechanism, so a suspended actor retains a working egress grant for the whole TTL. Key on `(atespace, name, actorUID)` — never `(atespace, name)` alone, or a recreated actor's stale cert passes the UID check from cache. Fold a trust-bundle generation counter into the key too, or a cached PASS outlives a rotation of `/run/actor-id-ca-certs/ca.crt` (`atenet-egress.yaml:76`).

**Extension point must be "in place of", never "in front of".** A module in front of ext_proc cannot suppress it, so on the allow path Envoy still makes the round trip and the Go handler still redoes every check — zero saving.

**What removing the sidecar actually costs.** Besides the RPC, the ext-proc sidecar owns the shutdown drain, writing `/var/run/atenet/drain-complete` (`router.go:338`, `drain.go:32-60`) that the Envoy container's preStop hook polls (`atenet-egress.yaml:260-263`), plus health/status. That handshake must be reimplemented.

**Honest expected win:** ~66-130 µs of CPU and ~85-190 allocations per CONNECT off the egress pod, **plus** — only if the actor check is cached, accepting the revocation-lag trade — the ext_proc hop and the `GetActor` RPC. Pitch it as removing the control-plane RPC, with the crypto saving secondary.

**The pure-Go cache in the egress package closes most of this gap with none of the Rust risk. That is what issue #592 is for.**

### 3.2 CONNECT tunnel / `main_internal` (#3)

**Correction to the folk understanding: ext_proc runs *once* per tunnelled request, not twice.** `buildConnectTerminateHCM` (`xds.go:908-951`) installs only `[authorityFilterStateFilter, router]` at `:936-944` — no ext_proc. So a CONNECT-tunnelled request pays exactly the same ext_proc cost as a plain ingress request.

Per **tunnel** (amortized away under keep-alive/H2): one extra connection object, one HTTP codec, one listener-filter chain, one connect_terminate access-log line (which fires at tunnel *close* — `flush_log_on_tunnel_successfully_established` is set in `atenet-egress.yaml:92-93` but **not** in `xds.go:912-923`), one request-id, the metadata passthrough (`xds.go:722-733`).

Per **request** inside the tunnel: one `main_internal` HCM decode, one CEL eval, one ext_proc stream, one `ResumeActor` RPC, one access-log line. `docs/architecture.md:355-357` states the design intent: "each request inside a long-lived tunnel still resumes the Actor and re-routes it independently if it moves workers."

**What a module removes:** the ext_proc hop and the RPC. **What it cannot remove:** the second HTTP codec — the tunnelled bytes genuinely must be re-parsed, which is *why* the internal listener exists.

**Two hard constraints:**

1. **`set_filter_state` must stay on `connect_terminate`/`_tls`.** `SharedWithUpstream: ONCE` (`xds.go:900`) is precisely what carries `dev.ate.authority` across the internal-listener hop, and `envoy_dynamic_module_callback_http_set_filter_state_bytes(ptr, key, value) -> bool` has **no** shared_with_upstream/lifespan parameter. Only the CEL/`request_attributes` transport (`xds.go:1026`) disappears. On `ingress_http`/`ingress_https` the filter *can* be dropped entirely, since a module reads `:authority` from headers directly — leaving `set_filter_state` on only the two CONNECT terminators.

2. **Security: the module must NOT fall back to the inner `:authority` on `main_internal`.** The Go handler deliberately hard-fails with a 404 when the attribute is empty (`ingress.go:101-103`), because a re-injected CONNECT tunnel's inner `:authority` is client-controlled and unrelated to the actor (`xds.go:870-875`, `ingress.go:96-99`). **The current prototype does fall back** (`rust-module/src/lib.rs:222-226`) and must have that gated on filter-chain/config or removed.

**Unverified and load-bearing:** that `get_filter_state_bytes` actually observes a `SharedWithUpstream: ONCE` value *after* the connect_terminate → main_internal hop. `demos/envoy-rust-dynamic-module/envoy/dynmod.yaml` has a plain-HTTP arm only and `bench/` is empty. Same-listener read is exercised; the CONNECT hop is not. **Test this before removing `request_attributes` from the main_internal ext_proc config.**

**Risk specific to this path:** per-request re-resolution is documented as the property that lets a tunnel follow a migrating actor. A TTL cache deliberately gives that up, and long-lived tunnels are exactly where mid-flight suspend or migration is most likely.

### 3.3 The instrumentation tax (#4)

Measured on the ingress path, per request, with real SDK providers and the production 1% sampler (`router.go:53`, `:169`):

| Item | Cost | Where |
|---|---|---|
| 4 JSON slog lines at Info | 2.9-3.4 µs discarded, **8.1-9.9 µs to a real pipe fd**; 1049-1165 B; 13 allocs (26 with a valid span) | `ingress.go:87,129,145,159`; JSON handler `serverboot.go:50-58`; default level `cmd.go:45` |
| 2 OTel spans (Extract + Start×2 + End×2) | 953 ns / 712 B / 10 allocs unsampled; **3227 ns / 2441 B / 13 allocs sampled** (3.4× — the claim that these are equal is wrong) | `ingress.go:93-95`, `resumer.go:167-169` |
| otelgrpc server stats handler | **+2.9-3.2 KB, +39 allocs** (~18% of the hop's Go-side allocations) | `extproc/extproc.go:82` |
| Route-duration histogram, 4 string attrs | 333 ns / 4 allocs / 552 B | `extproc/metrics.go:63-73` |
| QueryRecorder ring write | 18 ns uncontended, **61 ns at 18-way parallel, 0 allocs** — not a contention point | `extproc/record.go:50-64` |

Total ~4.3-6.4 µs, ~19-22 allocs, ~1.5 KB — **3-4% of the ~140-165 µs hop it measures**. Logging is 70-85% of it.

**Framing:** this is a **log-volume and GC-pressure** argument (~11 MB/s at 10k rps from the router alone, plus ate-apiserver's own ~1 KB "Handle RPC" line per RPC), not a latency win.

**What disappears with the hop:** spans, histogram, otelgrpc server handler, ring buffer. **What does not:** request logging — a module still wants it, and Envoy's access log is its natural replacement.

**Four things the port must handle:**

1. **Envoy-native does NOT already cover the SLI.** `atenet-router-monitoring.yaml:15-20` says so in its own comment: `envoy_http_downstream_rq_time` is "E2E *context* ... not an SLI we own (the SLI is the OTLP `atenet.router.route.duration` histogram)". `metrics.go:38-39, :48-51` define route.duration as ext_proc receipt → worker endpoint resolved, *excluding* actor execution. The two are disjoint. A module must re-emit it.
2. **The full stats API is available** — verified by extracting undefined symbols from the built `libate_router_module.so`: `http_filter_config_define_counter/_gauge/_histogram` and `http_filter_increment_counter/_set_gauge/_record_histogram_value`. So a real latency histogram, not just counters.
3. Envoy histograms use fixed default buckets in ms starting at 0.5 ms. Reproducing the 1 ms-30 s boundaries needs `stats_config.histogram_bucket_settings` in the inline bootstrap (`atenet-router.yaml:86-105`, which sets none).
4. **Cardinality is a DoS surface.** `classifyOutcome` (`metrics.go:75-111`) yields ~11 values × `RouterResumeKey` 3 = up to ~33 stat names per template. Envoy interns stat names in a symbol table that is **never evicted**. Stat-name segments must remain the control-plane-returned template ns/name (`ingress.go:139-140`) and must **never** include the client-supplied authority or actor name.
5. Spans get *better*: `http_get_active_span`, `http_span_spawn_child`, `http_span_set_tag`, `http_child_span_finish`, `http_span_get_trace_id` all exist, so the ResumeActor span becomes a child of Envoy's native span instead of re-extracting `traceparent`. **Caveat:** the HCM tracer is conditional — `buildTracing` returns nil when `otlpHost` is empty (`xds.go:1100`), and `setOtlpCollector` silently disables it when the collector is unreachable by Envoy's plaintext tracer cluster (`router.go:359-364`, `cmd.go:70`). In those deployments there is no active span to parent off. Also, spans would carry ServiceName `atenet-router-envoy` (`xds.go:1113`), not `atenet-router`.
6. `/statusz` loses its data source (`extproc.go:61`, `record.go:100-117`, `status.go:56/:119`, `dashboard.html:421`) and the parking-lot snapshot. Decide whether it's used. If reimplemented, preserve the query-string redaction at `record.go:94-98` (CWE-598).

### 3.4 Access logging (#7)

Five router HCMs emit unsampled stdout access logs with Envoy's bare default format: `xds.go:1039` (`accessLogConfig`, no `log_format`), `:1067-1074` on the HCM shared by `main_internal`/`ingress_http`/`ingress_https` (`:851, :1146, :1209`), and `:914-923` on `connect_terminate`, which carries its own TODO at `:914-915`: *"Envoy's default access log format is not very useful for CONNECT requests."* A repo-wide grep confirms **no `AccessLog` anywhere sets a `filter:`** — there is no sampling in substrate at all.

**Tier 1, no Rust, do it now:** drop the debug component-log-level (`atenet-router.yaml:275-276`) and add an `accesslogv3.AccessLog.Filter` in `xds.go` — the field is already vendored (`vendor/.../config/accesslog/v3/accesslog.pb.go:185`) — sampling the hot path while always logging non-2xx and non-empty response flags.

**Tier 2, a real module point:** `envoy.access_loggers.dynamic_modules` is compiled into stock 1.39.1 (`extensions_build_config.bzl:11` at the pinned rev), the xDS message `DynamicModuleAccessLog` is already in the vendored go-control-plane, and SDK `src/access_log.rs` ships the `AccessLoggerConfig`/`AccessLogger` traits. `LogContext` exposes `get_request_header`, `get_dynamic_metadata`, `get_filter_state`, `response_code`, `response_flags`, `timing_info`, `upstream_host/cluster`, `get_worker_index` — enough to key records by actor with no control-plane access. Config shape differs from the HTTP filter: `dynamic_module_config` + `logger_name` (string) + `logger_config` (Any), not `filter_name` + `filter_config` (StringValue). Status is alpha; security_posture `requires_trusted_downstream_and_upstream`.

**Two caveats.** The "collapse the CONNECT double-log" idea does not work: the two loggers run on different streams and `connect_terminate` fires last, at tunnel close — there is nothing to look ahead to. Just filter or drop it. And do **not** extend sampling to the egress loggers (`atenet-egress.yaml:94-100`, `atenet-egress-with-sdsmint.yaml:289-300, :418-430, :481-491`) — they record actor identity, SNI and peer SAN/serial and function as egress audit records.

**Before dropping the four slog lines**, note `xds.go:914-915`'s TODO: the default access-log format is inadequate for CONNECT. Configure a custom format for that path first, or attribution is genuinely lost. On the plain path, the default format already carries `%REQ(:AUTHORITY)%` (actor DNS name) and `%UPSTREAM_HOST%` (worker IP:port) — the actor→worker attribution `"Route ok"` provides.

### 3.5 Parking lot / circuit breaker (#6)

Confirmed: `docs/request-parking.md:73-76` — "Every parked request holds one ext_proc stream ... for its entire wait". Defaults: lot 1024 (`parking.go:37`), breaker 2048 derived as 2× the lot with a 1024 floor (`config.go:182-190`), `MessageTimeout` = budget+5s = 10 s (`dataplane.go:72-76`), fail-closed (`buildHcm` sets no `FailureModeAllow`; the egress dataplane sets `failure_mode_allow: false` explicitly at `atenet-egress.yaml:127` — an implicit-vs-explicit asymmetry).

**Corrections to the cost story.** The 1024/2048 pair bounds *resume/header-exchange* operations, not concurrent in-flight requests — `ingress.go:131` releases the lot slot as soon as `ResumeActor` returns, and an ordinary request holds a stream only for a millisecond-scale exchange (`docs/request-parking.md:73-75`). `min()` is wrong: `parking.enter` at `ingress.go:124` is unconditional, so the 1024 lot is always the binding gate.

Of the four costs usually enumerated, only **two** are attributable to ext_proc. The 1024 streams and the ~1024+ blocked goroutines go away. The decode-stopped filter chains do **not** — `StopIteration` is the proposed mechanism. The coalescing map does **not** — the `OnceLock`+`DashMap` is the same entry under a different name. At a full lot the sidecar-side saving is single-digit MB of goroutine stacks plus H2 machinery. Real, but not the headline.

**A separate finding worth filing on its own:** because `parking.enter` is unconditional, a full lot sheds requests to **already-running** actors with the same 503 "router at capacity". That contradicts the fast-path-headroom rationale at `xds.go:116-118` and `docs/request-parking.md:76-80` — the breaker headroom prevents Envoy truncating the lot, but it does not keep a saturated lot from starving hot actors, because the lot itself gates every request. A cache fixes this as a side effect; so does making admission conditional.

**Availability is a trade, not a win.** A sidecar crash resets 1024 streams; an unsandboxed module panic takes down Envoy and every connection in the pod.

**The graceful-drain guarantee must be rebuilt.** `docs/request-parking.md:120-129`: parked requests get their full budget and a real verdict mid-termination, via the ext_proc `GracefulStop` (`drain.go:141-157`), the derived drain timeout (`config.go:198-209`) and the preStop marker handshake (`atenet-router.yaml:281-284`). With no ext_proc server, there is no graceful stop to define "in-flight". That is a deliverable, not a detail.

### 3.6 MITM per-request egress policy (#9)

Both MITM HTTP chains carry an inert `#ATE_MITM_EXTPROC_FILTER` marker as the *first* http_filter (`atenet-egress-with-sdsmint.yaml:355-356` TLS chain, `:451-452` cleartext), spliced by `hack/experimental-additional-egress-extproc.sh:177-237` into a real ext_proc block plus its mTLS cluster at `#ATE_MITM_EXTPROC_CLUSTER` (`:698`). Contract documented at `:233-238`; passthrough exemption at `:239-242` (an opaque stream has no request to authorize — a limit, not something a module changes).

**This is the hottest amplification factor in the stack when enabled:** the outer egress ext_proc sits on a chain whose only route is `connect_matcher: {}` (`:119`), so it fires once per CONNECT; the MITM filters sit on HCMs *inside* the terminated tunnel, so an agent making 50 API calls over one tunnel triggers 50 round trips.

**But it is the lowest-priority Rust target on the table.** It requires *two* experimental flags (`--experimental-additional-egress-extproc-service` **and** `--experimental-use-sdsmint`; `install-ate.sh:257-258`, helper `:184-187`, `config.go:253`), `mitm_listener` exists only in the sdsmint manifest, `ATE_EXPERIMENTAL_USE_SDSMINT` defaults to false, and **there is no in-tree implementation of the ext_proc service** — `"extprocd"` appears exactly once in the repo, in a comment.

**Two corrections to the usual pitch.** (a) It is not redundant with the CONNECT check: identity arrives pre-computed as `request_attributes: filter_state['ate.actor.identity']` (helper `:82-83`), set at `:142-155` from `%DOWNSTREAM_PEER_URI_SAN%` with `shared_with_upstream: ONCE`, crossing the hop via `internal_upstream` on cluster `mitm_internal` (`:502-522`). What the filter authorizes is the hostname/method/path that CONNECT *cannot see* — CONNECT authority is a literal SO_ORIGINAL_DST address (`:190-194`). Caching on identity alone would be a correctness bug. (b) `GetActorEgressPolicy` exists (`controlapi/egress_policy.go:57`) but has **no enforcement consumer anywhere in-tree**, and its rules match on hostnames, ip_blocks, or `all` (`ateapi.proto:332-352`) — **there is no path matcher**.

**Module shape:** there is no timer/background API, so "ArcSwap refreshed off the hot path" isn't achievable. The working shape is the prototype's: a static `OnceLock<DashMap>` TTL cache with `send_http_callout` on miss.

**Deployment is not free:** the Envoy container is stock upstream pinned by digest (`:864`) mounting only ConfigMaps and cert dirs (`:915-932`), and **no manifest sets `ENVOY_DYNAMIC_MODULES_SEARCH_PATH`**. Shipping the `.so` needs a custom image or an init-container + emptyDir plus that env var. Both marker emitters (shell and `overlay.go:94-155`) hard-code ext_proc YAML and assert exactly 2 filter + 1 cluster markers.

**Framing:** additive, not substitutive. The flag deliberately exposes an *operator-supplied* policy service, isolated in its own pod with mTLS and a pinned SAN. An in-process module is unsandboxed and the egress gateway is `replicas: 1` (`:738`) — against the same file's stance of keeping the MITM signing key out of the dataplane (`:768-774`, `:856-861`). Ship the module as the in-process default policy; keep the marker as the operator hook.

### 3.7 SNI speculative resume (#11)

On `ingress_https` (`xds.go:1208-1245`) the filter chain at `:1231-1243` is single and unconditional and the listener has **no listener filters at all** (`ListenerFilters` appears once in the whole file, `xds.go:859`, on `main_internal`). CoreDNS maps every `<actor>.<atespace>.actors.resources.substrate.ate.dev` to the router IP (`corefile.go:44-47`), so the actor name is in the ClientHello.

Mechanism confirmed on real 1.39.1 source: `api/envoy/extensions/filters/network/dynamic_modules` exists; SDK `network.rs:273` `get_requested_server_name` is backed by `connectionInfoProvider().requestedServerName()` (`abi_impl.cc:340-344`), the field tls_inspector fills pre-handshake; `network.rs:439` `send_http_callout`/`on_http_callout_done` give the RPC; `connection_impl.cc:1074-1086` shows `initializeReadFilters()` invoking `onNewConnection()` before `onConnected()` starts the handshake.

Use a **network** filter, not a listener filter — a listener filter is destroyed at `on_close` (`listener.rs:137`) and cannot own a callout past the listener-filter chain.

**Three scope corrections.** (a) **Drop `connect_terminate_tls`** — there the outer TLS goes to the router's proxy socket and the actor is named in the CONNECT request line, which is why `authorityFilterStateFilter` is wired into `buildConnectTerminateHCM` at `:937`, and why `AllowConnect` (`:945-947`) lets one connection carry CONNECTs to several actors. (b) **Drop the cert-mint from the cost** — sdsmint is egress-only; ingress_https serves one static file cert (`xds.go:1182-1206, :1316`). (c) **Price it honestly:** the win is `min(handshake, resume)` and only on the first request of a new TLS connection — in-cluster, **low single-digit percent** of the 100 ms p95 target (`architecture.md:113`), larger only for WAN clients. It does nothing for the plain-HTTP ingress path, which is the repo's own default and benchmarked path (`atenet-router.yaml:363-367`; `internal/e2e/router_client.go:81, :129-130`; `benchmarking/nighthawk-ingress/runner.py:51`).

**Two hard requirements.** The SNI is unauthenticated, so it may **only** warm a store — the HTTP filter must still re-derive the authority from `filter_state['dev.ate.authority']` exactly as `ingress.go:100` does, keeping atunnel's `:authority`-based authorization intact. And the speculative resume must carry its own admission cap and per-source rate limit, because it fires **outside** the parking lot — otherwise one unauthenticated ClientHello forces a control-plane resume in any atespace, with no handshake and no request.

### 3.8 The measurement gap — a prerequisite, not an opportunity

**There is no committed latency baseline anywhere.** Every latency number on disk is a target (`architecture.md:113, :248, :360`), a configured limit (`xds.go:113, :137, :150, :524`; `parking.go:29-43`; `api-guide.md:332`), or an SLO gate the adaptive search binary-searches against (`tests.yaml:413/:422/:431/:440` `tailLatencySloMs: 25`; asserted in ns at `test_spec.py:112-113`). `find benchmarking -name '*.json'` returns zero files; git history shows none was ever committed and removed; `.github/workflows/` has no perf job.

The harness would emit `capacity.json` with `slo_max_rps` as "the verdict" (`nighthawk-ingress/README.md:165`) — but it measures a **different configuration than the one shipped**: `nighthawk_ingress.py` `pre_test()` (docstring `:118-122`) replaces the envoy container command, dropping the debug log flags, and pins both containers to Guaranteed QoS (`README.md:80-84`).

**Do this before writing any module.** Run the four-CPU-config sweep at `tests.yaml:405-440` both as-is and with the shipped debug flags retained, and commit `capacity.json`. `slo_max_rps` per Envoy CPU count is the number to move. ⚠️ `pre_test` patches the live Deployment with no unpatch (`nighthawk_ingress.py:121-122`) and each test tears substrate down — dev/benchmark cluster only.

Also: the Go microbenchmarks quoted throughout this report came from untracked scratch files (`cmd/atenet/internal/router/ingress/zzscratch_bench_test.go`, `zzrouterbench/`) that have since been deleted from this worktree. **Land a stable in-repo microbenchmark** so the ladder is reproducible.

---

## 4. What must NOT move to Rust

**1. The crash blast radius is a real downgrade, and it is asymmetric.** Modules are not sandboxed. `atenet-router` is `replicas: 1` (`atenet-router.yaml:150`); the egress gateway is `replicas: 1` (`atenet-egress-with-sdsmint.yaml:738`). Today an ext_proc failure fails one stream fail-closed; a module panic or segfault takes down Envoy and every live connection in the pod — including, on egress, every long-lived tunnel, against a codebase that already carries an unresolved drain TODO for exactly that (`atenet-egress.yaml:250-259`). The SDK's `catch_unwind.rs` traps Rust panics, but an `unsafe` bug does not. Every lookup path must be panic-free: no `unwrap` on header parsing, no unchecked indexing. And ABI compatibility is guaranteed only for Envoy X.Y and X.(Y+1), pinning module rebuilds to the `v1.39-latest` bump cycle.

**2. mTLS to ate-apiserver — movable, but it is a threat-model change, not a config change.** `internal/ateapiauth/client.go:58-85` re-reads the credential bundle on **every handshake** via `credbundle.ClientLoader` for in-place kubelet rotation. Envoy can match this with filesystem SDS + `watched_directory` (the pattern is already in `atenet-egress.yaml:194-201`), and `sdsmint` already mints for the dataplane. But doing so **relocates the router's ateapi client identity into the process terminating untrusted client traffic**. Get that reviewed explicitly.

**3. Kubernetes watches stay in Go.** The EndpointSlice resolver (`k8sresolver.NewBuilder`, `client.go:79`) and `ClusterTrustBundles` listing (`internal/ateclient/builder.go:213`) require a k8s client. A module has none. The correct split: Go keeps the watches and *programs Envoy* — EDS clusters plus SDS certs — so the module only ever talks to a named cluster.

**4. The xDS control plane and the ActorTemplate controller stay in Go.** `router.go:267-271`, `dataplane.go:52-85`, the `SnapshotCache` at `xds.go:207`. The module deletes a hop, not a binary. This is also convenient: it is the natural place to program the module's own callout cluster.

**5. Redis is not involved anywhere.** The only match in the tree is the word "rediscover" (`workflow_worker_delete.go:47`). Leases are Postgres rows (`atepg.go:1479-1513`). Nothing needs a distributed cache; the sharing that matters is cross-worker-*thread*, which `OnceLock`+`DashMap` handles in-process.

**6. Correctness and tenancy hazards from caching.**

- **Tenancy is safe, and provably so.** A stale binding cannot deliver a tenant's traffic to another tenant's actor: `:authority` is deliberately left untouched (`ingress.go:174-176`) and atunnel re-derives the actor from `Host` and rejects anything that isn't its currently-active actor, over mTLS with SPIFFE pinning (`internal/atunnel/ingress.go:492-521`, `:169-179`), returning 421 + `X-Ate-Assignment-Stale` (`:44-46`, `:524-527`). Failure mode is a failed request, not a cross-tenant serve.
- **There is no authorization decision to cache.** `docs/authentication.md:28`: "Authorization and RBAC are not implemented yet"; zero uses of the `principal` package inside `cmd/ateapi/internal/controlapi`. `ResumeActor` is authenticated as the router's own workload identity, identically for every actor. **This is the single biggest reason caching is defensible — and it means the cache design must be revisited before any per-actor authz lands in ateapi**, because it would start caching an authz outcome as a side effect.
- **The real regression is liveness.** A cache hit skips `ResumeActor`, which is what wakes a suspended actor. Without evict-and-retry-in-request, a suspended actor 421s for the whole TTL instead of resuming.
- **Cross-incarnation, not cross-tenant.** atunnel compares `ActorRef`, not uid (`ingress.go:508`). Delete-and-recreate on the same worker slips through. Pre-existing; the cache widens it from ms to T. Fix by threading `ExpectedActorUID` (already on the credential broker, `credential.go:44/:61-63/:148/:178`) into the comparison.
- **On egress the TTL is a revocation budget**, covering deleted / recreated-with-new-UID / not-running. Suspended actors keep egressing for T seconds. Argue it as a security decision.
- **Do not build a poller.** `ListActors` (`ateapi.proto:1272-1294`) is paginated polling with explicitly soft guarantees. Polling the world re-creates the load you're removing.

**7. The detached-resume invariant.** `resumer.go:185-193` — cancelling an in-flight `ResumeActor` strands a worker (#675). Any Rust reimplementation needs an owned task lifetime independent of the HTTP stream.

**8. The drain handshake.** Both the router's ext_proc `GracefulStop` (`drain.go:141-157`, `config.go:198-209`, `atenet-router.yaml:281-284`) and the egress sidecar's `/var/run/atenet/drain-complete` marker (`router.go:338`, `drain.go:32-60`, `atenet-egress.yaml:260-263`).

**9. The MITM CA signing key.** `atenet-egress-with-sdsmint.yaml:857`: "Keeping the signing key out of the data plane is the reason this is an SDS server rather than a file on disk." Putting it inside Envoy makes any module panic a key-holding crash.

**10. Egress audit logs.** The egress access loggers record actor identity, SNI, peer SAN and serial. Do not sample or aggregate them.

---

## 5. Migration path

No flag day. Six phases, each independently valuable and independently revertable.

### Phase 0 — Measure (blocking)

Run the `tests.yaml:405-440` sweep at 2/4/8/16 Envoy CPUs, **twice**: as the harness runs it, and with `--component-log-level` retained, so the shipped-vs-benchmarked gap is quantified. Commit `capacity.json`. Land a stable in-repo microbenchmark replacing the deleted scratch files. Add module-shaped counters to the *Go* path now (hit/miss would-be, binding-change events) so the deployed `C` and the achievable hit ratio are measured rather than assumed — the formula in §2.7 then predicts the safe TTL.

### Phase 0.5 — Bank the free wins, and separate the two reviews

Ship §1c in a batch: drop the debug log level, add `forward_rules.allowed_headers` at both ext_proc sites, swap `credbundle.Loader` into atunnel, delete the sdsmint re-parse, add the `RetryPolicy`/`X-Ate-Assignment-Stale` consumer, and add atunnel's `ModifyResponse` header strip.

**Then add the TTL cache in Go**, behind `--actor-binding-cache-ttl=0` (off by default), in `resumer.go` and `egress.go`. This is the crucial sequencing decision: it lets the **cache-correctness review** (staleness, revocation lag, eviction, uid keying) happen against a memory-safe, revertable Go change, entirely separate from the **unsandboxed-module review**. It also delivers most of the control-plane win — the 50× ResumeActor reduction and the Postgres QPS decoupling — before a single line of Rust ships.

Rollback: set the TTL to 0.

### Phase 1 — Module as a pure front cache, ext_proc untouched on miss

Build the `.so` (custom Envoy image or init-container + emptyDir with `ENVOY_DYNAMIC_MODULES_SEARCH_PATH`). Add `envoy.filters.http.dynamic_modules` **ahead of** ext_proc in `buildHcm` (`xds.go:1041-1058`), emitted only when a new `--experimental-router-module` flag is set — the Go router already programs the whole chain, so this is a `SnapshotCache` change, not a manifest change.

Hit: write the two dynamic-metadata strings, mark the hit in metadata, `Continue`. Miss: `Continue` into ext_proc unchanged — parking, singleflight, backoff, retry classification, `/statusz`, metrics all intact.

**Skipping ext_proc on hits requires the route-level mechanism** (`RouteMatch.dynamic_metadata` → `ExtProcPerRoute{disabled: true}`, or `match_delegate`). Land that in the same phase; without it the phase is correctness-safe but latency-neutral.

Cache population in this phase: the Go handler emits `state` and `actor_uid` into the metadata namespace (note `xds.go:1027-1034` currently forwards only `OriginalDstMetadataKey`, so that list must widen), and the module reads them back on the response path.

Rollback: drop the filter from `buildHcm`'s chain. Behavior is byte-identical to today.

### Phase 2 — Eviction and the response path

Implement `on_response_headers`: evict on `421 && x-ate-assignment-stale: true`; evict on upstream connect failure via `ResponseFlags`/`ResponseCodeDetails` or `on_http_filter_http_stream_reset`. **Evict-and-retry within the same request**, not evict-and-fail. Land the atunnel uid comparison here too.

This phase is where the TTL can safely rise from 1-5 s toward 30-60 s.

### Phase 3 — Module owns the miss path (optional; only if Phase 1+2 measurably underdeliver)

Requires the ateapi surface decision (§2.4): HTTP/JSON resolve endpoint, or hand-framed gRPC with the trailer question answered first, plus an SDS-fed mTLS callout cluster and an EDS cluster for ateapi. Also requires reimplementing bounded admission, the retry-code table, coalescing with leader/joiner labels, and the detached-flight invariant. **This is the largest, riskiest phase and it is not required for most of the win.** Consider stopping at Phase 2.

### Phase 4 — Egress

Same sequence, and the same "cache in Go first" discipline. The distinctive addition is the **cert-validator module** (§3.1 Part A), which is a cleaner win than the HTTP filter: it deletes XFCC, the percent-encoding, the PEM re-parse and the duplicated verify, runs once per connection, and needs no control-plane access. Ship it independently of the `GetActor` cache.

### Phase 5 — Observability migration

Only after Phase 1 is stable: module-defined Envoy histograms with the `histogram_bucket_settings` bootstrap change and the cardinality constraint; access-log formats for CONNECT (`xds.go:914-915`'s TODO) before dropping any slog line; `/statusz` reimplemented or retired. Migrate anything grepping `"ResumeActor result"` or the workerIP fields first.

### How to A/B

The Go router programs the entire HCM chain via xDS, so A/B is a control-plane concern, not a deployment one. Two options:

1. **Two Deployments, two Services, one harness.** `atenet-router` and `atenet-router-module`, identical except for the flag, driven by the nighthawk sweep at each CPU count. Cleanest for `slo_max_rps` comparison.
2. **Per-route split within one router** — emit the module filter on a route matched by a header or a fraction, ext_proc elsewhere. Riskier; only after Phase 2.

Note ECDS gives you a config-push channel into the module (redelivering `filter_config` re-runs `new_http_filter_config_fn` while process-global state survives, because the `.so` stays `dlopen`'d under `do_not_close`). That is the *only* Go→module write path that exists — useful for TTL changes and for an eviction list, but it is a config-push hack, not a custom xDS resource type.

### What to measure, per phase

| Signal | Source | Expectation |
|---|---|---|
| `slo_max_rps` per Envoy CPU | `capacity.json` from the sweep | The headline number |
| ResumeActor QPS at ate-apiserver | ateapi metrics | `N` → `M/T + C` (§2.7) |
| Postgres SELECT QPS | Postgres | Should decouple from dataplane QPS entirely — the most damaging half of the finding |
| Cache hit / miss / evict-421 / evict-upstream-failure | Module counters, via the admin `/stats/prometheus` scrape (`atenet-router-monitoring.yaml:15-35`) | Gives the deployed `C` and hit ratio; feeds the TTL decision |
| Client-visible 421 rate | Access log | Must stay ~0 once evict-and-retry lands |
| p50/p99 e2e | Nighthawk | Expect ~2× from hop removal, ~8× more from the cache (prototype README shape) |
| Go sidecar allocation rate | pprof / `go_memstats` | ~34 KB/req → near-zero on hits |
| `envoy_http_downstream_rq_time` | Already scraped | Context, not the SLI (`atenet-router-monitoring.yaml:15-20`) |
| Router pod and control-plane CPU | Both should stop scaling with dataplane QPS | The structural goal |

### Prototype status

`demos/envoy-rust-dynamic-module/` exists in this worktree but is **untracked** (`?? demos/envoy-rust-dynamic-module/` in git status). It contains a 373-line `rust-module/src/lib.rs` implementing the full ingress decision — filter-state read (`:218`), `:authority` fallback (`:222-226`), dynamic metadata + target-port header (`:200-212`), `OnceLock<DashMap>` TTL cache (`:143-145`, `:268-286`), `send_http_callout`/`on_http_callout_done` with `StopIteration` (`:294`, `:314`), local replies, and Envoy-native counters — plus `fakeate/`, a loadgen, and paired `envoy/baseline-bootstrap.yaml` vs `envoy/dynmod.yaml`. Build state in this worktree is **inconsistent between verification passes** (one pass extracted symbols from a built `libate_router_module.so`; another found no Rust toolchain and no `~/.cargo`). Treat it as a design reference and re-verify the build before relying on it. Its own README's "Honest limitations" already name the right gaps: TTL-only invalidation with no evict-on-failure, no request coalescing, no parking, plaintext HTTP/JSON instead of mTLS gRPC, and no sandbox.