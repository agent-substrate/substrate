# Envoy Rust dynamic module for the atenet ingress path

A working, measurable prototype that replaces the ingress **ext_proc gRPC hop**
with an **Envoy Rust dynamic module** running in-process inside the dataplane.

Everything here runs locally with `docker compose`. Arm A runs substrate's real
`atenet router` binary and its real `xds.go` control plane, unmodified, so the
baseline is the actual system rather than an approximation of it.

## Why this path

Every request through the ingress gateway does this today:

1. Envoy pauses the filter chain and sends the full header set to the Go router
   over an ext_proc bidi gRPC stream
   ([extproc.go](../../cmd/atenet/internal/router/extproc/extproc.go)).
2. The router flattens every header into a map, resolves the actor, and calls
   `ResumeActor` on ate-apiserver over gRPC
   ([ingress.go:126](../../cmd/atenet/internal/router/ingress/ingress.go#L126)).
3. It returns dynamic metadata that an `ORIGINAL_DST` cluster routes on
   ([xds.go:745](../../cmd/atenet/internal/router/xds.go#L745)).

Step 2 happens on **every request**, including requests to an actor that is
already `RUNNING` on a known worker. `singleflight` only collapses *concurrent*
callers, never sequential ones, and
[resumer.go](../../cmd/atenet/internal/router/ingress/resumer.go) says so
outright: *"the accepted cost of one control-plane RPC per hot actor"*.

For agent workloads — a working set of warm actors receiving many requests each
— that is one control-plane round trip per request that buys nothing.

## Measured results

4-CPU Colima VM, Envoy v1.39.1 (the version substrate pins), 50 hot actors, 20s
after a 5s warmup, load generated from inside the container network. `fakeate`
charges 1ms of simulated control-plane work per resolve. Reproduced twice,
within 2%.

| Arm | RPS | p50 | p95 | p99 | control-plane calls |
|---|---:|---:|---:|---:|---:|
| **A** — ext_proc → Go router (today) | 3,735 | 2.11 ms | 2.53 ms | 3.01 ms | 87,231 |
| **B1** — Rust module, cache **off** | 4,170 | 1.89 ms | 2.26 ms | 2.59 ms | 105,423 |
| **B2** — Rust module, cache **on**, no ext_proc | 44,817 | 0.16 ms | 0.28 ms | 0.42 ms | 361 |
| **C** — Rust module **+ ext_proc together** | 42,766 | 0.17 ms | 0.30 ms | 0.44 ms | 262 |

Three separate effects, deliberately measured apart:

* **Deleting the ext_proc hop** (A → B1, cache off, one resolve per request
  either way): **+12% throughput**, p95 −11%. Real, but small.
* **Not making the call at all** (B1 → B2): **12× throughput**, p95 −88%.
  Almost the entire win is the per-request `ResumeActor` RPC, not the gRPC hop.
* **Co-existence costs almost nothing** (B2 → C): arm C keeps ext_proc in the
  chain and still reaches **96% of the full replacement's throughput**. It gets
  there without the dataplane ever talking to ate-apiserver.

**CPU per request** (`docker stats`, steady state):

| Arm | dataplane | atenet-router | RPS | CPU-ms / request |
|---|---:|---:|---:|---:|
| A | ~39% | ~47% | 3,735 | ~0.23 |
| C | ~155% | **~1%** | 42,766 | ~0.037 |

About **6× less CPU per request**, with the Go router's own CPU down ~98%.

> Demo numbers on a laptop VM, not production numbers. `fakeate`'s 1ms stands in
> for real ate-apiserver work and the backends do nothing, so the absolute
> figures will not transfer. What transfers is the shape: the per-request
> control-plane RPC dominates, and removing it is where the win is.

**Arm C is the one to read.** See [DESIGN.md](DESIGN.md) for how it works and
what it would take to land.

## What the module does

[`rust-module/src/lib.rs`](rust-module/src/lib.rs) is a like-for-like
replacement for `ingress.Handler.HandleRequestHeaders`:

| Go handler | Rust module |
|---|---|
| reads `filter_state['dev.ate.authority']` via ext_proc `request_attributes` | `get_filter_state_bytes(b"dev.ate.authority")` |
| `resources.ParseActorDNSName` | `parse_actor_ref` |
| `ResumeActor` gRPC, every request | TTL cache; `send_http_callout` only on miss |
| returns `structpb` dynamic metadata | `set_dynamic_metadata_string(...)` |
| `HeaderMutation` for the atunnel port | `set_request_header(...)` |
| 404 / 503 via `ImmediateResponse` | `send_response(...)` |
| OTel histogram with string attributes | Envoy-native counters via `define_counter` |

The cache is a process-global `DashMap` behind a `OnceLock`, shared across every
Envoy worker thread (the `.so` is `dlopen`ed once). It holds only the
actor→worker binding, for a short TTL.

## Running it

Requires Docker (or Colima) and Go. **No Rust toolchain on the host** — the
module is built inside a container.

```bash
./bench/build.sh && docker compose up -d && ./bench/run.sh
```

Knobs: `DURATION`, `WARMUP`, `CONCURRENCY`, `ACTORS`, `RESUME_LATENCY`.

Ports: `21080` arm A, `21082` arm B1 (no cache), `21081` arm B2 (cached),
`21083` arm C (co-existence), `21088` fakeate stats,
`21900`/`21901`/`21903` Envoy admin.

```bash
curl -H "Host: actor-001.demo.actors.resources.substrate.ate.dev" http://127.0.0.1:21081/
curl -s http://127.0.0.1:21901/stats | grep ate_router   # cache hit/miss counters
```

## Honest limitations

These are the gaps between this prototype and something shippable.

* **The callout is plaintext HTTP/JSON, not mTLS gRPC.** The real router
  authenticates to ate-apiserver with a client certificate
  ([ateapiauth](../../internal/ateapiauth/)). `send_http_callout` targets an
  Envoy *cluster*, so the TLS context is Envoy's — workable, but it means the
  dataplane needs the router's identity, which is a real security design
  decision, not a detail. ate-apiserver would also need an HTTP/JSON resolve
  endpoint, or the module would need to frame gRPC by hand.
* **No request coalescing.** Concurrent misses for the same actor each issue
  their own callout; the Go path collapses them with `singleflight`. On a cold
  actor with a resume storm this is strictly worse until it is added.
* **Cache invalidation is TTL-only.** There is no watch API on ate-apiserver to
  subscribe to, so a suspended or migrated actor is served stale until the TTL
  expires. A routing failure should evict the entry — this prototype does not
  yet do that. The TTL must stay well under the idle-suspend timeout.
* **Parking is not implemented.** The Go path parks resume-gated requests with a
  bounded lot and a retry budget
  ([parking.go](../../cmd/atenet/internal/router/ingress/parking.go)). The
  module fails fast instead. A `new_scheduler` timer could do it, but it is not
  here.
* **Egress is untouched.** This is ingress only.
* **No sandbox.** A panic in the module takes Envoy down with it, unlike a
  crash in the Go router, which fails one ext_proc stream. This is the single
  biggest operational difference and it is not a small one.
* The demo skips atunnel mTLS on the worker leg
  (`--upstream-credential-bundle=`), so the backends speak plaintext on :443.

## Where else this applies

Findings from a survey of the tree at commit `69828945`, each checked against
the source:

1. **Egress CONNECT is the same bug, worse.** `egress.go:168` calls `GetActor`
   on *every* CONNECT with no cache and not even singleflight — and carries its
   own TODO saying so (`egress.go:167`). Each CONNECT also re-parses and
   re-verifies a certificate Envoy already validated (~31 µs of XFCC parsing +
   62-99 µs of chain verification). It fires per TCP connection rather than per
   request, so ingress is still the bigger total win.
2. **A `cert_validator` dynamic module** (new in 1.39) gets the raw DER chain at
   handshake time, which would delete the XFCC round trip entirely — that header
   exists only because CEL attributes cannot express substrate's custom
   `ActorIdentity` extension (`egress.go:60-63`).
3. **Requests inside a CONNECT tunnel resume the actor again**, per request, via
   `main_internal` — the same filter fixes it.
4. **Free wins needing no Rust at all**: drop
   `--component-log-level ...:debug` from the shipped router manifest
   (`atenet-router.yaml:275`); set `ext_proc forward_rules.allowed_headers`
   (1407 B → 329 B per request); use `credbundle.Loader` for atunnel's
   `GetCertificate` (67-79 µs → ~2.7 µs per worker handshake).

One claim in that analysis is **refuted by this demo**: it argues a module must
replace ext_proc rather than sit in front of it, because "a module in front of
ext_proc cannot suppress it." Arm C shows it can — `clear_route_cache()` plus a
route carrying `ExtProcPerRoute.disabled` — measured at 23 requests producing 2
`ext_proc.streams_started`.

## What should stay in Go

Not everything belongs in the dataplane:

* the **xDS control plane** ([xds.go](../../cmd/atenet/internal/router/xds.go)),
* the **ActorTemplate controller** and anything needing a Kubernetes watch,
* **egress actor-certificate authentication**, which needs the actor-identity CA
  and a custom X.509 extension parse
  ([egress.go](../../cmd/atenet/internal/router/egress/egress.go)),
* anything that has to survive a dataplane crash.

The module is a **cache and a fast path**, not a replacement for the router.
That is exactly what arm C is, and why it is the recommended shape: see
[DESIGN.md](DESIGN.md).
