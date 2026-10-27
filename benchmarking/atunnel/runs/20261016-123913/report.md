# atunnel benchmark: what the results mean

Run: `20261016-123913`. atunnel was given 1 CPU core and 1 GiB of memory
without swap, roughly what a worker pod gets. Raw numbers are in `results.txt`.

## What atunnel does

atunnel runs in every worker and carries the network traffic of the actors on
that worker:

- **Ingress (incoming):** requests from the router to an actor. atunnel checks
  which actor a request is for and forwards it into that actor's sandbox.
- **Egress (outgoing):** connections an actor opens to the internet. The
  sandbox redirects them to atunnel, which forwards them to the egress gateway.

The benchmark measures how much CPU and memory atunnel uses as traffic grows.

## How the test works

atunnel is started and hosts 10 actors with the worker's own code: each actor
gets a real sandbox network, with its own network namespaces, traffic
redirection, and certificate. Small programs stand in for the actors' own: an
ingress actor in every sandbox receives requests, and an egress actor in
actor-0's sandbox makes outgoing connections. A load generator sends traffic
at a fixed rate, 1000, 2000, 4000, then 8000 per second: ingress to the
ingress actors, and egress through the egress actor to a fake egress gateway.
At each rate, only atunnel's own CPU and memory are measured. The appendix
explains every column of `results.txt`.

The router reaches atunnel in one of two ways, set by its `--upstream-connect`
flag: through CONNECT tunnels (the default), or over HTTPS (`false`). The
scenarios cover both.

## Results by scenario

| Scenario | Router mode | What the generator sends | Last rate with verdict OK | Rate where atunnel used its whole core | CPU per unit while OK |
|---|---|---|---|---|---|
| IngressH1 | `false` | incoming HTTPS requests, HTTP/1.1, connections reused | 1,000/s (17% of the core) | 2,000/s | 170 µs per request |
| IngressH2 | `false` | incoming HTTPS requests, HTTP/2, many requests share a connection | 4,000/s (79%) | 8,000/s | 160–220 µs per request |
| Connect | default | a new secure connection per request, one request inside it | none: 1,000/s already used 98% | 1,000/s | 980 µs per connection |
| EgressHTTP | either | outgoing HTTP requests, connections reused | 4,000/s (69%) | 8,000/s | 170–200 µs per request |
| EgressNewConnection | either | outgoing HTTP requests, a new connection per request | 2,000/s (81%) | 4,000/s | 400–510 µs per request |
| EgressIdleConns | either | outgoing connections opened and left idle | 4,000 connections held | not CPU-bound; ran out of memory at 8,000 | — |
| EgressTCP | either | a continuous stream of data on 4 connections | — | used its whole core at 304 MB/s | — |

Memory in EgressIdleConns, where each connection carries one request and then
stays open with nothing sent:

| Idle connections | atunnel memory in use (after garbage collection) | atunnel container memory |
|---|---|---|
| 1,000 | 155 MB | 192 MB |
| 2,000 | 307 MB | 424 MB |
| 4,000 | 610 MB | 786 MB |
| 8,000 | killed for going over the 1 GiB limit, after about 2,400 connections | — |

## Findings

Code links are to upstream commit `3a2ca623`, the version benchmarked.

### Finding 1: each idle outgoing connection holds about 157 KB of memory

Filed as [#2759](https://github.com/agent-substrate/substrate/issues/2759).

- **Observed.** In EgressIdleConns, memory grew by about 157 KB and 5
  goroutines per open connection at every count tested, with no data moving.
  At 8,000 connections atunnel went over its 1 GiB limit and was killed.
- **Cause.** About 140 KB of it is data buffers allocated when a connection
  opens and kept until it closes: two 32 KB copy buffers in
  [`copyBothWays`](https://github.com/agent-substrate/substrate/blob/3a2ca62357485c4228fae466614ec98cf2caef69/internal/atunnel/egress/egress.go#L328),
  a 32 KB read buffer held by each HTTP/2 stream's background reader while it
  waits
  ([`client.go#L733`](https://github.com/agent-substrate/substrate/blob/3a2ca62357485c4228fae466614ec98cf2caef69/internal/atunnel/egress/client.go#L733)),
  and Go's 16 KB HTTP/2 send buffer.
- **Impact.** About 6,000 idle connections fit in 1 GiB, and far fewer right
  after a busy period: the kill at 8,000 came after only about 2,400 new
  connections, because memory from the 4,000 held just before was not yet
  returned. Idle connections are common: connection pools, keep-alive, long
  polling.
- **Fix direction.** Hold buffers only while data moves: read straight into
  the caller's buffer in the stream reader, and pool or shrink the copy
  buffers.
- **Verify.** Rerun EgressIdleConns for memory per connection, and EgressTCP
  to check throughput stays at about 304 MB/s.

### Finding 2: atunnel keeps only 2 spare connections per actor, so under load it opens a new connection for many requests

Applies when the router runs with `--upstream-connect=false`.

- **Observed.** In IngressH1, the ingress actors received 40,746 new
  connections for 96,807 requests. From 1,000 to 2,000 requests per second,
  atunnel's CPU per request rose from 170 µs to 500 µs, and atunnel used its
  whole core. IngressH2 saw 18,940 new connections for 150,000 requests.
- **Cause.** atunnel forwards to the actor with Go's default HTTP client
  settings
  ([`newProtocolMirrorTransport`](https://github.com/agent-substrate/substrate/blob/3a2ca62357485c4228fae466614ec98cf2caef69/internal/atunnel/ingress/ingress.go#L240-L242)),
  which keep at most 2 unused connections per destination. When more than 2
  requests to one actor overlap, the extra connections are opened and then
  closed. Under load more requests overlap, so more connections are opened,
  which costs more CPU. Each one is also a new connection into the actor's
  sandbox.
- **Impact.** Near capacity, a small rise in load can tip atunnel from healthy
  to saturated.
- **Fix direction.** Raise `MaxIdleConnsPerHost` on the two transports to about
  the number of requests the router sends one actor at once.
- **Verify.** Rerun IngressH1 and IngressH2 and compare the sink's
  `ingress_actor_connections` and CPU per request at 2,000 and 4,000 per second.

## Not measured here

gVisor itself (a plain process stands in for the actor's program), the real
router and egress gateway, actors being started and stopped during load, and
the Connect scenario's opposite: long-lived CONNECT tunnels carrying many
requests each, which is how the router uses them by default.

## Appendix

<details>
<summary>How to read <code>results.txt</code></summary>

Each scenario is a table with one row per rate. Example row, EgressIdleConns
holding 4,000 connections:

```
offered achieved   p50    p99     max  errs | cores µs/unit  rss-max  heap-max   gor  cg-cores  cg-mem   | A cores C cores verdict
   4000     4000 1.86ms 7.72ms 20.64ms    0 |  0.17    1083 734.8MiB  611.9MiB 20135     0.17 786.3MiB  |    0.10    0.10 OK
```

Columns before the first `|` describe the traffic. Columns between the two
`|` describe atunnel only. The last columns describe the test setup.

**Traffic**

| Column | Meaning |
|---|---|
| offered | units of work sent per second; for EgressIdleConns, connections to hold |
| achieved | units completed per second; for EgressIdleConns, connections opened |
| p50 | typical time per unit: half took less |
| p99 | slow-end time per unit: 99 in 100 took less |
| max | slowest unit |
| errs | units that failed |

A unit is one request, except in Connect (one new connection with one request
inside), EgressIdleConns (one connection opened), and EgressTCP (one 64 KB
write).

**atunnel's CPU**

| Column | Meaning |
|---|---|
| cores | CPU atunnel used, in cores: 1.00 means one core busy the whole time. Reported by the operating system for the atunnel process. |
| µs/unit | CPU time per unit of work, in microseconds: `cores` divided by the work done |
| cg-cores | the same, measured by the Linux kernel for atunnel's cgroup, which holds only atunnel. This is the counter its CPU limit is enforced on. |

`cores` and `cg-cores` measure the same CPU from two places, the process and
its cgroup, so they match; `cg-cores` is the one to compare with a pod's CPU
limit.

**atunnel's memory**, from smallest to largest:

| Column | Meaning |
|---|---|
| heap-max | memory atunnel's code allocated for its data, such as connection buffers, at its peak during the rung. May include data no longer used but not yet cleaned up. |
| rss-max | all of the atunnel process's memory in RAM at its peak: the heap, the program, thread stacks, and memory freed but not yet returned to the system |
| cg-mem | memory the kernel charges to atunnel's cgroup: everything in rss-max plus kernel memory used for atunnel, mainly network buffers. **This is checked against the memory limit and decides when atunnel is killed.** |

**Other**

| Column | Meaning |
|---|---|
| gor | goroutines, the lightweight threads of Go, the language atunnel is written in. atunnel runs about 5 per open outgoing connection, so this tracks work in progress. |
| A cores | CPU used by whatever generates the traffic: the load generator for ingress, the egress actor inside actor-0's sandbox for egress |
| C cores | CPU used by the far end: the ingress actors and the fake gateway |

`A cores` and `C cores` check the test setup itself. If either is near its
limit, the setup rather than atunnel is the bottleneck.

**Verdicts**

| Verdict | Meaning |
|---|---|
| OK | atunnel kept up with the offered rate |
| SATURATED | atunnel used its whole CPU limit, so work queued and times grew |
| SHORTFALL | atunnel fell behind without being at its CPU limit |
| RIG-LIMITED | the traffic generator or the far end was the bottleneck, so atunnel's numbers are a lower bound |
| ERRORS | some units failed |
| DOWN | atunnel was killed during the rung, here for going over its memory limit; the scenario's higher rates were skipped |

Under each table, "sink counters" are what the ingress actors and gateway
received during the scenario: requests, new connections, and bytes. They
should agree with what the generator sent. The last line of the report counts
atunnel's deaths and OOM kills.

</details>
