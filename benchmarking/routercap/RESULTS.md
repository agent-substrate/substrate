# atenet-router capacity

> ***Conclusion:*** scale `atenet-router` horizontally past 10,000 QPS. Keep
> router pods at 4 to 8 cores and add pods, not cores. For example, to serve
> 50,000 QPS, run five 8-core routers rather than one large one.

## Test environment

Run `runs/v130-w200`. Charts and every measured window are in
that run's `report.html`.

* **atenet-router:** one pod (Envoy v1.30 + the Go ext_proc sidecar) alone on
  a tainted `c3-standard-88` node. Envoy's CPU limit is the variable under
  test; the sidecar's limit is 8 cores in every arm.
* **Workers:** 200 pods on 4 tainted `c3-standard-88` nodes, one pre-warmed
  actor per pod.
* **Generator:** one pod alone on its own `c3-standard-88` node, dialing the
  router pod IP directly (no DNS or kube-proxy in the measured path).
* **Load:** each arm walks a ladder that rises 1,000 QPS per rung, 45 s per
  rung. The first 10 s of every rung are discarded so that no window blends
  the ramp to the new rate with its steady state.
* **Arms:** 2, 4, 8 and 16 cores with matched thread counts, three
  thread-count variants on the 8-core limit (2, 4 and 16 threads), and one
  two-replica arm (2 × 4 cores).

## Summary

Sustained QPS is the highest rung where completed and successful requests
both stayed within 1% of offered and no window's median latency exceeded
100 ms. Latency columns show that rung's worst window; the last column shows
the next rung, where the arm has already failed the criterion.

| Topology | Sustained QPS | p50 | p95 | Envoy CPU | % of limit | Next rung, p50 |
|---|---|---|---|---|---|---|
| 1 × 2c | 5,000 | 6.7 ms | 251.1 ms | 1.86 | 93% | 6,000 @ 1,539 ms |
| 1 × 4c | 9,000 | 14.2 ms | 47.1 ms | 3.47 | 87% | 10,000 @ 550 ms |
| 1 × 8c | 11,000 | 11.8 ms | 71.5 ms | 5.19 | 65% | 12,000 @ 23 ms |
| 1 × 16c | 13,000 | 62.4 ms | 252.3 ms | 8.49 | 53% | 14,000 @ 232 ms |
| 2 × 4c | 16,000 | 21.6 ms | 77.4 ms | 3.53 per pod | 88% | 17,000 @ 138 ms |

What the table says:

* The return on cores falls as they are added: each doubling buys half the
  previous increment (+4,000, +2,000, +1,000). That is the signature of a
  shared cost that grows with core count, not of any single exhausted
  resource.
* Only the small arms are CPU-bound. The 2- and 4-core arms die at 93% and
  87% of their limit; the 8- and 16-core arms die at 65% and 53%, with cores
  to spare.
* Splitting a pod beats growing it. Two 4-core replicas on one node sustained
  16,000 against 11,000 for one 8-core pod on the same silicon. Each replica
  carried 8,000 QPS at the pair's wall — 89% of the 9,000 a 4-core pod
  sustains running alone, so almost nothing is lost to sharing the node.
* Do not move to Envoy v1.39 until its regression is understood: it sustained
  20% to 38% less on every arm (measured below).

## Bottlenecks in vertical scaling

Why one Envoy stops absorbing load as it grows.

### Every added worker thread taxes every request

The same load costs 19% more CPU on 8 threads than on 2, and 38% more on 16,
because each extra event loop wakes more often for less work per wake, and
every wake has a fixed kernel cost.

**Context:** Envoy parallelises with worker threads. `--concurrency` sets how
many event loops it runs, every connection is served by one of them, and each
loop sleeps in the kernel until work arrives. To separate the thread count
from the cores, diagnostic arms pinned the CPU limit at 8 cores and varied
only `--concurrency`.

Envoy CPU, in cores, every column on the same 8-core limit (mean over the
rung's windows):

| Offered QPS | 2 threads | 4 threads | 8 threads | 16 threads |
|---|---|---|---|---|
| 3,000 | 1.29 | 1.20 | 1.29 | 1.43 |
| 4,000 | 1.60 | 1.68 | 1.62 | 1.95 |
| 5,000 | 1.86 | 2.10 | 2.21 | 2.57 |

Each rung holds 45 seconds, so a cell averages two or three windows; at low
rates the differences are smaller than window-to-window wobble (that is how
4 threads reads cheaper than 2 at 3,000 QPS). The 5,000 row is stable and is
the one to quote.

A perf profile taken mid-ladder (30 s of stack samples at 99 Hz while the
2-thread and the 8-thread configurations each held 4,000 QPS) shows where the
extra CPU goes: kernel scheduling. Context-switch time nearly doubles its
share and `epoll_wait` triples, while per-syscall batch work falls. The event
stream is fixed by the request rate, so more loops means each wakes more
often for fewer events per wake. Lock contention is not the cause: mutex
tracing showed ~50 ms of blocked-acquisition wait per 15-second window across
all threads, which is noise.

### Skewed threads

One overloaded thread can fail the pod while container CPU reads 29%
utilized, because container CPU is a sum across threads and a sum cannot show
the spread.

**Context:** Envoy assigns each downstream connection to one worker thread
for life, so threads do not share load. One thread drowning plus seven
napping reads the same as eight threads quarter-busy.

The 8-core arm's collapse windows measured the first case directly: the eight
workers read 0.91, 0.72, 0.31, 0.26, 0.06, 0.02, 0.00, 0.00 cores in the
worst window. The thread holding the busiest connections was near saturation
with its requests queueing, two threads did nothing, and the container total
showed capacity to spare. The collapse then compounds: waiting requests hold
their connections, the generator dials fresh ones, and each TLS handshake is
real work for whichever thread it lands on. Monitor per-thread CPU, not the
container average. The "hottest worker" vs "mean worker" series on the
per-thread CPU panel in [report.html](runs/v130-w200/report.html) plots
exactly this; the lines hugging is balanced load, a gap opening is skew.

### Router port ceiling

The router node's ephemeral port range caps concurrent upstream connections
at 28,232; it is the operating-system limit that everything else is ordered
under.

**Context:** Envoy holds each request open end to end, so every in-flight
request owns one connection, and one source port, toward the workers. The
28,232 is the Linux default `net.ipv4.ip_local_port_range` of 32768-60999
(60999 − 32768 + 1 ports), measured from the live pod rather than assumed.

Nothing in this study reached the ceiling, and that is by design: the circuit
breakers below sit under it so that overload surfaces as a counted Envoy
rejection rather than the kernel's opaque connect failure.

### Envoy limits that must be raised

Two circuit breakers default far below these capacities; left at their
defaults they would have opened well below every wall and been misread as
router capacity.

* The ext_proc cluster's `max_requests` defaults to twice the sidecar's
  parking lot, 2,048. Every in-flight request holds one ext_proc stream, so
  this breaker opens first of everything; the sweep raised it to 20,000 with
  `--extproc-max-requests` (the shipped default is unchanged — raise the
  flag for pods expected to carry more in flight).
* The actor cluster configured no breakers at all, leaving Envoy's default of
  1,024 against arms that reach 13,000 to 26,000 in flight. Set to 20,000 in
  `xds.go`, below the 28,232-port ceiling so the breaker trips before the
  kernel does.

Did the raised limits trip during the runs? Below every wall, never — no
overflow counter moved in any measured rung, so no sustained figure is
breaker-shaped. During post-wall collapse spirals the ext_proc cluster's
pending-request queue, which sat at Envoy's 1,024 default during these runs
(the flag's ceiling covers it too since then), overflowed and
shed up to ~30,000 requests in a window; the actor cluster's raised breakers
never opened at all.

### Envoy v1.39 regresses capacity

Upgrading the proxy from v1.30 to v1.39 cost 20% to 38% of sustained QPS on
every arm, and the loss grows with thread count, which points at per-thread
overhead rather than uniform slowdown.

**Context:** v1.39 was tested for its worker-pinning flag (next section), so
the version moved first, alone, flag off, same everything else. Full run:
[runs/v139-w200/report.html](runs/v139-w200/report.html).

| Arm | v1.30 | v1.39 | Change |
|---|---|---|---|
| 2c | 5,000 | 4,000 | −20% |
| 4c | 9,000 | 6,000 | −33% |
| 8c/2t | 5,000 | 4,000 | −20% |
| 8c/4t | 9,000 | 6,000 | −33% |
| 8c/8t | 11,000 | 7,000 | −36% |
| 16c | 13,000 | 8,000 | −38% |

v1.39 costs ~25% more CPU per request at every rate, and its event loops
stretch an order of magnitude earlier (worst per-worker iteration at 3,000
QPS: 1,108 µs against v1.30's 89 µs). Both structural findings replicate on
v1.39: the quota stays a bystander and returns on threads still fall. Worth
reporting upstream; the cluster and manifests stay on v1.30.

### CPU pinning regresses capacity further, but improves latency

On the v1.39 16-core arm, turning on `enable_worker_cpu_affinity` dropped
sustained QPS one more rung, from 8,000 (v1.39, flag off) to 7,000 — while
below the wall it delivered visibly better latency: p50 3.4 ms against 6.3 ms
at 5,000 QPS, and steadier event loops. A trade of peak capacity for
smoothness, and on this platform a bad one.

**Context:** the flag (new in v1.39) pins worker i to CPU i, and its
documented rationale is bare metal, where pinned cores are otherwise quiet.
On a GKE VM they are not: network interrupt work lands on the same
low-numbered cores, and an unpinned worker escapes to any of ~70 idle vCPUs
while a pinned one works buried. Pinning was verified live (workers 0-15 each
locked to their own physical core; on this machine type, vCPU i and i+44 are
hyperthread siblings, so the first 16 vCPUs are 16 distinct cores). The
pinned arm is `arm-16c-pinned` in the v1.39 report linked above.

### Finding: the thread count is the arm

Capacity follows the thread count, not the CPU limit — N threads can use at
most ~N cores, so the limit only matters once the threads can spend it — and
adding threads past the cores buys one more rung by hiding the wake-up stall.

**Context:** an arm normally raises the CPU limit and `--concurrency`
together, so which of the two sets capacity is invisible in the main sweep.
The decoupled arms hold the limit at 8 cores, vary only the thread count, and
run each ladder to its wall.

| Threads on 8 cores | Sustained QPS | Envoy cores at the wall | How it died |
|---|---|---|---|
| 2 | 5,000 | 1.87 | thread-bound — the 2-core arm's numbers exactly |
| 4 | 9,000 | 3.45 | thread-bound — the 4-core arm's numbers exactly |
| 8 | 11,000 | 5.19 (65%) | idle-stalled |
| 16 | 12,000 | 7.32 (92%) | CPU spent: collapsed at 14,000 at 7.94 of 8 |

Six spare cores of quota changed nothing for the 2- and 4-thread arms: for
capacity, the thread count *is* the arm. In the other direction, 16 threads
on the same limit sustain one rung more than the matched 8, because while one
thread waits for the kernel to wake it another has work in hand — and it is
the only configuration in the study that died with its CPU actually spent.
The price is the thread tax in full (2.57 cores at 5,000 QPS against the
8-thread arm's 2.21) and a wall p50 of 38 ms against 5.8 ms. Capacity tracks
threads with falling returns, and 16 threads on 16 cores reach only 13,000:
the wake-up stall, not the quota and not the tax, is what sets the wall.
Treat `--concurrency` at twice the cores as a lever for a pod that cannot be
split, not as a strategy.

## Harness ceilings (configurable, not bottlenecks)

The rig has its own limits. Guards watch them and abort an arm when the rig,
not the router, runs out; in this run every fatal trip landed past the
sustainable rung, so the trips are a footnote to valid numbers, not a warning
about them. (One of those trips is visible in the summary table: the 4-core
arm's next rung reads 550 ms from its ramp-up window, after which the
keep-alive guard ended the arm.)

### Generator port ceiling

The generator's source ports cap how many connections it can hold to the
router. The Job spec widens the range to 64,511 ports (`ip_local_port_range`
1025-65535), the binary reads its own range at startup, and the guard trips
at 80% of whatever it measured, so the ceiling follows the spec without
recompiling.

### Worker count

Each actor cliffs near 120 requests per second: the measured worker hop rises
from 2.8 ms at 110/s to 62 ms at 120/s. Size the pool at or above target QPS
divided by 120, or the workers become the wall before the router does. The
200-pod pool kept this study router-bound; doubling from 100 to 200 workers
moved the walls by at most one rung, which is what demoted the workers from
cause to co-limiter.
