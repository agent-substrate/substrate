# Metrics Best Practices

This document explains how to add a metric to a Substrate component: which
instrument to pick, how to name it, which labels it may carry, how to write the
Go, how to test it, and how to register it. It is the metrics counterpart of
[Tracing Best Practices](tracing.md).

Read [Actor Observability](../../observability.md#2-metrics) first if you have
not: it explains what already exists and how the registry is used. This
document is about adding to it.

## When to add a metric, and when not to

Add a metric when someone will **aggregate** the value: a rate, a ratio, a
percentile, a count by outcome, a fleet total. A cache hit ratio, a per-phase
latency distribution, and a queue depth are metrics.

Do not add a metric when the question is about **one actor**. Actor identity is
barred from metric labels (see [Labels](#labels-and-cardinality)), so a metric
cannot answer "how long did actor X take". Write a structured log record with
`ateattr.ActorLogAttrs` instead; `Restore timing breakdown` in atelet is the
model. A metric and a log record are not alternatives: atelet emits both for a
restore, the histogram for the fleet view and the record for the per-actor view.

Do not add a metric for a one-time or startup fact (a config value, a version).
Put it on the log line at startup, or on the resource.

## The checklist

Every metric PR has these parts. Reviewers check for each one.

1. The instrument is defined in Go against an injected `metric.Meter`, nil-safe,
   with a `metric.WithUnit` and a `metric.WithDescription`.
2. Every label key is a constant in `internal/ateattr`, and every label value
   either comes from a bounded set defined there or names an operator-created
   object (a template, a pool), as the [label rules](#labels-and-cardinality)
   allow.
3. A unit test collects the instrument through a `ManualReader` and asserts the
   name, the unit, the instrument kind, and the label set of each series.
4. The instrument and any new attribute are in
   `docs/metrics/registry/metrics.yaml`, and `hack/verify/metrics.sh` passes.
5. If the metric has a rule the registry cannot hold, or a blind spot it closes,
   `docs/metrics/substrate.yaml` is updated.
6. The metrics table in `docs/observability.md` has a row for it.

## Choosing the instrument

| You want to know | Instrument | Go constructor | Example in tree |
|---|---|---|---|
| How many times something happened, and its rate | Counter | `Int64Counter` | `ate.imagecache.requests` (`internal/imagecache/metrics.go`) |
| How long or how big each occurrence was, as a distribution | Histogram | `Float64Histogram` (seconds), `Int64Histogram` (bytes) | `ate.actor.restore.duration` (`cmd/atelet/metrics.go`), `atelet.snapshot.size` (`cmd/atelet/main.go`) |
| How many things exist right now, in a tally you keep by adding and subtracting | UpDownCounter | `Int64ObservableUpDownCounter` (you can enumerate them at collection time), `Int64UpDownCounter` (you own the increments; rare) | `ate.workerpool.workers` (`cmd/ateapi/internal/controlapi/metrics.go`) |
| A dial you read and write down, without adding or subtracting anything | Gauge | `Int64ObservableGauge` / `Float64ObservableGauge` | `ate.actor.stats.memory.working_set` (`cmd/atelet/statspoller.go`) |

Rules of thumb:

* **Prefer the observable form for "how many exist".** An observable callback
  reads the truth (a cache, a map) at collection time, so a missed decrement
  cannot drift the value, and a group that disappears from the source
  disappears from the export. Use a synchronous UpDownCounter when the change
  is in hand on the request path and a series that keeps its last value after
  the final decrement is acceptable, as the router's parked-request count
  does; use the observable form when you would otherwise be mirroring a data
  structure you already own.
* **UpDownCounter or gauge: are you counting, or reading a dial?** An
  UpDownCounter is a tally you keep yourself: something starts, you add one;
  it ends, you subtract one. `ate.workerpool.workers` is one because ateapi
  assigns and releases every worker, so it is the one keeping that tally. A
  gauge is a dial you read: memory working set is whatever the sandbox reports
  when atelet polls it (a cgroup on gVisor, the guest agent on micro-VM),
  written down per template. Ask "did I get this number by
  adding and subtracting, or by looking?" and the answer is the instrument.
  How a dashboard later aggregates the number does not enter into it. The
  type is a promise to the pipeline about what the datapoints are, and
  rollups and the actor relay act on that promise without checking.
* **Do not add a failure counter next to a success counter.** One instrument,
  with the failure on `error.type` or `ate.failure.reason`; the key's absence
  means success. See [Reporting failures](#reporting-failures).
* **One histogram, many phases**, rather than one histogram per phase, when
  the phases share dimensions. `ate.actor.restore.duration` carries
  `ate.snapshot.phase` as a label so the phases sit on one chart.

## Naming

* Instrument names are dotted and lowercase: `ate.<subsystem>.<noun>` for
  substrate-wide concepts (`ate.actor.crashes`, `ate.imagecache.requests`), or
  `<component>.<subsystem>.<noun>` when the metric is about the component's own
  mechanics (`atenet.router.parking.active`, `atelet.snapshot.size`). When in
  doubt, `ate.*`: a name that starts with a component ties the metric to that
  binary, and the same measurement from a second component then needs a second
  name.
* Name the thing measured, not the aggregation. `duration`, `size`, `requests`,
  `workers`. The unit lives in the instrument's unit field and each exporter
  renders it its own way: the Prometheus exporter on kind appends `_seconds`,
  `_bytes` and `_total`, while Cloud Monitoring keeps the OpenTelemetry name
  and appends only `_bucket`, `_count` and `_sum` (the shipped dashboards
  query `atelet.snapshot.size_bucket`). Put no unit or aggregation in the
  name, or one backend will show it twice.
* Use the upstream semantic-convention metric when one exists, with its name
  (`rpc.server.call.duration` comes from `otelgrpc` as is). When upstream has
  the shape but not the concept, mirror the shape under an `ate.*` name:
  `ate.workerpool.desired_workers` and `ate.workerpool.ready_workers` follow
  `k8s.deployment.desired_pods` and `k8s.deployment.available_pods`, two
  instruments rather than one labeled by state, because desired plus ready is
  not a sum.
* Follow the upstream attribute pattern when one exists: `*.operation.name`,
  `*.duration`, `error.type`. Reuse an upstream attribute verbatim rather than
  aliasing it into `ate.*` (`error.type` and `file.name` are used as is).
* A label key that belongs to a subsystem is rooted at the subsystem
  (`ate.imagecache.outcome`), not under `actor`, when every actor shares the
  thing it describes.

## Units and buckets

Use UCUM units, the same ones the rest of the tree uses:

| Quantity | Unit | Go value |
|---|---|---|
| time | `s` | `time.Since(start).Seconds()` as `float64` |
| size | `By` | `int64` bytes |
| count of a thing | `{thing}` in braces: `{request}`, `{worker}`, `{crash}` | `int64` |
| ratio | `1` (UCUM; no instrument in tree uses it yet) | `float64` |

Record seconds, not milliseconds, even for fast paths: the buckets carry the
resolution, and mixing units across instruments breaks every dashboard that
divides one by another.

Every histogram sets explicit bucket boundaries. The SDK defaults (`0, 5, 10,
25 … 10000`) were chosen for milliseconds and are wrong for a value recorded in
seconds: everything between zero and five seconds lands in one bucket. Pick boundaries
that cover both ends of what you have seen, and write down in a comment which
ends those are. Reuse an existing set when the quantity is comparable:

| Quantity | Boundaries used in tree |
|---|---|
| a lifecycle operation | `0.005 … 30` (`cmd/ateapi/internal/controlapi/metrics.go`) |
| a scheduler step | `0.0005 … 5` (same file) |
| a snapshot phase | `0.005 … 60` (`cmd/atelet/metrics.go`, `snapshotPhaseBuckets`) |
| a request wait | `0.001 … 60` (`cmd/atenet/internal/router/ingress/metrics.go`) |
| a size in bytes | `1e6 … 1e10` (`cmd/atelet/main.go`) |
| a small count (workers) | `0, 1, 2, 3, 5, 10, 20, 50, 100, 250` (`cmd/ateapi/internal/scheduling/metrics.go`) |

The registry entry repeats the boundaries under `annotations.substrate.buckets`
so a reader can find them without the code.

## Labels and cardinality

The rules live in `docs/metrics/substrate.yaml` under `cardinality_rules`. The
ones every new metric meets:

* **No actor identity.** `ate.actor.name`, `ate.actor.uid`, `ate.atespace`,
  `ate.actor.version` and `ate.actor.container.name` are never metric labels.
  They belong on spans and log records. This is the rule that keeps the series
  count independent of how many actors have ever existed.
* **Bounded or catalog-scoped.** Every `ate.*` label is either an enumeration
  with a fixed value list, or names an object an operator created (a template,
  a pool). Nothing on a metric label comes from a request payload, an error
  message, a hostname, or a path.
* **Normalize at the producer.** When a value arrives from the wire or from a
  file nothing validated, map it onto the bounded set before recording:
  `ateattr.NormalizeSandboxClass`, `ateattr.NormalizeOperationName`,
  `ateattr.SnapshotScopeValue`. An unrecognized value reports `unknown`, never
  the raw string.
* **Omit a label you do not know rather than emit it empty.** A resume that
  failed before a worker was picked has no pool; the pool keys are absent, not
  `""`. `ateattr.WorkerPoolAttributes` returns nil for that case.
* **Paired keys travel together.** `ate.workerpool.namespace` with
  `ate.workerpool.name`; `ate.failure.reason` with `ate.failure.domain`. Use
  the `ateattr` helpers that return the pair.

Every label key is a constant in `internal/ateattr/ateattr.go`, and every
bounded value set is a group of constants there beside it. A metric never
declares `attribute.Key("...")` locally; the one instrument that does, the
router's parking `outcome` label, predates the rule and is recorded under
`lint_exceptions` in `substrate.yaml`. Add the key and its values to `ateattr`
first; the registry entry and the code then agree by construction.

Before adding a label, ask what the dashboard groups by. A label nobody will
group or filter by multiplies the series count for nothing. Three or four
labels is typical; the restore histogram, with the most, has eight, and several
are conditional.

The SDK enforces a hard limit: 2000 distinct attribute sets per instrument,
held per reader (`OTEL_GO_X_CARDINALITY_LIMIT` changes it; unset means 2000).
Past that, every new combination is folded into one series carrying only
`otel.metric.overflow=true`, silently. Multiply the value counts of your labels
and keep the product well under that. The limit applies inside one process, so
a per-template label on atelet is bounded by the templates one node hosts, not
the cluster.

## Reporting failures

One instrument carries success and failure. Success is the absence of the
failure key. Which key depends on where the error is classified:

* **`error.type`** at an RPC boundary, holding the gRPC status code
  (`status.Code(err).String()`), as `ate.actor.lifecycle.operation.duration`
  does. Also for a bounded set of protocol statuses, as
  `ate.imagecache.requests` does with an allow-list of HTTP codes and `_OTHER`
  for everything else.
* **`ate.failure.reason` and `ate.failure.domain`** together, when the failure
  is one of substrate's own reasons (`internal/ateerrors`). Use
  `ateattr.FailureAttributes(reason)`; never set the two keys by hand. This is
  the right choice inside atelet and ateom handlers, where the gRPC status is
  assigned only after the handler returns and would read `Unknown`.

Separate a caller that gave up from a failure: `context.Canceled` and
`context.DeadlineExceeded` are their own outcomes (`cancelled`, `timeout`) on
the outcome label, not `error`. And record them: a cancelled pull still ran.
Pass the request context to `Add` and `Record` as it is, cancelled or not. The
SDK never checks `ctx.Err()`, so nothing is dropped; the only thing it reads
from the context is the span, so an exemplar can point at the sampled trace.

## Writing the Go

### Shape

Put a package's instruments in one `metrics.go` holding: the instrument name
constants, a struct of instruments, a constructor that takes a `metric.Meter`,
and unexported `record*` methods that are nil-safe. `internal/imagecache`,
`cmd/atelet` and `cmd/ateapi/internal/controlapi` follow this shape.

```go
const requestsMetric = "ate.snapshotcache.requests"

// Instruments holds the snapshot cache's instruments. A nil *Instruments is a
// valid no-op, so call sites need no guard.
type Instruments struct {
	requests metric.Int64Counter
}

func NewInstruments(meter metric.Meter) (*Instruments, error) {
	requests, err := meter.Int64Counter(
		requestsMetric,
		metric.WithUnit("{request}"),
		metric.WithDescription("Number of snapshot lookups in the node-local snapshot cache, by outcome."),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s counter: %w", requestsMetric, err)
	}
	return &Instruments{requests: requests}, nil
}

// recordRequest counts one lookup. kind is known before the lookup starts, so
// the label is present on every outcome, which is what lets the registry mark
// it required. failureOutcome and errorType are the same two helpers as in
// internal/imagecache/metrics.go.
func (i *Instruments) recordRequest(ctx context.Context, kind, outcome string, err error) {
	if i == nil || i.requests == nil {
		return
	}
	if err != nil {
		outcome = failureOutcome(err)
	}
	attrs := []attribute.KeyValue{
		ateattr.SnapshotCacheOutcomeKey.String(outcome),
		ateattr.SnapshotKindKey.String(kind),
	}
	if outcome == ateattr.SnapshotCacheOutcomeError {
		attrs = append(attrs, ateattr.ErrorTypeKey.String(errorType(err)))
	}
	i.requests.Add(ctx, 1, metric.WithAttributes(attrs...))
}
```

The nil-safety matters: tests, benchmarks and metric-free deployments construct
the subsystem without instruments, and the call sites stay unconditional.

Every label the registry marks `required` has to be set on every path through
the record method. Here `kind` is a parameter rather than something derived
after the fact, so a miss, a hit and a failure all carry it. If a label is only
known on some paths, mark it `conditionally_required` in the registry and omit
it on the others; do not emit it empty.

### Getting a meter

Take the meter as a dependency; do not call `otel.Meter` deep inside a library
package. The binary's `main` passes `otel.Meter("<component>")` in, with the
component name as the scope (`"atelet"`, `"ateapi"`, `"atecontroller"`):

* A package with an options struct adds a `WithMeter(metric.Meter) Option`
  (`internal/imagecache`).
* A package constructed once in `main` takes a `*Instruments` built there
  (`cmd/atelet`).

The meter provider itself is set up once per binary by
`serverboot.InitMetrics` (Prometheus reader plus OTLP push),
`serverboot.InitMetricsPushOnly` (OTLP push only; atecontroller), or
`serverboot.InitMetricsPushOnlyVia` (OTLP push over the atelet relay; ateom).
A new component calls one of these and defers `ShutdownProvider`; a new package
inside an existing component adds nothing there.

### Observable instruments

For a value you can enumerate at collection time, register a callback rather
than tracking increments. This is the size instrument from the
[worked example](#worked-example-a-node-local-snapshot-cache):

```go
const sizeMetric = "ate.snapshotcache.size"

// The cache owns its index and the bytes per kind partition it, so this is
// an UpDownCounter and not a gauge; observable, because the index is the
// truth to read at collection time.
size, err := meter.Int64ObservableUpDownCounter(sizeMetric,
	metric.WithUnit("By"),
	metric.WithDescription("Bytes of snapshots held in the node-local cache, by snapshot kind."))
...
_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
	for kind, n := range cache.bytesByKind() {
		o.ObserveInt64(size, n, metric.WithAttributes(ateattr.SnapshotKindKey.String(kind)))
	}
	return nil
}, size)
```

Two habits from `RegisterWorkerCount` in ateapi: if the source is unavailable,
return nil and observe nothing this cycle rather than observing zero; and seed
the series an alert depends on with an explicit zero, so "no idle workers" is a
`0` and not a missing series.

Keep the callback cheap and lock-free where you can. It runs on every collection
(every 10 s on kind, every 60 s in production) and on every Prometheus scrape.
The stats poller publishes a precomputed snapshot with an atomic pointer and the
callback only reads it.

## Testing

Test through a `ManualReader`; never through the global provider, which makes
tests order-dependent and blocks `t.Parallel`. The pattern in
`cmd/atelet/metrics_test.go` and `internal/imagecache/metrics_test.go`:

```go
func newTestInstruments(t *testing.T) (*Instruments, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	inst, err := NewInstruments(mp.Meter("atelet"))
	if err != nil {
		t.Fatalf("NewInstruments: %v", err)
	}
	return inst, reader
}
```

Then `reader.Collect(ctx, &rm)` and walk `rm.ScopeMetrics[*].Metrics` to find
the instrument by name. Assert:

* the **name** and **unit** (a typo here breaks every dashboard silently);
* the **data type** (`metricdata.Sum[int64]` with `IsMonotonic` true for a
  counter, `metricdata.Histogram[float64]` for a duration histogram, a
  `Gauge` for a gauge);
* the **label set of each series**, including that a label is absent when it
  should be (`error.type` on a success, the pool pair before assignment);
* the **value normalization**: an unknown wire value lands as `unknown`, a
  cancelled call lands as `cancelled` and not `error`.

Test the behaviors that keep cardinality bounded, not just the happy path. The
image cache tests feed a registry error with an unlisted status and assert
`_OTHER`.

Once the metric exists on kind, the e2e metrics suite
(`internal/e2e/suites/metrics`) can assert it reached the collector by scraping
the collector's Prometheus endpoint (`e2e.ScrapeCollectorMetrics`). Add it to
the list the suite checks when it is a metric an operator will alert on.

## Registering it

### `docs/metrics/registry/metrics.yaml`

The registry is an [OpenTelemetry Weaver](https://github.com/open-telemetry/weaver)
semantic convention registry and the only authoritative list of what substrate
emits. Two kinds of entry:

**An attribute**, in an `attribute_group` for its subsystem, with every
permitted value as a member. Put a new group beside `registry.ate.imagecache`:

```yaml
  - id: registry.ate.snapshotcache
    type: attribute_group
    brief: >
      The labels of the snapshot cache on the node. The key starts with the name
      of the subsystem and not with actor: every actor on the node shares it.
    attributes:
      - id: ate.snapshotcache.outcome
        stability: development
        brief: >
          The result of one snapshot lookup. Calculate the hit ratio as
          hit / (hit + miss). Do not put the failures or the stopped lookups in
          the denominator.
        type:
          members:
            - id: hit
              stability: development
              value: hit
              brief: The node holds the snapshot.
            - id: miss
              stability: development
              value: miss
              brief: The lookup must download the snapshot.
            - id: error
              stability: development
              value: error
              brief: The lookup failed. Only this outcome has an error.type key.
            - id: cancelled
              stability: development
              value: cancelled
              brief: The caller stopped the lookup.
            - id: timeout
              stability: development
              value: timeout
              brief: The time limit of the caller ended.
```

**A metric**, in the section for its component:

```yaml
  - id: metric.ate.snapshotcache.requests
    type: metric
    metric_name: ate.snapshotcache.requests
    instrument: counter
    unit: "{request}"
    stability: development
    brief: The number of snapshot lookups in the snapshot cache on the node, by outcome.
    note: >
      A miss causes a download. Thus the hit ratio of a node is an early sign
      of the resume time.
    annotations:
      substrate:
        emitted_by: [atelet]
        golden_signals: [latency, errors]
        code_anchor: cmd/atelet/internal/snapshotcache/metrics.go
        cuj: Resumes became slower on one node. Does the snapshot cache miss?
    attributes:
      - ref: ate.snapshotcache.outcome
        requirement_level: required
      - ref: ate.snapshot.kind
        requirement_level: required
      - ref: error.type
        requirement_level:
          conditionally_required: The outcome is error.
```

The `annotations.substrate` block is substrate's own and Weaver passes it
through: `emitted_by` names the binaries, `code_anchor` the file that creates
the instrument, `cuj` the question an operator answers with it,
`golden_signals` which of the four golden signals it serves (`latency`,
`traffic`, `errors`, `saturation`; list every one that applies, so a dashboard
author can find the saturation instruments without reading each brief), and
`buckets` the histogram boundaries. Fill all applicable fields, including
`buckets` for a histogram; a reader of the registry should not need the code.

Attributes that already exist (`ate.template.name`, `ate.snapshot.kind`,
`error.type`, `ate.failure.reason`) are referenced with `ref:`, not redefined.
Mark a label `conditionally_required` and say when, rather than `required`, if
the code omits it on some path.

Then run the check CI runs:

```sh
hack/verify/metrics.sh      # local weaver only if it is exactly v0.25.1 (another version is an error), else the pinned image via docker
```

### `docs/metrics/substrate.yaml`

Update this file when the metric closes a listed `blind_spot` (remove the
entry), when it has a rule Weaver cannot express (add it under
`cardinality_rules` with what could enforce it), or when it deliberately
breaks a convention (add a `lint_exceptions` entry with the reason).

### `docs/observability.md`

Add a row to the metrics table under "2. Metrics": name, emitting component,
instrument kind, and one sentence on what it measures with the label list. If
the label semantics need more than a sentence (as the snapshot labels did), add
a short paragraph below the table rather than growing the row.

### Dashboards

Cloud Monitoring dashboards live in `tools/setup-gcp/dashboards/`. A new metric
does not need a new dashboard in the same PR, but a metric an operator will
alert on should get a panel in a follow-up, and the PR description should say
which question the panel answers.

## Worked example: a node-local snapshot cache

A cache in front of snapshot downloads, in atelet, wants to answer: is the cache
helping, how much does a miss cost, and how much disk does it hold. That is
four instruments, not one:

| Question | Instrument | Labels |
|---|---|---|
| Is it helping? | `ate.snapshotcache.requests` counter, by outcome | `ate.snapshotcache.outcome`, `ate.snapshot.kind`, `error.type` on error |
| What does a miss cost? | `ate.snapshotcache.fill.duration` histogram, seconds, same boundaries as `snapshotPhaseBuckets` in `cmd/atelet/metrics.go` (unexported there, so copy or lift it) | `ate.snapshot.kind`, `ate.template.atespace`, `ate.template.name`, failure pair on failure |
| How much does it hold? | `ate.snapshotcache.size` observable UpDownCounter, bytes (the cache owns the total and kind partitions it, so not a gauge), plus an `ate.snapshotcache.evictions` counter | `ate.snapshot.kind`; evictions also carry an `ate.snapshotcache.eviction.reason` enum (`capacity`, `ttl`, `explicit`) |

What is deliberately **not** a label: the snapshot name or digest (one per
actor, unbounded), the object-storage URL (a path), the actor. The per-actor
"which snapshot did this actor's resume hit" question is a log record with
`ateattr.ActorLogAttrs` on the restore path, where `Restore timing breakdown`
already lives.

Steps, in order:

1. In `internal/ateattr/ateattr.go`: `SnapshotCacheOutcomeKey`,
   `SnapshotCacheEvictionReasonKey`, and their value constants. Reuse
   `SnapshotKindKey`, `TemplateAtespaceKey`, `TemplateNameKey`, `ErrorTypeKey`,
   `FailureAttributes`.
2. `cmd/atelet/internal/snapshotcache/metrics.go` (a package one binary uses
   goes under `cmd/<binary>/internal/`): the `Instruments` struct, constructor
   against a `metric.Meter`, nil-safe `record*` methods, the size callback
   reading the cache's index under its existing lock.
3. `cmd/atelet/main.go`: build the instruments with `otel.Meter("atelet")` and
   pass them into the cache.
4. `cmd/atelet/internal/snapshotcache/metrics_test.go`: `ManualReader`; a miss-then-hit
   test, a failure test asserting `error.type` only on `error`, an eviction
   test per reason, a size test after an insert and an evict.
5. `docs/metrics/registry/metrics.yaml`: the `registry.ate.snapshotcache`
   attribute group and the four metric entries; `hack/verify/metrics.sh`.
6. `docs/metrics/substrate.yaml`: nothing to add. The size and eviction
   instruments ship with the cache, so it starts with no blind spot; the
   `image cache cost and eviction` entry there is the image cache's gap, not
   this one's.
7. `docs/observability.md`: four rows in the table, one paragraph on the
   outcome semantics.
