# Troubleshooting Agent Substrate

These guides start from a symptom that a user or an operator sees, and end at
the component that caused it. Each one is a sequence of steps. Each step gives
a query, tells you how to read the result, and sends you to the next step or to
a different guide.

| Guide | Start here when |
|---|---|
| [Requests are slow or return 503](requests-are-slow.md) | A client waited too long, or got a 503 |
| [Substrate has no capacity for an actor](capacity-is-full.md) | A resume fails because no worker is free, or a constraint hides the free ones |
| [Resumes are slow](resumes-are-slow.md) | The activation of a suspended actor is slow |

**Start with [Requests are slow](requests-are-slow.md)** when a user reports a
fault. It is the triage: it tells you whether the cause is capacity, a resume,
the router, or the code of the actor. The other two guides go deeper into one
cause each.

The instruments and their labels are defined in
[`docs/metrics/registry/metrics.yaml`](../metrics/registry/metrics.yaml), which
is the source of truth. [`docs/observability.md`](../observability.md)
describes logs, metrics, and traces as a whole.

**These guides own the decision tree. The registry owns the definitions.** A
guide tells you which query to run, how to read the result, and where to go
next. It does not repeat the permitted values of a label, or the default of a
flag. For those, open the registry, or the document that owns the flag. A copy
in a guide goes out of date without a signal.

## The two paths

Substrate has two paths, and the first question is always which one is slow.

| Path | Who calls it | What it carries |
|---|---|---|
| Data plane | A client of the application | The traffic of the workload to an actor |
| Control plane | An operator, or the router | The lifecycle operations: create, resume, suspend, pause, delete |

Each actor has one address, which atenet resolves:

```
<actor-name>.<atespace>.actors.resources.substrate.ate.dev
```

Envoy receives the request, and the atenet router makes the route decision
through the ext_proc interface of Envoy. The router resumes the actor first if
it is not on a worker.

```
client -> atenet DNS -> Envoy --ext_proc--> atenet router -> ateapi (if a resume is necessary)
                          |                                      |
                          +--> worker pod <-- atelet restore <---+
```

## The names on your backend

Each step gives its query twice, once for each spelling of the names. Which one
you need depends on how the telemetry was ingested, not on where the cluster
runs.

Substrate emits one name over OTLP, for example `ate.actor.crashes`. A path
that writes the Prometheus format applies three edits: each dot becomes an
underscore, the unit goes into the name, and a counter gets `_total`. A path
that takes the OTLP data as it is keeps the name of the instrument.

| Ingest path | Name to query |
|---|---|
| Collector Prometheus exporter, or the default OTLP receiver of Prometheus | `ate_actor_crashes_total` |
| Google `googlemanagedprometheus` collector exporter | `ate_actor_crashes_total` |
| Google Telemetry API (`telemetry.googleapis.com`), used by the GKE managed collector | `ate.actor.crashes` |
| Prometheus with `otlp: translation_strategy: NoTranslation` | `ate.actor.crashes` |

A dotted name is not a valid PromQL identifier, thus a backend that keeps the
dots needs the quoted form. Prometheus 3.0 and the Google Managed Prometheus
API both accept it:

| Standard name | Dotted name |
|---|---|
| `atenet_router_route_duration_seconds_bucket` | `{__name__="atenet.router.route.duration_bucket"}` |
| `atenet_router_parking_rejected_total` | `{__name__="atenet.router.parking.rejected"}` |
| `sum by (ate_router_resume)` | `sum by ("ate.router.resume")` |

**One query cannot serve both.** A name that matches the two spellings needs a
regular expression, and Cloud Monitoring refuses one on the name of a metric:
`=~ is an unsupported matchtype for the __name__ label`. A regular expression
on each other label is permitted.

Do not ingest by both paths at the same time. Each path makes its own metric
descriptor, thus one instrument becomes two sets of data.

## A query that returns nothing

An empty result reads like a healthy system, thus it is the most dangerous
answer a query can give. There are four causes, and only one is a fault.

1. **The name is wrong for the backend.** A standard underscore name does not
   fail on Cloud Monitoring. It returns no data. Try the dotted name.
2. **Nothing happened in the window.** `rate(...[5m])` needs two samples in the
   last five minutes. Widen the window, or read the counter with no window and
   no function, which always draws a line:

   ```promql
   sum by (ate_router_outcome) (atenet_router_route_duration_seconds_count)
   ```

3. **The condition never occurred.** An OpenTelemetry counter is exported only
   after its first increase, and a histogram after its first measurement.
   `ate.actor.restore.duration` is absent on a cluster that only boots new
   actors, because a boot is not a restore.
4. **The instrument is not in the deployed binary.** No metric can report its
   own absence. Confirm the build rather than the query, for example:

   ```sh
   kubectl logs -n ate-system -l app=atelet --tail=-1 | grep "Actor stats poller starting"
   ```

## The subsystems with no metrics

Each guide ends with the blind spots that apply to it. Read them before you
give the cause of a fault to a component that has metrics.
[`docs/metrics/substrate.yaml`](../metrics/substrate.yaml) holds the full list,
with the cardinality rules and the known exceptions.
