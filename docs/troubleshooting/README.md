# Troubleshooting Agent Substrate

These guides start from a symptom that a user or an operator sees, and end at
the component that caused it. Each guide is a sequence of questions. Each
answer sends you to the next question, to a different guide, or to the logs of
one component.

| Guide | Start here when |
|---|---|
| [Requests are slow or return 503](requests-are-slow.md) | A client waited too long, or got a 503 |
| [Substrate has no capacity for an actor](capacity-is-full.md) | A resume fails because no worker has room for the actor |
| [Resumes are slow](resumes-are-slow.md) | The activation of a suspended actor is slow, or fails |

**Start with [Requests are slow](requests-are-slow.md)** when a user reports a
fault. It is the triage: it tells you whether the cause is capacity, a resume,
the router, or the code of the actor. The other two guides go deeper into one
cause each.

## How to read a step

Each step has the same four parts:

| Part | What it gives |
|---|---|
| **Question** | What you want to know. |
| **Signal** | The instrument that answers it, with a link to its definition. |
| **Query** | One example query. |
| **Read** | What each answer means, and where to go next. |

**These guides own the questions and the order. The registry owns the
definitions.** [`docs/metrics/registry/`](../metrics/registry/) defines each
instrument, each label, and each permitted value of a label. A guide names a
label value only when the value is the decision. For the full list of values,
and for what each value means, open the group in the registry that the step
names. For the default of a flag, open the document that owns the flag.

The registry changes more often than the questions do. If a step and the
registry do not agree, the registry is correct. Then correct the step.

[`docs/observability.md`](../observability.md) describes logs, metrics, and
traces as a whole.

## The two paths

Substrate has two paths, and the first question is always which one is slow.

| Path | Who calls it | What it carries |
|---|---|---|
| Data plane | A client of the application | The traffic of the workload to an actor |
| Control plane | An operator, or the router | The lifecycle operations of an actor, such as a resume or a suspend |

A request on the data plane goes through these components. The router asks
ateapi for a resume only when the actor is not on a worker.

```
client -> atenet DNS -> Envoy --ext_proc--> atenet router -> ateapi (if a resume is necessary)
                          |                                      |
                          +--> worker pod <-- atelet restore <---+
```

| Component | Its part of a slow or failed request |
|---|---|
| atenet router | Finds the worker of the actor. Parks the request while a resume is in progress. |
| ateapi | Runs the resume. Its scheduler finds a worker with room. |
| atelet | Restores the snapshot on the node of the worker. |
| ateom | Runs the actors in the sandbox on the worker. |
| The actor | Does the work and sends the response. No Substrate metric measures this part. |

## The names on your backend

Each query in these guides uses the standard Prometheus names, for example
`ate_actor_crashes_total`. Which name your backend needs depends on how the
telemetry was ingested, not on where the cluster runs.

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
API both accept it. To change a query in these guides to the dotted form, make
these edits:

| Standard name | Dotted name |
|---|---|
| `atenet_router_route_duration_seconds_bucket` | `{__name__="atenet.router.route.duration_bucket"}` |
| `atenet_router_parking_rejected_total` | `{__name__="atenet.router.parking.rejected"}` |
| `sum by (ate_router_resume)` | `sum by ("ate.router.resume")` |
| `{ate_router_resume="triggered"}` | `{"ate.router.resume"="triggered"}` |

**One query cannot serve both.** A name that matches the two spellings needs a
regular expression, and Cloud Monitoring refuses one on the name of a metric:
`=~ is an unsupported matchtype for the __name__ label`. A regular expression
on each other label is permitted.

Do not ingest by both paths at the same time. Each path makes its own metric
descriptor, thus one instrument becomes two sets of data.

## A query that returns nothing

An empty result reads like a healthy system, thus it is the most dangerous
answer a query can give. Examine these five causes before you trust it.

1. **The name is wrong for the backend.** A standard underscore name does not
   fail on Cloud Monitoring. It returns no data. Try the dotted name.
2. **The name or the value is not in the registry any more.** A query with a
   removed label value also returns no data and no error. Compare the query
   with the registry.
3. **Nothing happened in the window.** `rate(...[5m])` needs two samples in the
   last five minutes. Widen the window, or read the counter with no window and
   no function, which always draws a line:

   ```promql
   sum by (ate_router_outcome) (atenet_router_route_duration_seconds_count)
   ```

4. **The condition never occurred.** An OpenTelemetry counter is exported only
   after its first increase, and a histogram after its first measurement.
   `ate.actor.restore.duration` is absent on a cluster that only boots new
   actors, because a boot is not a restore.
5. **The instrument is not in the deployed binary.** No metric can report its
   own absence. Confirm the build rather than the query, for example:

   ```sh
   kubectl logs -n ate-system -l app=atelet --tail=-1 | grep "Actor stats poller starting"
   ```

## The subsystems with no metrics

Some components have no metrics. A fault there looks like unmeasured time, or
like a fault in a component that has metrics. Read `blind_spots` in
[`docs/metrics/substrate.yaml`](../metrics/substrate.yaml) before you give the
cause of a fault to a component. That file holds the full list, with the
cardinality rules and the known exceptions. Each guide names only the blind
spots that change its questions.
