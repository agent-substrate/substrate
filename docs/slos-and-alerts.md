# SLOs and alerting

This page explains how operators can
[defend](#appendix:-monitoring-ate-and-defending-slos) SLOs and monitor ATE in
production. It defines SLIs, suggests SLOs, and suggests other alerts operators
can use to ensure their ATE deployments are working correctly.

# Background

## Goals

Operators need to monitor their ATE deployments for two reasons:

* **Incident management:** Incidents include cases such as bad ATE releases,
  problematic clients, or other misconfigurations of their ATE deployment that
  lead to bad behaviour for their users.
* **Optimizing tradeoffs:** Operators can set a "reasonable" rate of failed
  actor resumptions to maintain a high density, or vice versa. There are other
  cases where operators may want to trade off factors such as availability and
  performance for cost.

All the scenarios below are tagged with "ICT" if they could represent incidents,
or "OPT" if they help optimize the deployment. Of course, some scenarios can
represent both.

## Surfaces

Each primary surface of ATE needs alerting to meet the goals above. These
surfaces are:

* **ateapi (API server):** Start and manage actors, snapshots, etc.
* **K8s APIs and operations:** Create the ATE installation itself and ensure
  it's healthy; create and manage worker pools.
* **Actor execution:** As actors execute, they must be able to use local \*nix
  mechanisms like process exits and signals (e.g. OOMs), exposing probers, and
  producing stdout/stderr. We include mounting access to storage under this
  category.
* **Actor network requests:** Actors can accept incoming network requests, even
  if the actor is currently suspended, and can make outbound network requests
  through ATE-managed gateway.

The following surfaces are *not* considered to be primary, at least for now,
because their behaviour is easily observable through a different (primary)
surface. Observability of these surfaces is important for diagnosing problems,
but are not required for alerting:

* **Snapshot storage:** The time spent storing or retrieving an actor's snapshot
  is visible via the API server.
* **PostgreSQL or other backends:** Similarly, any problems here should be
  visible in the API server or other surfaces.

Finally, the following are out of scope:

* **Sandbox operation:** ATE is not responsible for the correct operation of the
  sandbox; that is, ATE could work correctly even though the sandboxes are
  running slowly or freezing.

# High-level approach

This document defines three types of signals and associated alerting.

## SLIs, SLOs, and SLO-based alerts

These are traditional alerts that cover good and bad events, and have error
budgets. SLIs and SLOs are expressed using PromQL rules, following the [SRE
workbook standard](https://sre.google/workbook/alerting-on-slos/). This document
uses OTel-style metric names (e.g. UTF-8 metrics). Operators are encouraged to
review and customize these alerts, including converting to traditional
Prometheus syntax if needed.

While ATE developers cannot *defend* SLOs, we will *demonstrate* that the P0 and
P1 SLOs can be met under reasonable conditions. (**details tbd**).

## Non-SLI metrics and custom alerts

These are conditions that can be described as metrics, but not good-bad events
and don't have an error budget. Examples include K8s-style "not-at-intent"
problems, such as the intended and actual number of workers not converging after
a period of time. it's not an error condition for the two to be out of sync
(e.g. if the number of intended workers is constantly changing), but it is an
error if they are not constantly converging.

As with SLOs, recommended alerts are expressed as Prometheus configs.

## Error logs

Many other errors are not worth having dedicated metrics; these often include
failures that will *eventually* cause another recommended SLO or alert to fail.
While we *could* introduce a one-off metric for every possible failure
condition, operators would likely not update their alerting configs and it seems
like unnecessary work to keep them in sync.

The problem with using logging for alerting (as opposed to debugging or
observability) is that it is hard to enforce that error messages are only
produced when there are genuine, non-retryable, actionable conditions that an
operator needs to investigate. While we will attempt to reduce the number of
unactionable errors that ATE components emit, it's unlikely we'll get to (or
stay at) zero, especially considering errors that could come from libraries.

This may seem resolvable if we introduce a new general-purpose "failure" metric
that can be set by many parts of the code, the unfortunately the same problem
would eventually apply: some authors might overuse this metric, and others might
ignore it. This is a fundamental problem associated with providing a complex
piece of infrastructure with a huge number of points of failures.

As a result, operators will have two options:

* They could configure alerts whenever the error rate breaches some threshold,
  and accept that occasionally this will lead to false alerts.
* They could manually monitor error messages at high-risk times, e.g. while
  upgrading Substrate.

Finally, in as many cases as possible in the scenarios below, we've described
what happens if error messages are missed. In many cases, this will cause
another SLO or signal to move, but potentially much later when the situation is
already affecting users and/or is harder to resolve. There are a few cases where
this isn't true (e.g. losing stdout from actors), but these are generally
lower-severity problems.

# Initial set of monitoring scenarios

In this section, we discuss the general approach for each surface listed above,
as well as specific CUJs within that surface and how those CUJs will be
monitored and alerted.

Ultimately, the owners of each ATE surface will be responsible for implementing
SLOs, non-SLO alerts, and error logs as described above, and this document will
become incomplete and/or out of date. The purpose of this section is to list the
current (as of late 2026), most critical alerting requirements that we'll
implement up-front, both to kickstart the observability, and also to illustrate
how the approach described above will actually work in practice.

## A: API server

All metrics related to `ate-api-server` will be reported per-RPC, including
pass/fail results and latency.

### \[ICT, OPT\] A.1: Actors cannot be executed at the expected success rate

There are legitimate reasons that actors might not be able to be run; most
importantly, the operator may not choose to size the worker pool for absolute
peak capacity. Therefore, the success rate of `ResumeActor` is a critical SLI,
with one alert for P1 (e.g. pool is not large enough) and a lower threshold for
P0 (e.g. most or all workers are broken).

Note that this configuration uses `ate.actor.lifecycle.operation`, not
`rpc.server.call`, since request parking bypasses the RPC but not the actor
operation.

```
  # ============================================================================
  # ResumeActor Capacity Availability (ResourceExhausted vs Success)
  # Target: 99.9% capacity availability over 30d (error budget = 0.001)
  # For strategy, see https://sre.google/workbook/alerting-on-slos/
  # ============================================================================
  - name: ate_resume_actor_capacity_slo_recording_rules
    rules:
      # 1. Base 5m event rates (the only rules that scan the raw OTel metric)
      - record: job:ate_actor_resume_no_capacity:rate5m
        expr: |
          sum(rate({"ate.actor.lifecycle.operation.duration_count", top_level_controller_name="ate-api-server", "ate.actor.operation.name"="resume", "error.type"="ResourceExhausted"}[5m]))
          or vector(0)
      - record: job:ate_actor_resume_valid:rate5m
        expr: |
          sum(rate({"ate.actor.lifecycle.operation.duration_count", top_level_controller_name="ate-api-server", "ate.actor.operation.name"="resume", "error.type"=~"|ResourceExhausted"}[5m]))
      # 2. Pre-computed multi-window error ratios (rolled up from the 5m rates)
      - record: job:ate_actor_resume_no_capacity:ratio_rate5m
        # This average-of-average looks weird, but without it the results
        # are unstable and can go to infinity. There may be a better way
        # to do this.
        expr: |
          avg_over_time(job:ate_actor_resume_no_capacity:rate5m[5m])
          /
          avg_over_time(job:ate_actor_resume_valid:rate5m[5m])
      - record: job:ate_actor_resume_no_capacity:ratio_rate30m
        expr: |
          avg_over_time(job:ate_actor_resume_no_capacity:rate5m[30m])
          /
          avg_over_time(job:ate_actor_resume_valid:rate5m[30m])
      - record: job:ate_actor_resume_no_capacity:ratio_rate1h
        expr: |
          avg_over_time(job:ate_actor_resume_no_capacity:rate5m[1h])
          /
          avg_over_time(job:ate_actor_resume_valid:rate5m[1h])
      - record: job:ate_actor_resume_no_capacity:ratio_rate6h
        expr: |
          avg_over_time(job:ate_actor_resume_no_capacity:rate5m[6h])
          /
          avg_over_time(job:ate_actor_resume_valid:rate5m[6h])
      - record: job:ate_actor_resume_no_capacity:ratio_rate3d
        expr: |
          avg_over_time(job:ate_actor_resume_no_capacity:rate5m[3d])
          /
          avg_over_time(job:ate_actor_resume_valid:rate5m[3d])
  - name: ate_resume_actor_capacity_slo_alerts
    rules:
      # Page on fast burn (14.4x over 1h/5m) OR slow burn (6x over 6h/30m)
      - alert: AteResumeActorCapacityBurnPage
        expr: |
          (
            job:ate_actor_resume_no_capacity:ratio_rate1h > (14.4 * 0.001)
            and
            job:ate_actor_resume_no_capacity:ratio_rate5m > (14.4 * 0.001)
          )
          or
          (
            job:ate_actor_resume_no_capacity:ratio_rate6h > (6 * 0.001)
            and
            job:ate_actor_resume_no_capacity:ratio_rate30m > (6 * 0.001)
          )
        labels:
          severity: page
          slo: ate-resume-actor-capacity
        annotations:
          summary: "High ResumeActor out-of-capacity error budget burn rate"
          description: "ResumeActor ResourceExhausted failures are burning the 30d 99.9% capacity budget at a paging rate (current={{ $value | humanizePercentage }})."
      # Ticket on sustained burn (1x over 3d/6h)
      - alert: AteResumeActorCapacityBurnTicket
        expr: |
          job:ate_actor_resume_no_capacity:ratio_rate3d > (1 * 0.001)
          and
          job:ate_actor_resume_no_capacity:ratio_rate6h > (1 * 0.001)
        labels:
          severity: ticket
          slo: ate-resume-actor-capacity
        annotations:
          summary: "Sustained ResumeActor out-of-capacity error budget burn"
          description: "ResumeActor ResourceExhausted failure rate has exceeded the 1x burn rate threshold over 3d and 6h."
```

### \[ICT\] A.2: The API server or its dependencies are broken or degraded

Most operations should be insensitive to the state of the system, and we should
maintain very high (e.g. \>99.9%) success and low latencies. These are fairly
straightforward.

```
groups:
  # ============================================================================
  # ateapi Per-Method RPC Server-Side Error Rate (5xx-equivalent gRPC codes)
  # Target: 99.9% availability over 30d per RPC method (error budget = 0.001)
  # For strategy, see https://sre.google/workbook/alerting-on-slos/
  # ============================================================================
  - name: ateapi_rpc_slo_recording_rules
    rules:
      # 1. Base 5m event rates per rpc.method (the only rules that scan the raw OTel metric)
      - record: job_rpc_method:ateapi_rpc_calls:rate5m
        # In Google Managed Prometheus, at least up to and including GKE 1.37,
        # labels in a "sum by(xxx)" expression can only include traditional
        # names (e.g. rpc_method) and not Otel-style names (e.g. rpc.method).
        # This is due to github.com/GoogleCloudPlatform/prometheus-engine
        # depending on an older version of the Prometheus parser this dependency
        # was updated on 23-09-2026 (5c449d70c4) to v3.13.0, which fixes this
        # bug, but it may take some time for this to be released in GKE.
        # 
        # NB: the three PRs that fixed this issue appear to be:
        # https://github.com/prometheus/prometheus/pull/14482
        # https://github.com/prometheus/prometheus/pull/15531
        # https://github.com/prometheus/prometheus/pull/16928
        #
        # If you are using an updated version of GKE, or are not using GMP, you
        # can remove the label_replace functions (in this rule and the next), and
        # replace all occurrences of rpc_method with rpc.method in the rest of
        # this file fragment, including the alert descriptions.
        expr: |
          sum by (rpc_method) (
            label_replace(
              rate({"rpc.server.call.duration_count", top_level_controller_name="ate-api-server"}[5m]),
              "rpc_method", "$1", "rpc.method", "(.*)"
            )
          )
      - record: job_rpc_method:ateapi_rpc_server_errors:rate5m
        expr: |
          sum by (rpc_method) (
            label_replace(
              rate({"rpc.server.call.duration_count", top_level_controller_name="ate-api-server", "rpc.response.status_code"=~"INTERNAL|UNAVAILABLE|DATA_LOSS|UNKNOWN|DEADLINE_EXCEEDED|UNIMPLEMENTED"}[5m]),
              "rpc_method", "$1", "rpc.method", "(.*)"
            )
          )
          or on (rpc_method)
          sum by (rpc_method) (0 * job_rpc_method:ateapi_rpc_calls:rate5m)
 
      # 2. Pre-computed multi-window error ratios per rpc.method (rolled up from the 5m rates)
      - record: job_rpc_method:ateapi_rpc_server_errors:ratio_rate5m
        # This average-of-average looks weird, but without it the results
        # are unstable and can go to infinity. There may be a better way
        # to do this.
        expr: |
          avg_over_time(job_rpc_method:ateapi_rpc_server_errors:rate5m[5m])
          /
          avg_over_time(job_rpc_method:ateapi_rpc_calls:rate5m[5m])
      - record: job_rpc_method:ateapi_rpc_server_errors:ratio_rate30m
        expr: |
          avg_over_time(job_rpc_method:ateapi_rpc_server_errors:rate5m[30m])
          /
          avg_over_time(job_rpc_method:ateapi_rpc_calls:rate5m[30m])
      - record: job_rpc_method:ateapi_rpc_server_errors:ratio_rate1h
        expr: |
          avg_over_time(job_rpc_method:ateapi_rpc_server_errors:rate5m[1h])
          /
          avg_over_time(job_rpc_method:ateapi_rpc_calls:rate5m[1h])
      - record: job_rpc_method:ateapi_rpc_server_errors:ratio_rate6h
        expr: |
          avg_over_time(job_rpc_method:ateapi_rpc_server_errors:rate5m[6h])
          /
          avg_over_time(job_rpc_method:ateapi_rpc_calls:rate5m[6h])
      - record: job_rpc_method:ateapi_rpc_server_errors:ratio_rate3d
        expr: |
          avg_over_time(job_rpc_method:ateapi_rpc_server_errors:rate5m[3d])
          /
          avg_over_time(job_rpc_method:ateapi_rpc_calls:rate5m[3d])
  - name: ateapi_rpc_slo_alerts
    rules:
      # Page if ANY rpc.method hits fast burn (14.4x over 1h/5m) OR slow burn (6x over 6h/30m)
      - alert: AteApiRpcErrorBudgetBurnPage
        expr: |
          (
            job_rpc_method:ateapi_rpc_server_errors:ratio_rate1h > (14.4 * 0.001)
            and
            job_rpc_method:ateapi_rpc_server_errors:ratio_rate5m > (14.4 * 0.001)
          )
          or
          (
            job_rpc_method:ateapi_rpc_server_errors:ratio_rate6h > (6 * 0.001)
            and
            job_rpc_method:ateapi_rpc_server_errors:ratio_rate30m > (6 * 0.001)
          )
        labels:
          severity: page
          slo: ateapi-rpc-availability
        annotations:
          summary: "High server-side error budget burn rate on {{ index $labels \"rpc_method\" }}"
          description: "ateapi RPC {{ index $labels \"rpc_method\" }} 5xx-equivalent gRPC error rate is burning the 30d 99.9% error budget at a paging rate (current={{ $value | humanizePercentage }})."

     # Ticket if ANY rpc.method hits sustained burn (1x over 3d/6h)
      - alert: AteApiRpcErrorBudgetBurnTicket
        expr: |
          job_rpc_method:ateapi_rpc_server_errors:ratio_rate3d > (1 * 0.001)
          and
          job_rpc_method:ateapi_rpc_server_errors:ratio_rate6h > (1 * 0.001)
        labels:
          severity: ticket
          slo: ateapi-rpc-availability
        annotations:
          summary: "Sustained server-side error budget burn on {{ index $labels \"rpc_method\" }}"
          description: "ateapi RPC {{ index $labels \"rpc_method\" }} 5xx-equivalent gRPC error rate has exceeded the 1x burn rate threshold over 3d and 6h."
```

## B: K8s APIs and asynchronous operations

Substrate introduces the following CRDs:

* `WorkerPool`: declares and manages workers. Inherently K8s-centric since it
  creates and manages pods, and can be autoscaled via HPA.
* `SandboxConfig`: declares sandbox types. Might be moved to the API server now
  that find-grained auth is supported through OpenFGA. Read directly by atelets.
* `CSIDriverConfig`: gives ATE direct access to CSI drivers without using
  PV/PVCs. Read directly by atelets.

In addition, ATE uses Secrets to project trust bundles into Actors,
`StorageClass` to set up access to storage, and `PodCertificateRequest` to
provision SPIFFE mTLS identities to workers. Finally, the atelet is deployed as
a daemonset on all nodes.

While failure conditions may be displayed on CRD status fields, these are hard
to monitor and do not cover all the cases listed above. As a result, the primary
signal for failures with K8s resources and their controllers will be error logs,
unless otherwise listed below.

### \[ICT, OPT\] B.1: Workers cannot be created

K8s may not be able to schedule the requested pods for workers, which could be a
tradeoff issue (e.g. the cluster is not large enough). But there are many other
reasons the number of workers may not be at intent: for example, the
`WorkerPool` reconciler could fail to create the associated `Deployment`, or the
pods could be created but not noticed by the rest of the system due to a faulty
network policy. These are always incidents.

We already track the two necessary metrics for this scenario:

* `ate.workerpool.desired_workers` is emitted as soon as the data is read from
  the WorkerPool CRD, so will be accurate as long as the controller is running
  at all.
* `ate.workerpool.workers` is emitted once workers are actually registered with
  ateapi.

Since the intended worker pool size can change over time, the following alert
verifies that the current size is \>= the requested size from five minutes ago,
and fires an alert if not. While this will not catch failures to *release*
workers, this seems like a far less likely problem.

```
groups:
  - name: substrate.workerpool.rules
    rules:
      - alert: SubstrateWorkerPoolCapacityNotDelivered
        # 1. Deduplicate ate.workerpool.workers across ate-api-server
        #    replicas with max(), excluding unschedulable workers and
        #    workers with an unknown sandbox class.
        # 2. Sum across the schedulable states (idle + partial +
        #    at_capacity) per WorkerPool.
        # 3. Alert if that total is below desired_workers from 5m ago
        #    (and still below current desired_workers, so intentional
        #    scale-downs don't page).
        expr: |
          sum by ("ate.workerpool.namespace", "ate.workerpool.name") (
            max by ("ate.workerpool.namespace", "ate.workerpool.name", "ate.worker.state", "ate.sandbox.class") (
              {"ate.workerpool.workers", "ate.worker.state"!="unschedulable", "ate.sandbox.class"!="unknown"}
            )
          )
          <
          max by ("ate.workerpool.namespace", "ate.workerpool.name") (
            {"ate.workerpool.desired_workers"} offset 5m
          )
          and
          sum by ("ate.workerpool.namespace", "ate.workerpool.name") (
            max by ("ate.workerpool.namespace", "ate.workerpool.name", "ate.worker.state", "ate.sandbox.class") (
              {"ate.workerpool.workers", "ate.worker.state"!="unschedulable", "ate.sandbox.class"!="unknown"}
            )
          )
          <
          max by ("ate.workerpool.namespace", "ate.workerpool.name") (
            {"ate.workerpool.desired_workers"}
          )
        for: 0m
        labels:
          severity: warning
        annotations:
          summary: 'WorkerPool {{ index $labels "ate.workerpool.namespace" }}/{{ index $labels "ate.workerpool.name" }} has not reached target capacity in ateapi'
          description: >-
            WorkerPool {{ index $labels "ate.workerpool.namespace" }}/{{ index $labels "ate.workerpool.name" }}
            currently has {{ $value }} schedulable workers registered in ateapi,
            which is fewer than the number of workers requested 5 minutes ago.
```

### \[ICT\] B.2: Existing workers become unreachable

If ateapi stops being able to reach the atelet *after* creation \- for example,
because the atelet has crashed, or because a new NetworkPolicy breaks
communication \- there is currently no metric that will move (as of Oct 2026).
Worse, because ateapi assigns an actor to a worker before attempting to contact
the atelet, and because it treats `Unavailable` as a transient error, any actor
assigned to an unreachable worker will fail permanently. The worker doesn't
become a traditional black hole \- it will only "accept" as many actors as it
originally had capacity for \- once the actor is removed, the zombie worker will
trap *future* actors.

**TODO: add a heartbeat between ateapi and atelets; after a period of time,
declare the worker lost, decrease ate.workerpool.workers (so the alert from B.1
fires) and stop assigning new workers to it.

### \[ICT\] B.3: The atelet (or other components) experiences local failures

The atelet can fail in any number of ways which are not necessarily exposed to
the API server RPCs. For example:

* It could fail to receive the capacity or status of the worker or register this
  information back to the API server (e.g. via `SetWorkerCapacity`). This
  could silently prevent the API server from attempting to use the worker,
  without causing any RPC to fail.
* The atelet could fail to clean up after an actor eviction or to prune local
  snapshots.

In addition, components should emit errors if there is an *impending* failure.
For example, a certificate used for mTLS that is expiring in fewer than M days
very likely indicates that the certificate distribution system has broken down,
and should be exposed as an error (not warning) message.

**Signal and alerting**: error logs. If ignored, these will eventually reduce
capacity, leading to violations of A.1.

### \[ICT, OPT\] B.4: The atelet experiences performance degradations

The atelet is responsible for storing and retrieving OCI images, actor
snapshots, and other assets. If these operations fail completely, the failures
will be detected via other surfaces (e.g. A.1 and B.3 at a minimum), but if
they're merely slower than expected, we would need dedicated signals to detect
the problem.

Unfortunately, pure durations (e.g. `{"ate.actor.checkpoint.duration",
"ate.snapshot.phase"="persist"}`) are not an adequate metric, since assets can
vary in size by orders of magnitude. So we need a measure of throughput that's
resistant to very small assets. One possible approach is:

* Batch together all assets so we're not measuring very small files
* Only calculate throughput for batches larger than the expected
  [bandwidth-delay
  product](https://en.wikipedia.org/wiki/Bandwidth-delay_product). For example,
  for a minimum delay of 25ms and an expected throughput of 200MiB/S, we would
  only include batches larger than \~5MiB.

**Signals and alerting:** Add throughput metrics and set up the associated SLOs
and SLO-based alerting. This is low priority at this time.

## C: Actor execution

For the most part, ATE is not responsible for ensuring that actors can execute
correctly; this is primarily the responsibility of the owners of the actors
(e.g. the authors of the code and whomever sets resource requests) and the
sandboxes themselves. However, there are some possible failures from ATE itself,
plus ATE operators must be able to monitor the operation of actors.

### \[OPT\] C.1: Actors do not have enough resources (e.g. CPU or RAM)

**Signal:** metrics showing CPU usage, RAM usage, and counters showing the
number of OOMs.

**Alerting:** n/a. Operators should monitor these signals for overall system
health, and *may* set SLOs on the percentage of successful executions if they
also control the workloads themselves, but resource exhaustion does not indicate
a problem with ATE.

### \[ICT\] C.2: Ateom cannot access the probers

**Signal and alerting:** Error logs. If ignored, see C.4.

### \[ICT\] C.3: Ateom cannot receive and store stdout/stderr

**Signal and alerting:** Error logs. No backup if ignored.

### \[ICT\] C.4: Actors are failing for other reasons.

E.g. atelets could fail to mount/unmount drives, or any other kind of local
failure.

**Signal:** tbd, basically just ensure that \# and reasons for crashes are
adequately exposed.

**Alerting:** n/a

## D: Actor network requests

### \[ICT\] D.1: Inbound network requests cannot be routed

End-to-end ingress routing success can be measured by Envoy using
`envoy_http_downstream_rq_total` and `envoy_http_downstream_rq_xx`.
Unfortunately, if the actor returns a 5xx status code, Envoy can't distinguish
between an error from ATE and from the Actor, so we cannot put an SLO on this
ratio unless we're very confident that the actors will not return such codes.

**TODO: introduce better metrics here to make alerting easier**

### \[ICT\] D.2: Outbound network requests cannot be routed

TBD

# Appendix: monitoring ATE and defending SLOs
{#appendix:-monitoring-ate-and-defending-slos}

"Defending" an SLO on ATE means:

1. Monitoring its SLIs and [alerting on potential
violations](https://sre.google/workbook/alerting-on-slos/);
2. Taking actions to resolve the violations.

In a self-managed deployment such as ATE, it is obviously not possible for
Google (or any other cloud provider) to take actions to resolve violations on
user-owned systems. For example, Google cannot:

* Increase the number of workers (or underlying nodes) to increase capacity
* Roll back a problematic change
* Ban or rate-limit problematic clients

There may be a certain class of problems that cloud providers *could* resolve,
but it's hard to say what those might be with any confidence, and it's highly
likely that these problems would not be specific to ATE anyway.

In addition, even centralized monitoring is not possible. Google generally only
has access to metrics from Google-owned components (aka "system metrics"), but
since an ATE deployment is not a Google-owned component, its metrics are not
accessible to Google, [except under exceptional
conditions](https://cloud.google.com/security/products/access-transparency).
There could be a theoretical path by which we ask customers to provide
permission for Google to view these metrics, but they could always say no. These
restrictions presumably apply to other cloud providers as well.

For this reason, the best we can do is to:

* Give users useful SLIs so they can defend their own SLOs;
* Test ATE in controlled environments to maximize the likelihood that users will
  be able to successfully defend their own SLOs, e.g. through performance
  optimization.
