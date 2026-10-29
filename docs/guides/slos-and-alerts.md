---
title: "SLOs and Alerting"
linkTitle: "SLOs and Alerting"
weight: 13
description: >
  Define SLIs, SLOs, and Prometheus alerting rules to monitor Agent Substrate in production.
---

# SLOs and alerting

This page explains how operators can
[defend](#appendix-monitoring-ate-and-defending-slos) SLOs and monitor ATE in
production. It defines SLIs, suggests SLOs, and suggests other alerts operators
can use to ensure their ATE deployments are working correctly.

# Background

## Goals

Operators need to monitor their ATE deployments for two reasons:

* **Incident management:** Incidents include cases such as bad ATE releases,
  problematic clients, or other misconfigurations of their ATE deployment that
  lead to bad behavior for their users.
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
  through the ATE-managed gateway.

The following surfaces are *not* considered to be primary, at least for now,
because their behavior is easily observable through a different (primary)
surface. Observability of these surfaces is important for diagnosing problems,
but is not required for alerting:

* **Snapshot storage:** The time spent storing or retrieving an actor's snapshot
  is visible via the API server, but see also section B.4 below for some
  subtleties in how the times should be interpreted.
* **PostgreSQL or other backends:** Similarly, any problems here should be
  visible in the API server or other surfaces.

Finally, the following are out of scope:

* **Sandbox operation:** ATE is not responsible for the correct operation of the
  underlying sandbox technology (e.g. gVisor, microVMs); that is, ATE could work
  correctly even though the sandboxes are running slowly. These components are
  pulled directly from upstream; see the documentation of those projects for
  observability options and recommended monitoring.

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
a period of time. It's not an error condition for the two to be out of sync
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
that can be set by many parts of the code, but unfortunately the same problem
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

All metrics related to `ate-api-server` are reported per-RPC, including
pass/fail results and latency.

### \[ICT, OPT\] A.1: Actors cannot be executed at the expected success rate

There are legitimate reasons that actors might not be able to be run; most
importantly, the operator may not choose to size the worker pool for absolute
peak capacity. Therefore, the failure rate of `ResumeActor` due to exhaustion is
a critical SLI, with one alert for P1 (e.g. pool is not large enough) and a
lower threshold for P0 (e.g. most or all workers are broken). (For failures due
to causes other than exhaustion, see section A.2).

Note that this configuration uses `ate.actor.lifecycle.operation`, not
`rpc.server.call`, since the former correctly (in this case) excludes "resume"
operations where the actor was already running, while the latter does not.

One current weakness of this alert is that it greatly overcounts failures that
occur during request parking, since request parking can retry about 20 times.
(100ms initial delay, 1.1x multiplier, 5s budget). Right now, there's no way to
tell the difference between "retryable" failures and "final" failures.

**Example:** In the [sample rules](../examples/gmp-alerting-rules.yaml), see
`ate_resume_actor_capacity_slo_recording_rules` and
`ate_resume_actor_capacity_slo_alerts`.


### \[ICT\] A.2: The API server or its dependencies are broken or degraded

Most operations should be insensitive to the state of the system, and we should
maintain very high (e.g. \>99.9%) success rate. These are fairly
straightforward.

These SLIs/SLOs only track the equivalent of 5xx codes. Notably, resource
exhaustion is excluded; see section A.1 for that.

**Example:** In the [sample rules](../examples/gmp-alerting-rules.yaml), see
`ateapi_rpc_availability_slo_recording_rules` and
`ateapi_rpc_availability_slo_alerts`. Note that these may be too sensitive for
low-traffic methods (e.g. `CreateActorTemplate`); see the file for possible
approaches to resolve this.

_NB: the sample rules only cover availability, not latency, but the availability
SLOs can easily be adapted into multiple tiers of latency SLOs as needed._


## B: K8s APIs and asynchronous operations

Substrate introduces the following CRDs:

* `WorkerPool`: declares and manages workers. Inherently K8s-centric since it
  creates and manages pods, and can be autoscaled via HPA.
* `SandboxConfig`: declares sandbox types. Might be moved to the API server now
  that fine-grained auth is supported through OpenFGA. Read directly by atelets.
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
verifies that the current size is \>= the smallest requested size over the last
five minutes, and fires an alert after five minutes if not. While this will not
catch failures to *release* workers, this seems like a far less likely problem.

**Example:** In the [sample rules](../examples/gmp-alerting-rules.yaml), see
`ate_workerpool_alerts`.

### \[ICT\] B.2: Existing workers become unreachable

If ateapi stops being able to reach the atelet *after* creation \- for example,
because the atelet has crashed, or because a new NetworkPolicy breaks
communication \- there is currently no metric that will move (as of Oct 2026),
until a client attempts to contact it directly or indirectly via
`ate-api-server` (at which point the client will receive `UNAVAILABLE`, which is
caught by A.2).

Worse, because ateapi assigns an actor to a worker before attempting to contact
the atelet, and because it treats `Unavailable` as a transient error, any actor
assigned to an unreachable worker will fail permanently. The worker doesn't
become a traditional black hole \- it will only "accept" as many actors as it
originally had capacity for \- but once the actor is removed, the zombie worker will
trap *future* actors.

**TODO:** consider adding a heartbeat between ateapi and atelets; after a period
of time, declare the worker lost, decrease ate.workerpool.workers (so the alert
from B.1 fires) and stop assigning new actors to it. We could also wait for
external RPCs instead of heartbeats combined with a timeout or failure count.

### \[ICT\] B.3: The atelet (or other components) experiences local failures

The atelet can fail in any number of ways which are not necessarily exposed to
the API server RPCs. For example:

* It could fail to receive the capacity or status of the worker or register this
  information back to the API server. This could silently prevent the API server
  from attempting to use the worker, without causing any RPC to fail.
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
number of OOMs. Many of these are available in the
[metrics](../metrics/registry/metrics.yaml) and [events](../metrics/registry/events.yaml) registries,
such as `ate.actor.stats` and `ate.actor.usage_sampled`. Also see the
[observability](observability.md) docs for information on log attributes such as
`ate.sandbox.oom_kills`.

**Alerting:** n/a. Operators should monitor these signals for overall system
health, and *may* set SLOs on the percentage of successful executions if they
also control the workloads themselves, but resource exhaustion does not indicate
a problem with ATE.

### \[ICT\] C.2: Ateworker cannot access the probers

**Signal and alerting:** Error logs. If ignored, see C.4.

### \[ICT\] C.3: Ateworker cannot receive and store stdout/stderr

**Signal and alerting:** Error logs. No backup if ignored.

### \[ICT\] C.4: Actors are failing for other reasons.

E.g. atelets could fail to mount/unmount drives, or any other kind of local
failure.

**Signal:** See C.1, and specifically the `ate.actor.crashes` metric and
`ate.actor.crashed` OTLP log event.

**Alerting:** n/a

## D: Actor network requests

### \[ICT\] D.1: Inbound network requests cannot be routed

End-to-end ingress routing success can be measured by Envoy using
`envoy_http_downstream_rq_total` and `envoy_http_downstream_rq_xx`.
Unfortunately, if the actor returns a 5xx status code, Envoy can't distinguish
between an error from ATE and from the Actor, so these are coarse signals that
shouldn't be used for SLOs. By contrast, `atenet.router.route.duration` contains
all the outcomes (see [observability](observability.md)) which is a more
reliable SLI, but may be missing some failure cases.

### \[ICT\] D.2: Outbound network requests cannot be routed

TBD

# Appendix: monitoring ATE and defending SLOs

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
