# Atelet Node Placement — Design

**Status:** Draft (pending review)
**Author:** eliranw
**Date:** 2026-05-28
**Tracks:** [issue #9](https://github.com/agent-substrate/substrate/issues/9)

## Summary

Today the atelet DaemonSet runs on every schedulable node in the cluster, even on nodes that host no ateom workloads. This wastes resources on shared clusters and runs counter to the principle that atelet exists only to serve ateoms on the same node.

This design introduces **reactive node labeling**: a new controller watches ateom Pods and maintains a substrate-owned label (`ate.dev/atelet=true`) on each Node currently hosting at least one ateom. The atelet DaemonSet's `nodeSelector` is tied to that label, so atelet's footprint automatically follows ateom placement. An init container in every ateom pod waits for the local atelet's gRPC port to become reachable before the main container starts, eliminating the cold-start race between pod scheduling and atelet readiness.

The operator's role is unchanged from today: they create WorkerPools. They do not pre-label nodes; substrate handles that.

## Goals

- atelet only runs on nodes that currently host ateom workloads
- No operator workflow changes (no manual `kubectl label` step required)
- Existing installs upgrade without manual node-labeling migration
- Autoscaling-friendly: new nodes added by the cluster autoscaler are automatically used or ignored based on whether ateoms land on them
- Multiple WorkerPools may share nodes; each pool's lifecycle is independent
- Robust to controller restarts: no durable in-memory state

## Non-goals

- Substrate deciding *which nodes* the kube-scheduler should pick (we let the scheduler choose freely)
- Per-WorkerPool node-pool selection or `spec.nodeSelector` (deferred — when heterogeneous pools are needed, add the field then)
- Node taints / tolerations as a placement mechanism (label-only design)
- Pre-emptive pre-warming of atelet on nodes ahead of ateom demand
- An `ate.dev/eligible=true` opt-in candidate-pool label (deferred; not required for v1)
- Per-component labels á la gpu-operator's `nvidia.com/gpu.deploy.<component>` (single label sufficient)
- Replacing the existing ateom Deployment with a custom workload controller (separate future design)

## Motivation

[Issue #9](https://github.com/agent-substrate/substrate/issues/9) proposes two possible shapes for solving this:

1. *Manage atelets from the same workload controller as ateoms, so that it can place atelet pods on the correct nodes.*
2. *Come up with a convention for tainting certain nodes as only for ateoms, not regular workloads.*

This design follows the spirit of bullet 1, but lighter: a small Pod-keyed reconciler reactively labels Nodes based on actual ateom placement, rather than a heavier controller that pre-decides placement and races kube-scheduler.

The `docs/roadmap.md` priorities frame the choice: priority 1 is "pinning down architectural decisions." Reactive labeling is the smallest architectural commitment that closes the issue; it leaves room for richer schemes (worker-class–driven placement, autoscaling-aware capacity reservations, fungible pools) to layer on top without conflicting.

## Architecture

### Components

1. **Substrate-managed node label.** `ate.dev/atelet=true`. Present on a Node iff at least one ateom pod is currently scheduled to that Node. Substrate's controller is the sole writer; the operator never touches it.

2. **Substrate-managed claim annotations.** `ate.dev/claim.<workerpool-uid>=""` on each Node, one per WorkerPool whose ateom pods currently occupy the Node. The label is present iff at least one claim annotation exists. Using the WorkerPool's `metadata.uid` (not its name) makes refcounting robust to renames and namespace moves.

3. **Atelet DaemonSet** (`manifests/ate-install/atelet.yaml`). Carries `nodeSelector: ate.dev/atelet: "true"`. The DS controller naturally adds/removes atelet pods as labels appear/disappear.

4. **Ateom Deployment** (created by `WorkerPoolReconciler`). Has no `nodeSelector` — kube-scheduler places ateom pods freely across schedulable nodes. Each pod gets a new init container.

5. **Init container in every ateom pod.** Image `busybox:1.36`, runs `until nc -z "$HOST_IP" 8085; do sleep 1; done` where `$HOST_IP` comes from the downward API (`status.hostIP`). Tests the actual gRPC contract atelet serves; succeeds when atelet is reachable.

6. **New reconciler: `AteletNodeReconciler`** (`internal/controllers/ateletnode_controller.go`). Pod-keyed. On every ateom-pod event, computes the set of distinct WorkerPool UIDs currently occupying the affected Node, then SSA-patches the Node to converge claim annotations and label.

7. **Finalizer on WorkerPool.** `ate.dev/release-node-claims`. Added by `WorkerPoolReconciler` on first observation; held until no Node retains an `ate.dev/claim.<this-uid>` annotation. Ensures clean release of claims when a WorkerPool is deleted.

### Data flow

#### Cold start (fresh cluster, no atelets running)

```
1. Operator creates a WorkerPool { replicas: 3 }
2. WorkerPoolReconciler creates the Deployment (no nodeSelector; with init container)
3. kube-scheduler places the 3 ateom pods on nodes N1, N2, N3
4. Pods enter Pending → ContainerCreating → Init:0/1 (init container starts polling)
5. AteletNodeReconciler observes each pod with spec.nodeName set; SSA-patches N1, N2, N3:
     labels:      ate.dev/atelet=true
     annotations: ate.dev/claim.<workerpool-uid>=""
6. atelet DaemonSet controller observes the label change → schedules atelet pods on N1, N2, N3
7. atelets pull image, start, bind :8085 (TCP via hostPort)
8. Each ateom's init container's `nc -z` succeeds → init exits → ateom main container starts
9. Steady state: 3 atelets, 3 ateoms, exactly the right nodes
```

#### Scale-down (WorkerPool.replicas 3 → 1)

```
1. WorkerPoolReconciler re-applies the Deployment with replicas=1
2. Deployment controller scales the ReplicaSet down → 2 ateom pods are deleted
3. AteletNodeReconciler observes pod-deletion events on N2, N3:
     Lists remaining ateom pods on each node.
     N2, N3 no longer host any ateom pod for this pool.
     SSA-patches both: removes ate.dev/claim.<this-pool-uid>. If no other claims, removes the label.
4. atelet DaemonSet controller observes label removal → terminates atelet on N2, N3
5. Steady state: 1 atelet, 1 ateom, on the remaining node
```

#### WorkerPool deletion

```
1. Operator deletes the WorkerPool
2. WorkerPoolReconciler observes DeletionTimestamp; runs handleDeletion:
     - Triggers Deployment cascade (already implicit via owner references)
     - Polls until no Node retains ate.dev/claim.<this-uid>
     - Removes the finalizer
3. While polling: Deployment cascade deletes pods → AteletNodeReconciler removes claims/labels naturally
4. Steady state: WorkerPool gone; no claims remain; atelet drained from all formerly-claimed nodes
```

#### Pod evicted and rescheduled

```
1. Node N1 becomes NotReady; kube-controller-manager evicts ateom pod P1
2. AteletNodeReconciler observes P1 deletion event:
     If no other ateom pod for this pool on N1 → remove the pool's claim from N1
3. Deployment controller creates replacement pod P1' (different name)
4. kube-scheduler places P1' on N4
5. AteletNodeReconciler observes P1' with spec.nodeName=N4 → adds claim+label to N4
6. atelet starts on N4; P1's init container exits; ateom runs
```

### API impact

- No changes to `WorkerPool` Spec.
- New optional fields on `WorkerPool` Status considered but **deferred**: claim count, ready-node count. Not needed for v1 functionality.

### RBAC impact

The atecontroller's role gains:

```go
//+kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;patch
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
```

`nodes/patch` is the consequential addition: substrate gains write access to a cluster-scoped resource (`Node`) it does not currently touch. The blast radius is narrow — patches only add/remove `ate.dev/atelet` label and `ate.dev/claim.*` annotations — but worth being explicit about during code review.

## File-level changes

### New files

- `internal/controllers/ateletnode_controller.go` — `AteletNodeReconciler` implementation, ~180 lines
- `internal/controllers/ateletnode_controller_test.go` — unit + envtest coverage, ~300–500 lines

### Modified files

- `internal/controllers/workerpool_controller.go`
  - Add init container to the pod template in `buildDeploymentApplyConfig`
  - Add `ate.dev/release-node-claims` finalizer handling: ensure-on-create, release-on-delete with claim-cleanup loop
  - Preserve the existing `ate.dev/worker-pool: <name>` label on pod templates — the new reconciler depends on it

- `cmd/atecontroller/main.go` (or wherever the manager is wired)
  - Register `AteletNodeReconciler` alongside the existing reconcilers

- `manifests/ate-install/atelet.yaml`
  - Add `spec.template.spec.nodeSelector: { ate.dev/atelet: "true" }` to the DaemonSet

- Wherever kubebuilder RBAC is rendered (typically `config/rbac/` or generated)
  - Picked up automatically from the new `+kubebuilder:rbac` markers on each reconciler; regenerate via the project's standard `make` target

- `internal/controllers/workerpool_controller_test.go`
  - Extend with assertions for the init container's presence in the generated Deployment
  - Add a test for finalizer handling on WorkerPool deletion

- `docs/architecture.md` (if a "node placement" section doesn't already exist)
  - Brief paragraph on atelet placement

### Files explicitly unchanged

- `pkg/api/v1alpha1/workerpool_types.go` — no new spec fields
- `cmd/atelet/main.go` — atelet remains a TCP gRPC server on :8085; no sentinel file; no startup change
- `manifests/ate-install/kind/` — no kind-specific label step needed (substrate labels nodes itself)

## Key design decisions

### Why reactive labeling and not proactive

A proactive design would have the controller pre-select N nodes per WorkerPool and label them before the Deployment exists. This requires substrate to invent a node-selection policy (which N nodes? from where? with what spreading?) that the kube-scheduler already implements better. Reactive labeling lets kube-scheduler choose; substrate just records the choice.

### Why a Pod-keyed reconciler, not WorkerPool-keyed

The fact the reconciler converges on is "what ateom pods currently sit on each node." That derives from Pod events directly. A WorkerPool-keyed reconciler would re-list Pods on every reconcile; a Pod-keyed one only re-derives state for the one node affected by each event. This is the same pattern node-feature-discovery and pod-disruption-budget controllers use.

### Why an init container instead of controller-side polling

A controller polling DaemonSet status to know when atelet is Ready before creating the Deployment introduces an explicit state machine and adds visible latency to scale-up. An init container moves the wait into the per-pod startup path, where Kubernetes already has mature mechanisms for "wait, then start." It also catches a robustness scenario the controller can't: atelet upgrade or crash mid-life, where ateom should pause and resume rather than die.

### Why TCP probe instead of file sentinel

Atelet's actual contract is `:8085` gRPC, not a filesystem marker. Probing the contract directly is more honest than probing a proxy for it. It also requires no atelet code change. The hostPort 8085 on the atelet DS makes `$HOST_IP:8085` reachable from any pod on the same node.

### Why use WorkerPool UID, not name, in claim annotations

UID is stable across renames and namespace moves; name is not. If a WorkerPool were ever moved or renamed (uncommon but legal), name-keyed annotations would silently orphan; UID-keyed annotations would not.

## Failure modes

| Failure | Behavior | Notes |
|---|---|---|
| Controller down when ateom pod schedules | Pod sits in `Init:0/1` until controller returns; reconciler then labels; atelet starts; init exits | Self-healing. Eventual consistency. |
| Controller restart | New process List(Pods) → reconciles each → reaches steady state | No durable in-memory state |
| Node NotReady mid-claim | Pods become NotReady; kube-scheduler reschedules elsewhere; claim eventually moves with the pod | Stale claim on dead node persists until node deletion |
| Two pods of the same WorkerPool on the same node | One annotation, set-semantics keyed by pool UID | Refcount is per-pool, not per-pod |
| WorkerPool deleted with running ateoms | Cascade deletes Deployment → ReplicaSet → Pods; reactive reconciler removes claims as pods disappear; finalizer waits until no claims remain | Cleanly ordered via the finalizer |
| Pod evicted and rescheduled to a different node | Old node's claim removed on Delete; new node's claim added on Update with new `spec.nodeName` | Standard reactive flow |
| Atelet pod misclassified as ateom | Atelet pod lacks the `ate.dev/worker-pool` label; predicate filters it out | Tested explicitly |
| Operator manually adds the label to a node | Substrate's reconcile observes no matching claims and removes it | Substrate owns the label |
| Init container's busybox image fails to pull | Pod stuck `Init:ImagePullBackOff`; visible in `kubectl describe pod` | Standard K8s failure mode |
| Atelet never starts on a node | Init container loops forever; ateom pod never enters Running | Visible in pod status; no automatic remediation in v1 |

## Testing strategy

### Unit tests (`internal/controllers/ateletnode_controller_test.go`)

Table-driven coverage of:

- **Predicate:** ateom pods admitted (carry `ate.dev/worker-pool`), atelet pods rejected, other system pods rejected
- **podToNode mapper:** returns empty for unscheduled pods, single request for scheduled pods
- **reconcileNode patch generation:**
  - Single pool, single pod → label + one annotation added
  - Single pool, two pods on the same node → label + one annotation (not two)
  - Two pools on the same node → label + two annotations
  - Pod deleted, no other claims remain → label + annotation removed
  - Pod deleted, other pool still has claim → only this pool's annotation removed; label kept
  - Externally-set label, no claims → label removed (substrate owns it)

### Integration tests (envtest)

Extend or add to `internal/controllers/workerpool_controller_test.go`:

- WorkerPool create → Deployment created with init container in pod template
- ateom pod gains `spec.nodeName` (set via test helper) → node acquires label + claim
- Pod deleted → claim removed; label removed if last
- WorkerPool with finalizer → deletion blocks on claim cleanup
- Two WorkerPools sharing a node → independent claim/release lifecycles

### Out of scope for v1 tests

- Real-cluster e2e with rolling node restarts (substrate's e2e infra doesn't yet support this per `docs/roadmap.md`)
- Performance / scale testing under thousands of pods

## Migration and rollout

Existing clusters: when the new controller boots, it observes existing ateom pods (running on whatever nodes the scheduler had picked) and labels those nodes. atelet DaemonSet converges to that footprint.

The one-time risk window is **the order in which the controller deployment and the DaemonSet manifest update land** during upgrade. If the DaemonSet manifest with the new `nodeSelector` is applied before the new controller is running, the DS controller will start terminating atelet pods on all nodes (none labeled yet), briefly disrupting in-flight ateoms.

### Recommended two-phase rollout (documented in release notes)

1. **Phase 1 — deploy controller first.** Apply the new atecontroller image and RBAC. Do not update the atelet DS manifest yet. The controller boots, observes existing ateom pods, and labels the nodes they're running on. Verify via `kubectl get nodes -l ate.dev/atelet=true`.
2. **Phase 2 — update the atelet DS.** Apply the manifest change. The DS rolling update terminates atelet pods on unlabeled nodes (correct — no ateoms there) and retains them on labeled nodes (no disruption to running ateoms).

For installs done via a single `kubectl apply -k .`, document that the first apply may briefly disrupt running ateoms during the rolling update; re-applying after ~30 seconds ensures convergence.

Fresh installs have no migration concern.

## Alternatives considered

### Convention-based (no controller)

Operator labels nodes manually (or via IaC). Atelet DS and ateom Deployment both `nodeSelector` on the label. Smallest possible change (~40 lines). Rejected in favor of seamless: explicit operator workflow burden, doesn't autoscale, requires a flag-day migration documented in install README.

### Static taint convention

Operator taints "ate-pool" nodes with `ate.dev/dedicated:NoSchedule`; atelet and ateom tolerate. Rejected: tolerations don't *attract* pods, only bypass exclusion — so we'd still need a nodeSelector. Adds an additional governance concept (taints) that's not load-bearing.

### Proactive labeling with controller-side polling

Controller pre-selects N nodes per WorkerPool, labels them, polls DS until atelet is Ready, then creates the Deployment. Rejected: introduces a node-selection policy (which N nodes? from where?) and an explicit state machine. Adds scale-up latency. Init container approach makes proactive selection unnecessary.

### Two-tier labeling (eligibility + active)

Operator pre-marks `ate.dev/eligible=true` candidate nodes; substrate sub-selects `ate.dev/atelet=true` within that pool. Rejected for v1: solves a problem we don't have yet (excluding specific nodes from ate use). Easy to add later if needed.

## Open implementation items

These don't affect the design; they'll be resolved during implementation:

- The exact RBAC marker syntax this project uses (kubebuilder-generated vs handwritten)
- Whether substrate has a `controllerutil` import path or rolls its own finalizer helpers
- The atelet DS update strategy (default is `RollingUpdate` with `maxUnavailable: 1` — confirm acceptable during the upgrade window)
- Whether `cmd/atecontroller/main.go` uses `ctrl.NewManager` directly or via a wrapper — to determine where to wire the new reconciler
- Whether substrate uses field selectors for Pod listing (`spec.nodeName`) — potential performance optimization for `reconcileNode`'s pod-list step

## References

- [Issue #9 — Atelet should only run on nodes where ateoms are running](https://github.com/agent-substrate/substrate/issues/9)
- [Issue #47 — Need a way for actors to select workers](https://github.com/agent-substrate/substrate/issues/47) (adjacent; not addressed here)
- `docs/roadmap.md` — substrate roadmap context (priorities, security goals)
- `internal/controllers/workerpool_controller.go` — current WorkerPool reconciler
- `manifests/ate-install/atelet.yaml` — current atelet DS manifest
- `cmd/atelet/main.go:152` — atelet's TCP gRPC listen on port 8085
- gpu-operator's [`controllers/state_manager.go`](https://github.com/NVIDIA/gpu-operator/blob/main/controllers/state_manager.go) — reference for the labeling pattern (different problem shape: discovery-driven vs demand-driven)
