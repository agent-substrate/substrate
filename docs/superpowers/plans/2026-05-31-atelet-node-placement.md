# Atelet Node Placement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make atelet run only on nodes that currently host ateom pods, by adding a Pod-keyed reconciler that maintains a substrate-owned `ate.dev/atelet=true` node label, plus an init container in every ateom pod that waits for atelet's local gRPC port.

**Architecture:** Reactive labeling driven by ateom-pod events. Substrate owns the label and per-pool claim annotations (`ate.dev/claim.<workerpool-uid>`). The atelet DaemonSet's `nodeSelector` ties to that label. A `busybox:1.36` init container in every ateom pod TCP-probes `$HOST_IP:8085` until atelet is serving. A finalizer on `WorkerPool` releases this pool's claims on deletion.

**Tech Stack:** Go 1.x, `sigs.k8s.io/controller-runtime` v0.24.1, Kubernetes Server-Side Apply via `k8s.io/client-go/applyconfigurations`, kubebuilder-style RBAC markers, envtest for integration tests.

**Spec reference:** `docs/superpowers/specs/2026-05-28-atelet-node-placement-design.md` (commit `b84ad03`)

---

## Pre-flight

- [ ] **Step 1: Verify branch and current test baseline**

Run:
```bash
git status
git log --oneline -5
cd /Users/eliranw/conductor/workspaces/substrate/manama && go test ./...
```

Expected: clean working tree on `eliranw/gpu-operator-issue-9`, HEAD at `b84ad03 docs(spec): atelet node placement design (#9)`, all tests pass.

If any test fails before changes, stop and investigate — don't proceed on a red baseline.

---

## Task 1: Add shared constants and worker-pool UID label

**Why first:** The new reconciler needs the WorkerPool's UID on each ateom pod to refcount claims correctly. Adding the UID label upfront means later tasks can read it directly off pods without an extra API call.

**Files:**
- Modify: `internal/controllers/workerpool_controller.go`

- [ ] **Step 1: Add constants and modify `buildDeploymentApplyConfig` to label pods with the WorkerPool UID**

In `internal/controllers/workerpool_controller.go`, near the top, add a new constant block alongside the existing `workerPoolFieldOwner`:

```go
const (
    workerPoolFieldOwner = "workerpool-controller"

    // WorkerPoolLabelKey identifies an ateom pod's owning WorkerPool by name.
    WorkerPoolLabelKey = "ate.dev/worker-pool"
    // WorkerPoolUIDLabelKey identifies an ateom pod's owning WorkerPool by UID.
    // Used by the AteletNodeReconciler for per-pool refcounting; survives
    // WorkerPool rename / namespace move.
    WorkerPoolUIDLabelKey = "ate.dev/worker-pool-uid"
)
```

Then in `buildDeploymentApplyConfig`, change the pod-template labels block from:

```go
WithLabels(map[string]string{
    "app":                 wp.Name,
    "ate.dev/worker-pool": wp.Name,
}).
```

to:

```go
WithLabels(map[string]string{
    "app":               wp.Name,
    WorkerPoolLabelKey:  wp.Name,
    WorkerPoolUIDLabelKey: string(wp.UID),
}).
```

- [ ] **Step 2: Run existing tests to verify nothing broke**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && go test ./internal/controllers/...
```

Expected: All existing tests pass. The existing `TestWorkerPoolCreatesDeployment` checks `Labels["ate.dev/worker-pool"] != wp.Name` — that label is still set, so the test still passes.

- [ ] **Step 3: Commit**

```bash
git add internal/controllers/workerpool_controller.go
git commit -m "$(cat <<'EOF'
controllers: add worker-pool UID label to ateom pods (#9)

The new AteletNodeReconciler will refcount node claims per WorkerPool
UID. Embedding the UID directly on the pod template means the
reconciler can read it from pod labels without a separate
WorkerPool lookup — which matters because the WorkerPool may be
mid-deletion when its pods are being reconciled.
EOF
)"
```

---

## Task 2: Add init container to ateom pod template

**Why before the new reconciler:** The init container fix stands on its own — it makes ateom pods robust to atelet restarts even without the new label scheme. Shipping it first means an early commit that's useful in isolation.

**Files:**
- Modify: `internal/controllers/workerpool_controller.go`
- Test: `internal/controllers/workerpool_controller_test.go`

- [ ] **Step 1: Write the failing test**

In `internal/controllers/workerpool_controller_test.go`, after `TestWorkerPoolCreatesDeployment`, add:

```go
// TestAteomDeploymentHasInitContainer verifies that the controller adds an
// init container that waits for the local atelet's gRPC port before the main
// ateom container starts.
func TestAteomDeploymentHasInitContainer(t *testing.T) {
    wp := makeWorkerPool("test-init-container", "default", 1, "ateom:v1")
    if err := k8sClient.Create(testCtx, wp); err != nil {
        t.Fatalf("create WorkerPool: %v", err)
    }
    t.Cleanup(func() { k8sClient.Delete(testCtx, wp) }) //nolint:errcheck

    eventually(t, func(ctx context.Context) (bool, error) {
        dep, err := getDeployment(ctx, wp)
        if err != nil {
            return false, nil
        }
        inits := dep.Spec.Template.Spec.InitContainers
        if len(inits) != 1 {
            return false, nil
        }
        ic := inits[0]
        if ic.Name != "wait-for-atelet" {
            return false, nil
        }
        if ic.Image != "busybox:1.36" {
            return false, nil
        }
        // Must reference HOST_IP via downward API.
        hasHostIP := false
        for _, e := range ic.Env {
            if e.Name == "HOST_IP" && e.ValueFrom != nil &&
                e.ValueFrom.FieldRef != nil &&
                e.ValueFrom.FieldRef.FieldPath == "status.hostIP" {
                hasHostIP = true
                break
            }
        }
        return hasHostIP, nil
    })
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && go test ./internal/controllers/... -run TestAteomDeploymentHasInitContainer -v
```

Expected: FAIL — `condition not met within timeout` (no init containers in the Deployment yet).

- [ ] **Step 3: Add the init container to `buildDeploymentApplyConfig`**

In `internal/controllers/workerpool_controller.go`, locate the `buildDeploymentApplyConfig` function. Inside the `corev1ac.PodSpec()` block (after `WithSecurityContext` but before `WithVolumes`), add a `WithInitContainers` call.

The full pod-spec block should look like:

```go
WithSpec(corev1ac.PodSpec().
    WithInitContainers(corev1ac.Container().
        WithName("wait-for-atelet").
        WithImage("busybox:1.36").
        WithCommand("sh", "-c", `until nc -z "$HOST_IP" 8085; do sleep 1; done`).
        WithEnv(corev1ac.EnvVar().
            WithName("HOST_IP").
            WithValueFrom(corev1ac.EnvVarSource().
                WithFieldRef(corev1ac.ObjectFieldSelector().
                    WithFieldPath("status.hostIP"))))).
    WithContainers(corev1ac.Container().
        WithName("ateom").
        WithImage(wp.Spec.AteomImage).
        WithArgs(
            "-pod-namespace=$(POD_NAMESPACE)",
            "-pod-name=$(POD_NAME)",
        ).
        WithSecurityContext(corev1ac.SecurityContext().
            WithPrivileged(true).
            WithRunAsUser(0).
            WithRunAsGroup(0)).
        WithEnv(
            corev1ac.EnvVar().
                WithName("POD_NAMESPACE").
                WithValueFrom(corev1ac.EnvVarSource().
                    WithFieldRef(corev1ac.ObjectFieldSelector().
                        WithFieldPath("metadata.namespace"))),
            corev1ac.EnvVar().
                WithName("POD_NAME").
                WithValueFrom(corev1ac.EnvVarSource().
                    WithFieldRef(corev1ac.ObjectFieldSelector().
                        WithFieldPath("metadata.name"))),
        ).
        WithVolumeMounts(corev1ac.VolumeMount().
            WithName("run-ateom").
            WithMountPath("/run/ateom-gvisor"))).
    WithSecurityContext(corev1ac.PodSecurityContext().
        WithRunAsUser(0).
        WithRunAsGroup(0)).
    WithVolumes(corev1ac.Volume().
        WithName("run-ateom").
        WithHostPath(corev1ac.HostPathVolumeSource().
            WithPath("/run/ateom-gvisor").
            WithType(corev1.HostPathDirectoryOrCreate))))
```

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && go test ./internal/controllers/... -run TestAteomDeploymentHasInitContainer -v
```

Expected: PASS.

- [ ] **Step 5: Run the full controllers test suite**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && go test ./internal/controllers/...
```

Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add internal/controllers/workerpool_controller.go internal/controllers/workerpool_controller_test.go
git commit -m "$(cat <<'EOF'
controllers: ateom pods wait for local atelet via init container (#9)

Add a busybox:1.36 init container to every ateom pod that probes
\$HOST_IP:8085 until atelet's gRPC port is reachable. \$HOST_IP comes
from the downward API (status.hostIP). This makes ateom robust to
atelet upgrades, restarts, and node-cold-start races: the pod waits
in Init:0/1 instead of crashlooping when atelet isn't yet serving.

Independent of any node-labeling change — useful on its own.
EOF
)"
```

---

## Task 3: Add nodeSelector to atelet DaemonSet manifest

**Why now:** With Task 2 deployed, ateom pods will wait for atelet patiently. We can now safely scope atelet to a subset of nodes — the new reconciler in subsequent tasks will populate that subset.

**Files:**
- Modify: `manifests/ate-install/atelet.yaml`

- [ ] **Step 1: Add nodeSelector to the atelet DaemonSet pod template**

In `manifests/ate-install/atelet.yaml`, locate the `kind: DaemonSet` resource (around line 47). Inside `spec.template.spec`, add a `nodeSelector` field immediately after `serviceAccountName: atelet`:

```yaml
spec:
  selector:
    matchLabels:
      app: atelet
  template:
    metadata:
      labels:
        app: atelet
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
    spec:
      serviceAccountName: atelet
      nodeSelector:
        ate.dev/atelet: "true"
      containers:
      - name: atelet
        # ... rest unchanged ...
```

- [ ] **Step 2: Verify kind kustomize still builds**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && kubectl kustomize manifests/ate-install/kind --load-restrictor LoadRestrictionsNone | grep -A2 "name: atelet$" | head -20
```

Expected: Output includes the DaemonSet with `nodeSelector: { ate.dev/atelet: "true" }` preserved through the kind overlay (the overlay only patches container env, not nodeSelector).

- [ ] **Step 3: Verify Go tests still pass**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && go test ./...
```

Expected: All tests pass (this is a manifest change; Go code is unaffected).

- [ ] **Step 4: Commit**

```bash
git add manifests/ate-install/atelet.yaml
git commit -m "$(cat <<'EOF'
manifests(atelet): scope DaemonSet to ate.dev/atelet=true nodes (#9)

Atelet DaemonSet now requires the ate.dev/atelet=true label to
schedule a pod. Substrate's new AteletNodeReconciler (next commit)
will populate this label on nodes that host ateom workloads, and
remove it when they don't. The init container added in the prior
commit absorbs the atelet startup gap, so this change does not
introduce a race for ateom pods.

NOTE: at first apply on an existing cluster, the DS rolling update
will terminate atelet pods on nodes that don't have the label yet.
Deploy the new atecontroller image first; it will label nodes that
currently host ateoms before the DS update has fully rolled out.
See release notes.
EOF
)"
```

---

## Task 4: Scaffold AteletNodeReconciler — file, types, predicate, podToNode

**Files:**
- Create: `internal/controllers/ateletnode_controller.go`
- Create: `internal/controllers/ateletnode_controller_test.go`

- [ ] **Step 1: Write the failing tests for the predicate and mapper**

Create `internal/controllers/ateletnode_controller_test.go`:

```go
// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestAteomPodPredicate(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{
			name:   "ateom pod with worker-pool label is admitted",
			labels: map[string]string{WorkerPoolLabelKey: "pool-a", WorkerPoolUIDLabelKey: "uid-a"},
			want:   true,
		},
		{
			name:   "atelet pod (no worker-pool label) is rejected",
			labels: map[string]string{"app": "atelet"},
			want:   false,
		},
		{
			name:   "pod with empty labels is rejected",
			labels: map[string]string{},
			want:   false,
		},
		{
			name:   "pod with nil labels is rejected",
			labels: nil,
			want:   false,
		},
	}
	pred := ateomPodPredicate()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: tc.labels}}
			got := pred.Create(eventForObject(pod))
			if got != tc.want {
				t.Errorf("Create event: got %v, want %v", got, tc.want)
			}
			got = pred.Update(updateEventForObject(pod, pod))
			if got != tc.want {
				t.Errorf("Update event: got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPodToNode(t *testing.T) {
	tests := []struct {
		name     string
		nodeName string
		want     int // number of reconcile requests expected
	}{
		{name: "scheduled pod returns one request", nodeName: "node-1", want: 1},
		{name: "unscheduled pod returns zero requests", nodeName: "", want: 0},
	}
	r := &AteletNodeReconciler{}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Labels:    map[string]string{WorkerPoolLabelKey: "pool-a"},
				},
				Spec: corev1.PodSpec{NodeName: tc.nodeName},
			}
			got := r.podToNode(testCtx, pod)
			if len(got) != tc.want {
				t.Errorf("got %d requests, want %d", len(got), tc.want)
			}
			if len(got) == 1 && got[0].Name != tc.nodeName {
				t.Errorf("got request for %q, want %q", got[0].Name, tc.nodeName)
			}
		})
	}
}
```

This file also needs helpers `eventForObject` and `updateEventForObject` — add them in this test file:

```go
import (
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func eventForObject(obj client.Object) event.TypedCreateEvent[client.Object] {
	return event.TypedCreateEvent[client.Object]{Object: obj}
}

func updateEventForObject(oldObj, newObj client.Object) event.TypedUpdateEvent[client.Object] {
	return event.TypedUpdateEvent[client.Object]{ObjectOld: oldObj, ObjectNew: newObj}
}
```

Plus the import for `client`:
```go
import (
	"sigs.k8s.io/controller-runtime/pkg/client"
)
```

The merged imports block:

```go
import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
)
```

- [ ] **Step 2: Run tests to verify they fail to compile (no symbol exists yet)**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && go test ./internal/controllers/... -run "TestAteomPodPredicate|TestPodToNode" -v
```

Expected: build error — undefined `ateomPodPredicate`, undefined `AteletNodeReconciler`, undefined `WorkerPoolUIDLabelKey` (only if Task 1 wasn't done).

- [ ] **Step 3: Create the reconciler skeleton with predicate and mapper**

Create `internal/controllers/ateletnode_controller.go`:

```go
// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// AteletNodeLabel is the substrate-owned label that gates atelet DS
	// scheduling. Present on a Node iff at least one ateom pod is
	// currently scheduled to that Node.
	AteletNodeLabel      = "ate.dev/atelet"
	AteletNodeLabelValue = "true"

	// AteletNodeClaimAnnoPrefix is the prefix for per-pool claim
	// annotations on Nodes. Full key: ate.dev/claim.<workerpool-uid>.
	AteletNodeClaimAnnoPrefix = "ate.dev/claim."

	// AteletNodeFieldOwner is the SSA field owner for substrate-managed
	// Node fields (the label and claim annotations).
	AteletNodeFieldOwner = "atelet-node-controller"

	// PodNodeNameIndex is the field-indexer key for Pod.Spec.NodeName.
	// Required for List(client.MatchingFields{PodNodeNameIndex: ...}).
	PodNodeNameIndex = "spec.nodeName"
)

// AteletNodeReconciler reconciles ateom Pod events into Node labels and
// claim annotations so the atelet DaemonSet schedules only on Nodes
// currently hosting ateom workloads.
type AteletNodeReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;patch
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

// Reconcile is keyed by Node name (we map Pod events to Node names in
// SetupWithManager). It converges the Node's substrate-owned label and
// claim annotations to match the set of WorkerPool UIDs currently
// scheduled to it.
func (r *AteletNodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return r.reconcileNode(ctx, req.Name)
}

func (r *AteletNodeReconciler) reconcileNode(_ context.Context, _ string) (ctrl.Result, error) {
	// Implementation deferred to Task 5.
	return ctrl.Result{}, nil
}

// podToNode maps a Pod event to a reconcile request for the Pod's
// assigned Node. Returns no requests for unscheduled Pods.
func (r *AteletNodeReconciler) podToNode(_ context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}
	if pod.Spec.NodeName == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: pod.Spec.NodeName}}}
}

// ateomPodPredicate returns true for pods that look like ateom workloads
// (those carrying the WorkerPoolLabelKey). Atelet's own DS pods and other
// system pods are filtered out.
func ateomPodPredicate() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		labels := obj.GetLabels()
		if labels == nil {
			return false
		}
		_, ok := labels[WorkerPoolLabelKey]
		return ok
	})
}

// SetupWithManager registers the reconciler with the manager and adds
// a field indexer on Pod.Spec.NodeName so reconcileNode can List pods
// efficiently.
func (r *AteletNodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &corev1.Pod{}, PodNodeNameIndex, func(o client.Object) []string {
		p := o.(*corev1.Pod)
		if p.Spec.NodeName == "" {
			return nil
		}
		return []string{p.Spec.NodeName}
	}); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("atelet-node").
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(r.podToNode),
			builder.WithPredicates(ateomPodPredicate()),
		).
		Complete(r)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && go test ./internal/controllers/... -run "TestAteomPodPredicate|TestPodToNode" -v
```

Expected: PASS for both tests.

- [ ] **Step 5: Run full controllers test suite to confirm no regression**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && go test ./internal/controllers/...
```

Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add internal/controllers/ateletnode_controller.go internal/controllers/ateletnode_controller_test.go
git commit -m "$(cat <<'EOF'
controllers: scaffold AteletNodeReconciler (#9)

New Pod-keyed reconciler that will maintain the ate.dev/atelet=true
node label and ate.dev/claim.<workerpool-uid> annotations. This
commit lands the file skeleton plus the predicate and pod-to-node
mapping with unit tests. The reconcile logic itself is stubbed
(returns no-op); subsequent commits add it.

Field indexer on spec.nodeName is registered in SetupWithManager
so future List(client.MatchingFields) calls work against the
cached client.
EOF
)"
```

---

## Task 5: Implement reconcileNode core logic

**Files:**
- Modify: `internal/controllers/ateletnode_controller.go`
- Modify: `internal/controllers/ateletnode_controller_test.go`

- [ ] **Step 1: Wire the new reconciler into the envtest TestMain**

This is required so the integration tests have the reconciler running. In `internal/controllers/workerpool_controller_test.go`, modify TestMain after the existing `WorkerPoolReconciler` setup block:

```go
	if err := (&WorkerPoolReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		fmt.Fprintf(os.Stderr, "controller setup failed: %v\n", err)
		os.Exit(1)
	}

	if err := (&AteletNodeReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		fmt.Fprintf(os.Stderr, "atelet-node controller setup failed: %v\n", err)
		os.Exit(1)
	}
```

- [ ] **Step 2: Write the failing integration test for single-pod claim**

In `internal/controllers/ateletnode_controller_test.go`, add (along with existing tests):

```go
import (
	// ... existing imports ...
	"context"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/types"
)

// TestSinglePodAddsClaimAndLabel verifies that scheduling a single
// ateom pod to a node causes the reconciler to label the node and
// add a single claim annotation.
func TestSinglePodAddsClaimAndLabel(t *testing.T) {
	const nodeName = "test-node-single"
	const poolUID = "uid-single"

	node := makeNode(nodeName)
	if err := k8sClient.Create(testCtx, node); err != nil {
		t.Fatalf("create node: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(testCtx, node) }) //nolint:errcheck

	pod := makeAteomPod("pod-single", "default", "pool-single", poolUID)
	if err := k8sClient.Create(testCtx, pod); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(testCtx, pod) }) //nolint:errcheck

	// Bind the pod to the node (envtest doesn't schedule; we set nodeName directly).
	bindPodToNode(t, pod, nodeName)

	eventually(t, func(ctx context.Context) (bool, error) {
		n := &corev1.Node{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, n); err != nil {
			return false, nil
		}
		if n.Labels[AteletNodeLabel] != AteletNodeLabelValue {
			return false, nil
		}
		_, ok := n.Annotations[AteletNodeClaimAnnoPrefix+poolUID]
		return ok, nil
	})
}

// --- helpers ---

func makeNode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}
}

func makeAteomPod(name, namespace, poolName, poolUID string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				WorkerPoolLabelKey:    poolName,
				WorkerPoolUIDLabelKey: poolUID,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "ateom",
				Image: "ateom:test",
			}},
		},
	}
}

// bindPodToNode updates Pod.Spec.NodeName via the Binding subresource,
// which is the canonical "schedule a pod" operation in envtest.
func bindPodToNode(t *testing.T, pod *corev1.Pod, nodeName string) {
	t.Helper()
	binding := &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace},
		Target:     corev1.ObjectReference{Kind: "Node", Name: nodeName},
	}
	if err := k8sClient.SubResource("binding").Create(testCtx, pod, binding); err != nil {
		t.Fatalf("bind pod to node: %v", err)
	}
}
```

Add the imports if not already present (`context`, `time`, `types`, `wait`, `strings`).

- [ ] **Step 3: Run test to verify it fails**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && go test ./internal/controllers/... -run TestSinglePodAddsClaimAndLabel -v
```

Expected: FAIL — condition not met within timeout (reconcileNode is a stub).

- [ ] **Step 4: Implement reconcileNode and applyNodeClaims**

In `internal/controllers/ateletnode_controller.go`, replace the stub `reconcileNode` and add helpers. First, update imports:

```go
import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)
```

Then replace the stub `reconcileNode` with:

```go
func (r *AteletNodeReconciler) reconcileNode(ctx context.Context, nodeName string) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("node", nodeName)

	node := &corev1.Node{}
	if err := r.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		if k8errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get node %q: %w", nodeName, err)
	}

	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.MatchingFields{PodNodeNameIndex: nodeName}); err != nil {
		return ctrl.Result{}, fmt.Errorf("list pods on node %q: %w", nodeName, err)
	}

	poolUIDs := map[string]struct{}{}
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Spec.NodeName != nodeName {
			continue
		}
		// Ignore pods that have not yet been assigned a UID label
		// (in particular, atelet's own DS pods). The predicate
		// filters Watch events; the List can still return broader
		// results depending on cache configuration, so re-check here.
		uid, ok := pod.Labels[WorkerPoolUIDLabelKey]
		if !ok || uid == "" {
			continue
		}
		poolUIDs[uid] = struct{}{}
	}

	logger.V(1).Info("reconciling node claims", "pool_count", len(poolUIDs))
	return ctrl.Result{}, r.applyNodeClaims(ctx, nodeName, poolUIDs)
}

// applyNodeClaims SSA-patches the Node so that substrate-owned fields
// (the ate.dev/atelet label and ate.dev/claim.<uid> annotations) match
// the given set of WorkerPool UIDs. Any previously-owned fields not in
// the set are removed via SSA's field-ownership semantics.
func (r *AteletNodeReconciler) applyNodeClaims(ctx context.Context, nodeName string, poolUIDs map[string]struct{}) error {
	nodeAC := corev1ac.Node(nodeName)

	labels := map[string]string{}
	if len(poolUIDs) > 0 {
		labels[AteletNodeLabel] = AteletNodeLabelValue
	}
	nodeAC.Labels = labels

	annotations := map[string]string{}
	for uid := range poolUIDs {
		annotations[AteletNodeClaimAnnoPrefix+uid] = ""
	}
	nodeAC.Annotations = annotations

	if err := r.Apply(ctx, nodeAC, client.FieldOwner(AteletNodeFieldOwner), client.ForceOwnership); err != nil {
		return fmt.Errorf("apply node %q: %w", nodeName, err)
	}
	return nil
}
```

- [ ] **Step 5: Run test to verify it passes**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && go test ./internal/controllers/... -run TestSinglePodAddsClaimAndLabel -v
```

Expected: PASS.

- [ ] **Step 6: Run full controllers suite to confirm no regression**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && go test ./internal/controllers/...
```

Expected: All tests pass.

- [ ] **Step 7: Commit**

```bash
git add internal/controllers/ateletnode_controller.go internal/controllers/ateletnode_controller_test.go internal/controllers/workerpool_controller_test.go
git commit -m "$(cat <<'EOF'
controllers: implement AteletNodeReconciler.reconcileNode (#9)

reconcileNode lists ateom pods on the node (via cached
spec.nodeName field selector), computes the set of distinct
WorkerPool UIDs present, and SSA-applies the desired label +
per-pool claim annotations. SSA's granular-map semantics let us
add and remove individual claim keys without disturbing other
field owners.

Also registers AteletNodeReconciler in the envtest TestMain
so integration tests against the reconciler can run.
EOF
)"
```

---

## Task 6: Test multi-pool claim sharing

**Files:**
- Modify: `internal/controllers/ateletnode_controller_test.go`

- [ ] **Step 1: Write the test for two pools on the same node**

Append to `internal/controllers/ateletnode_controller_test.go`:

```go
// TestTwoPoolsOnSameNode verifies that two ateom pods from different
// WorkerPools, both scheduled to the same node, result in two claim
// annotations and a single label.
func TestTwoPoolsOnSameNode(t *testing.T) {
	const nodeName = "test-node-two-pools"
	const poolA = "uid-pool-a"
	const poolB = "uid-pool-b"

	node := makeNode(nodeName)
	if err := k8sClient.Create(testCtx, node); err != nil {
		t.Fatalf("create node: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(testCtx, node) }) //nolint:errcheck

	podA := makeAteomPod("pod-a", "default", "pool-a", poolA)
	podB := makeAteomPod("pod-b", "default", "pool-b", poolB)
	for _, p := range []*corev1.Pod{podA, podB} {
		if err := k8sClient.Create(testCtx, p); err != nil {
			t.Fatalf("create pod %s: %v", p.Name, err)
		}
		t.Cleanup(func(pod *corev1.Pod) func() { return func() { k8sClient.Delete(testCtx, pod) } }(p)) //nolint:errcheck
		bindPodToNode(t, p, nodeName)
	}

	eventually(t, func(ctx context.Context) (bool, error) {
		n := &corev1.Node{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, n); err != nil {
			return false, nil
		}
		if n.Labels[AteletNodeLabel] != AteletNodeLabelValue {
			return false, nil
		}
		_, okA := n.Annotations[AteletNodeClaimAnnoPrefix+poolA]
		_, okB := n.Annotations[AteletNodeClaimAnnoPrefix+poolB]
		return okA && okB, nil
	})
}

// TestPodDeletionRemovesOneClaim verifies that deleting one pod when
// another pool's pod still shares the node removes only that pool's
// claim and keeps the label.
func TestPodDeletionRemovesOneClaim(t *testing.T) {
	const nodeName = "test-node-pod-delete"
	const poolA = "uid-delete-a"
	const poolB = "uid-delete-b"

	node := makeNode(nodeName)
	if err := k8sClient.Create(testCtx, node); err != nil {
		t.Fatalf("create node: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(testCtx, node) }) //nolint:errcheck

	podA := makeAteomPod("pod-delete-a", "default", "pool-a", poolA)
	podB := makeAteomPod("pod-delete-b", "default", "pool-b", poolB)
	for _, p := range []*corev1.Pod{podA, podB} {
		if err := k8sClient.Create(testCtx, p); err != nil {
			t.Fatalf("create pod %s: %v", p.Name, err)
		}
		bindPodToNode(t, p, nodeName)
	}
	t.Cleanup(func() { k8sClient.Delete(testCtx, podB) }) //nolint:errcheck

	// Wait until both claims appear.
	eventually(t, func(ctx context.Context) (bool, error) {
		n := &corev1.Node{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, n); err != nil {
			return false, nil
		}
		_, okA := n.Annotations[AteletNodeClaimAnnoPrefix+poolA]
		_, okB := n.Annotations[AteletNodeClaimAnnoPrefix+poolB]
		return okA && okB, nil
	})

	// Delete pod A; pod B remains.
	if err := k8sClient.Delete(testCtx, podA); err != nil {
		t.Fatalf("delete pod A: %v", err)
	}

	eventually(t, func(ctx context.Context) (bool, error) {
		n := &corev1.Node{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, n); err != nil {
			return false, nil
		}
		_, hasA := n.Annotations[AteletNodeClaimAnnoPrefix+poolA]
		_, hasB := n.Annotations[AteletNodeClaimAnnoPrefix+poolB]
		hasLabel := n.Labels[AteletNodeLabel] == AteletNodeLabelValue
		return !hasA && hasB && hasLabel, nil
	})
}

// TestLastPodDeletionRemovesLabel verifies that when the last ateom
// pod on a node is deleted, both its claim annotation AND the label
// are removed.
func TestLastPodDeletionRemovesLabel(t *testing.T) {
	const nodeName = "test-node-last-delete"
	const poolUID = "uid-last"

	node := makeNode(nodeName)
	if err := k8sClient.Create(testCtx, node); err != nil {
		t.Fatalf("create node: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(testCtx, node) }) //nolint:errcheck

	pod := makeAteomPod("pod-last", "default", "pool-last", poolUID)
	if err := k8sClient.Create(testCtx, pod); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	bindPodToNode(t, pod, nodeName)

	// Wait for label + claim to appear.
	eventually(t, func(ctx context.Context) (bool, error) {
		n := &corev1.Node{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, n); err != nil {
			return false, nil
		}
		return n.Labels[AteletNodeLabel] == AteletNodeLabelValue, nil
	})

	// Delete the pod.
	if err := k8sClient.Delete(testCtx, pod); err != nil {
		t.Fatalf("delete pod: %v", err)
	}

	eventually(t, func(ctx context.Context) (bool, error) {
		n := &corev1.Node{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, n); err != nil {
			return false, nil
		}
		_, hasClaim := n.Annotations[AteletNodeClaimAnnoPrefix+poolUID]
		_, hasLabel := n.Labels[AteletNodeLabel]
		return !hasClaim && !hasLabel, nil
	})
}
```

- [ ] **Step 2: Run all new tests**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && go test ./internal/controllers/... -run "TestTwoPoolsOnSameNode|TestPodDeletionRemovesOneClaim|TestLastPodDeletionRemovesLabel" -v
```

Expected: All three PASS.

If `TestLastPodDeletionRemovesLabel` fails (label not removed when poolUIDs is empty), the issue is most likely the SSA empty-map semantics. Inspect with `kubectl get node <nodeName> -o yaml --show-managed-fields` to see what field ownership the API server records. If the label is owned by `atelet-node-controller` but isn't being removed, the apply-config's `Labels: map[string]string{}` may be serializing as nil. Workaround: assign labels to a *non-nil* empty map via the apply config's exported field (the code in Task 5 already does this — `nodeAC.Labels = labels` with `labels := map[string]string{}`).

- [ ] **Step 3: Run full controllers test suite**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && go test ./internal/controllers/...
```

Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add internal/controllers/ateletnode_controller_test.go
git commit -m "$(cat <<'EOF'
controllers: test multi-pool claim sharing on AteletNodeReconciler (#9)

Three new envtest cases:
- Two pools on the same node → two claim annotations, one label
- Deleting one pool's pod with another pool still present →
  only that pool's annotation removed, label sticks
- Deleting the last ateom pod → both the claim and the label are
  removed via SSA's granular-map removal

Validates the per-pool refcounting design.
EOF
)"
```

---

## Task 7: Wire AteletNodeReconciler into the atecontroller binary

**Files:**
- Modify: `cmd/atecontroller/main.go`

- [ ] **Step 1: Register the new reconciler alongside the existing ones**

In `cmd/atecontroller/main.go`, after the `WorkerPoolReconciler` and `ActorTemplateReconciler` setup blocks (around line 89), add:

```go
	if err = (&controllers.AteletNodeReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "AteletNode")
		os.Exit(1)
	}
```

The resulting block (existing + new) should look like:

```go
	if err = (&controllers.WorkerPoolReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "WorkerPool")
		os.Exit(1)
	}

	if err = (&controllers.ActorTemplateReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		AteClient: ateapiClient,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "ActorTemplate")
		os.Exit(1)
	}

	if err = (&controllers.AteletNodeReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "AteletNode")
		os.Exit(1)
	}

	//+kubebuilder:scaffold:builder
```

- [ ] **Step 2: Build the binary to verify it compiles**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && go build ./cmd/atecontroller/...
```

Expected: builds without error. No binary is produced (the cmd uses ko via the Makefile), but `go build` is enough to verify the wiring.

- [ ] **Step 3: Run all tests to be sure**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && go test ./...
```

Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add cmd/atecontroller/main.go
git commit -m "$(cat <<'EOF'
atecontroller: register AteletNodeReconciler in the manager (#9)

Wire the new reconciler into the atecontroller binary so it runs
alongside WorkerPool and ActorTemplate reconcilers in production.
EOF
)"
```

---

## Task 8: Add finalizer constant and on-create logic to WorkerPoolReconciler

**Files:**
- Modify: `internal/controllers/workerpool_controller.go`

- [ ] **Step 1: Write the failing test**

In `internal/controllers/workerpool_controller_test.go`, add:

```go
// TestWorkerPoolHasFinalizer verifies that the controller adds the
// node-claims finalizer to every WorkerPool on first reconcile.
func TestWorkerPoolHasFinalizer(t *testing.T) {
	wp := makeWorkerPool("test-finalizer", "default", 1, "ateom:v1")
	if err := k8sClient.Create(testCtx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(testCtx, wp) }) //nolint:errcheck

	eventually(t, func(ctx context.Context) (bool, error) {
		current := &atev1alpha1.WorkerPool{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: wp.Name, Namespace: wp.Namespace}, current); err != nil {
			return false, nil
		}
		for _, f := range current.Finalizers {
			if f == ReleaseClaimsFinalizer {
				return true, nil
			}
		}
		return false, nil
	})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && go test ./internal/controllers/... -run TestWorkerPoolHasFinalizer -v
```

Expected: compile error — `ReleaseClaimsFinalizer` not defined.

- [ ] **Step 3: Add the finalizer constant and ensure-on-create logic**

In `internal/controllers/workerpool_controller.go`, add the constant:

```go
const (
    workerPoolFieldOwner = "workerpool-controller"

    WorkerPoolLabelKey    = "ate.dev/worker-pool"
    WorkerPoolUIDLabelKey = "ate.dev/worker-pool-uid"

    // ReleaseClaimsFinalizer ensures atelet node claims tracked under
    // ate.dev/claim.<workerpool-uid> are released when the WorkerPool
    // is deleted.
    ReleaseClaimsFinalizer = "ate.dev/release-node-claims"
)
```

Add the import:

```go
import (
    // ... existing imports ...
    "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)
```

Modify `Reconcile` to handle the finalizer. The current Reconcile is:

```go
func (r *WorkerPoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	wp := &atev1alpha1.WorkerPool{}
	if err := r.Get(ctx, req.NamespacedName, wp); err != nil {
		if k8errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get worker pool %q: %w", req.NamespacedName, err)
	}

	if !wp.GetDeletionTimestamp().IsZero() {
		log.Info("WorkerPool is being deleted")
		return ctrl.Result{}, nil
	}

	if err := r.reconcileWorkerPool(ctx, wp); err != nil {
		log.Error(err, "Failed to reconcile worker pool")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}
```

Change to:

```go
func (r *WorkerPoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	wp := &atev1alpha1.WorkerPool{}
	if err := r.Get(ctx, req.NamespacedName, wp); err != nil {
		if k8errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get worker pool %q: %w", req.NamespacedName, err)
	}

	if !wp.GetDeletionTimestamp().IsZero() {
		return r.handleDeletion(ctx, wp)
	}

	if !controllerutil.ContainsFinalizer(wp, ReleaseClaimsFinalizer) {
		controllerutil.AddFinalizer(wp, ReleaseClaimsFinalizer)
		if err := r.Update(ctx, wp); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		// Subsequent reconcile will continue with normal flow.
		return ctrl.Result{}, nil
	}

	if err := r.reconcileWorkerPool(ctx, wp); err != nil {
		log.Error(err, "Failed to reconcile worker pool")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// handleDeletion is invoked when DeletionTimestamp is set. It waits
// until all of this WorkerPool's claim annotations have been released
// from Nodes (which happens naturally as the Deployment cascade
// deletes ateom pods and AteletNodeReconciler observes the deletions),
// then removes the finalizer.
func (r *WorkerPoolReconciler) handleDeletion(ctx context.Context, wp *atev1alpha1.WorkerPool) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(wp, ReleaseClaimsFinalizer) {
		return ctrl.Result{}, nil
	}

	claimKey := AteletNodeClaimAnnoPrefix + string(wp.UID)
	nodes := &corev1.NodeList{}
	if err := r.List(ctx, nodes); err != nil {
		return ctrl.Result{}, fmt.Errorf("list nodes: %w", err)
	}

	for i := range nodes.Items {
		if _, ok := nodes.Items[i].Annotations[claimKey]; ok {
			// At least one claim remains; the Pod cascade deletion
			// is still in progress. Requeue to re-check.
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	controllerutil.RemoveFinalizer(wp, ReleaseClaimsFinalizer)
	if err := r.Update(ctx, wp); err != nil {
		return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
	}
	return ctrl.Result{}, nil
}
```

Add the required imports to the top of the file:

```go
import (
    // ... existing imports ...
    "time"

    corev1 "k8s.io/api/core/v1"
    "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)
```

`corev1` is already imported. Just add `time` and `controllerutil`.

- [ ] **Step 4: Run test to verify it passes**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && go test ./internal/controllers/... -run TestWorkerPoolHasFinalizer -v
```

Expected: PASS.

- [ ] **Step 5: Run full controllers suite**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && go test ./internal/controllers/...
```

Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add internal/controllers/workerpool_controller.go internal/controllers/workerpool_controller_test.go
git commit -m "$(cat <<'EOF'
controllers: add release-node-claims finalizer to WorkerPool (#9)

The finalizer holds WorkerPool deletion until every Node has
released the per-pool claim annotation (ate.dev/claim.<wp.UID>).
Claim release happens naturally as the Deployment cascade deletes
the ateom pods and AteletNodeReconciler observes the deletions.

handleDeletion lists Nodes and requeues with backoff until no
claim remains, then removes the finalizer.
EOF
)"
```

---

## Task 9: Test WorkerPool deletion releases claims

**Files:**
- Modify: `internal/controllers/workerpool_controller_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/controllers/workerpool_controller_test.go`:

```go
// TestWorkerPoolDeletionReleasesClaims is an end-to-end check that
// deleting a WorkerPool eventually clears its claim annotation off
// every Node that hosted its pods, and removes the finalizer so the
// WorkerPool itself is fully deleted.
func TestWorkerPoolDeletionReleasesClaims(t *testing.T) {
	const nodeName = "test-node-deletion"

	// Pre-create a node so the test isn't dependent on Pod scheduling.
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}
	if err := k8sClient.Create(testCtx, node); err != nil {
		t.Fatalf("create node: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(testCtx, node) }) //nolint:errcheck

	wp := makeWorkerPool("test-deletion", "default", 1, "ateom:v1")
	if err := k8sClient.Create(testCtx, wp); err != nil {
		t.Fatalf("create WorkerPool: %v", err)
	}
	// Re-fetch to get the UID assigned by the API server.
	if err := k8sClient.Get(testCtx, types.NamespacedName{Name: wp.Name, Namespace: wp.Namespace}, wp); err != nil {
		t.Fatalf("re-fetch WorkerPool: %v", err)
	}

	// Wait until the controller has set up the Deployment + finalizer.
	eventually(t, func(ctx context.Context) (bool, error) {
		current := &atev1alpha1.WorkerPool{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: wp.Name, Namespace: wp.Namespace}, current); err != nil {
			return false, nil
		}
		for _, f := range current.Finalizers {
			if f == ReleaseClaimsFinalizer {
				return true, nil
			}
		}
		return false, nil
	})

	// Simulate a pod from this WorkerPool landing on the node.
	pod := makeAteomPod("pod-deletion", "default", wp.Name, string(wp.UID))
	if err := k8sClient.Create(testCtx, pod); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	bindPodToNode(t, pod, nodeName)

	// Wait for the claim to appear on the node.
	eventually(t, func(ctx context.Context) (bool, error) {
		n := &corev1.Node{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, n); err != nil {
			return false, nil
		}
		_, ok := n.Annotations[AteletNodeClaimAnnoPrefix+string(wp.UID)]
		return ok, nil
	})

	// Delete the pod first to simulate the Deployment cascade. (envtest
	// does not run kube-controller-manager, so the cascade does not happen
	// automatically.)
	if err := k8sClient.Delete(testCtx, pod); err != nil {
		t.Fatalf("delete pod: %v", err)
	}

	// Delete the WorkerPool. The finalizer should hold deletion until the
	// claim is gone, then release.
	if err := k8sClient.Delete(testCtx, wp); err != nil {
		t.Fatalf("delete WorkerPool: %v", err)
	}

	eventually(t, func(ctx context.Context) (bool, error) {
		current := &atev1alpha1.WorkerPool{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: wp.Name, Namespace: wp.Namespace}, current)
		// Success: 404 (gone)
		return k8errors.IsNotFound(err), nil
	})

	// And confirm the claim is gone.
	n := &corev1.Node{}
	if err := k8sClient.Get(testCtx, types.NamespacedName{Name: nodeName}, n); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if _, ok := n.Annotations[AteletNodeClaimAnnoPrefix+string(wp.UID)]; ok {
		t.Fatalf("claim annotation still present after WorkerPool deletion: %v", n.Annotations)
	}
}
```

- [ ] **Step 2: Run test to verify it passes**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && go test ./internal/controllers/... -run TestWorkerPoolDeletionReleasesClaims -v
```

Expected: PASS. If it times out waiting for the WorkerPool to be deleted, inspect `kubectl get workerpool` (against envtest's apiserver) — most likely the finalizer's claim-cleanup loop is requeuing forever because the pod's claim is still listed. Verify the pod actually got deleted in envtest by tightening the test or adding sleeps.

- [ ] **Step 3: Run full test suite**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && go test ./...
```

Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add internal/controllers/workerpool_controller_test.go
git commit -m "$(cat <<'EOF'
controllers: e2e test for WorkerPool deletion releasing claims (#9)

End-to-end envtest path: WorkerPool created → finalizer present
→ pod bound to node → claim annotation appears → pod deleted →
WorkerPool deleted → finalizer holds until claim is gone → both
WorkerPool and claim annotation are absent.
EOF
)"
```

---

## Task 10: Regenerate RBAC role manifest

**Files:**
- Modify: `manifests/ate-install/generated/role.yaml` (auto-regenerated)

- [ ] **Step 1: Run `go generate` to update RBAC manifests**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && bash hack/update/go-generate.sh
```

Expected: `manifests/ate-install/generated/role.yaml` is updated. New rules should include `nodes/get;list;watch;patch` on the empty `apiGroups` (core).

- [ ] **Step 2: Verify the generated role.yaml includes nodes/patch**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && grep -A 8 "nodes" manifests/ate-install/generated/role.yaml
```

Expected: Output shows a block like:

```yaml
- apiGroups:
  - ""
  resources:
  - nodes
  verbs:
  - get
  - list
  - watch
  - patch
```

If the patch verb isn't there, double-check that the `+kubebuilder:rbac` markers in `internal/controllers/ateletnode_controller.go` are syntactically correct.

- [ ] **Step 3: Run the full verify pipeline**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && make verify
```

Expected: All checks pass (tests, fmt, lint, codegen).

- [ ] **Step 4: Commit**

```bash
git add manifests/ate-install/generated/role.yaml
git commit -m "$(cat <<'EOF'
manifests: regenerate RBAC for AteletNodeReconciler (#9)

Adds nodes/get;list;watch;patch and pods/get;list;watch to the
ate-controller ClusterRole, picked up from the +kubebuilder:rbac
markers on the new reconciler.
EOF
)"
```

---

## Task 11: Update docs/architecture.md with the placement section

**Files:**
- Modify: `docs/architecture.md`

- [ ] **Step 1: Read the current architecture doc**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && wc -l docs/architecture.md && head -40 docs/architecture.md
```

This is to confirm structure and find a good insertion point. The doc currently has no section about node placement; add one as the last subsection of the architecture description.

- [ ] **Step 2: Append the placement section**

Append the following at the end of `docs/architecture.md`:

```markdown

## Atelet Node Placement

The atelet DaemonSet runs only on nodes that currently host at least one ateom pod. Substrate's `AteletNodeReconciler` is a Pod-keyed reconciler that watches ateom pods (identified by the `ate.dev/worker-pool` label) and maintains a substrate-owned `ate.dev/atelet=true` label on each Node currently hosting an ateom workload. The atelet DaemonSet uses `nodeSelector: ate.dev/atelet=true`, so its footprint follows ateom placement.

Per-pool refcounting is via annotations: each Node carries one `ate.dev/claim.<workerpool-uid>` annotation per WorkerPool whose ateom pods occupy it. The label is present iff at least one claim annotation exists. When the last ateom from a pool leaves a node, that pool's claim is removed; when the last claim across all pools is removed, the label is removed and atelet drains.

Every ateom pod starts with a `wait-for-atelet` init container that probes the local atelet's gRPC port (`$HOST_IP:8085`) until it responds. This absorbs the gap between pod scheduling and atelet readiness, and makes ateom robust to atelet upgrades or restarts mid-life.

WorkerPool deletion is protected by a finalizer (`ate.dev/release-node-claims`) so claims are reliably released before the WorkerPool is removed.

See `docs/superpowers/specs/2026-05-28-atelet-node-placement-design.md` for the full design.
```

- [ ] **Step 3: Commit**

```bash
git add docs/architecture.md
git commit -m "$(cat <<'EOF'
docs(architecture): document atelet node placement (#9)
EOF
)"
```

---

## Task 12: Final verification

- [ ] **Step 1: Run the whole test suite**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && go test ./...
```

Expected: All tests pass.

- [ ] **Step 2: Run lint and format checks**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && make verify
```

Expected: All checks pass.

- [ ] **Step 3: Verify kind kustomize still resolves cleanly**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && kubectl kustomize manifests/ate-install/kind --load-restrictor LoadRestrictionsNone > /tmp/ate-install-kind.yaml && grep -c "kind:" /tmp/ate-install-kind.yaml
```

Expected: positive integer (manifests still build). Diff against pre-change baseline if you want to audit what changed.

- [ ] **Step 4: Verify the atelet DS has the new nodeSelector in the rendered output**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && grep -A 2 "nodeSelector" /tmp/ate-install-kind.yaml | head -10
```

Expected: Output includes `nodeSelector: { ate.dev/atelet: "true" }` (or equivalent multi-line representation) under the atelet DS.

- [ ] **Step 5: Review the git log**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && git log --oneline b84ad03..HEAD
```

Expected: Commit list matching the tasks above:

```
... docs(architecture): document atelet node placement (#9)
... manifests: regenerate RBAC for AteletNodeReconciler (#9)
... controllers: e2e test for WorkerPool deletion releasing claims (#9)
... controllers: add release-node-claims finalizer to WorkerPool (#9)
... atecontroller: register AteletNodeReconciler in the manager (#9)
... controllers: test multi-pool claim sharing on AteletNodeReconciler (#9)
... controllers: implement AteletNodeReconciler.reconcileNode (#9)
... controllers: scaffold AteletNodeReconciler (#9)
... manifests(atelet): scope DaemonSet to ate.dev/atelet=true nodes (#9)
... controllers: ateom pods wait for local atelet via init container (#9)
... controllers: add worker-pool UID label to ateom pods (#9)
```

- [ ] **Step 6: Push to the fork**

Run:
```bash
cd /Users/eliranw/conductor/workspaces/substrate/manama && git push -u origin eliranw/gpu-operator-issue-9
```

Expected: Branch pushed to `eliranw/substrate`.

- [ ] **Step 7: Open a draft PR against upstream**

Run:
```bash
gh pr create --repo agent-substrate/substrate --base main --head eliranw:eliranw/gpu-operator-issue-9 --draft --title "Atelet node placement (#9)" --body "$(cat <<'EOF'
## Summary

Closes #9. Atelet now only runs on nodes that currently host ateom workloads.

A new `AteletNodeReconciler` watches ateom pods and maintains a substrate-owned `ate.dev/atelet=true` label on each Node currently hosting an ateom. The atelet DaemonSet's `nodeSelector` ties to that label. Per-pool refcounting via `ate.dev/claim.<workerpool-uid>` annotations supports multiple WorkerPools sharing a node.

Every ateom pod gets a `wait-for-atelet` init container (`nc -z $HOST_IP 8085`) that absorbs the scheduling → atelet-ready gap, making ateoms robust to atelet upgrades/restarts mid-life.

A `ate.dev/release-node-claims` finalizer on WorkerPool ensures claims are cleanly released on deletion.

Design doc: `docs/superpowers/specs/2026-05-28-atelet-node-placement-design.md`

## Test plan

- [x] envtest: single pod creates label + claim
- [x] envtest: two pools on same node create two claims, one label
- [x] envtest: deleting one pool's pod with another pool present keeps label
- [x] envtest: deleting last pod removes label
- [x] envtest: WorkerPool deletion releases claim via finalizer
- [x] envtest: ateom Deployment has wait-for-atelet init container
- [x] `make verify` passes
- [x] kustomize manifests resolve cleanly

## Migration / rollout

This change replaces the atelet DS's "run on all nodes" behavior with "run on labeled nodes only." Recommended two-phase rollout for existing clusters:

1. Apply the new atecontroller image + RBAC first. The new reconciler observes existing ateom pods and labels their nodes.
2. Apply the atelet DS manifest update. The DS rolling update terminates atelet pods on unlabeled nodes (correct — no ateoms there) and keeps them on labeled nodes (no disruption).

For installs done via a single `kubectl apply -k`, the first apply may briefly disrupt running ateoms during the rolling update; the init container in ateom pods (also added in this PR) ensures they wait rather than crash, so disruption is mostly invisible.
EOF
)"
```

Expected: PR URL printed. PR is in draft status pending review.

---

## Self-review checklist

After all tasks complete:

- [ ] **Spec coverage:** Every section in `docs/superpowers/specs/2026-05-28-atelet-node-placement-design.md` is implemented. Specifically: label key (✓ T4), claim annotations (✓ T4-5), atelet DS nodeSelector (✓ T3), init container (✓ T2), AteletNodeReconciler (✓ T4-7), finalizer (✓ T8-9), RBAC additions (✓ T10), failure modes covered by tests (✓ T6).
- [ ] **No placeholders:** All steps have concrete code, no TODOs.
- [ ] **Type consistency:** `WorkerPoolLabelKey`, `WorkerPoolUIDLabelKey`, `AteletNodeLabel`, `AteletNodeClaimAnnoPrefix`, `ReleaseClaimsFinalizer`, `PodNodeNameIndex` — all defined in Tasks 1 / 4 / 8, all used consistently in Tasks 5–9.
