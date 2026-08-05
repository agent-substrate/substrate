// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controlapi

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	atefake "github.com/agent-substrate/substrate/pkg/client/clientset/versioned/fake"
	"github.com/agent-substrate/substrate/pkg/client/informers/externalversions"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/fake"
)

// setupSyncerTest sets up a real store with fake Redis and a fake K8s client with informer.
func setupSyncerTest(t *testing.T, ctx context.Context, initPools ...*atev1alpha1.WorkerPool) (store.Interface, *fake.Clientset, *atefake.Clientset, func()) {
	persistence, fakeK8s, fakeAte, _, cleanup := setupSyncerTestWithStore(t, ctx, nil, initPools...)
	return persistence, fakeK8s, fakeAte, cleanup
}

func setupSyncerTestWithStore(t *testing.T, ctx context.Context, wrapStore func(store.Interface) store.Interface, initPools ...*atev1alpha1.WorkerPool) (store.Interface, *fake.Clientset, *atefake.Clientset, *WorkerPoolSyncer, func()) {
	t.Helper()

	persistence, cleanup := storetest.SetupTestStore(t)
	if wrapStore != nil {
		persistence = wrapStore(persistence)
	}

	fakeK8s := fake.NewSimpleClientset()
	workerFactory, workerInformer := WorkerPodInformer(fakeK8s)

	objects := make([]runtime.Object, len(initPools))
	for i, pool := range initPools {
		objects[i] = pool
	}
	//nolint:staticcheck // NewSimpleClientset is the only available fake clientset for versioned CRDs.
	fakeAte := atefake.NewSimpleClientset(objects...)
	ateInformerFactory := externalversions.NewSharedInformerFactory(fakeAte, 0)
	workerPoolLister := ateInformerFactory.Api().V1alpha1().WorkerPools().Lister()

	syncer := NewWorkerPoolSyncer(persistence, workerInformer, workerPoolLister)
	syncer.Start(ctx)

	workerFactory.Start(ctx.Done())
	ateInformerFactory.Start(ctx.Done())

	workerFactory.WaitForCacheSync(ctx.Done())
	ateInformerFactory.WaitForCacheSync(ctx.Done())

	return persistence, fakeK8s, fakeAte, syncer, cleanup
}

func TestSyncer_Lifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	ns := "ns-syncer-lifecycle"
	podName := "worker-unit-1"
	poolName := "pool1"

	pool := &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      poolName,
			Namespace: ns,
			Labels:    map[string]string{"foo": "bar"},
		},
		Spec: atev1alpha1.WorkerPoolSpec{
			SandboxClass: "gvisor",
		},
	}

	persistence, fakeK8s, _, cleanup := setupSyncerTest(t, ctx, pool)
	defer func() {
		// Stop syncer before closing store to prevent panics on closed miniredis.
		cancel()
		cleanup()
	}()

	// 1. Verify no workers in Redis initially
	workers, _, err := persistence.ListWorkers(context.Background(), 1000, "")
	if err != nil {
		t.Fatalf("failed to list workers: %v", err)
	}
	if len(workers) != 0 {
		t.Fatalf("expected 0 workers, got %d", len(workers))
	}

	// 2. Add pod with no IP
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: ns,
			UID:       "11111111-1111-1111-1111-111111111111",
			Labels: map[string]string{
				workerPodLabel: poolName,
			},
		},
		Spec: corev1.PodSpec{
			NodeName:   "node1",
			Containers: []corev1.Container{{Name: "main", Image: "nginx"}},
		},
	}

	_, err = fakeK8s.CoreV1().Pods(ns).Create(context.Background(), pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create pod: %v", err)
	}

	// 3. Check it's not there (polled for 500ms)
	err = wait.PollUntilContextTimeout(context.Background(), 50*time.Millisecond, 500*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		_, err := persistence.GetWorker(ctx, ns, poolName, podName)
		if err == nil {
			return false, fmt.Errorf("worker unexpectedly found in Redis")
		}
		if !errors.Is(err, store.ErrNotFound) {
			return false, err
		}
		return false, nil // Keep polling
	})
	if err != nil {
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Poll failed unexpectedly: %v", err)
		}
		// Success: timeout expired without finding the worker!
	}

	// 4. Add an IP
	updatedPod := pod.DeepCopy()
	updatedPod.Status.PodIP = "127.0.0.1"
	updatedPod.Status.PodIPs = []corev1.PodIP{{IP: "127.0.0.1"}}
	updatedPod.Status.Phase = corev1.PodRunning

	_, err = fakeK8s.CoreV1().Pods(ns).Update(context.Background(), updatedPod, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("failed to update pod: %v", err)
	}

	// 5. Check that it's added (eventually by polling)
	err = wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 2*time.Second, true, func(ctx context.Context) (bool, error) {
		w, err := persistence.GetWorker(ctx, ns, poolName, podName)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return false, nil
			}
			return false, err
		}
		if w.Ip != "127.0.0.1" {
			return false, nil
		}
		if w.SandboxClass != "gvisor" {
			return false, fmt.Errorf("expected SandboxClass gvisor, got %q", w.SandboxClass)
		}
		if !maps.Equal(w.Labels, map[string]string{"foo": "bar"}) {
			return false, fmt.Errorf("expected labels map[foo:bar], got %v", w.Labels)
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("Worker not found in Redis after update: %v", err)
	}

	// 8. Delete it
	err = fakeK8s.CoreV1().Pods(ns).Delete(context.Background(), podName, metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("failed to delete pod: %v", err)
	}

	// 9. Verify it's gone
	err = wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 2*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := persistence.GetWorker(ctx, ns, poolName, podName)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return true, nil
			}
			return false, err
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("Worker still found in Redis after deletion: %v", err)
	}
}

func TestSyncer_DeleteBoundWorker_ClearsActor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	ns, pool, pod, ip := "ns-orphan", "pool1", "worker-orphan", "10.0.0.1"
	workerPool := &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pool,
			Namespace: ns,
			Labels:    map[string]string{"foo": "bar"},
		},
		Spec: atev1alpha1.WorkerPoolSpec{
			SandboxClass: "gvisor",
		},
	}

	persistence, fakeK8s, _, cleanup := setupSyncerTest(t, ctx, workerPool)
	defer func() {
		// Stop syncer before closing store to prevent panics on closed miniredis.
		cancel()
		cleanup()
	}()
	if _, err := fakeK8s.CoreV1().Pods(ns).Create(ctx,
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pod,
				Namespace: ns,
				UID:       "11111111-1111-1111-1111-111111111111",
				Labels:    map[string]string{workerPodLabel: pool},
			},
			Spec: corev1.PodSpec{
				NodeName:   "node1",
				Containers: []corev1.Container{{Name: "main", Image: "nginx"}},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning, PodIP: ip,
				PodIPs: []corev1.PodIP{{IP: ip}},
			},
		}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	if err := wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 2*time.Second, true, func(c context.Context) (bool, error) {
		_, gerr := persistence.GetWorker(c, ns, pool, pod)
		return gerr == nil, nil
	}); err != nil {
		t.Fatalf("worker row not materialised: %v", err)
	}
	actorName := "actor-orphan"
	if _, err := persistence.CreateActor(ctx, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: actorName, Atespace: "team-orphan"}, ActorTemplateNamespace: ns, ActorTemplateName: "tmpl",
		Status: ateapipb.Actor_STATUS_RUNNING,
		WorkerAssignment: &ateapipb.WorkerAssignment{
			WorkerNamespace: ns, WorkerPool: pool, WorkerPod: pod, WorkerPodIp: ip,
		},
		InProgressSnapshot: "gs://snapshots/partial",
		LatestSnapshot:     &ateapipb.ObjectRef{Atespace: "team-orphan", Name: "last"},
	}); err != nil {
		t.Fatalf("create actor: %v", err)
	}
	w, _ := persistence.GetWorker(ctx, ns, pool, pod)
	w.Assignment = &ateapipb.Assignment{
		ActorTemplate: &ateapipb.KubeNamespacedObjectRef{
			Namespace: ns,
			Name:      "tmpl",
		},
		Actor: &ateapipb.ObjectRef{
			Name:     actorName,
			Atespace: "team-orphan",
		},
	}
	if err := persistence.UpdateWorker(ctx, w, w.Version); err != nil {
		t.Fatalf("update worker: %v", err)
	}

	if err := fakeK8s.CoreV1().Pods(ns).Delete(ctx, pod, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete pod: %v", err)
	}
	// The actor was STATUS_RUNNING when its pod vanished (it never suspended
	// cleanly), so cleanup marks it STATUS_CRASHED.
	var got *ateapipb.Actor
	if err := wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 2*time.Second, true, func(c context.Context) (bool, error) {
		a, gerr := persistence.GetActor(c, resources.ActorRef{Atespace: "team-orphan", Name: actorName})
		if gerr != nil {
			return false, gerr
		}
		got = a
		return a.GetStatus() == ateapipb.Actor_STATUS_CRASHED, nil
	}); err != nil {
		t.Fatalf("actor not reset to CRASHED: %v", err)
	}
	if got.GetWorkerAssignment() != nil || got.InProgressSnapshot != "" {
		t.Errorf("bind fields not cleared: %+v", got)
	}
	if got.GetLatestSnapshot().GetName() == "" {
		t.Errorf("LatestSnapshot must be preserved")
	}
}

func TestSyncer_OmittedFields(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	ns := "ns-syncer-omitted"
	podName := "worker-unit-1"
	poolName := "pool1"

	// Create a pool with omitted sandbox class and no labels
	pool := &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      poolName,
			Namespace: ns,
		},
		Spec: atev1alpha1.WorkerPoolSpec{
			// Spec has no SandboxClass and no Labels
		},
	}

	persistence, fakeK8s, _, cleanup := setupSyncerTest(t, ctx, pool)
	defer func() {
		// Stop syncer before closing store to prevent panics on closed miniredis.
		cancel()
		cleanup()
	}()

	// Create a pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: ns,
			UID:       "11111111-1111-1111-1111-111111111111",
			Labels: map[string]string{
				workerPodLabel: poolName,
			},
		},
		Spec: corev1.PodSpec{
			NodeName:   "node1",
			Containers: []corev1.Container{{Name: "main", Image: "nginx"}},
		},
		Status: corev1.PodStatus{
			Phase:  corev1.PodRunning,
			PodIP:  "127.0.0.1",
			PodIPs: []corev1.PodIP{{IP: "127.0.0.1"}},
		},
	}

	_, err := fakeK8s.CoreV1().Pods(ns).Create(context.Background(), pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create pod: %v", err)
	}

	// Verify that it is created in Redis with empty SandboxClass and empty Labels
	err = wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 2*time.Second, true, func(ctx context.Context) (bool, error) {
		w, err := persistence.GetWorker(ctx, ns, poolName, podName)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return false, nil
			}
			return false, err
		}
		if w.Ip != "127.0.0.1" {
			return false, nil
		}
		if w.SandboxClass != "" {
			return false, fmt.Errorf("expected SandboxClass to be empty, got %q", w.SandboxClass)
		}
		if len(w.Labels) != 0 {
			return false, fmt.Errorf("expected labels to be empty, got %v", w.Labels)
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("Worker state check failed: %v", err)
	}
}

// setupReconcileTest builds a syncer whose informer indexer can be seeded
// directly, for tests that drive reconcile synchronously without starting
// factories or worker goroutines.
func setupReconcileTest(t *testing.T, persistence store.Interface, initPools ...*atev1alpha1.WorkerPool) *WorkerPoolSyncer {
	t.Helper()
	fakeK8s := fake.NewSimpleClientset()
	_, workerInformer := WorkerPodInformer(fakeK8s)

	objects := make([]runtime.Object, len(initPools))
	for i, pool := range initPools {
		objects[i] = pool
	}
	//nolint:staticcheck // NewSimpleClientset is the only available fake clientset for versioned CRDs.
	fakeAte := atefake.NewSimpleClientset(objects...)
	ateInformerFactory := externalversions.NewSharedInformerFactory(fakeAte, 0)
	workerPoolLister := ateInformerFactory.Api().V1alpha1().WorkerPools().Lister()
	stopCh := make(chan struct{})
	t.Cleanup(func() { close(stopCh) })
	ateInformerFactory.Start(stopCh)
	ateInformerFactory.WaitForCacheSync(stopCh)

	return NewWorkerPoolSyncer(persistence, workerInformer, workerPoolLister)
}

// TestSyncer_SoftDelete_MarksDraining verifies that a pod entering Terminating
// (DeletionTimestamp set) flips its worker to STATE_DRAINING without deleting the
// worker record or touching the bound actor — the actor is still gracefully
// shutting down inside the pod.
func TestSyncer_SoftDelete_MarksDraining(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := setupReconcileTest(t, persistence)

	ns, pool, pod, ip := "ns-drain", "pool1", "worker-drain", "10.0.0.2"
	if err := persistence.CreateWorker(ctx, &ateapipb.Worker{
		WorkerNamespace: ns, WorkerPool: pool, WorkerPod: pod, Ip: ip,
		WorkerPodUid: "11111111-1111-1111-1111-111111111111", NodeName: "node1",
		State: ateapipb.Worker_STATE_ACTIVE,
	}); err != nil {
		t.Fatalf("create worker: %v", err)
	}

	deleting := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              pod,
			Namespace:         ns,
			Labels:            map[string]string{workerPodLabel: pool},
			DeletionTimestamp: &metav1.Time{Time: time.Unix(1, 0)},
		},
		Status: corev1.PodStatus{PodIP: ip, PodIPs: []corev1.PodIP{{IP: ip}}},
	}
	if err := s.workerInformer.GetIndexer().Add(deleting); err != nil {
		t.Fatalf("seed indexer: %v", err)
	}
	if err := s.reconcile(ctx, workerKey{namespace: ns, pool: pool, name: pod}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	w, err := persistence.GetWorker(ctx, ns, pool, pod)
	if err != nil {
		t.Fatalf("worker should still exist while draining: %v", err)
	}
	if w.GetState() != ateapipb.Worker_STATE_DRAINING {
		t.Errorf("expected worker state DRAINING, got %v", w.GetState())
	}
}

// TestSyncer_SoftDelete_NoPodIP pins the draining mark for a Terminating pod
// reporting no IP, the shape a pod takes once its sandbox is torn down. It
// fails if the isWorkerEligible gate moves back ahead of the deletion check.
func TestSyncer_SoftDelete_NoPodIP(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := setupReconcileTest(t, persistence)

	ns, pool, pod := "ns-drain-noip", "pool1", "worker-drain-noip"
	if err := persistence.CreateWorker(ctx, &ateapipb.Worker{
		WorkerNamespace: ns, WorkerPool: pool, WorkerPod: pod, Ip: "10.0.0.3",
		WorkerPodUid: "11111111-1111-1111-1111-111111111111", NodeName: "node1",
		State: ateapipb.Worker_STATE_ACTIVE,
	}); err != nil {
		t.Fatalf("create worker: %v", err)
	}

	deleting := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              pod,
			Namespace:         ns,
			Labels:            map[string]string{workerPodLabel: pool},
			DeletionTimestamp: &metav1.Time{Time: time.Unix(1, 0)},
		},
		// Same object as the draining test above, minus the pod IP.
		Status: corev1.PodStatus{},
	}
	if err := s.workerInformer.GetIndexer().Add(deleting); err != nil {
		t.Fatalf("seed indexer: %v", err)
	}
	if err := s.reconcile(ctx, workerKey{namespace: ns, pool: pool, name: pod}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	w, err := persistence.GetWorker(ctx, ns, pool, pod)
	if err != nil {
		t.Fatalf("get worker: %v", err)
	}
	if w.GetState() != ateapipb.Worker_STATE_DRAINING {
		t.Errorf("worker state = %v, want DRAINING", w.GetState())
	}
}

// TestMarkWorkerDraining verifies markWorkerDraining's return contract: it returns
// nil when the operation is already complete (the worker record is gone, or is
// already draining) and nil after a successful ACTIVE->DRAINING transition, which
// it persists.
func TestMarkWorkerDraining(t *testing.T) {
	ctx := context.Background()
	ns, pool, pod := "ns-mark", "pool1", "worker-mark"
	newWorker := func(state ateapipb.Worker_State) *ateapipb.Worker {
		return &ateapipb.Worker{
			WorkerNamespace: ns, WorkerPool: pool, WorkerPod: pod, Ip: "10.0.0.4",
			WorkerPodUid: "11111111-1111-1111-1111-111111111111", NodeName: "node1",
			State: state,
		}
	}

	t.Run("worker not found returns nil", func(t *testing.T) {
		persistence, cleanup := storetest.SetupTestStore(t)
		defer cleanup()
		s := &WorkerPoolSyncer{persistence: persistence}
		if err := s.markWorkerDraining(ctx, ns, pool, pod); err != nil {
			t.Errorf("markWorkerDraining on missing worker = %v, want nil", err)
		}
	})

	t.Run("already draining returns nil", func(t *testing.T) {
		persistence, cleanup := storetest.SetupTestStore(t)
		defer cleanup()
		if err := persistence.CreateWorker(ctx, newWorker(ateapipb.Worker_STATE_DRAINING)); err != nil {
			t.Fatalf("create worker: %v", err)
		}
		s := &WorkerPoolSyncer{persistence: persistence}
		if err := s.markWorkerDraining(ctx, ns, pool, pod); err != nil {
			t.Errorf("markWorkerDraining on already-draining worker = %v, want nil", err)
		}
	})

	t.Run("active worker marked draining", func(t *testing.T) {
		persistence, cleanup := storetest.SetupTestStore(t)
		defer cleanup()
		if err := persistence.CreateWorker(ctx, newWorker(ateapipb.Worker_STATE_ACTIVE)); err != nil {
			t.Fatalf("create worker: %v", err)
		}
		s := &WorkerPoolSyncer{persistence: persistence}
		if err := s.markWorkerDraining(ctx, ns, pool, pod); err != nil {
			t.Fatalf("markWorkerDraining = %v, want nil", err)
		}
		w, err := persistence.GetWorker(ctx, ns, pool, pod)
		if err != nil {
			t.Fatalf("get worker: %v", err)
		}
		if w.GetState() != ateapipb.Worker_STATE_DRAINING {
			t.Errorf("worker state = %v, want DRAINING", w.GetState())
		}
	})
}

// TestReconcileDeadWorker verifies the happy path returns nil after releasing the
// bound actor (RUNNING -> CRASHED) and deleting the worker record.
func TestReconcileDeadWorker(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	s := &WorkerPoolSyncer{persistence: persistence}

	ns, pool, pod := "ns-rdw", "pool1", "worker-rdw"
	atespace, actorID := "team-rdw", "actor-rdw"
	if _, err := persistence.CreateActor(ctx, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: actorID, Atespace: atespace}, ActorTemplateNamespace: ns, ActorTemplateName: "tmpl",
		Status: ateapipb.Actor_STATUS_RUNNING,
		WorkerAssignment: &ateapipb.WorkerAssignment{
			WorkerNamespace: ns, WorkerPool: pool, WorkerPod: pod, WorkerPodIp: "10.0.0.5",
		},
	}); err != nil {
		t.Fatalf("create actor: %v", err)
	}
	if err := persistence.CreateWorker(ctx, &ateapipb.Worker{
		WorkerNamespace: ns, WorkerPool: pool, WorkerPod: pod, Ip: "10.0.0.5",
		WorkerPodUid: "11111111-1111-1111-1111-111111111111", NodeName: "node1",
		State: ateapipb.Worker_STATE_DRAINING,
		Assignment: &ateapipb.Assignment{
			ActorTemplate: &ateapipb.KubeNamespacedObjectRef{Namespace: ns, Name: "tmpl"},
			Actor:         &ateapipb.ObjectRef{Name: actorID, Atespace: atespace},
		},
	}); err != nil {
		t.Fatalf("create worker: %v", err)
	}

	if err := s.reconcileDeadWorker(ctx, ns, pool, pod); err != nil {
		t.Fatalf("reconcileDeadWorker = %v, want nil", err)
	}
	if _, err := persistence.GetWorker(ctx, ns, pool, pod); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("worker not deleted: err=%v", err)
	}
	got, err := persistence.GetActor(ctx, resources.ActorRef{Name: actorID, Atespace: atespace})
	if err != nil {
		t.Fatalf("get actor: %v", err)
	}
	if got.GetStatus() != ateapipb.Actor_STATUS_CRASHED {
		t.Errorf("actor status = %v, want CRASHED", got.GetStatus())
	}
}

// TestSyncer_ReconcileOrphanedWorkers verifies the startup reconcile: a stored
// worker whose pod no longer exists is cleaned up (and its actor released), while
// a worker whose pod is still live is preserved. This is the durability backstop
// for delete events missed across an ate-api-server restart.
func TestSyncer_ReconcileOrphanedWorkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	persistence, cleanup := storetest.SetupTestStore(t)
	defer cleanup()

	ns, pool := "ns-recon", "pool1"
	workerPool := &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Name: pool, Namespace: ns},
		Spec:       atev1alpha1.WorkerPoolSpec{},
	}
	s := setupReconcileTest(t, persistence, workerPool)

	liveUID := "11111111-1111-1111-1111-111111111111"
	livePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "worker-live",
			Namespace: ns,
			UID:       types.UID(liveUID),
			Labels:    map[string]string{workerPodLabel: pool},
		},
		Spec:   corev1.PodSpec{NodeName: "node1", Containers: []corev1.Container{{Name: "main", Image: "nginx"}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.9", PodIPs: []corev1.PodIP{{IP: "10.0.0.9"}}},
	}
	// Seed the indexer so it is an authoritative snapshot of live pods.
	if err := s.workerInformer.GetIndexer().Add(livePod); err != nil {
		t.Fatalf("seed indexer: %v", err)
	}

	// A worker whose pod is live must be preserved.
	if err := persistence.CreateWorker(ctx, &ateapipb.Worker{
		WorkerNamespace: ns, WorkerPool: pool, WorkerPod: "worker-live", Ip: "10.0.0.9",
		WorkerPodUid: liveUID, NodeName: "node1", State: ateapipb.Worker_STATE_ACTIVE,
	}); err != nil {
		t.Fatalf("create live worker: %v", err)
	}

	// An orphan worker (no pod) whose actor is still RUNNING must be cleaned up.
	atespace, actorID := "team-recon", "actor-recon"
	if _, err := persistence.CreateActor(ctx, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Name: actorID, Atespace: atespace}, ActorTemplateNamespace: ns, ActorTemplateName: "tmpl",
		Status: ateapipb.Actor_STATUS_RUNNING,
		WorkerAssignment: &ateapipb.WorkerAssignment{
			WorkerNamespace: ns, WorkerPool: pool, WorkerPod: "worker-orphan", WorkerPodIp: "10.0.0.10",
		},
	}); err != nil {
		t.Fatalf("create actor: %v", err)
	}
	if err := persistence.CreateWorker(ctx, &ateapipb.Worker{
		WorkerNamespace: ns, WorkerPool: pool, WorkerPod: "worker-orphan", Ip: "10.0.0.10",
		WorkerPodUid: "22222222-2222-2222-2222-222222222222", NodeName: "node1",
		State: ateapipb.Worker_STATE_DRAINING,
		Assignment: &ateapipb.Assignment{
			ActorTemplate: &ateapipb.KubeNamespacedObjectRef{Namespace: ns, Name: "tmpl"},
			Actor:         &ateapipb.ObjectRef{Name: actorID, Atespace: atespace},
		},
	}); err != nil {
		t.Fatalf("create orphan worker: %v", err)
	}

	// Drive the startup pass synchronously: enqueue all stored workers and
	// drain the queue in this goroutine.
	s.enqueueStoredWorkers(ctx)
	for s.queue.Len() > 0 {
		s.processNextWorkItem(ctx)
	}

	if _, err := persistence.GetWorker(ctx, ns, pool, "worker-orphan"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("orphan worker not removed: err=%v", err)
	}
	if _, err := persistence.GetWorker(ctx, ns, pool, "worker-live"); err != nil {
		t.Errorf("live worker was wrongly removed: %v", err)
	}
	got, err := persistence.GetActor(ctx, resources.ActorRef{Name: actorID, Atespace: atespace})
	if err != nil {
		t.Fatalf("get actor: %v", err)
	}
	if got.GetStatus() != ateapipb.Actor_STATUS_CRASHED {
		t.Errorf("orphaned actor status = %v, want CRASHED", got.GetStatus())
	}
	if got.GetWorkerAssignment() != nil {
		t.Errorf("orphaned actor pod pointer not cleared: %+v", got)
	}
}

// TestReleaseActorOnDeadWorker_StatusTransitions verifies that a running actor on
// a deleted worker becomes CRASHED, while an actor that had already suspended
// cleanly stays SUSPENDED (resumable).
func TestReleaseActorOnDeadWorker_StatusTransitions(t *testing.T) {
	ns, pool, pod, ip := "ns-status", "pool1", "worker-status", "10.0.0.3"
	tests := []struct {
		name       string
		start      ateapipb.Actor_Status
		wantStatus ateapipb.Actor_Status
		wantOp     string
		wantMetric bool
	}{
		{name: "running becomes crashed", start: ateapipb.Actor_STATUS_RUNNING, wantStatus: ateapipb.Actor_STATUS_CRASHED, wantOp: ateattr.OperationUnknown, wantMetric: true},
		{name: "resuming becomes crashed", start: ateapipb.Actor_STATUS_RESUMING, wantStatus: ateapipb.Actor_STATUS_CRASHED, wantOp: ateattr.OperationResume, wantMetric: true},
		{name: "suspending becomes crashed", start: ateapipb.Actor_STATUS_SUSPENDING, wantStatus: ateapipb.Actor_STATUS_CRASHED, wantOp: ateattr.OperationSuspend, wantMetric: true},
		{name: "pausing becomes crashed", start: ateapipb.Actor_STATUS_PAUSING, wantStatus: ateapipb.Actor_STATUS_CRASHED, wantOp: ateattr.OperationPause, wantMetric: true},
		{name: "suspended stays suspended", start: ateapipb.Actor_STATUS_SUSPENDED, wantStatus: ateapipb.Actor_STATUS_SUSPENDED, wantMetric: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			if err := RegisterActorCrashes(mp.Meter("ateapi")); err != nil {
				t.Fatalf("RegisterActorCrashes: %v", err)
			}

			ctx := context.Background()
			persistence, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			s := &WorkerPoolSyncer{persistence: persistence}

			atespace, actorID := "team-status", "actor-status"
			if _, err := persistence.CreateActor(ctx, &ateapipb.Actor{
				Metadata:               &ateapipb.ResourceMetadata{Name: actorID, Atespace: atespace},
				ActorTemplateNamespace: ns,
				ActorTemplateName:      "tmpl",
				Status:                 tc.start,
				WorkerAssignment: &ateapipb.WorkerAssignment{
					WorkerNamespace: ns, WorkerPool: pool, WorkerPod: pod, WorkerPodIp: ip, WorkerPodUid: "uid",
				},
			}); err != nil {
				t.Fatalf("create actor: %v", err)
			}
			if err := persistence.CreateWorker(ctx, &ateapipb.Worker{
				WorkerNamespace: ns, WorkerPool: pool, WorkerPod: pod, Ip: ip,
				WorkerPodUid: "08675309-4a65-6e6e-7973-6e756d626572", NodeName: "node1",
				SandboxClass: "gvisor",
				State:        ateapipb.Worker_STATE_ACTIVE,
				Assignment: &ateapipb.Assignment{
					ActorTemplate: &ateapipb.KubeNamespacedObjectRef{Namespace: ns, Name: "tmpl"},
					Actor:         &ateapipb.ObjectRef{Name: actorID, Atespace: atespace},
				},
			}); err != nil {
				t.Fatalf("create worker: %v", err)
			}

			if err := s.releaseActorOnDeadWorker(ctx, ns, pool, pod); err != nil {
				t.Fatalf("releaseActorOnDeadWorker: %v", err)
			}

			got, err := persistence.GetActor(ctx, resources.ActorRef{Name: actorID, Atespace: atespace})
			if err != nil {
				t.Fatalf("get actor: %v", err)
			}
			if got.GetStatus() != tc.wantStatus {
				t.Errorf("actor status = %v, want %v", got.GetStatus(), tc.wantStatus)
			}
			if tc.wantStatus == ateapipb.Actor_STATUS_CRASHED && got.GetWorkerAssignment() != nil {
				t.Errorf("crashed actor WorkerAssignment = %v, want cleared", got.GetWorkerAssignment())
			}

			if tc.wantMetric {
				assertCrashMetricDatapoint(t, reader, tc.wantOp, ateattr.ReasonWorkerPodGone, ns, "tmpl", pool, "gvisor", 1)
			}
		})
	}

}

type conflictStore struct {
	store.Interface
	conflictTriggered atomic.Bool
	onUpdate          func(ctx context.Context, worker *ateapipb.Worker)
}

func (c *conflictStore) UpdateWorker(ctx context.Context, worker *ateapipb.Worker, expectedVersion int64) error {
	if c.onUpdate != nil && c.conflictTriggered.CompareAndSwap(false, true) {
		c.onUpdate(ctx, worker)
	}
	return c.Interface.UpdateWorker(ctx, worker, expectedVersion)
}

func TestSyncer_UpdateWorker_RetryOnVersionConflict(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	ns := "ns-syncer-conflict"
	podName := "worker-unit-conflict"
	poolName := "pool-conflict"

	pool := &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      poolName,
			Namespace: ns,
			Labels:    map[string]string{"foo": "bar"},
		},
		Spec: atev1alpha1.WorkerPoolSpec{
			SandboxClass: "gvisor",
		},
	}

	var cs *conflictStore
	persistence, fakeK8s, fakeAte, syncer, cleanup := setupSyncerTestWithStore(t, ctx, func(s store.Interface) store.Interface {
		cs = &conflictStore{Interface: s}
		return cs
	}, pool)
	defer func() {
		// Stop syncer before closing store to prevent panics on closed miniredis.
		cancel()
		cleanup()
	}()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: ns,
			UID:       "11111111-1111-1111-1111-111111111111",
			Labels: map[string]string{
				workerPodLabel: poolName,
			},
		},
		Spec: corev1.PodSpec{
			NodeName:   "node1",
			Containers: []corev1.Container{{Name: "main", Image: "nginx"}},
		},
		Status: corev1.PodStatus{
			Phase:  corev1.PodRunning,
			PodIP:  "10.0.0.1",
			PodIPs: []corev1.PodIP{{IP: "10.0.0.1"}},
		},
	}

	_, err := fakeK8s.CoreV1().Pods(ns).Create(context.Background(), pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create pod: %v", err)
	}

	err = wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		w, err := persistence.GetWorker(ctx, ns, poolName, podName)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return false, nil
			}
			return false, err
		}
		return w.Ip == "10.0.0.1", nil
	})
	if err != nil {
		t.Fatalf("Worker state check failed: %v", err)
	}

	// Update the pool SandboxClass in K8s (a mutable worker field).
	updatedPool, err := fakeAte.ApiV1alpha1().WorkerPools(ns).Get(context.Background(), poolName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get pool: %v", err)
	}
	updatedPool.Spec.SandboxClass = "microvm"
	if _, err := fakeAte.ApiV1alpha1().WorkerPools(ns).Update(context.Background(), updatedPool, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("failed to update pool: %v", err)
	}

	// Wait until the WorkerPool informer cache reflects the updated SandboxClass before triggering the pod update.
	err = wait.PollUntilContextTimeout(context.Background(), 50*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		p, err := syncer.workerPoolLister.WorkerPools(ns).Get(poolName)
		if err != nil {
			return false, nil
		}
		return p.Spec.SandboxClass == "microvm", nil
	})
	if err != nil {
		t.Fatalf("pool informer cache failed to update: %v", err)
	}

	// Configure conflictStore to inject a concurrent version bump in Redis when the syncer calls UpdateWorker.
	cs.onUpdate = func(c context.Context, w *ateapipb.Worker) {
		if cw, err := cs.Interface.GetWorker(c, ns, poolName, podName); err == nil {
			cw.NodeName = "node2"
			_ = cs.Interface.UpdateWorker(c, cw, cw.Version)
		}
	}

	// Touch the pod ONCE in K8s so the syncer reconciles it. The first reconcile's
	// UpdateWorker hits ErrVersionConflict (injected by conflictStore), which requeues
	// the key with backoff; the retry re-fetches the latest version from Redis.
	updatedPod, err := fakeK8s.CoreV1().Pods(ns).Get(context.Background(), podName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get pod: %v", err)
	}
	if updatedPod.Annotations == nil {
		updatedPod.Annotations = make(map[string]string)
	}
	updatedPod.Annotations["trigger"] = "update"
	if _, err := fakeK8s.CoreV1().Pods(ns).Update(context.Background(), updatedPod, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("failed to update pod: %v", err)
	}

	// Verify that the worker in Redis eventually gets updated to the new SandboxClass despite the version conflict.
	err = wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		w, err := persistence.GetWorker(ctx, ns, poolName, podName)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return false, nil
			}
			return false, err
		}
		return w.SandboxClass == "microvm" && w.NodeName == "node2", nil
	})
	if err != nil {
		t.Fatalf("Worker failed to update SandboxClass after version conflict: %v", err)
	}
}

// TestSyncer_RequeueOnMissingWorkerPool verifies that a pod whose WorkerPool is
// not yet in the lister cache is requeued rather than dropped, and converges
// once the pool appears.
func TestSyncer_RequeueOnMissingWorkerPool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	ns := "ns-syncer-latepool"
	podName := "worker-late-1"
	poolName := "pool-late"

	persistence, fakeK8s, fakeAte, syncer, cleanup := setupSyncerTestWithStore(t, ctx, nil) // no pools yet
	defer func() {
		// Stop syncer before closing store to prevent panics on closed miniredis.
		cancel()
		cleanup()
	}()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: ns,
			UID:       "11111111-1111-1111-1111-111111111111",
			Labels: map[string]string{
				workerPodLabel: poolName,
			},
		},
		Spec: corev1.PodSpec{
			NodeName:   "node1",
			Containers: []corev1.Container{{Name: "main", Image: "nginx"}},
		},
		Status: corev1.PodStatus{
			Phase:  corev1.PodRunning,
			PodIP:  "10.0.0.5",
			PodIPs: []corev1.PodIP{{IP: "10.0.0.5"}},
		},
	}
	if _, err := fakeK8s.CoreV1().Pods(ns).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create pod: %v", err)
	}

	// Wait until the syncer has attempted to reconcile the pod and requeued it due to the missing pool.
	key := workerKey{namespace: ns, pool: poolName, name: podName}
	err := wait.PollUntilContextTimeout(context.Background(), 10*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		return syncer.queue.NumRequeues(key) > 0, nil
	})
	if err != nil {
		t.Fatalf("syncer did not requeue pod on missing WorkerPool: %v", err)
	}

	pool := &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      poolName,
			Namespace: ns,
		},
		Spec: atev1alpha1.WorkerPoolSpec{
			SandboxClass: "gvisor",
		},
	}
	if _, err := fakeAte.ApiV1alpha1().WorkerPools(ns).Create(context.Background(), pool, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	err = wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		w, err := persistence.GetWorker(ctx, ns, poolName, podName)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return false, nil
			}
			return false, err
		}
		return w.SandboxClass == "gvisor", nil
	})
	if err != nil {
		t.Fatalf("Worker not created after pool appeared: %v", err)
	}
}

// TestSyncer_SoftDelete_ViaInformer verifies end-to-end (through informer events
// and the queue) that a pod entering graceful termination flips its worker to
// STATE_DRAINING without deleting the record.
func TestSyncer_SoftDelete_ViaInformer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	ns := "ns-syncer-softdelete"
	podName := "worker-soft-1"
	poolName := "pool1"

	pool := &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      poolName,
			Namespace: ns,
		},
		Spec: atev1alpha1.WorkerPoolSpec{
			SandboxClass: "gvisor",
		},
	}

	persistence, fakeK8s, _, cleanup := setupSyncerTest(t, ctx, pool)
	defer func() {
		// Stop syncer before closing store to prevent panics on closed miniredis.
		cancel()
		cleanup()
	}()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: ns,
			UID:       "11111111-1111-1111-1111-111111111111",
			Labels: map[string]string{
				workerPodLabel: poolName,
			},
		},
		Spec: corev1.PodSpec{
			NodeName:   "node1",
			Containers: []corev1.Container{{Name: "main", Image: "nginx"}},
		},
		Status: corev1.PodStatus{
			Phase:  corev1.PodRunning,
			PodIP:  "10.0.0.6",
			PodIPs: []corev1.PodIP{{IP: "10.0.0.6"}},
		},
	}
	if _, err := fakeK8s.CoreV1().Pods(ns).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create pod: %v", err)
	}

	if err := wait.PollUntilContextTimeout(context.Background(), 50*time.Millisecond, 2*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := persistence.GetWorker(ctx, ns, poolName, podName)
		return err == nil, nil
	}); err != nil {
		t.Fatalf("worker row not materialised: %v", err)
	}

	// The fake clientset stores updates verbatim, so setting DeletionTimestamp
	// simulates a pod in graceful termination that is still in the cache.
	deleting := pod.DeepCopy()
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	if _, err := fakeK8s.CoreV1().Pods(ns).Update(context.Background(), deleting, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("failed to update pod: %v", err)
	}

	if err := wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 2*time.Second, true, func(ctx context.Context) (bool, error) {
		w, err := persistence.GetWorker(ctx, ns, poolName, podName)
		if err != nil {
			return false, err
		}
		return w.GetState() == ateapipb.Worker_STATE_DRAINING, nil
	}); err != nil {
		t.Fatalf("Worker not marked DRAINING after DeletionTimestamp set: %v", err)
	}
}

// TestSyncer_PodRecreatedWithNewUID verifies that when a pod is deleted and
// recreated under the same name, the store row converges to the new pod's UID
// and IP even if the queue coalesces the delete and add events.
func TestSyncer_PodRecreatedWithNewUID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	ns := "ns-syncer-recreate"
	podName := "worker-recreate-1"
	poolName := "pool1"

	pool := &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      poolName,
			Namespace: ns,
		},
		Spec: atev1alpha1.WorkerPoolSpec{
			SandboxClass: "gvisor",
		},
	}

	persistence, fakeK8s, _, cleanup := setupSyncerTest(t, ctx, pool)
	defer func() {
		// Stop syncer before closing store to prevent panics on closed miniredis.
		cancel()
		cleanup()
	}()

	makePod := func(uid, ip string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: ns,
				UID:       types.UID(uid),
				Labels: map[string]string{
					workerPodLabel: poolName,
				},
			},
			Spec: corev1.PodSpec{
				NodeName:   "node1",
				Containers: []corev1.Container{{Name: "main", Image: "nginx"}},
			},
			Status: corev1.PodStatus{
				Phase:  corev1.PodRunning,
				PodIP:  ip,
				PodIPs: []corev1.PodIP{{IP: ip}},
			},
		}
	}

	oldUID := "11111111-1111-1111-1111-111111111111"
	newUID := "22222222-2222-2222-2222-222222222222"

	if _, err := fakeK8s.CoreV1().Pods(ns).Create(context.Background(), makePod(oldUID, "10.0.0.7"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create pod: %v", err)
	}
	if err := wait.PollUntilContextTimeout(context.Background(), 50*time.Millisecond, 2*time.Second, true, func(ctx context.Context) (bool, error) {
		w, err := persistence.GetWorker(ctx, ns, poolName, podName)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return false, nil
			}
			return false, err
		}
		return w.WorkerPodUid == oldUID, nil
	}); err != nil {
		t.Fatalf("worker row not materialised: %v", err)
	}

	// Update the pod in K8s directly to the new UID (simulating coalesced Delete+Create events where
	// the syncer sees the new Pod incarnation while the old UID record is still in the store).
	if _, err := fakeK8s.CoreV1().Pods(ns).Update(context.Background(), makePod(newUID, "10.0.0.8"), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("failed to update pod with new UID: %v", err)
	}

	if err := wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		w, err := persistence.GetWorker(ctx, ns, poolName, podName)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return false, nil
			}
			return false, err
		}
		return w.WorkerPodUid == newUID && w.Ip == "10.0.0.8", nil
	}); err != nil {
		t.Fatalf("Worker row did not converge to recreated pod: %v", err)
	}

	// Verify via ListWorkers that no stale worker record with oldUID remains in the store.
	workers, _, err := persistence.ListWorkers(context.Background(), 100, "")
	if err != nil {
		t.Fatalf("failed to list workers: %v", err)
	}
	if len(workers) != 1 {
		t.Fatalf("expected exactly 1 worker in store, got %d", len(workers))
	}
	if workers[0].GetWorkerPodUid() != newUID {
		t.Fatalf("expected worker in store to have UID %q, got %q", newUID, workers[0].GetWorkerPodUid())
	}
}

// TestSyncer_DeleteNeverEligiblePod verifies that deleting a pod that never got
// an IP (and thus never had a store row) is a no-op and does not error-loop.
func TestSyncer_DeleteNeverEligiblePod(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	ns := "ns-syncer-neverip"
	podName := "worker-noip-1"
	poolName := "pool1"

	pool := &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      poolName,
			Namespace: ns,
		},
		Spec: atev1alpha1.WorkerPoolSpec{
			SandboxClass: "gvisor",
		},
	}

	persistence, fakeK8s, _, cleanup := setupSyncerTest(t, ctx, pool)
	defer func() {
		// Stop syncer before closing store to prevent panics on closed miniredis.
		cancel()
		cleanup()
	}()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: ns,
			UID:       "11111111-1111-1111-1111-111111111111",
			Labels: map[string]string{
				workerPodLabel: poolName,
			},
		},
		Spec: corev1.PodSpec{
			NodeName:   "node1",
			Containers: []corev1.Container{{Name: "main", Image: "nginx"}},
		},
	}
	if _, err := fakeK8s.CoreV1().Pods(ns).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create pod: %v", err)
	}
	if err := fakeK8s.CoreV1().Pods(ns).Delete(context.Background(), podName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("failed to delete pod: %v", err)
	}

	// The store must stay empty throughout: the pod never had an IP so no row
	// was created, and the delete path is an idempotent no-op.
	err := wait.PollUntilContextTimeout(context.Background(), 50*time.Millisecond, 500*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		workers, _, err := persistence.ListWorkers(ctx, 1000, "")
		if err != nil {
			return false, err
		}
		if len(workers) != 0 {
			return false, fmt.Errorf("expected 0 workers, got %d", len(workers))
		}
		return false, nil // keep polling until timeout
	})
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("store did not stay empty: %v", err)
	}
}

// TestSyncer_InvalidWorkerIsTerminal drives reconcile directly and verifies
// that a validation failure is terminal (nil error, no requeue) and writes
// nothing to the store.
func TestSyncer_InvalidWorkerIsTerminal(t *testing.T) {
	ctx := context.Background()

	ns := "ns-syncer-invalid"
	podName := "worker-invalid-1"
	poolName := "pool1"

	persistence, cleanup := storetest.SetupTestStore(t)
	defer cleanup()

	pool := &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      poolName,
			Namespace: ns,
		},
		Spec: atev1alpha1.WorkerPoolSpec{
			SandboxClass: "gvisor",
		},
	}
	s := setupReconcileTest(t, persistence, pool)

	// Seed the indexer directly (no factories started, no workers running) so
	// reconcile can be driven synchronously.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: ns,
			UID:       "not-a-valid-uuid", // fails ValidateWorker
			Labels: map[string]string{
				workerPodLabel: poolName,
			},
		},
		Spec: corev1.PodSpec{
			NodeName:   "node1",
			Containers: []corev1.Container{{Name: "main", Image: "nginx"}},
		},
		Status: corev1.PodStatus{
			Phase:  corev1.PodRunning,
			PodIP:  "10.0.0.9",
			PodIPs: []corev1.PodIP{{IP: "10.0.0.9"}},
		},
	}
	if err := s.workerInformer.GetIndexer().Add(pod); err != nil {
		t.Fatalf("failed to seed indexer: %v", err)
	}

	key := workerKey{namespace: ns, pool: poolName, name: podName}
	if err := s.reconcile(ctx, key); err != nil {
		t.Fatalf("reconcile returned error for invalid worker (should be terminal): %v", err)
	}
	if _, err := persistence.GetWorker(ctx, ns, poolName, podName); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected no worker row for invalid worker, got err=%v", err)
	}
}
