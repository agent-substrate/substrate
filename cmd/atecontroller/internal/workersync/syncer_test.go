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

package workersync

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	atefake "github.com/agent-substrate/substrate/pkg/client/clientset/versioned/fake"
	"github.com/agent-substrate/substrate/pkg/client/informers/externalversions"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"
)

// testPodUID is the pod UID most syncer tests give their single worker pod.
// The syncer names Workers after their pod UID, so it is also the name the
// resulting registry record is keyed by.
const (
	testPodUID  = "11111111-1111-1111-1111-111111111111"
	otherPodUID = "22222222-2222-2222-2222-222222222222"
)

// workerPod builds a pod the informer's label selector matches. An empty ip
// leaves the pod ineligible, the shape a pod has before its sandbox is up.
func workerPod(ns, name, poolName, uid, ip string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID(uid),
			Labels:    map[string]string{workerPodLabel: poolName},
		},
		Spec: corev1.PodSpec{
			NodeName:   "node1",
			Containers: []corev1.Container{{Name: "main", Image: "nginx"}},
		},
	}
	if ip != "" {
		pod.Status = corev1.PodStatus{
			Phase:  corev1.PodRunning,
			PodIP:  ip,
			PodIPs: []corev1.PodIP{{IP: ip}},
			// Eligibility requires Ready in addition to an IP: readiness is
			// what says ateom is actually serving, not just that the sandbox
			// got an address.
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
		}
	}
	return pod
}

func workerPool(ns, name, sandboxClass string, labels map[string]string) *atev1alpha1.WorkerPool {
	return &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: labels},
		Spec:       atev1alpha1.WorkerPoolSpec{SandboxClasses: []atev1alpha1.WorkerPoolSandboxClass{{Name: atev1alpha1.SandboxClass(sandboxClass)}}},
	}
}

// registeredWorker is a Worker in the shape the syncer would have registered
// for the given pod, for seeding the fake API before a test runs.
func registeredWorker(ns, poolName, podName, uid, ip string) *ateapipb.Worker {
	return &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: uid},
		WorkerNamespace: ns,
		WorkerPool:      poolName,
		WorkerPod:       podName,
		WorkerPodUid:    uid,
		Ips:             []string{ip},
		NodeName:        "node1",
	}
}

// newWorkerPoolInformer builds the WorkerPool informer the syncer reads, backed
// by a fake clientset that holds initPools. Nothing is cached until the factory
// starts.
func newWorkerPoolInformer(initPools ...*atev1alpha1.WorkerPool) (*atefake.Clientset, externalversions.SharedInformerFactory, cache.SharedIndexInformer) {
	objs := make([]runtime.Object, 0, len(initPools))
	for _, pool := range initPools {
		objs = append(objs, pool)
	}
	//nolint:staticcheck // NewSimpleClientset is the only available fake clientset for versioned CRDs.
	client := atefake.NewSimpleClientset(objs...)
	factory := externalversions.NewSharedInformerFactory(client, 0)
	return client, factory, factory.Api().V1alpha1().WorkerPools().Informer()
}

// setupSyncerTest wires a running syncer to a fake Control API and a fake
// Kubernetes, driving it end to end through pod and pool events and the queue.
// It returns the fake Kubernetes and the fake clientset the pools live in.
func setupSyncerTest(t *testing.T, ctx context.Context, api *fakeControl, initPools ...*atev1alpha1.WorkerPool) (*fake.Clientset, *atefake.Clientset) {
	t.Helper()

	//nolint:staticcheck // NewSimpleClientset is what the informer machinery takes.
	fakeK8s := fake.NewSimpleClientset()
	workerFactory, workerInformer := WorkerPodInformer(fakeK8s)
	poolClient, poolFactory, workerPoolInformer := newWorkerPoolInformer(initPools...)

	// Start before the factories: the informer's initial list is what seeds the
	// queue with the pods that already exist.
	NewWorkerPoolSyncer(api, fakeK8s.CoreV1(), fakeK8s.AppsV1(), workerInformer, workerPoolInformer).Start(ctx)
	workerFactory.Start(ctx.Done())
	poolFactory.Start(ctx.Done())
	workerFactory.WaitForCacheSync(ctx.Done())
	poolFactory.WaitForCacheSync(ctx.Done())

	return fakeK8s, poolClient
}

// setupReconcileTest builds a syncer whose pod and pool caches can be seeded
// directly, for tests that drive reconcile synchronously without starting
// factories or worker goroutines. It returns those caches alongside the syncer.
// The syncer's pod and apps clients are a fake clientset that the caches do not
// watch.
func setupReconcileTest(t *testing.T, api *fakeControl, initPools ...*atev1alpha1.WorkerPool) (*WorkerPoolSyncer, cache.Indexer, cache.Indexer) {
	t.Helper()

	//nolint:staticcheck // NewSimpleClientset is what the informer machinery takes.
	fakeK8s := fake.NewSimpleClientset()
	_, workerInformer := WorkerPodInformer(fakeK8s)
	_, _, workerPoolInformer := newWorkerPoolInformer()
	poolIndexer := workerPoolInformer.GetIndexer()
	for _, pool := range initPools {
		if err := poolIndexer.Add(pool); err != nil {
			t.Fatalf("seeding WorkerPool %s/%s: %v", pool.Namespace, pool.Name, err)
		}
	}

	return NewWorkerPoolSyncer(api, fakeK8s.CoreV1(), fakeK8s.AppsV1(), workerInformer, workerPoolInformer), workerInformer.GetIndexer(), poolIndexer
}

// seedPod puts a pod in the syncer's cache as though the informer had delivered
// it, and returns the key a pod event for it would have enqueued.
func seedPod(t *testing.T, pods cache.Indexer, pod *corev1.Pod) workerKey {
	t.Helper()
	if err := pods.Add(pod); err != nil {
		t.Fatalf("seeding pod %s/%s: %v", pod.Namespace, pod.Name, err)
	}
	return workerKey{namespace: pod.Namespace, name: pod.Name, uid: string(pod.UID)}
}

// mustReconcile drives one reconcile of key and fails the test if it errors.
func mustReconcile(t *testing.T, ctx context.Context, s *WorkerPoolSyncer, key workerKey) {
	t.Helper()
	if err := s.reconcile(ctx, key); err != nil {
		t.Fatalf("reconcile(%+v): %v", key, err)
	}
}

// waitForWorker polls the registry until the named worker satisfies cond.
func waitForWorker(t *testing.T, ctx context.Context, api *fakeControl, name string, cond func(*ateapipb.Worker) bool) *ateapipb.Worker {
	t.Helper()
	var got *ateapipb.Worker
	err := wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, 5*time.Second, true, func(context.Context) (bool, error) {
		got = api.get(name)
		return cond(got), nil
	})
	if err != nil {
		t.Fatalf("waiting on worker %s: %v (last seen %v)", name, err, got)
	}
	return got
}

func TestSyncer_Lifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ns, podName, poolName := "ns-syncer-lifecycle", "worker-unit-1", "pool1"

	api := newFakeControl()
	fakeK8s, _ := setupSyncerTest(t, ctx, api, workerPool(ns, poolName, "gvisor", map[string]string{"foo": "bar"}))

	// A pod with no IP is not registered: there is nothing to route to yet.
	pod := workerPod(ns, podName, poolName, testPodUID, "")
	if _, err := fakeK8s.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	// There is no state to wait for, so this gives the syncer a window to get it
	// wrong in: the poll only ends early if a worker shows up.
	err := wait.PollUntilContextTimeout(ctx, 20*time.Millisecond, 300*time.Millisecond, true, func(context.Context) (bool, error) {
		if got := api.names(); len(got) != 0 {
			return false, fmt.Errorf("registry holds %v for a pod with no IP, want it empty", got)
		}
		return false, nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("while checking a pod with no IP stays unregistered: %v", err)
	}

	// Once the pod reports an IP it becomes eligible and is registered, with the
	// fields the syncer copies off its pool.
	if _, err := fakeK8s.CoreV1().Pods(ns).Update(ctx, workerPod(ns, podName, poolName, testPodUID, "127.0.0.1"), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update pod: %v", err)
	}

	got := waitForWorker(t, ctx, api, testPodUID, func(w *ateapipb.Worker) bool { return w != nil })
	if want := []string{"127.0.0.1"}; !slices.Equal(got.GetIps(), want) {
		t.Errorf("worker ips = %q, want %q", got.GetIps(), want)
	}
	if got.GetSandboxClass() != "gvisor" {
		t.Errorf("worker sandbox class = %q, want gvisor", got.GetSandboxClass())
	}
	if !maps.Equal(got.GetLabels(), map[string]string{"foo": "bar"}) {
		t.Errorf("worker labels = %v, want map[foo:bar]", got.GetLabels())
	}
	if got.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
		t.Errorf("worker state = %v, want ACTIVE", got.GetStatus().GetState())
	}

	if err := fakeK8s.CoreV1().Pods(ns).Delete(ctx, podName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete pod: %v", err)
	}
	waitForWorker(t, ctx, api, testPodUID, func(w *ateapipb.Worker) bool { return w == nil })
}

// A pool that sets neither a sandbox class nor labels registers a worker
// carrying neither, rather than one carrying some default.
func TestSyncer_OmittedFields(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ns, podName, poolName := "ns-syncer-omitted", "worker-unit-1", "pool1"

	api := newFakeControl()
	fakeK8s, _ := setupSyncerTest(t, ctx, api, workerPool(ns, poolName, "", nil))

	pod := workerPod(ns, podName, poolName, testPodUID, "127.0.0.1")
	if _, err := fakeK8s.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}

	got := waitForWorker(t, ctx, api, testPodUID, func(w *ateapipb.Worker) bool { return w != nil })
	if got.GetSandboxClass() != "" {
		t.Errorf("worker sandbox class = %q, want it empty", got.GetSandboxClass())
	}
	if len(got.GetLabels()) != 0 {
		t.Errorf("worker labels = %v, want none", got.GetLabels())
	}
}

// Capacity belongs to the Worker, which sets it over WorkerService. The
// syncer must not infer one from the pod, even when the ateom container spells
// its limits out: the pod's limits bound the sandbox, but what the ateom can
// actually supply to Actors is the ateom's to say.
func TestSyncer_DoesNotInferCapacityFromThePod(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ns, podName, poolName := "ns-syncer-capacity", "worker-unit-1", "pool1"

	api := newFakeControl()
	fakeK8s, _ := setupSyncerTest(t, ctx, api, workerPool(ns, poolName, "gvisor", nil))

	pod := workerPod(ns, podName, poolName, testPodUID, "127.0.0.1")
	pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{
		Name:  "ateom",
		Image: "ateom",
		Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("4"),
			corev1.ResourceMemory: resource.MustParse("8Gi"),
		}},
	})
	if _, err := fakeK8s.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}

	// The registry here is a fake, so what it holds is exactly what the syncer
	// sent. The syncer writes no capacity: it comes from the Worker's own
	// report, which is the ateom's to make.
	got := waitForWorker(t, ctx, api, testPodUID, func(w *ateapipb.Worker) bool { return w != nil })
	if got.GetStatus().GetCapacity() != nil {
		t.Errorf("worker capacity = %v, want none", got.GetStatus().GetCapacity())
	}
}

// TestSyncer_SoftDelete_MarksDraining verifies that a pod entering Terminating
// (DeletionTimestamp set) flips its worker to STATE_DRAINING without deleting
// the worker record — the actor inside is still gracefully shutting down.
func TestSyncer_SoftDelete_MarksDraining(t *testing.T) {
	ctx := context.Background()
	ns, poolName, podName, ip := "ns-drain", "pool1", "worker-drain", "10.0.0.2"

	api := newFakeControl()
	api.put(registeredWorker(ns, poolName, podName, testPodUID, ip))
	s, pods, _ := setupReconcileTest(t, api)

	pod := workerPod(ns, podName, poolName, testPodUID, ip)
	pod.DeletionTimestamp = &metav1.Time{Time: time.Unix(1, 0)}
	mustReconcile(t, ctx, s, seedPod(t, pods, pod))

	w := api.get(testPodUID)
	if w == nil {
		t.Fatal("worker should still exist while draining")
	}
	if w.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_DRAINING {
		t.Errorf("worker state = %v, want DRAINING", w.GetStatus().GetState())
	}
}

// TestSyncer_SoftDelete_NoPodIP pins the draining mark for a Terminating pod
// reporting no IP, the shape a pod takes once its sandbox is torn down. It
// fails if the isWorkerEligible gate moves back ahead of the deletion check.
func TestSyncer_SoftDelete_NoPodIP(t *testing.T) {
	ctx := context.Background()
	ns, poolName, podName := "ns-drain-noip", "pool1", "worker-drain-noip"

	api := newFakeControl()
	api.put(registeredWorker(ns, poolName, podName, testPodUID, "10.0.0.3"))
	s, pods, _ := setupReconcileTest(t, api)

	// Same pod as the draining test above, minus the pod IP.
	pod := workerPod(ns, podName, poolName, testPodUID, "")
	pod.DeletionTimestamp = &metav1.Time{Time: time.Unix(1, 0)}
	mustReconcile(t, ctx, s, seedPod(t, pods, pod))

	if got := api.get(testPodUID).GetStatus().GetState(); got != ateapipb.WorkerState_WORKER_STATE_DRAINING {
		t.Errorf("worker state = %v, want DRAINING", got)
	}
}

// TestMarkWorkerDraining verifies markWorkerDraining's return contract: nil
// when the work is already done (the record is gone, or is already draining)
// and nil after a successful ACTIVE->DRAINING transition, which it persists.
func TestMarkWorkerDraining(t *testing.T) {
	ctx := context.Background()
	ns, poolName, podName := "ns-mark", "pool1", "worker-mark"
	key := workerKey{namespace: ns, name: podName, uid: testPodUID}

	t.Run("worker not found returns nil", func(t *testing.T) {
		s := &WorkerPoolSyncer{client: newFakeControl()}
		if err := s.markWorkerDraining(ctx, key); err != nil {
			t.Errorf("markWorkerDraining on missing worker = %v, want nil", err)
		}
	})

	t.Run("already draining returns nil without a write", func(t *testing.T) {
		api := newFakeControl()
		w := registeredWorker(ns, poolName, podName, testPodUID, "10.0.0.4")
		w.Status = &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_DRAINING}
		seeded := api.put(w)

		s := &WorkerPoolSyncer{client: api}
		if err := s.markWorkerDraining(ctx, key); err != nil {
			t.Errorf("markWorkerDraining on already-draining worker = %v, want nil", err)
		}
		// Every pod event on a Terminating pod re-drives the drain, so a repeat
		// must not churn the version.
		if got := api.get(testPodUID).GetMetadata().GetVersion(); got != seeded.GetMetadata().GetVersion() {
			t.Errorf("worker version = %d, want it unchanged at %d", got, seeded.GetMetadata().GetVersion())
		}
	})

	t.Run("active worker marked draining", func(t *testing.T) {
		api := newFakeControl()
		api.put(registeredWorker(ns, poolName, podName, testPodUID, "10.0.0.4"))

		s := &WorkerPoolSyncer{client: api}
		if err := s.markWorkerDraining(ctx, key); err != nil {
			t.Fatalf("markWorkerDraining = %v, want nil", err)
		}
		if got := api.get(testPodUID).GetStatus().GetState(); got != ateapipb.WorkerState_WORKER_STATE_DRAINING {
			t.Errorf("worker state = %v, want DRAINING", got)
		}
	})
}

// TestReconcileDeadWorker covers reconcileDeadWorker's return contract: the
// record is gone afterwards, and a record that was already gone is success
// rather than an error, which is what makes re-driving a reconcile safe.
func TestReconcileDeadWorker(t *testing.T) {
	ctx := context.Background()
	ns, poolName, podName := "ns-del", "pool1", "worker-del"
	key := workerKey{namespace: ns, name: podName, uid: testPodUID}

	t.Run("registered worker is removed", func(t *testing.T) {
		api := newFakeControl()
		api.put(registeredWorker(ns, poolName, podName, testPodUID, "10.0.0.5"))

		s := &WorkerPoolSyncer{client: api}
		if err := s.reconcileDeadWorker(ctx, key); err != nil {
			t.Fatalf("reconcileDeadWorker = %v, want nil", err)
		}
		if got := api.get(testPodUID); got != nil {
			t.Errorf("worker still registered: %v", got)
		}
	})

	t.Run("absent worker returns nil", func(t *testing.T) {
		s := &WorkerPoolSyncer{client: newFakeControl()}
		if err := s.reconcileDeadWorker(ctx, key); err != nil {
			t.Errorf("reconcileDeadWorker on missing worker = %v, want nil", err)
		}
	})
}

// TestSyncer_ReconcileOrphanedWorkers verifies the startup scan: a registered
// worker whose pod no longer exists is cleaned up, while a worker whose pod is
// still live is preserved. This is the durability backstop for delete events
// missed across an ate-controller restart.
func TestSyncer_ReconcileOrphanedWorkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ns, poolName := "ns-recon", "pool1"
	api := newFakeControl()
	api.put(registeredWorker(ns, poolName, "worker-live", testPodUID, "10.0.0.9"))
	api.put(registeredWorker(ns, poolName, "worker-orphan", otherPodUID, "10.0.0.10"))

	s, pods, _ := setupReconcileTest(t, api, workerPool(ns, poolName, "", nil))
	// The cache holds only the live pod, so it is an authoritative snapshot.
	seedPod(t, pods, workerPod(ns, "worker-live", poolName, testPodUID, "10.0.0.9"))

	// Drive the startup pass synchronously: scan the registry into the queue,
	// then reconcile everything it enqueued.
	s.enqueueRegisteredWorkers(ctx)
	for range s.queue.Len() {
		s.processNextWorkItem(ctx)
	}

	if got := api.get(otherPodUID); got != nil {
		t.Errorf("orphan worker not removed: %v", got)
	}
	if api.get(testPodUID) == nil {
		t.Error("live worker was wrongly removed")
	}
}

// A transient failure mid-scan must not abandon the startup enqueue of
// registered workers: enqueueRegisteredWorkers retries the list so a blip does
// not skip workers (leaving ghost records) until the next restart. The per-key
// workqueue retries reconciles, but not this initial scan.
func TestSyncer_EnqueueRegisteredWorkers_RetriesTransientListError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	api := newFakeControl()
	api.put(registeredWorker("ns-enq-retry", "pool1", "worker-1", testPodUID, "10.0.0.10"))

	failsLeft := 1
	api.setListHook(func(*ateapipb.ListWorkersRequest) error {
		if failsLeft > 0 {
			failsLeft--
			return status.Error(codes.Unavailable, "transient failure")
		}
		return nil
	})

	s, _, _ := setupReconcileTest(t, api)
	// Shrink the retry backoff so the test's single retry is fast.
	s.listBackoff = time.Millisecond
	s.enqueueRegisteredWorkers(ctx)

	if failsLeft != 0 {
		t.Errorf("injected failure was never consumed; ListWorkers should have been retried")
	}
	if got := s.queue.Len(); got != 1 {
		t.Errorf("queue holds %d keys, want 1 (the worker must be enqueued after the retry)", got)
	}
}

// listWorkersPageWithRetry must stop retrying and return once the context is
// cancelled, rather than spinning forever, when the API stays unavailable.
func TestSyncer_ListWorkersPageWithRetry_StopsOnContextCancel(t *testing.T) {
	api := newFakeControl()
	api.setListHook(func(*ateapipb.ListWorkersRequest) error {
		return status.Error(codes.Unavailable, "still down")
	})
	s, _, _ := setupReconcileTest(t, api)
	s.listBackoff = time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if _, err := s.listWorkersPageWithRetry(ctx, ""); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("listWorkersPageWithRetry() error = %v, want it to wrap context.DeadlineExceeded", err)
	}
}

// enqueueRegisteredWorkers streams pages: a transient error on a later page must
// retry just that page (the earlier pages stay enqueued) and the scan must still
// enumerate every worker across all pages.
func TestSyncer_EnqueueRegisteredWorkers_StreamsPagesAndRetriesLatePage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	api := newFakeControl()
	api.listPageSize = 1
	for i, uid := range []string{
		"aaaaaaaa-0000-0000-0000-000000000000",
		"bbbbbbbb-0000-0000-0000-000000000000",
		"cccccccc-0000-0000-0000-000000000000",
	} {
		api.put(registeredWorker("ns", "p", fmt.Sprintf("w%d", i), uid, "10.0.0.1"))
	}

	// The second page fails once before serving.
	failed := false
	api.setListHook(func(req *ateapipb.ListWorkersRequest) error {
		if req.GetPageToken() == "1" && !failed {
			failed = true
			return status.Error(codes.Unavailable, "transient failure on page")
		}
		return nil
	})

	s, _, _ := setupReconcileTest(t, api)
	s.listBackoff = time.Millisecond
	s.enqueueRegisteredWorkers(ctx)

	if !failed {
		t.Error("expected the injected page-2 failure to be exercised")
	}
	if got := s.queue.Len(); got != 3 {
		t.Errorf("queue holds %d keys, want 3 (every worker across all pages must be enqueued despite the transient page error)", got)
	}
}

// A dual-stack pod registers every IP Kubernetes reports, in order.
func TestSyncer_CreateWorker_DualStack(t *testing.T) {
	ctx := context.Background()

	ns, podName, poolName := "ns-syncer-dual-stack", "worker-unit-dual-stack", "pool-dual-stack"

	api := newFakeControl()
	s, pods, _ := setupReconcileTest(t, api, workerPool(ns, poolName, "gvisor", nil))
	pod := workerPod(ns, podName, poolName, testPodUID, "fd00::1")
	pod.Status.PodIPs = append(pod.Status.PodIPs, corev1.PodIP{IP: "10.0.0.1"})
	key := seedPod(t, pods, pod)

	mustReconcile(t, ctx, s, key)
	if got, want := api.get(testPodUID).GetIps(), []string{"fd00::1", "10.0.0.1"}; !slices.Equal(got, want) {
		t.Fatalf("worker ips = %q, want %q", got, want)
	}
}

// A concurrent write between the syncer's read and its update loses the version
// precondition. The key requeues, and the retry must re-read rather than replay
// its stale copy — which is what lets the concurrent change and the syncer's own
// change both survive.
func TestSyncer_UpdateWorker_RetryOnVersionConflict(t *testing.T) {
	ctx := context.Background()

	ns, podName, poolName := "ns-syncer-conflict", "worker-unit-conflict", "pool-conflict"

	api := newFakeControl()
	s, pods, poolIndexer := setupReconcileTest(t, api, workerPool(ns, poolName, "gvisor", map[string]string{"foo": "bar"}))
	key := seedPod(t, pods, workerPod(ns, podName, poolName, testPodUID, "10.0.0.1"))

	mustReconcile(t, ctx, s, key)
	if got, want := api.get(testPodUID).GetIps(), []string{"10.0.0.1"}; !slices.Equal(got, want) {
		t.Fatalf("worker ips = %q, want %q", got, want)
	}

	// Change a mutable worker field on the pool, so the next reconcile has an
	// update to make.
	if err := poolIndexer.Update(workerPool(ns, poolName, "gvisor", map[string]string{"foo": "baz"})); err != nil {
		t.Fatalf("updating pool: %v", err)
	}

	// Land a concurrent version bump the moment the syncer calls UpdateWorker.
	// The injected change is a drain: it is mutable, and unlike the labels the
	// syncer's update path does not write it, so it survives the retry only if
	// the retry re-read.
	conflicted := false
	api.setUpdateHookOnce(func(name string) {
		conflicted = true
		if _, err := api.DrainWorker(ctx, &ateapipb.DrainWorkerRequest{Worker: &ateapipb.ObjectRef{Name: name}}); err != nil {
			t.Errorf("injecting conflict: drain worker: %v", err)
		}
	})

	// The first attempt loses the precondition and comes back as an error, which
	// processNextWorkItem requeues with backoff.
	if err := s.reconcile(ctx, key); status.Code(err) != codes.Aborted {
		t.Fatalf("reconcile error = %v, want ABORTED from the lost version precondition", err)
	}
	// The retry is only being tested if the first attempt actually lost the
	// precondition.
	if !conflicted {
		t.Fatal("the syncer never called UpdateWorker, so no conflict was injected")
	}

	// The retry re-reads, so both changes end up on the record.
	mustReconcile(t, ctx, s, key)
	got := api.get(testPodUID)
	if got.GetLabels()["foo"] != "baz" {
		t.Errorf("worker labels = %v, want foo=baz", got.GetLabels())
	}
	if got.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_DRAINING {
		t.Errorf("worker state = %v, want the concurrently injected DRAINING to survive the retry", got.GetStatus().GetState())
	}
}

// Editing a pool's sandboxClass rolls its pods rather than reclassifying them,
// so a pod that outlives the edit keeps the class it was built with. Writing the
// pool's new value back would be rejected as an immutable-field change, and
// would misreport this pod's shape to the scheduler if it were not.
func TestSyncer_SandboxClassDriftLeavesWorkerAlone(t *testing.T) {
	ctx := context.Background()

	ns, podName, poolName := "ns-syncer-class", "worker-unit-class", "pool-class"

	api := newFakeControl()
	s, pods, poolIndexer := setupReconcileTest(t, api, workerPool(ns, poolName, "gvisor", nil))
	key := seedPod(t, pods, workerPod(ns, podName, poolName, testPodUID, "10.0.0.1"))

	mustReconcile(t, ctx, s, key)
	registered := api.get(testPodUID)

	if err := poolIndexer.Update(workerPool(ns, poolName, "microvm", nil)); err != nil {
		t.Fatalf("updating pool: %v", err)
	}
	mustReconcile(t, ctx, s, key)

	got := api.get(testPodUID)
	if got.GetSandboxClass() != "gvisor" {
		t.Errorf("worker sandbox class = %q, want it left at gvisor", got.GetSandboxClass())
	}
	if got.GetMetadata().GetVersion() != registered.GetMetadata().GetVersion() {
		t.Errorf("worker version = %d, want %d: the drift must not provoke a write",
			got.GetMetadata().GetVersion(), registered.GetMetadata().GetVersion())
	}
}

// TestSyncer_MissingWorkerPool verifies that a pod whose WorkerPool is not in
// the lister gets no Worker, and that the Worker comes once the pool does.
func TestSyncer_MissingWorkerPool(t *testing.T) {
	ctx := context.Background()

	ns, podName, poolName := "ns-syncer-missingpool", "worker-orphan-1", "pool-orphan"
	pool := workerPool(ns, poolName, "gvisor", nil)

	api := newFakeControl()
	// The pod is there; its pool is not yet.
	s, pods, poolIndexer := setupReconcileTest(t, api)
	key := seedPod(t, pods, workerPod(ns, podName, poolName, testPodUID, "10.0.0.5"))

	mustReconcile(t, ctx, s, key)
	if got := api.names(); len(got) != 0 {
		t.Errorf("registry holds %v, want nothing written before the pool exists", got)
	}

	// The pool appears and enqueues its pods, which then register.
	if err := poolIndexer.Add(pool); err != nil {
		t.Fatalf("adding pool: %v", err)
	}
	s.enqueueWorkerPool(pool)
	if got := s.queue.Len(); got != 1 {
		t.Fatalf("queued workers = %d after the pool appeared, want 1", got)
	}
	mustReconcile(t, ctx, s, key)
	if got := api.get(testPodUID).GetSandboxClass(); got != "gvisor" {
		t.Errorf("worker sandbox class = %q, want gvisor", got)
	}
}

// owned sets owner as the controller of obj.
func owned(obj, owner metav1.Object, kind string) {
	obj.SetOwnerReferences([]metav1.OwnerReference{{
		Kind: kind, Name: owner.GetName(), UID: owner.GetUID(), Controller: ptr.To(true),
	}})
}

// poolChain returns the Deployment and ReplicaSet that own pod, linked as the
// WorkerPool controller links them, ending at a WorkerPool of the given name.
func poolChain(pod *corev1.Pod, poolName string) (*appsv1.Deployment, *appsv1.ReplicaSet) {
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: poolName, Namespace: pod.Namespace, UID: "dep-uid"}}
	owned(dep, &metav1.ObjectMeta{Name: poolName, UID: "pool-uid"}, "WorkerPool")
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: poolName + "-abc", Namespace: pod.Namespace, UID: "rs-uid"}}
	owned(rs, dep, "Deployment")
	owned(pod, rs, "ReplicaSet")
	return dep, rs
}

// TestSyncer_DeletedWorkerPool verifies what happens to a registered Worker
// whose pod is live but whose WorkerPool is gone. The Worker is never deleted
// outright, which would crash its Actors: a pod the garbage collector is still
// removing is left alone, and one that outlives its pool is deleted gracefully.
func TestSyncer_DeletedWorkerPool(t *testing.T) {
	ns, poolName, podName, ip := "ns-deleted-pool", "pool1", "worker-1", "10.0.0.5"

	for _, tc := range []struct {
		name string
		// shape edits the chain, which starts intact, before it is stored.
		shape func(pod *corev1.Pod, rs *appsv1.ReplicaSet, dep *appsv1.Deployment) (store []runtime.Object)
		// deleted is whether the pod is expected to be deleted.
		deleted bool
	}{
		{
			name: "background cascade",
			shape: func(_ *corev1.Pod, rs *appsv1.ReplicaSet, dep *appsv1.Deployment) []runtime.Object {
				return []runtime.Object{rs, dep}
			},
		},
		{
			name: "background cascade, deployment collected",
			shape: func(_ *corev1.Pod, rs *appsv1.ReplicaSet, _ *appsv1.Deployment) []runtime.Object {
				return []runtime.Object{rs}
			},
		},
		{
			name: "background cascade, replicaset collected",
			shape: func(*corev1.Pod, *appsv1.ReplicaSet, *appsv1.Deployment) []runtime.Object {
				return nil
			},
		},
		{
			name: "replicaset replaced under its name",
			shape: func(_ *corev1.Pod, rs *appsv1.ReplicaSet, dep *appsv1.Deployment) []runtime.Object {
				rs.UID = "new-uid"
				return []runtime.Object{rs, dep}
			},
		},
		{
			name:    "orphaned deployment",
			deleted: true,
			shape: func(_ *corev1.Pod, rs *appsv1.ReplicaSet, dep *appsv1.Deployment) []runtime.Object {
				dep.OwnerReferences = nil
				return []runtime.Object{rs, dep}
			},
		},
		{
			name:    "orphaned replicaset",
			deleted: true,
			shape: func(_ *corev1.Pod, rs *appsv1.ReplicaSet, dep *appsv1.Deployment) []runtime.Object {
				rs.OwnerReferences = nil
				return []runtime.Object{rs, dep}
			},
		},
		{
			name:    "orphaned pod",
			deleted: true,
			shape: func(pod *corev1.Pod, rs *appsv1.ReplicaSet, dep *appsv1.Deployment) []runtime.Object {
				pod.OwnerReferences = nil
				return []runtime.Object{rs, dep}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			api := newFakeControl()
			api.put(registeredWorker(ns, poolName, podName, testPodUID, ip))
			s, pods, _ := setupReconcileTest(t, api)

			pod := workerPod(ns, podName, poolName, testPodUID, ip)
			dep, rs := poolChain(pod, poolName)
			objs := tc.shape(pod, rs, dep)
			//nolint:staticcheck // NewSimpleClientset is the fake the syncer's clients take.
			fakeK8s := fake.NewSimpleClientset(append(objs, pod.DeepCopy())...)
			s.pods, s.apps = fakeK8s.CoreV1(), fakeK8s.AppsV1()
			mustReconcile(t, ctx, s, seedPod(t, pods, pod))

			got := api.get(testPodUID)
			if got == nil {
				t.Fatal("worker deleted outright, want it left to the pod's own deletion")
			}
			_, err := fakeK8s.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
			if gone := apierrors.IsNotFound(err); gone != tc.deleted {
				t.Errorf("pod deleted = %v, want %v", gone, tc.deleted)
			}
			wantState := ateapipb.WorkerState_WORKER_STATE_ACTIVE
			if tc.deleted {
				wantState = ateapipb.WorkerState_WORKER_STATE_DRAINING
			}
			if state := got.GetStatus().GetState(); state != wantState {
				t.Errorf("worker state = %v, want %v", state, wantState)
			}
		})
	}
}

// TestSyncer_DeletedWorkerPool_Errors verifies that a failed read of the pod's
// owners requeues the key and deletes nothing, rather than guessing the pod was
// orphaned.
func TestSyncer_DeletedWorkerPool_Errors(t *testing.T) {
	ctx := context.Background()
	ns, poolName, podName, ip := "ns-deleted-pool-err", "pool1", "worker-1", "10.0.0.5"

	api := newFakeControl()
	api.put(registeredWorker(ns, poolName, podName, testPodUID, ip))
	s, pods, _ := setupReconcileTest(t, api)

	pod := workerPod(ns, podName, poolName, testPodUID, ip)
	poolChain(pod, poolName)
	//nolint:staticcheck // NewSimpleClientset is the fake the syncer's clients take.
	fakeK8s := fake.NewSimpleClientset(pod.DeepCopy())
	fakeK8s.PrependReactor("get", "replicasets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("apiserver unavailable")
	})
	s.pods, s.apps = fakeK8s.CoreV1(), fakeK8s.AppsV1()

	if err := s.reconcile(ctx, seedPod(t, pods, pod)); err == nil {
		t.Fatal("reconcile succeeded despite failing to read the pod's owners, want an error so the key requeues")
	}
	if _, err := fakeK8s.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{}); err != nil {
		t.Errorf("get pod: %v, want it left alone", err)
	}
	if got := api.get(testPodUID).GetStatus().GetState(); got != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
		t.Errorf("worker state = %v, want ACTIVE", got)
	}
}

// TestSyncer_DeletedWorkerPool_ReplacementPod verifies that the pod a surviving
// ReplicaSet starts in place of an orphaned one is left alone: it has no Worker,
// so deleting it would only make the ReplicaSet start another.
func TestSyncer_DeletedWorkerPool_ReplacementPod(t *testing.T) {
	ctx := context.Background()
	ns, poolName, podName := "ns-deleted-pool-repl", "pool1", "worker-2"

	api := newFakeControl()
	s, pods, _ := setupReconcileTest(t, api)

	pod := workerPod(ns, podName, poolName, otherPodUID, "10.0.0.6")
	//nolint:staticcheck // NewSimpleClientset is the fake the syncer's clients take.
	fakeK8s := fake.NewSimpleClientset(pod.DeepCopy())
	s.pods, s.apps = fakeK8s.CoreV1(), fakeK8s.AppsV1()
	mustReconcile(t, ctx, s, seedPod(t, pods, pod))

	if _, err := fakeK8s.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{}); err != nil {
		t.Errorf("get pod: %v, want it left alone", err)
	}
	if got := api.names(); len(got) != 0 {
		t.Errorf("registry holds %v, want nothing written", got)
	}
}

// TestSyncer_OrphanedPodsOfDeletedPool_ViaInformer verifies that deleting a
// WorkerPool without deleting its pods, as kubectl delete --cascade=orphan
// does, shuts their Workers down through the pods' own deletion without waiting
// for a pod event.
func TestSyncer_OrphanedPodsOfDeletedPool_ViaInformer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ns, podName, poolName := "ns-syncer-orphaned", "worker-orphaned-1", "pool1"

	api := newFakeControl()
	fakeK8s, poolClient := setupSyncerTest(t, ctx, api, workerPool(ns, poolName, "gvisor", nil))

	pod := workerPod(ns, podName, poolName, testPodUID, "10.0.0.7")
	if _, err := fakeK8s.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	waitForWorker(t, ctx, api, testPodUID, func(w *ateapipb.Worker) bool {
		return w.GetStatus().GetState() == ateapipb.WorkerState_WORKER_STATE_ACTIVE
	})

	if err := poolClient.ApiV1alpha1().WorkerPools(ns).Delete(ctx, poolName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete pool: %v", err)
	}
	waitForWorker(t, ctx, api, testPodUID, func(w *ateapipb.Worker) bool { return w == nil })

	if _, err := fakeK8s.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("get pod = %v, want NotFound", err)
	}
}

// TestSyncer_StartWaitsForWorkerPoolCache verifies that no pod is reconciled
// before the WorkerPool cache has synced: against an empty cache every pool
// reads as missing, which would shut down every live Worker at startup.
func TestSyncer_StartWaitsForWorkerPoolCache(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ns, podName, poolName := "ns-syncer-poolsync", "worker-poolsync-1", "pool1"
	poolLabels := map[string]string{"foo": "bar"}

	api := newFakeControl()
	api.put(registeredWorker(ns, poolName, podName, testPodUID, "10.0.0.8"))

	//nolint:staticcheck // NewSimpleClientset is what the informer machinery takes.
	fakeK8s := fake.NewSimpleClientset(workerPod(ns, podName, poolName, testPodUID, "10.0.0.8"))
	workerFactory, workerInformer := WorkerPodInformer(fakeK8s)
	_, poolFactory, workerPoolInformer := newWorkerPoolInformer(workerPool(ns, poolName, "", poolLabels))

	NewWorkerPoolSyncer(api, fakeK8s.CoreV1(), fakeK8s.AppsV1(), workerInformer, workerPoolInformer).Start(ctx)
	workerFactory.Start(ctx.Done())
	workerFactory.WaitForCacheSync(ctx.Done())

	// The pod cache is warm and the pool cache is not. There is no state to wait
	// for, so this gives the syncer a window to get it wrong in.
	err := wait.PollUntilContextTimeout(ctx, 20*time.Millisecond, 300*time.Millisecond, true, func(context.Context) (bool, error) {
		if api.get(testPodUID).GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
			return false, errors.New("worker shut down before the WorkerPool cache synced")
		}
		return false, nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("while the WorkerPool cache is cold: %v", err)
	}

	// Once the pool cache syncs, the pod is reconciled against the pool, which
	// copies the pool's labels onto the surviving Worker.
	poolFactory.Start(ctx.Done())
	got := waitForWorker(t, ctx, api, testPodUID, func(w *ateapipb.Worker) bool {
		return w != nil && maps.Equal(w.GetLabels(), poolLabels)
	})
	if got == nil {
		t.Fatal("worker gone after the WorkerPool cache synced")
	}
}

// TestEnqueueWorkerPool verifies that a WorkerPool change requeues every pod
// in that pool, so its Worker record receives updated labels promptly.
func TestEnqueueWorkerPool(t *testing.T) {
	api := newFakeControl()
	s, pods, _ := setupReconcileTest(t, api)
	pool := workerPool("ns-pool-update", "pool-a", "gvisor", nil)

	first := seedPod(t, pods, workerPod(pool.Namespace, "worker-1", pool.Name, testPodUID, "10.0.0.1"))
	second := seedPod(t, pods, workerPod(pool.Namespace, "worker-2", pool.Name, otherPodUID, "10.0.0.2"))
	seedPod(t, pods, workerPod(pool.Namespace, "other-pool-worker", "pool-b", "33333333-3333-3333-3333-333333333333", "10.0.0.3"))

	s.enqueueWorkerPool(pool)
	if got := s.queue.Len(); got != 2 {
		t.Fatalf("queued workers = %d, want 2", got)
	}

	got := map[workerKey]bool{}
	for range 2 {
		key, quit := s.queue.Get()
		if quit {
			t.Fatal("queue shut down while reading enqueued workers")
		}
		got[key] = true
		s.queue.Done(key)
	}
	if !got[first] || !got[second] {
		t.Errorf("queued workers = %v, want %v and %v", got, first, second)
	}
}

// TestEnqueueWorkerPool_DeletedFinalStateUnknown verifies that a pool delete the
// informer missed, delivered as a tombstone, still requeues the pool's pods.
func TestEnqueueWorkerPool_DeletedFinalStateUnknown(t *testing.T) {
	api := newFakeControl()
	s, pods, _ := setupReconcileTest(t, api)
	pool := workerPool("ns-pool-tombstone", "pool-a", "gvisor", nil)
	seedPod(t, pods, workerPod(pool.Namespace, "worker-1", pool.Name, testPodUID, "10.0.0.1"))

	s.enqueueWorkerPool(cache.DeletedFinalStateUnknown{Key: pool.Namespace + "/" + pool.Name, Obj: pool})
	if got := s.queue.Len(); got != 1 {
		t.Errorf("queued workers = %d, want 1", got)
	}

	s.enqueueWorkerPool(cache.DeletedFinalStateUnknown{Key: "ns/other", Obj: &corev1.Pod{}})
	if got := s.queue.Len(); got != 1 {
		t.Errorf("queued workers = %d after a tombstone of the wrong type, want it unchanged at 1", got)
	}
}

// TestSyncer_SoftDelete_ViaInformer walks the whole transition through the
// informer rather than seeding an already-registered worker: a pod is
// registered ACTIVE, then enters graceful termination and flips to DRAINING
// with its record intact.
func TestSyncer_SoftDelete_ViaInformer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ns, podName, poolName := "ns-syncer-softdelete", "worker-soft-1", "pool1"

	api := newFakeControl()
	fakeK8s, _ := setupSyncerTest(t, ctx, api, workerPool(ns, poolName, "gvisor", nil))

	pod := workerPod(ns, podName, poolName, testPodUID, "10.0.0.6")
	if _, err := fakeK8s.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	waitForWorker(t, ctx, api, testPodUID, func(w *ateapipb.Worker) bool {
		return w.GetStatus().GetState() == ateapipb.WorkerState_WORKER_STATE_ACTIVE
	})

	// The fake clientset stores updates verbatim, so setting DeletionTimestamp
	// simulates a pod in graceful termination that is still in the cache.
	deleting := pod.DeepCopy()
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	if _, err := fakeK8s.CoreV1().Pods(ns).Update(ctx, deleting, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update pod: %v", err)
	}

	waitForWorker(t, ctx, api, testPodUID, func(w *ateapipb.Worker) bool {
		return w.GetStatus().GetState() == ateapipb.WorkerState_WORKER_STATE_DRAINING
	})
}

// TestSyncer_TerminalPod_DeletesPod verifies that a worker pod in a terminal
// phase is deleted, preconditioned on its UID, and its worker marked DRAINING
// until the Pod Deleted event removes the record.
func TestSyncer_TerminalPod_DeletesPod(t *testing.T) {
	ns, poolName, podName, ip := "ns-terminal", "pool1", "worker-terminal", "10.0.0.20"

	for _, phase := range []corev1.PodPhase{corev1.PodFailed, corev1.PodSucceeded} {
		t.Run(string(phase), func(t *testing.T) {
			ctx := context.Background()
			api := newFakeControl()
			api.put(registeredWorker(ns, poolName, podName, testPodUID, ip))
			s, pods, _ := setupReconcileTest(t, api, workerPool(ns, poolName, "gvisor", nil))

			pod := workerPod(ns, podName, poolName, testPodUID, ip)
			pod.Status.Phase = phase
			//nolint:staticcheck // NewSimpleClientset is the fake the syncer's pod client takes.
			fakeK8s := fake.NewSimpleClientset(pod.DeepCopy())
			s.pods = fakeK8s.CoreV1()
			mustReconcile(t, ctx, s, seedPod(t, pods, pod))

			if _, err := fakeK8s.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Errorf("get pod after reconcile = %v, want NotFound", err)
			}
			var deletes []k8stesting.DeleteAction
			for _, a := range fakeK8s.Actions() {
				if d, ok := a.(k8stesting.DeleteAction); ok {
					deletes = append(deletes, d)
				}
			}
			if len(deletes) != 1 {
				t.Fatalf("pod deletes = %d, want 1", len(deletes))
			}
			if got := deletes[0].GetDeleteOptions().Preconditions; got == nil || got.UID == nil || *got.UID != testPodUID {
				t.Errorf("delete preconditions = %+v, want UID %s", got, testPodUID)
			}
			if got := api.get(testPodUID).GetStatus().GetState(); got != ateapipb.WorkerState_WORKER_STATE_DRAINING {
				t.Errorf("worker state = %v, want DRAINING", got)
			}
		})
	}
}

// TestSyncer_TerminalPod_AlreadyGoneOrReplaced verifies that a terminal pod
// whose delete finds it gone (NotFound) or replaced under its name (the UID
// precondition fails with Conflict) reconciles cleanly rather than requeueing.
func TestSyncer_TerminalPod_AlreadyGoneOrReplaced(t *testing.T) {
	ns, poolName, podName := "ns-terminal-gone", "pool1", "worker-terminal-gone"

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"not found", apierrors.NewNotFound(corev1.Resource("pods"), podName)},
		{"conflict", apierrors.NewConflict(corev1.Resource("pods"), podName, errors.New("precondition failed: UID"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s, pods, _ := setupReconcileTest(t, newFakeControl())

			//nolint:staticcheck // NewSimpleClientset is the fake the syncer's pod client takes.
			fakeK8s := fake.NewSimpleClientset()
			fakeK8s.PrependReactor("delete", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, tc.err
			})
			s.pods = fakeK8s.CoreV1()

			pod := workerPod(ns, podName, poolName, testPodUID, "")
			pod.Status.Phase = corev1.PodFailed
			mustReconcile(t, ctx, s, seedPod(t, pods, pod))
		})
	}
}

// TestSyncer_TerminalPod_ViaInformer walks a registered worker through its pod
// failing: the syncer deletes the pod, and the resulting Pod Deleted event
// removes the worker record, which is what releases its Actors.
func TestSyncer_TerminalPod_ViaInformer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ns, podName, poolName := "ns-syncer-terminal", "worker-terminal-1", "pool1"

	api := newFakeControl()
	fakeK8s, _ := setupSyncerTest(t, ctx, api, workerPool(ns, poolName, "gvisor", nil))

	pod := workerPod(ns, podName, poolName, testPodUID, "10.0.0.21")
	if _, err := fakeK8s.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	waitForWorker(t, ctx, api, testPodUID, func(w *ateapipb.Worker) bool {
		return w.GetStatus().GetState() == ateapipb.WorkerState_WORKER_STATE_ACTIVE
	})

	failed := pod.DeepCopy()
	failed.Status.Phase = corev1.PodFailed
	failed.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
	if _, err := fakeK8s.CoreV1().Pods(ns).UpdateStatus(ctx, failed, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update pod status: %v", err)
	}

	waitForWorker(t, ctx, api, testPodUID, func(w *ateapipb.Worker) bool { return w == nil })
	if _, err := fakeK8s.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("get pod = %v, want NotFound", err)
	}
}

// TestSyncer_PodRecreatedWithNewUID verifies that when a pod is deleted and
// recreated under the same name, the registry converges to the new pod's UID
// and IP. A coalesced Delete+Create surfaces as a single update event, which
// enqueues both incarnations' keys, so both are reconciled here.
func TestSyncer_PodRecreatedWithNewUID(t *testing.T) {
	ctx := context.Background()

	ns, podName, poolName := "ns-syncer-recreate", "worker-recreate-1", "pool1"

	api := newFakeControl()
	s, pods, _ := setupReconcileTest(t, api, workerPool(ns, poolName, "gvisor", nil))
	oldKey := seedPod(t, pods, workerPod(ns, podName, poolName, testPodUID, "10.0.0.7"))

	mustReconcile(t, ctx, s, oldKey)
	if api.get(testPodUID) == nil {
		t.Fatal("the original pod's worker was not registered")
	}

	// Replace the pod with a same-named one under a new UID.
	if err := pods.Update(workerPod(ns, podName, poolName, otherPodUID, "10.0.0.8")); err != nil {
		t.Fatalf("replacing pod: %v", err)
	}
	newKey := workerKey{namespace: ns, name: podName, uid: otherPodUID}

	// The old key now names a dead incarnation: the pod under that name has a
	// different UID, so its record is dropped rather than adopted.
	mustReconcile(t, ctx, s, oldKey)
	mustReconcile(t, ctx, s, newKey)

	got := api.get(otherPodUID)
	if got == nil {
		t.Fatal("the recreated pod's worker was not registered")
	}
	if want := []string{"10.0.0.8"}; !slices.Equal(got.GetIps(), want) {
		t.Errorf("worker ips = %q, want %q", got.GetIps(), want)
	}
	if got.GetWorkerPodUid() != otherPodUID {
		t.Errorf("worker pod uid = %q, want %q", got.GetWorkerPodUid(), otherPodUID)
	}
	// The old incarnation's record must not linger.
	if names := api.names(); len(names) != 1 {
		t.Errorf("registry holds %v, want only the recreated pod's worker", names)
	}
}

// TestSyncer_DeleteNeverEligiblePod verifies that deleting a pod that never got
// an IP (and so was never registered) is a no-op rather than an error, which is
// what keeps it from error-looping.
func TestSyncer_DeleteNeverEligiblePod(t *testing.T) {
	ctx := context.Background()

	ns, podName, poolName := "ns-syncer-neverip", "worker-noip-1", "pool1"

	api := newFakeControl()
	s, pods, _ := setupReconcileTest(t, api, workerPool(ns, poolName, "gvisor", nil))
	pod := workerPod(ns, podName, poolName, testPodUID, "")
	key := seedPod(t, pods, pod)

	mustReconcile(t, ctx, s, key)
	if err := pods.Delete(pod); err != nil {
		t.Fatalf("deleting pod: %v", err)
	}

	// The pod never had an IP so nothing was registered, and the delete path is
	// an idempotent no-op: no error, so nothing requeues.
	mustReconcile(t, ctx, s, key)
	if got := api.names(); len(got) != 0 {
		t.Errorf("registry holds %v, want it empty", got)
	}
}

// TestSyncer_PodWithIPButNotReadyIsNotRegistered pins the readiness half of
// the eligibility gate: an IP alone no longer registers a worker, because the
// sandbox having an address says nothing about ateom serving yet (#1106).
// Both not-Ready shapes are covered — condition absent (kubelet hasn't probed
// yet) and condition explicitly False (probe failing).
func TestSyncer_PodWithIPButNotReadyIsNotRegistered(t *testing.T) {
	ctx := context.Background()
	ns, poolName := "ns-syncer-notready", "pool1"

	for _, tc := range []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{name: "no Ready condition", mutate: func(p *corev1.Pod) {
			p.Status.Conditions = nil
		}},
		{name: "Ready=False", mutate: func(p *corev1.Pod) {
			p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := newFakeControl()
			s, pods, _ := setupReconcileTest(t, api, workerPool(ns, poolName, "gvisor", nil))
			pod := workerPod(ns, "worker-notready-1", poolName, testPodUID, "10.0.0.9")
			tc.mutate(pod)
			key := seedPod(t, pods, pod)

			mustReconcile(t, ctx, s, key)
			if got := api.names(); len(got) != 0 {
				t.Errorf("registry holds %v for a not-Ready pod, want it empty", got)
			}
		})
	}
}

// TestSyncer_ReadinessFlapDoesNotDeregister pins that eligibility only gates
// registration: once a worker exists, a readiness blip must not remove it —
// a bound actor keeps running through a failed probe, and deregistering would
// strand it.
func TestSyncer_ReadinessFlapDoesNotDeregister(t *testing.T) {
	ctx := context.Background()
	ns, podName, poolName := "ns-syncer-flap", "worker-flap-1", "pool1"

	api := newFakeControl()
	s, pods, _ := setupReconcileTest(t, api, workerPool(ns, poolName, "gvisor", nil))
	pod := workerPod(ns, podName, poolName, testPodUID, "10.0.0.9")
	key := seedPod(t, pods, pod)

	mustReconcile(t, ctx, s, key)
	if got := api.get(testPodUID); got == nil {
		t.Fatalf("worker not registered for a Ready pod; registry holds %v", api.names())
	}

	notReady := pod.DeepCopy()
	notReady.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
	if err := pods.Update(notReady); err != nil {
		t.Fatalf("updating pod: %v", err)
	}

	mustReconcile(t, ctx, s, key)
	got := api.get(testPodUID)
	if got == nil {
		t.Fatalf("worker deregistered on a readiness flap; registry holds %v", api.names())
	}
	if got.GetStatus().GetState() == ateapipb.WorkerState_WORKER_STATE_DRAINING {
		t.Errorf("worker marked DRAINING on a readiness flap, want it left ACTIVE")
	}
}

// The syncer builds its requests from pod and pool state, so a request the API
// rejects as invalid would be resent verbatim on every retry. INVALID_ARGUMENT
// is therefore dropped, while every other code — including the UNAVAILABLE a
// transport failure surfaces as — requeues.
func TestSyncer_ReconcileErrorsAreClassified(t *testing.T) {
	ctx := context.Background()
	ns, podName, poolName := "ns-syncer-classify", "worker-classify-1", "pool1"

	tests := []struct {
		name         string
		code         codes.Code
		wantRequeues int
	}{
		{name: "invalid argument is dropped", code: codes.InvalidArgument, wantRequeues: 0},
		{name: "unavailable requeues", code: codes.Unavailable, wantRequeues: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			api := newFakeControl()
			api.setCreateHook(func(*ateapipb.Worker) error {
				return status.Error(tc.code, "rejected")
			})
			s, pods, _ := setupReconcileTest(t, api, workerPool(ns, poolName, "gvisor", nil))
			key := seedPod(t, pods, workerPod(ns, podName, poolName, testPodUID, "10.0.0.9"))

			s.queue.Add(key)
			if !s.processNextWorkItem(ctx) {
				t.Fatal("processNextWorkItem reported the queue was shut down")
			}
			if got := s.queue.NumRequeues(key); got != tc.wantRequeues {
				t.Errorf("queue requeues = %d, want %d", got, tc.wantRequeues)
			}
			if got := api.names(); len(got) != 0 {
				t.Errorf("registry holds %v, want nothing written for a rejected create", got)
			}
		})
	}
}
