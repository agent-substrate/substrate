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
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	atefake "github.com/agent-substrate/substrate/pkg/client/clientset/versioned/fake"
	"github.com/agent-substrate/substrate/pkg/client/informers/externalversions"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/fake"
)

// testPodUID is the UID shared by the worker pods created in these tests and
// the actor/worker rows bound to them.
const testPodUID = "08675309-4a65-6e6e-7973-6e756d626572"

// setupSyncerTest sets up a real store with fake Redis and a fake K8s client with informer.
func setupSyncerTest(t *testing.T, ctx context.Context, initPools ...*atev1alpha1.WorkerPool) (store.Interface, *fake.Clientset, *atefake.Clientset, func()) {
	t.Helper()

	persistence, cleanup := storetest.SetupTestStore(t)

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

	syncer := NewWorkerPoolSyncer(persistence, fakeK8s, workerInformer, workerPoolLister)
	syncer.Start(ctx)

	workerFactory.Start(ctx.Done())
	ateInformerFactory.Start(ctx.Done())

	workerFactory.WaitForCacheSync(ctx.Done())
	ateInformerFactory.WaitForCacheSync(ctx.Done())

	return persistence, fakeK8s, fakeAte, cleanup
}

func TestSyncer_Lifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	defer cleanup()

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
			UID:       testPodUID,
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
	defer cancel()

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
	defer cleanup()
	if _, err := fakeK8s.CoreV1().Pods(ns).Create(ctx,
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pod,
				Namespace: ns,
				UID:       testPodUID,
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
		Status:            ateapipb.Actor_STATUS_RUNNING,
		AteomPodNamespace: ns, AteomPodName: pod, AteomPodIp: ip, AteomPodUid: testPodUID,
		InProgressSnapshot: "gs://snapshots/partial",
		LatestSnapshotInfo: &ateapipb.SnapshotInfo{
			Data: &ateapipb.SnapshotInfo_External{
				External: &ateapipb.ExternalSnapshotInfo{
					SnapshotUriPrefix: "gs://snapshots/last",
				},
			},
		},
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
	// The actor was RUNNING when its worker vanished, so state since the
	// last snapshot is lost: the release must surface that as CRASHED.
	var got *ateapipb.Actor
	if err := wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 2*time.Second, true, func(c context.Context) (bool, error) {
		a, gerr := persistence.GetActor(c, "team-orphan", actorName)
		if gerr != nil {
			return false, gerr
		}
		got = a
		return a.GetStatus() == ateapipb.Actor_STATUS_CRASHED, nil
	}); err != nil {
		t.Fatalf("actor not marked CRASHED: %v", err)
	}
	if got.AteomPodName != "" || got.AteomPodNamespace != "" || got.AteomPodIp != "" {
		t.Errorf("bind fields not cleared: %+v", got)
	}
	if got.GetLatestSnapshotInfo().GetExternal().SnapshotUriPrefix == "" {
		t.Errorf("External SnapshotUriPrefix must be preserved")
	}
}

func TestAteomTerminated(t *testing.T) {
	terminated := func(exitCode int32) corev1.ContainerState {
		return corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{ExitCode: exitCode},
		}
	}
	tests := []struct {
		name     string
		statuses []corev1.ContainerStatus
		want     bool
	}{
		{name: "no statuses", statuses: nil, want: false},
		{name: "running", statuses: []corev1.ContainerStatus{
			{Name: ateomContainerName, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
		}, want: false},
		{name: "wrong container", statuses: []corev1.ContainerStatus{
			{Name: "other", LastTerminationState: terminated(1)},
		}, want: false},
		{name: "exit 0 in last state", statuses: []corev1.ContainerStatus{
			{Name: ateomContainerName, LastTerminationState: terminated(0)},
		}, want: true},
		{name: "exit 1 in last state", statuses: []corev1.ContainerStatus{
			{Name: ateomContainerName, LastTerminationState: terminated(1)},
		}, want: true},
		{name: "exit 1 in current state", statuses: []corev1.ContainerStatus{
			{Name: ateomContainerName, State: terminated(1)},
		}, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: tc.statuses}}
			if got := ateomTerminated(pod); got != tc.want {
				t.Errorf("ateomTerminated() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSyncer_AteomTerminated covers the crash reaction end to end through
// the informer: any termination of the ateom container — whatever the exit
// code — marks the assigned actor CRASHED and recycles the doomed pod.
func TestSyncer_AteomTerminated(t *testing.T) {
	tests := []struct {
		name       string
		exitCode   int32
		wantStatus ateapipb.Actor_Status
	}{
		{
			name:       "nonzero exit crashes actor",
			exitCode:   1,
			wantStatus: ateapipb.Actor_STATUS_CRASHED,
		},
		{
			name:       "zero exit crashes actor",
			exitCode:   0,
			wantStatus: ateapipb.Actor_STATUS_CRASHED,
		},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			ns, pool, pod, ip := fmt.Sprintf("ns-crash-%d", i), "pool1", "worker-crash", "10.0.0.2"
			workerPool := &atev1alpha1.WorkerPool{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pool,
					Namespace: ns,
				},
				Spec: atev1alpha1.WorkerPoolSpec{
					SandboxClass: "gvisor",
				},
			}

			persistence, fakeK8s, _, cleanup := setupSyncerTest(t, ctx, workerPool)
			defer cleanup()
			created, err := fakeK8s.CoreV1().Pods(ns).Create(ctx,
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      pod,
						Namespace: ns,
						UID:       testPodUID,
						Labels:    map[string]string{workerPodLabel: pool},
					},
					Spec: corev1.PodSpec{
						NodeName:   "node1",
						Containers: []corev1.Container{{Name: ateomContainerName, Image: "ateom"}},
					},
					Status: corev1.PodStatus{
						Phase: corev1.PodRunning, PodIP: ip,
						PodIPs: []corev1.PodIP{{IP: ip}},
					},
				}, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("create pod: %v", err)
			}
			if err := wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 2*time.Second, true, func(c context.Context) (bool, error) {
				_, gerr := persistence.GetWorker(c, ns, pool, pod)
				return gerr == nil, nil
			}); err != nil {
				t.Fatalf("worker row not materialised: %v", err)
			}
			actorName := "actor-crash"
			_, err = persistence.CreateActor(ctx, &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Name: actorName, Atespace: "team-crash"}, ActorTemplateNamespace: ns, ActorTemplateName: "tmpl",
				Status:            ateapipb.Actor_STATUS_RUNNING,
				AteomPodNamespace: ns, AteomPodName: pod, AteomPodIp: ip, AteomPodUid: string(created.UID),
				LatestSnapshotInfo: &ateapipb.SnapshotInfo{
					Data: &ateapipb.SnapshotInfo_External{
						External: &ateapipb.ExternalSnapshotInfo{
							SnapshotUriPrefix: "gs://snapshots/last",
						},
					},
				},
			})
			if err != nil {
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
					Atespace: "team-crash",
				},
			}
			if err := persistence.UpdateWorker(ctx, w, w.Version); err != nil {
				t.Fatalf("update worker: %v", err)
			}

			// ateom exits and kubelet restarts it in place: the pod stays
			// Running and the termination record lands in lastState.
			crashed := created.DeepCopy()
			crashed.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name:         ateomContainerName,
				RestartCount: 1,
				LastTerminationState: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						ExitCode: tc.exitCode,
					},
				},
			}}
			if _, err := fakeK8s.CoreV1().Pods(ns).Update(ctx, crashed, metav1.UpdateOptions{}); err != nil {
				t.Fatalf("update pod with crash status: %v", err)
			}

			var got *ateapipb.Actor
			if err := wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 2*time.Second, true, func(c context.Context) (bool, error) {
				a, gerr := persistence.GetActor(c, "team-crash", actorName)
				if gerr != nil {
					return false, gerr
				}
				got = a
				return a.GetStatus() == tc.wantStatus, nil
			}); err != nil {
				t.Fatalf("actor status = %v, want %v: %v", got.GetStatus(), tc.wantStatus, err)
			}
			if got.AteomPodName != "" || got.AteomPodNamespace != "" || got.AteomPodIp != "" || got.WorkerPoolName != "" {
				t.Errorf("bind fields not cleared: %+v", got)
			}
			if got.GetLatestSnapshotInfo().GetExternal().SnapshotUriPrefix == "" {
				t.Errorf("External SnapshotUriPrefix must be preserved")
			}
			if err := wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 2*time.Second, true, func(c context.Context) (bool, error) {
				_, gerr := persistence.GetWorker(c, ns, pool, pod)
				return errors.Is(gerr, store.ErrNotFound), nil
			}); err != nil {
				t.Fatalf("worker row not deleted from store: %v", err)
			}
			if err := wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 2*time.Second, true, func(c context.Context) (bool, error) {
				_, gerr := fakeK8s.CoreV1().Pods(ns).Get(c, pod, metav1.GetOptions{})
				return apierrors.IsNotFound(gerr), nil
			}); err != nil {
				t.Fatalf("crashed worker pod not deleted: %v", err)
			}
		})
	}
}

// TestCrashActorOnDeadWorker_Crashed covers the actor status write without
// going through informer events: any actor still bound to a worker that
// vanished lost its live state and must surface as CRASHED, whatever status
// it was in when the worker died.
func TestCrashActorOnDeadWorker_Crashed(t *testing.T) {
	tests := []struct {
		name       string
		status     ateapipb.Actor_Status
		wantStatus ateapipb.Actor_Status
	}{
		{
			name:       "running actor",
			status:     ateapipb.Actor_STATUS_RUNNING,
			wantStatus: ateapipb.Actor_STATUS_CRASHED,
		},
		{
			name:       "suspending actor",
			status:     ateapipb.Actor_STATUS_SUSPENDING,
			wantStatus: ateapipb.Actor_STATUS_CRASHED,
		},
		{
			name:       "resuming actor",
			status:     ateapipb.Actor_STATUS_RESUMING,
			wantStatus: ateapipb.Actor_STATUS_CRASHED,
		},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			persistence, cleanup := storetest.SetupTestStore(t)
			defer cleanup()

			ns, pool, pod := fmt.Sprintf("ns-release-crash-%d", i), "pool1", "worker-release-crash"
			actorName := "actor-release-crash"
			if err := persistence.CreateWorker(ctx, &ateapipb.Worker{
				WorkerNamespace: ns,
				WorkerPool:      pool,
				WorkerPod:       pod,
				WorkerPodUid:    testPodUID,
				Ip:              "10.0.0.3",
			}); err != nil {
				t.Fatalf("create worker: %v", err)
			}
			if _, err := persistence.CreateActor(ctx, &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Name: actorName, Atespace: "team-release"}, ActorTemplateNamespace: ns, ActorTemplateName: "tmpl",
				Status:            tc.status,
				AteomPodNamespace: ns, AteomPodName: pod, AteomPodIp: "10.0.0.3", AteomPodUid: testPodUID,
			}); err != nil {
				t.Fatalf("create actor: %v", err)
			}
			w, _ := persistence.GetWorker(ctx, ns, pool, pod)
			w.Assignment = &ateapipb.Assignment{
				Actor: &ateapipb.ObjectRef{Name: actorName, Atespace: "team-release"},
			}
			if err := persistence.UpdateWorker(ctx, w, w.Version); err != nil {
				t.Fatalf("update worker: %v", err)
			}

			syncer := &WorkerPoolSyncer{persistence: persistence}
			if err := syncer.crashActorOnDeadWorker(ctx, ns, pool, pod); err != nil {
				t.Fatalf("crashActorOnDeadWorker: %v", err)
			}

			got, err := persistence.GetActor(ctx, "team-release", actorName)
			if err != nil {
				t.Fatalf("get actor: %v", err)
			}
			if got.GetStatus() != tc.wantStatus {
				t.Errorf("actor status = %v, want %v", got.GetStatus(), tc.wantStatus)
			}
			if got.AteomPodName != "" || got.AteomPodNamespace != "" || got.AteomPodIp != "" {
				t.Errorf("bind fields not cleared: %+v", got)
			}
		})
	}
}

// failingActorStore wraps a store.Interface and fails GetActor, to exercise
// handleTerminatedAteom's release-failure path.
type failingActorStore struct {
	store.Interface
}

func (f *failingActorStore) GetActor(ctx context.Context, atespace, name string) (*ateapipb.Actor, error) {
	return nil, errors.New("injected store failure")
}

// TestHandleTerminatedAteom_ReleaseFailureKeepsPod verifies that when the
// actor release fails, handleTerminatedAteom returns early without deleting
// the worker row or the pod: the pod's termination record is the retry
// trigger, and the informer resync replays it.
func TestHandleTerminatedAteom_ReleaseFailureKeepsPod(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	defer cleanup()

	ns, pool, podName := "ns-release-fail", "pool1", "worker-release-fail"
	if err := persistence.CreateWorker(ctx, &ateapipb.Worker{
		WorkerNamespace: ns,
		WorkerPool:      pool,
		WorkerPod:       podName,
		Ip:              "10.0.0.4",
		Assignment: &ateapipb.Assignment{
			Actor: &ateapipb.ObjectRef{Name: "actor-release-fail", Atespace: "team-release-fail"},
		},
	}); err != nil {
		t.Fatalf("create worker: %v", err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: ns,
			UID:       testPodUID,
			Labels:    map[string]string{workerPodLabel: pool},
		},
	}
	fakeK8s := fake.NewSimpleClientset(pod)

	syncer := &WorkerPoolSyncer{persistence: &failingActorStore{persistence}, kubeClient: fakeK8s}
	syncer.handleTerminatedAteom(ctx, pod)

	if _, err := persistence.GetWorker(ctx, ns, pool, podName); err != nil {
		t.Errorf("worker row must survive a failed release: %v", err)
	}
	if _, err := fakeK8s.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{}); err != nil {
		t.Errorf("pod must survive a failed release: %v", err)
	}
}

func TestSyncer_OmittedFields(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	defer cleanup()

	// Create a pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: ns,
			UID:       testPodUID,
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
