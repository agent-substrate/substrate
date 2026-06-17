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
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/fake"
)

// setupSyncerTest sets up a real store with fake Redis and a fake K8s client with informer.
func setupSyncerTest(t *testing.T, ctx context.Context) (store.Interface, *fake.Clientset, func()) {
	t.Helper()

	persistence, cleanup := storetest.SetupTestStore(t)

	fakeK8s := fake.NewSimpleClientset()
	workerFactory, workerInformer := WorkerPodInformer(fakeK8s)

	syncer := NewWorkerPoolSyncer(persistence, workerInformer)
	syncer.Start(ctx)

	workerFactory.Start(ctx.Done())
	workerFactory.WaitForCacheSync(ctx.Done())

	return persistence, fakeK8s, cleanup
}

func TestSyncer_Lifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	persistence, fakeK8s, cleanup := setupSyncerTest(t, ctx)
	defer cleanup()

	ns := "ns-syncer-lifecycle"
	podName := "worker-unit-1"
	poolName := "pool1"

	// 1. Verify no workers in Redis initially
	workers, err := persistence.ListWorkers(context.Background())
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
			Labels: map[string]string{
				workerPodLabel: poolName,
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "node1",
			Containers: []corev1.Container{
				{Name: "main", Image: "nginx"},
			},
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
		return w.Ip == "127.0.0.1", nil
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

	persistence, fakeK8s, cleanup := setupSyncerTest(t, ctx)
	defer cleanup()

	ns, pool, pod, ip := "ns-orphan", "pool1", "worker-orphan", "10.0.0.1"
	if _, err := fakeK8s.CoreV1().Pods(ns).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: pod, Namespace: ns,
			Labels: map[string]string{workerPodLabel: pool}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: ip,
			PodIPs: []corev1.PodIP{{IP: ip}}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	if err := wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 2*time.Second, true, func(c context.Context) (bool, error) {
		_, gerr := persistence.GetWorker(c, ns, pool, pod)
		return gerr == nil, nil
	}); err != nil {
		t.Fatalf("worker row not materialised: %v", err)
	}
	actorID := "actor-orphan"
	if err := persistence.CreateActor(ctx, &ateapipb.Actor{
		ActorId: actorID, ActorTemplateNamespace: ns, ActorTemplateName: "tmpl",
		Status:            ateapipb.Actor_STATUS_RUNNING,
		AteomPodNamespace: ns, AteomPodName: pod, AteomPodIp: ip,
		InProgressSnapshot: "gs://snapshots/partial",
		LatestSnapshotInfo: &ateapipb.SnapshotInfo{
			Type: ateapipb.SnapshotType_SNAPSHOT_TYPE_EXTERNAL,
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
	w.ActorId, w.ActorNamespace, w.ActorTemplate = actorID, ns, "tmpl"
	if err := persistence.UpdateWorker(ctx, w, w.Version); err != nil {
		t.Fatalf("update worker: %v", err)
	}

	if err := fakeK8s.CoreV1().Pods(ns).Delete(ctx, pod, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete pod: %v", err)
	}
	var got *ateapipb.Actor
	if err := wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 2*time.Second, true, func(c context.Context) (bool, error) {
		a, gerr := persistence.GetActor(c, actorID)
		if gerr != nil {
			return false, gerr
		}
		got = a
		return a.GetStatus() == ateapipb.Actor_STATUS_SUSPENDED, nil
	}); err != nil {
		t.Fatalf("actor not reset to SUSPENDED: %v", err)
	}
	if got.AteomPodName != "" || got.AteomPodNamespace != "" || got.AteomPodIp != "" || got.InProgressSnapshot != "" {
		t.Errorf("bind fields not cleared: %+v", got)
	}
	if got.GetLatestSnapshotInfo().GetExternal().SnapshotUriPrefix == "" {
		t.Errorf("External SnapshotUriPrefix must be preserved")
	}
}
