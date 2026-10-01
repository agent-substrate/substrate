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
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	corev1 "k8s.io/api/core/v1"
)

// withAteomRestarts reports the pod's ateom container as having restarted n
// times.
func withAteomRestarts(pod *corev1.Pod, n int32) *corev1.Pod {
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{Name: "sidecar", RestartCount: 7},
		{Name: ateomContainer, RestartCount: n},
	}
	return pod
}

func TestPodEpoch(t *testing.T) {
	pod := workerPod("ns", "worker", "pool", testPodUID, "10.0.0.1")
	if got := podEpoch(pod); got != 0 {
		t.Errorf("podEpoch with no ateom status = %d, want 0", got)
	}
	if got := podEpoch(withAteomRestarts(pod, 0)); got != 1 {
		t.Errorf("podEpoch on the first run = %d, want 1", got)
	}
	if got := podEpoch(withAteomRestarts(pod, 4)); got != 5 {
		t.Errorf("podEpoch after 4 restarts = %d, want 5", got)
	}
}

func TestSyncer_RegistersEpoch(t *testing.T) {
	ctx := context.Background()
	ns, podName, poolName := "ns-syncer-epoch", "worker-epoch-1", "pool1"

	api := newFakeControl()
	s, pods, _ := setupReconcileTest(t, api, workerPool(ns, poolName, "gvisor", nil))
	key := seedPod(t, pods, withAteomRestarts(workerPod(ns, podName, poolName, testPodUID, "10.0.0.1"), 2))

	mustReconcile(t, ctx, s, key)
	if got := api.get(testPodUID).GetEpoch(); got != 3 {
		t.Errorf("registered epoch = %d, want 3", got)
	}
}

func TestSyncer_RaisesEpochOnAteomRestart(t *testing.T) {
	ctx := context.Background()
	ns, podName, poolName := "ns-syncer-restart", "worker-restart-1", "pool1"

	for _, tc := range []struct {
		name  string
		ready bool
	}{
		{name: "Ready", ready: true},
		// A restarting ateom is not Ready, but its Actors are already lost.
		{name: "not Ready", ready: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := newFakeControl()
			s, pods, _ := setupReconcileTest(t, api, workerPool(ns, poolName, "gvisor", nil))
			pod := withAteomRestarts(workerPod(ns, podName, poolName, testPodUID, "10.0.0.1"), 0)
			key := seedPod(t, pods, pod)
			mustReconcile(t, ctx, s, key)
			if got := api.get(testPodUID).GetEpoch(); got != 1 {
				t.Fatalf("registered epoch = %d, want 1", got)
			}

			restarted := withAteomRestarts(pod.DeepCopy(), 1)
			if !tc.ready {
				restarted.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
			}
			if err := pods.Update(restarted); err != nil {
				t.Fatalf("updating pod: %v", err)
			}
			mustReconcile(t, ctx, s, key)
			got := api.get(testPodUID)
			if got.GetEpoch() != 2 {
				t.Errorf("epoch after a restart = %d, want 2", got.GetEpoch())
			}

			// Unchanged on the next pass: no further write.
			version := got.GetMetadata().GetVersion()
			mustReconcile(t, ctx, s, key)
			if v := api.get(testPodUID).GetMetadata().GetVersion(); v != version {
				t.Errorf("worker version = %d after an unchanged pass, want %d", v, version)
			}
		})
	}
}

// TestSyncer_NoPlacementUnderAnEarlierEpoch walks an ateom restart through
// the pod states kubelet reports, and pins that no write leaves the Worker
// ACTIVE while its epoch trails the ateom that is running. An Actor placed
// then would be stamped with the earlier epoch and crashed with the Actors the
// restart lost.
func TestSyncer_NoPlacementUnderAnEarlierEpoch(t *testing.T) {
	ctx := context.Background()
	ns, podName, poolName := "ns-syncer-placement", "worker-placement-1", "pool1"
	notReady := func(pod *corev1.Pod) *corev1.Pod {
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
		return pod
	}

	for _, tc := range []struct {
		name  string
		steps []*corev1.Pod
		want  []ateapipb.WorkerState
	}{
		{
			name: "every transition observed",
			steps: []*corev1.Pod{
				// ateom exited; kubelet has not started it again.
				notReady(withAteomRestarts(workerPod(ns, podName, poolName, testPodUID, "10.0.0.1"), 0)),
				// ateom running again, readiness probe not yet passed.
				notReady(withAteomRestarts(workerPod(ns, podName, poolName, testPodUID, "10.0.0.1"), 1)),
				withAteomRestarts(workerPod(ns, podName, poolName, testPodUID, "10.0.0.1"), 1),
			},
			want: []ateapipb.WorkerState{
				ateapipb.WorkerState_WORKER_STATE_UNAVAILABLE,
				ateapipb.WorkerState_WORKER_STATE_UNAVAILABLE,
				ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			},
		},
		{
			// The informer coalesces updates, so the syncer may only see the
			// pod Ready again after the restart.
			name: "not Ready never observed",
			steps: []*corev1.Pod{
				withAteomRestarts(workerPod(ns, podName, poolName, testPodUID, "10.0.0.1"), 1),
			},
			want: []ateapipb.WorkerState{ateapipb.WorkerState_WORKER_STATE_ACTIVE},
		},
		{
			// The epoch has to be raised before the Worker is marked available.
			name: "Ready again under a new epoch",
			steps: []*corev1.Pod{
				notReady(withAteomRestarts(workerPod(ns, podName, poolName, testPodUID, "10.0.0.1"), 0)),
				withAteomRestarts(workerPod(ns, podName, poolName, testPodUID, "10.0.0.1"), 1),
			},
			want: []ateapipb.WorkerState{
				ateapipb.WorkerState_WORKER_STATE_UNAVAILABLE,
				ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			},
		},
		{
			// The restart is first seen with ateom running again but not Ready.
			name: "not Ready under a new epoch",
			steps: []*corev1.Pod{
				notReady(withAteomRestarts(workerPod(ns, podName, poolName, testPodUID, "10.0.0.1"), 1)),
				withAteomRestarts(workerPod(ns, podName, poolName, testPodUID, "10.0.0.1"), 1),
			},
			want: []ateapipb.WorkerState{
				ateapipb.WorkerState_WORKER_STATE_UNAVAILABLE,
				ateapipb.WorkerState_WORKER_STATE_ACTIVE,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := newFakeControl()
			s, pods, _ := setupReconcileTest(t, api, workerPool(ns, poolName, "gvisor", nil))
			key := seedPod(t, pods, withAteomRestarts(workerPod(ns, podName, poolName, testPodUID, "10.0.0.1"), 0))
			mustReconcile(t, ctx, s, key)
			if state := api.get(testPodUID).GetStatus().GetState(); state != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
				t.Fatalf("state after registering = %v, want %v", state, ateapipb.WorkerState_WORKER_STATE_ACTIVE)
			}
			api.writes()

			for i, pod := range tc.steps {
				if err := pods.Update(pod); err != nil {
					t.Fatalf("step %d: updating pod: %v", i, err)
				}
				mustReconcile(t, ctx, s, key)
				if state := api.get(testPodUID).GetStatus().GetState(); state != tc.want[i] {
					t.Errorf("step %d: state = %v, want %v", i, state, tc.want[i])
				}
				for j, w := range api.writes() {
					if w.GetStatus().GetState() == ateapipb.WorkerState_WORKER_STATE_ACTIVE && w.GetEpoch() < podEpoch(pod) {
						t.Errorf("step %d, write %d: worker ACTIVE at epoch %d while the pod reports epoch %d", i, j, w.GetEpoch(), podEpoch(pod))
					}
				}
			}
			if got := api.get(testPodUID).GetEpoch(); got != 2 {
				t.Errorf("epoch after the restart = %d, want 2", got)
			}
		})
	}
}

// A pod first seen not Ready after ateom restarts registers unavailable at
// the epoch of the ateom it is running.
func TestSyncer_NotReadyPodRegistersAtItsEpoch(t *testing.T) {
	ctx := context.Background()
	ns, poolName := "ns-syncer-notready-restart", "pool1"

	api := newFakeControl()
	s, pods, _ := setupReconcileTest(t, api, workerPool(ns, poolName, "gvisor", nil))
	pod := withAteomRestarts(workerPod(ns, "worker-1", poolName, testPodUID, "10.0.0.1"), 3)
	pod.Status.Conditions = nil
	key := seedPod(t, pods, pod)

	mustReconcile(t, ctx, s, key)
	got := api.get(testPodUID)
	if got.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_UNAVAILABLE {
		t.Errorf("state = %v, want %v", got.GetStatus().GetState(), ateapipb.WorkerState_WORKER_STATE_UNAVAILABLE)
	}
	if got.GetEpoch() != 4 {
		t.Errorf("epoch = %d, want 4", got.GetEpoch())
	}
}

func TestSyncer_EpochNeverLowered(t *testing.T) {
	ctx := context.Background()
	ns, podName, poolName := "ns-syncer-lower", "worker-lower-1", "pool1"

	api := newFakeControl()
	s, pods, _ := setupReconcileTest(t, api, workerPool(ns, poolName, "gvisor", nil))
	registered := registeredWorker(ns, poolName, podName, testPodUID, "10.0.0.1")
	registered.SandboxClass = "gvisor"
	registered.Epoch = 5
	api.put(registered)
	key := seedPod(t, pods, withAteomRestarts(workerPod(ns, podName, poolName, testPodUID, "10.0.0.1"), 0))

	mustReconcile(t, ctx, s, key)
	got := api.get(testPodUID)
	if got.GetEpoch() != 5 {
		t.Errorf("epoch = %d, want 5 left alone", got.GetEpoch())
	}
	if got.GetMetadata().GetVersion() != 1 {
		t.Errorf("worker version = %d, want 1: nothing to write", got.GetMetadata().GetVersion())
	}
}
