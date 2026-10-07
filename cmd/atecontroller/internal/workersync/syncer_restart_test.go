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
	"slices"
	"testing"

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

func TestSyncer_UpdatesChangedIP(t *testing.T) {
	ctx := context.Background()
	ns, podName, poolName := "ns-syncer-ip", "worker-ip-1", "pool1"

	for _, tc := range []struct {
		name  string
		ready bool
	}{
		{name: "Ready", ready: true},
		// The new IP is written before the pod is Ready again.
		{name: "not Ready", ready: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := newFakeControl()
			s, pods, _ := setupReconcileTest(t, api, workerPool(ns, poolName, "gvisor", nil))
			pod := withAteomRestarts(workerPod(ns, podName, poolName, testPodUID, "10.0.0.1"), 0)
			key := seedPod(t, pods, pod)
			mustReconcile(t, ctx, s, key)

			changed := withAteomRestarts(pod.DeepCopy(), 1)
			changed.Status.PodIP = "10.0.0.9"
			changed.Status.PodIPs = []corev1.PodIP{{IP: "10.0.0.9"}}
			if !tc.ready {
				changed.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
			}
			if err := pods.Update(changed); err != nil {
				t.Fatalf("updating pod: %v", err)
			}
			mustReconcile(t, ctx, s, key)
			got := api.get(testPodUID)
			if !slices.Equal(got.GetIps(), []string{"10.0.0.9"}) {
				t.Errorf("ips after the pod's IP changed = %v, want [10.0.0.9]", got.GetIps())
			}
			if got.GetEpoch() != 2 {
				t.Errorf("epoch after the pod's IP changed = %d, want 2", got.GetEpoch())
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

func TestSyncer_NotReadyPodWithoutIPKeepsIP(t *testing.T) {
	ctx := context.Background()
	ns, podName, poolName := "ns-syncer-noip", "worker-noip-1", "pool1"

	api := newFakeControl()
	s, pods, _ := setupReconcileTest(t, api, workerPool(ns, poolName, "gvisor", nil))
	pod := withAteomRestarts(workerPod(ns, podName, poolName, testPodUID, "10.0.0.1"), 0)
	key := seedPod(t, pods, pod)
	mustReconcile(t, ctx, s, key)

	// Between sandboxes the pod reports no IP; the registered one is kept.
	recreating := withAteomRestarts(pod.DeepCopy(), 1)
	recreating.Status.PodIP = ""
	recreating.Status.PodIPs = nil
	recreating.Status.Conditions = nil
	if err := pods.Update(recreating); err != nil {
		t.Fatalf("updating pod: %v", err)
	}
	mustReconcile(t, ctx, s, key)
	got := api.get(testPodUID)
	if !slices.Equal(got.GetIps(), []string{"10.0.0.1"}) {
		t.Errorf("ips while the pod reports none = %v, want [10.0.0.1] kept", got.GetIps())
	}
	if got.GetEpoch() != 2 {
		t.Errorf("epoch = %d, want 2 raised regardless", got.GetEpoch())
	}
}

func TestSyncer_NotReadyUnregisteredPodIsNotRegistered(t *testing.T) {
	ctx := context.Background()
	ns, poolName := "ns-syncer-notready-restart", "pool1"

	api := newFakeControl()
	s, pods, _ := setupReconcileTest(t, api, workerPool(ns, poolName, "gvisor", nil))
	pod := withAteomRestarts(workerPod(ns, "worker-1", poolName, testPodUID, "10.0.0.1"), 3)
	pod.Status.Conditions = nil
	key := seedPod(t, pods, pod)

	mustReconcile(t, ctx, s, key)
	if got := api.names(); len(got) != 0 {
		t.Errorf("registry holds %v for a not-Ready pod, want it empty", got)
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
