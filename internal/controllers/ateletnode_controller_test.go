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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func eventForObject(obj client.Object) event.TypedCreateEvent[client.Object] {
	return event.TypedCreateEvent[client.Object]{Object: obj}
}

func updateEventForObject(oldObj, newObj client.Object) event.TypedUpdateEvent[client.Object] {
	return event.TypedUpdateEvent[client.Object]{ObjectOld: oldObj, ObjectNew: newObj}
}

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

// bindPodToNode schedules a pod via the Binding subresource (envtest has no scheduler).
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

func TestTwoPoolsOnSameNode(t *testing.T) {
	const nodeName = "test-node-two-pools"
	const poolA = "uid-pool-a"
	const poolB = "uid-pool-b"

	node := makeNode(nodeName)
	if err := k8sClient.Create(testCtx, node); err != nil {
		t.Fatalf("create node: %v", err)
	}
	t.Cleanup(func() { k8sClient.Delete(testCtx, node) }) //nolint:errcheck

	podA := makeAteomPod("pod-two-pools-a", "default", "pool-a", poolA)
	podB := makeAteomPod("pod-two-pools-b", "default", "pool-b", poolB)
	for _, p := range []*corev1.Pod{podA, podB} {
		if err := k8sClient.Create(testCtx, p); err != nil {
			t.Fatalf("create pod %s: %v", p.Name, err)
		}
		t.Cleanup(func(pod *corev1.Pod) func() {
			return func() { k8sClient.Delete(testCtx, pod, client.GracePeriodSeconds(0)) } //nolint:errcheck
		}(p))
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
	// GracePeriodSeconds(0): envtest has no kubelet, so a normal delete would
	// leave the pod lingering with a DeletionTimestamp and keep the claim alive.
	t.Cleanup(func() { k8sClient.Delete(testCtx, podA, client.GracePeriodSeconds(0)) }) //nolint:errcheck
	t.Cleanup(func() { k8sClient.Delete(testCtx, podB, client.GracePeriodSeconds(0)) }) //nolint:errcheck

	eventually(t, func(ctx context.Context) (bool, error) {
		n := &corev1.Node{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, n); err != nil {
			return false, nil
		}
		_, okA := n.Annotations[AteletNodeClaimAnnoPrefix+poolA]
		_, okB := n.Annotations[AteletNodeClaimAnnoPrefix+poolB]
		return okA && okB, nil
	})

	if err := k8sClient.Delete(testCtx, podA, client.GracePeriodSeconds(0)); err != nil {
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
	// GracePeriodSeconds(0): envtest has no kubelet (see TestPodDeletionRemovesOneClaim).
	t.Cleanup(func() { k8sClient.Delete(testCtx, pod, client.GracePeriodSeconds(0)) }) //nolint:errcheck
	bindPodToNode(t, pod, nodeName)

	eventually(t, func(ctx context.Context) (bool, error) {
		n := &corev1.Node{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: nodeName}, n); err != nil {
			return false, nil
		}
		_, hasClaim := n.Annotations[AteletNodeClaimAnnoPrefix+poolUID]
		return n.Labels[AteletNodeLabel] == AteletNodeLabelValue && hasClaim, nil
	})

	if err := k8sClient.Delete(testCtx, pod, client.GracePeriodSeconds(0)); err != nil {
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
