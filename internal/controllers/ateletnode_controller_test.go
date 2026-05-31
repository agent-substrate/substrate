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

	// Bind the pod to the node (envtest doesn't schedule; we set nodeName via Binding).
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

// bindPodToNode sets Pod.Spec.NodeName via the Binding subresource,
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
