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
