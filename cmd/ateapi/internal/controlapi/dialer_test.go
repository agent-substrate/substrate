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
	"testing"

	"google.golang.org/grpc/connectivity"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/lru"
)

func TestAteletDialerClosesEvictedConn(t *testing.T) {
	workerIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		byNamespaceAndName: func(obj any) ([]string, error) {
			pod := obj.(*corev1.Pod)
			return []string{pod.Namespace + "/" + pod.Name}, nil
		},
	})
	ateletIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		byNode: func(obj any) ([]string, error) {
			return []string{obj.(*corev1.Pod).Spec.NodeName}, nil
		},
	})

	for _, pod := range []*corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{Namespace: "worker-ns", Name: "worker-1"},
			Spec:       corev1.PodSpec{NodeName: "node-1"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Namespace: "worker-ns", Name: "worker-2"},
			Spec:       corev1.PodSpec{NodeName: "node-2"},
		},
	} {
		if err := workerIndexer.Add(pod); err != nil {
			t.Fatalf("adding worker pod: %v", err)
		}
	}

	for _, pod := range []*corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ate-system", Name: "atelet-1"},
			Spec:       corev1.PodSpec{NodeName: "node-1"},
			Status:     corev1.PodStatus{PodIPs: []corev1.PodIP{{IP: "127.0.0.1"}}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ate-system", Name: "atelet-2"},
			Spec:       corev1.PodSpec{NodeName: "node-2"},
			Status:     corev1.PodStatus{PodIPs: []corev1.PodIP{{IP: "127.0.0.2"}}},
		},
	} {
		if err := ateletIndexer.Add(pod); err != nil {
			t.Fatalf("adding atelet pod: %v", err)
		}
	}

	d := NewAteletDialer(workerIndexer, ateletIndexer)
	d.ateletConns = lru.NewWithEvictionFunc(1, onEvict)

	firstConn, err := d.DialForWorker("worker-ns", "worker-1")
	if err != nil {
		t.Fatalf("DialForWorker(worker-1): %v", err)
	}

	secondConn, err := d.DialForWorker("worker-ns", "worker-2")
	if err != nil {
		t.Fatalf("DialForWorker(worker-2): %v", err)
	}
	t.Cleanup(func() { _ = secondConn.Close() })

	if got := firstConn.GetState(); got != connectivity.Shutdown {
		t.Errorf("evicted connection state = %v, want %v", got, connectivity.Shutdown)
	}
	if got := secondConn.GetState(); got == connectivity.Shutdown {
		t.Error("cached connection unexpectedly closed")
	}
}
