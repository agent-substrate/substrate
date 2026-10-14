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

package main

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

var fakeAteletLabels = map[string]string{"app": "atelet", "ate.dev/fake-atelet": "true"}

func testNode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status:     corev1.NodeStatus{Allocatable: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("8Gi")}},
	}
}

func testPod(name, namespace, nodeName string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
		Spec:       corev1.PodSpec{NodeName: nodeName},
	}
}

// testKubeCluster returns a kubeCluster on kc with a five-minute node grace,
// and a function that advances its clock.
func testKubeCluster(kc *fake.Clientset) (*kubeCluster, func(time.Duration)) {
	now := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	cl := &kubeCluster{
		kc: kc, ateletNS: "ate-system", ateletLabel: "ate.dev/fake-atelet=true",
		nodeGrace: 5 * time.Minute, now: func() time.Time { return now },
	}
	return cl, func(d time.Duration) { now = now.Add(d) }
}

func nodeNames(t *testing.T, cl *kubeCluster) []string {
	t.Helper()
	nodes, err := cl.Nodes(t.Context())
	if err != nil {
		t.Fatalf("Nodes() error = %v", err)
	}
	var names []string
	for _, n := range nodes {
		names = append(names, n.name)
		if n.allocatable.Memory().IsZero() {
			t.Errorf("node %s has no allocatable memory", n.name)
		}
	}
	return names
}

// fake-workersync is not ready until it has adopted the registered Workers,
// so the install's rollout wait covers ate-api-server answering it.
func TestHealthHandlerReadyAfterAdopt(t *testing.T) {
	var ready atomic.Bool
	h := healthHandler(&ready)
	get := func(path string) int {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec.Code
	}
	if got := get("/healthz"); got != http.StatusOK {
		t.Errorf("/healthz = %d, want 200", got)
	}
	if got := get("/readyz"); got != http.StatusServiceUnavailable {
		t.Errorf("/readyz before adopt = %d, want 503", got)
	}
	ready.Store(true)
	if got := get("/readyz"); got != http.StatusOK {
		t.Errorf("/readyz after adopt = %d, want 200", got)
	}
}

// Workers go only to nodes that run a fake-atelet pod: a node without one,
// such as a tainted control plane node, has no relay to report through.
func TestKubeClusterNodesAreTheFakeAteletNodes(t *testing.T) {
	kc := fake.NewClientset(
		testNode("node-b"), testNode("node-a"), testNode("control-plane"), testNode("unscheduled"), testNode("other-ns"),
		testPod("fake-atelet-b", "ate-system", "node-b", fakeAteletLabels),
		testPod("fake-atelet-a", "ate-system", "node-a", fakeAteletLabels),
		testPod("fake-atelet-pending", "ate-system", "", fakeAteletLabels),
		testPod("not-fake", "ate-system", "control-plane", map[string]string{"app": "atenet-router"}),
		testPod("fake-atelet-elsewhere", "default", "other-ns", fakeAteletLabels),
	)
	cl, _ := testKubeCluster(kc)

	if got, want := nodeNames(t, cl), []string{"node-a", "node-b"}; !slices.Equal(got, want) {
		t.Errorf("Nodes() = %v, want %v", got, want)
	}
}

// After a restart, a node whose fake-atelet is being replaced has no pod; the
// adopted Workers' nodes keep it in the set.
func TestKubeClusterRemembersAdoptedNodes(t *testing.T) {
	kc := fake.NewClientset(
		testNode("node-a"), testNode("node-b"), testNode("gone-later"),
		testPod("fake-atelet-a", "ate-system", "node-a", fakeAteletLabels),
	)
	cl, _ := testKubeCluster(kc)
	cl.remember([]string{"node-b", "missing"})
	if got, want := nodeNames(t, cl), []string{"node-a", "node-b"}; !slices.Equal(got, want) {
		t.Errorf("Nodes() = %v, want %v", got, want)
	}
}

// A DaemonSet rollout leaves a node without a fake-atelet pod for a moment;
// the node stays, so its Workers are not moved. A deleted node goes.
func TestKubeClusterNodesSurviveAFakeAteletReplacement(t *testing.T) {
	kc := fake.NewClientset(
		testNode("node-a"), testNode("node-b"),
		testPod("fake-atelet-a", "ate-system", "node-a", fakeAteletLabels),
		testPod("fake-atelet-b", "ate-system", "node-b", fakeAteletLabels),
	)
	cl, _ := testKubeCluster(kc)
	if got, want := nodeNames(t, cl), []string{"node-a", "node-b"}; !slices.Equal(got, want) {
		t.Fatalf("Nodes() = %v, want %v", got, want)
	}

	if err := kc.CoreV1().Pods("ate-system").Delete(t.Context(), "fake-atelet-a", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if got, want := nodeNames(t, cl), []string{"node-a", "node-b"}; !slices.Equal(got, want) {
		t.Errorf("Nodes() during node-a's fake-atelet replacement = %v, want %v", got, want)
	}

	if err := kc.CoreV1().Nodes().Delete(t.Context(), "node-b", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if got, want := nodeNames(t, cl), []string{"node-a"}; !slices.Equal(got, want) {
		t.Errorf("Nodes() after node-b is deleted = %v, want %v", got, want)
	}
}

// A node that stops running fake-atelet for good leaves the set once the
// grace has passed, so its Workers move to a node with a relay.
func TestKubeClusterNodesExpireWithoutAFakeAtelet(t *testing.T) {
	kc := fake.NewClientset(
		testNode("node-a"), testNode("node-b"),
		testPod("fake-atelet-a", "ate-system", "node-a", fakeAteletLabels),
		testPod("fake-atelet-b", "ate-system", "node-b", fakeAteletLabels),
	)
	cl, advance := testKubeCluster(kc)
	nodeNames(t, cl)
	if err := kc.CoreV1().Pods("ate-system").Delete(t.Context(), "fake-atelet-b", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}

	advance(4 * time.Minute)
	if got, want := nodeNames(t, cl), []string{"node-a", "node-b"}; !slices.Equal(got, want) {
		t.Errorf("Nodes() within the grace = %v, want %v", got, want)
	}
	advance(2 * time.Minute)
	if got, want := nodeNames(t, cl), []string{"node-a"}; !slices.Equal(got, want) {
		t.Errorf("Nodes() past the grace = %v, want %v", got, want)
	}
}
