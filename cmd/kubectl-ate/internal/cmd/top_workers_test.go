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

package cmd

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
)

type mockWorkerLister struct {
	workers []*ateapipb.Worker
	err     error
}

func (m *mockWorkerLister) ListWorkers(ctx context.Context, req *ateapipb.ListWorkersRequest, opts ...grpc.CallOption) (*ateapipb.ListWorkersResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &ateapipb.ListWorkersResponse{Workers: m.workers}, nil
}

type mockPodMetricsLister struct {
	metrics []metricsv1beta1.PodMetrics
	err     error
}

func (m *mockPodMetricsLister) ListPodMetrics(ctx context.Context, namespace string, opts metav1.ListOptions) ([]metricsv1beta1.PodMetrics, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.metrics, nil
}

func TestTopWorkersRunner_Success(t *testing.T) {
	workers := []*ateapipb.Worker{
		{
			WorkerNamespace: "ate-demo-counter",
			WorkerPool:      "counter",
			WorkerPod:       "counter-worker-pool-7b9f8-x123",
			SandboxClass:    "gvisor",
			Labels:          map[string]string{"ate.dev/worker-pool": "counter"},
			Status: &ateapipb.WorkerStatus{
				Assignment: &ateapipb.ActorAssignment{
					Actor: &ateapipb.ObjectRef{
						Atespace: "ate-demo-counter",
						Name:     "my-counter-1",
					},
				},
			},
		},
		{
			WorkerNamespace: "ate-demo-counter",
			WorkerPool:      "counter",
			WorkerPod:       "counter-worker-pool-7b9f8-y456",
			SandboxClass:    "microvm",
			Labels:          map[string]string{"ate.dev/worker-pool": "counter"},
		},
	}

	podMetrics := []metricsv1beta1.PodMetrics{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "counter-worker-pool-7b9f8-x123",
				Namespace: "ate-demo-counter",
			},
			Containers: []metricsv1beta1.ContainerMetrics{
				{
					Name: "ateom",
					Usage: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("342m"),
						corev1.ResourceMemory: resource.MustParse("412Mi"),
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "counter-worker-pool-7b9f8-y456",
				Namespace: "ate-demo-counter",
			},
			Containers: []metricsv1beta1.ContainerMetrics{
				{
					Name: "ateom",
					Usage: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("2m"),
						corev1.ResourceMemory: resource.MustParse("64Mi"),
					},
				},
			},
		},
	}

	var buf bytes.Buffer
	runner := &TopWorkersRunner{
		workerLister:     &mockWorkerLister{workers: workers},
		podMetricsLister: &mockPodMetricsLister{metrics: podMetrics},
		outputFmt:        "table",
		out:              &buf,
	}

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}

	expected := `NAME                             POOL      CLASS     STATUS     ASSIGNED ACTOR                  CPU(CORES)   MEMORY(bytes)
counter-worker-pool-7b9f8-x123   counter   gvisor    ASSIGNED   ate-demo-counter/my-counter-1   342m         412Mi
counter-worker-pool-7b9f8-y456   counter   microvm   FREE       <none>                          2m           64Mi
`
	if diff := cmp.Diff(expected, buf.String()); diff != "" {
		t.Errorf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestTopWorkersRunner_FilterNamespace(t *testing.T) {
	workers := []*ateapipb.Worker{
		{
			WorkerNamespace: "ns-1",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-1",
			SandboxClass:    "gvisor",
		},
		{
			WorkerNamespace: "ns-2",
			WorkerPool:      "pool-2",
			WorkerPod:       "pod-2",
		},
	}

	var buf bytes.Buffer
	runner := &TopWorkersRunner{
		workerLister:     &mockWorkerLister{workers: workers},
		podMetricsLister: &mockPodMetricsLister{metrics: nil},
		namespace:        "ns-1",
		outputFmt:        "table",
		out:              &buf,
	}

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}

	expected := `NAME    POOL     CLASS    STATUS   ASSIGNED ACTOR   CPU(CORES)            MEMORY(bytes)
pod-1   pool-1   gvisor   FREE     <none>           metrics unavailable   metrics unavailable
`
	if diff := cmp.Diff(expected, buf.String()); diff != "" {
		t.Errorf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestTopWorkersRunner_FilterAtespace(t *testing.T) {
	workers := []*ateapipb.Worker{
		{
			WorkerNamespace: "ns-1",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-1",
			SandboxClass:    "microvm",
			Status: &ateapipb.WorkerStatus{
				Assignment: &ateapipb.ActorAssignment{
					Actor: &ateapipb.ObjectRef{Atespace: "space-a", Name: "actor-a"},
				},
			},
		},
		{
			WorkerNamespace: "ns-1",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-2",
			Status: &ateapipb.WorkerStatus{
				Assignment: &ateapipb.ActorAssignment{
					Actor: &ateapipb.ObjectRef{Atespace: "space-b", Name: "actor-b"},
				},
			},
		},
	}

	var buf bytes.Buffer
	runner := &TopWorkersRunner{
		workerLister:     &mockWorkerLister{workers: workers},
		podMetricsLister: &mockPodMetricsLister{metrics: nil},
		atespace:         "space-a",
		outputFmt:        "table",
		out:              &buf,
	}

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}

	expected := `NAME    POOL     CLASS     STATUS     ASSIGNED ACTOR    CPU(CORES)            MEMORY(bytes)
pod-1   pool-1   microvm   ASSIGNED   space-a/actor-a   metrics unavailable   metrics unavailable
`
	if diff := cmp.Diff(expected, buf.String()); diff != "" {
		t.Errorf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestTopWorkersRunner_FilterSelector(t *testing.T) {
	workers := []*ateapipb.Worker{
		{
			WorkerNamespace: "ns-1",
			WorkerPool:      "counter",
			WorkerPod:       "pod-1",
			SandboxClass:    "gvisor",
			Labels:          map[string]string{"ate.dev/worker-pool": "counter"},
		},
		{
			WorkerNamespace: "ns-1",
			WorkerPool:      "other",
			WorkerPod:       "pod-2",
			Labels:          map[string]string{"ate.dev/worker-pool": "other"},
		},
	}

	var buf bytes.Buffer
	runner := &TopWorkersRunner{
		workerLister:     &mockWorkerLister{workers: workers},
		podMetricsLister: &mockPodMetricsLister{metrics: nil},
		selector:         "ate.dev/worker-pool=counter",
		outputFmt:        "table",
		out:              &buf,
	}

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}

	expected := `NAME    POOL      CLASS    STATUS   ASSIGNED ACTOR   CPU(CORES)            MEMORY(bytes)
pod-1   counter   gvisor   FREE     <none>           metrics unavailable   metrics unavailable
`
	if diff := cmp.Diff(expected, buf.String()); diff != "" {
		t.Errorf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestTopWorkersRunner_MetricsUnavailable(t *testing.T) {
	workers := []*ateapipb.Worker{
		{
			WorkerNamespace: "ns-1",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-1",
		},
	}

	var buf bytes.Buffer
	runner := &TopWorkersRunner{
		workerLister:     &mockWorkerLister{workers: workers},
		podMetricsLister: &mockPodMetricsLister{err: errors.New("metrics-server unavailable")},
		outputFmt:        "table",
		out:              &buf,
	}

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}

	expected := `NAME    POOL     CLASS   STATUS   ASSIGNED ACTOR   CPU(CORES)            MEMORY(bytes)
pod-1   pool-1           FREE     <none>           metrics unavailable   metrics unavailable
`
	if diff := cmp.Diff(expected, buf.String()); diff != "" {
		t.Errorf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestTopWorkersRunner_InvalidSelector(t *testing.T) {
	runner := &TopWorkersRunner{
		workerLister: &mockWorkerLister{workers: nil},
		selector:     "invalid==selector==",
	}

	if err := runner.Run(context.Background()); err == nil {
		t.Errorf("expected error for invalid label selector, got nil")
	}
}
