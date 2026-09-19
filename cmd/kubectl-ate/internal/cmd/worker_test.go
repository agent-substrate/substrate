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
	"time"

	"github.com/agent-substrate/substrate/cmd/kubectl-ate/internal/printer"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
)

func TestWorkerCommandArgs(t *testing.T) {
	runCommandArgsTests(t, []commandArgsTest{
		{name: "list", command: getWorkersCmd},
		{name: "get", command: getWorkersCmd, args: []string{"worker-1"}},
		{name: "get multiple", command: getWorkersCmd, args: []string{"worker-1", "worker-2"}},
		{name: "top list", command: topWorkersCmd},
		{name: "top rejects argument", command: topWorkersCmd, args: []string{"worker-1"}, wantErr: true},
	})
}

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

// mockActorLister serves the actors of one atespace at a time, as the real
// listing does, so an atespace filter sees only that atespace's placements.
type mockActorLister struct {
	byAtespace map[string][]*ateapipb.Actor
	err        error
}

func (m *mockActorLister) ListActors(ctx context.Context, req *ateapipb.ListActorsRequest, opts ...grpc.CallOption) (*ateapipb.ListActorsResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &ateapipb.ListActorsResponse{Actors: m.byAtespace[req.GetAtespace()]}, nil
}

// actorOn is an actor of atespace placed on the named worker.
func actorOn(atespace, name, workerName string) *ateapipb.Actor {
	return &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
		Status: &ateapipb.ActorStatus{
			WorkerAssignment: &ateapipb.WorkerAssignment{Worker: &ateapipb.ObjectRef{Name: workerName}},
		},
	}
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

// mockWorkerGetter serves GetWorker calls from a name-keyed map, as the
// GetWorkersRunner requires when getting workers by name.
type mockWorkerGetter struct {
	workers map[string]*ateapipb.Worker
	err     error
}

func (m *mockWorkerGetter) GetWorker(ctx context.Context, req *ateapipb.GetWorkerRequest, opts ...grpc.CallOption) (*ateapipb.Worker, error) {
	if m.err != nil {
		return nil, m.err
	}
	worker, ok := m.workers[req.GetWorker().GetName()]
	if !ok {
		return nil, errors.New("worker not found")
	}
	return worker, nil
}

func pinTime(t *testing.T, now time.Time) {
	t.Helper()
	prev := printer.TimeNow
	printer.TimeNow = func() time.Time { return now }
	t.Cleanup(func() { printer.TimeNow = prev })
}

func TestGetWorkersRunner_Filters(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	pinTime(t, now)

	workers := []*ateapipb.Worker{
		{
			Metadata:        &ateapipb.ResourceMetadata{Name: "worker-1", CreateTime: timestamppb.New(now.Add(-5 * time.Minute))},
			WorkerNamespace: "ns-1",
			WorkerPool:      "counter",
			WorkerPod:       "pod-1",
			SandboxClass:    "microvm",
			Labels:          map[string]string{"ate.dev/worker-pool": "counter"},
			Status: &ateapipb.WorkerStatus{
				State:     ateapipb.WorkerState_WORKER_STATE_ACTIVE,
				Capacity:  &ateapipb.WorkerResources{Actors: 1},
				Allocated: &ateapipb.WorkerResources{Actors: 1},
			},
		},
		{
			Metadata:        &ateapipb.ResourceMetadata{Name: "worker-2", CreateTime: timestamppb.New(now.Add(-3 * time.Hour))},
			WorkerNamespace: "ns-1",
			WorkerPool:      "other",
			WorkerPod:       "pod-2",
			SandboxClass:    "gvisor",
			Labels:          map[string]string{"ate.dev/worker-pool": "other"},
			Status:          &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE},
		},
		{
			Metadata:        &ateapipb.ResourceMetadata{Name: "worker-3", CreateTime: timestamppb.New(now.Add(-72 * time.Hour))},
			WorkerNamespace: "ns-2",
			WorkerPool:      "counter",
			WorkerPod:       "pod-3",
			SandboxClass:    "gvisor",
			Labels:          map[string]string{"ate.dev/worker-pool": "counter"},
			Status: &ateapipb.WorkerStatus{
				State:     ateapipb.WorkerState_WORKER_STATE_ACTIVE,
				Capacity:  &ateapipb.WorkerResources{Actors: 1},
				Allocated: &ateapipb.WorkerResources{Actors: 1},
			},
		},
	}
	actors := &mockActorLister{byAtespace: map[string][]*ateapipb.Actor{
		"space-a": {actorOn("space-a", "actor-a", "worker-1")},
		"space-b": {actorOn("space-b", "actor-b", "worker-3")},
	}}

	header := "NAME       POOL      STATE                 ACTORS   CPU   MEMORY   POD          AGE\n"
	row1 := "worker-1   counter   WORKER_STATE_ACTIVE   1/1      -     -        ns-1/pod-1   5m\n"
	row2 := "worker-2   other     WORKER_STATE_ACTIVE   0/0      -     -        ns-1/pod-2   3h\n"
	row3 := "worker-3   counter   WORKER_STATE_ACTIVE   1/1      -     -        ns-2/pod-3   3d\n"

	tests := []struct {
		name         string
		namespace    string
		atespace     string
		selector     string
		sandboxClass string
		expected     string
	}{
		{name: "no filter", expected: header + row1 + row2 + row3},
		{name: "namespace", namespace: "ns-1", expected: header + row1 + row2},
		{name: "atespace", atespace: "space-a", expected: header + row1},
		// With no matching rows the tabwriter sizes columns to the header alone.
		{name: "atespace excludes free workers", atespace: "no-such-space", expected: "NAME   POOL   STATE   ACTORS   CPU   MEMORY   POD   AGE\n"},
		{name: "selector", selector: "ate.dev/worker-pool=counter", expected: header + row1 + row3},
		{name: "sandbox class", sandboxClass: "microvm", expected: header + row1},
		{name: "sandbox class gvisor", sandboxClass: "gvisor", expected: header + row2 + row3},
		{name: "combined", namespace: "ns-1", selector: "ate.dev/worker-pool=counter", expected: header + row1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			runner := &GetWorkersRunner{
				workerLister: &mockWorkerLister{workers: workers},
				actorLister:  actors,
				namespace:    test.namespace,
				atespace:     test.atespace,
				selector:     test.selector,
				sandboxClass: test.sandboxClass,
				outputFmt:    "table",
				out:          &buf,
			}
			if err := runner.Run(context.Background()); err != nil {
				t.Fatalf("Run() unexpected error: %v", err)
			}
			if diff := cmp.Diff(test.expected, buf.String()); diff != "" {
				t.Errorf("output mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGetWorkersRunner_InvalidSelector(t *testing.T) {
	runner := &GetWorkersRunner{
		workerLister: &mockWorkerLister{workers: nil},
		selector:     "invalid==selector==",
	}

	if err := runner.Run(context.Background()); err == nil {
		t.Errorf("expected error for invalid label selector, got nil")
	}
}

func TestGetWorkersRunner_GetByName(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	pinTime(t, now)

	getter := &mockWorkerGetter{workers: map[string]*ateapipb.Worker{
		"worker-1": {
			Metadata:        &ateapipb.ResourceMetadata{Name: "worker-1", CreateTime: timestamppb.New(now.Add(-5 * time.Minute))},
			WorkerNamespace: "ns-1",
			WorkerPool:      "counter",
			WorkerPod:       "pod-1",
			Status: &ateapipb.WorkerStatus{
				State: ateapipb.WorkerState_WORKER_STATE_ACTIVE,
				Capacity: &ateapipb.WorkerResources{Actors: 4, Resources: &ateapipb.Resources{Limits: []*ateapipb.Limits{
					{Name: "cpu", Quantity: "2"}, {Name: "memory", Quantity: "4Gi"},
				}}},
				Allocated: &ateapipb.WorkerResources{Actors: 1, Resources: &ateapipb.Resources{Limits: []*ateapipb.Limits{
					{Name: "cpu", Quantity: "500m"}, {Name: "memory", Quantity: "1Gi"},
				}}},
			},
		},
		"worker-2": {
			Metadata:        &ateapipb.ResourceMetadata{Name: "worker-2", CreateTime: timestamppb.New(now.Add(-72 * time.Hour))},
			WorkerNamespace: "ns-1",
			WorkerPool:      "counter",
			WorkerPod:       "pod-2",
			Status:          &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_DRAINING},
		},
	}}

	var buf bytes.Buffer
	runner := &GetWorkersRunner{
		workerGetter: getter,
		names:        []string{"worker-1", "worker-2"},
		outputFmt:    "table",
		out:          &buf,
	}
	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}

	expected := "NAME       POOL      STATE                   ACTORS   CPU      MEMORY    POD          AGE\n" +
		"worker-1   counter   WORKER_STATE_ACTIVE     1/4      500m/2   1Gi/4Gi   ns-1/pod-1   5m\n" +
		"worker-2   counter   WORKER_STATE_DRAINING   0/0      -        -         ns-1/pod-2   3d\n"
	if diff := cmp.Diff(expected, buf.String()); diff != "" {
		t.Errorf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestGetWorkersRunner_GetByName_Single(t *testing.T) {
	getter := &mockWorkerGetter{workers: map[string]*ateapipb.Worker{
		"worker-1": {Metadata: &ateapipb.ResourceMetadata{Name: "worker-1"}},
	}}

	var buf bytes.Buffer
	runner := &GetWorkersRunner{
		workerGetter: getter,
		names:        []string{"worker-1"},
		outputFmt:    "json",
		out:          &buf,
	}
	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}

	expected := `{
  "metadata": {
    "name": "worker-1"
  }
}
`
	if diff := cmp.Diff(expected, buf.String()); diff != "" {
		t.Errorf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestGetWorkersRunner_GetByName_Error(t *testing.T) {
	runner := &GetWorkersRunner{
		workerGetter: &mockWorkerGetter{err: errors.New("rpc failed")},
		names:        []string{"missing-worker"},
	}

	if err := runner.Run(context.Background()); err == nil {
		t.Errorf("expected error for failed GetWorker, got nil")
	}
}

func TestGetWorkersRunner_GetByName_RejectsFilters(t *testing.T) {
	tests := []struct {
		name   string
		runner GetWorkersRunner
	}{
		{name: "namespace", runner: GetWorkersRunner{names: []string{"worker-1"}, namespace: "ns-1"}},
		{name: "atespace", runner: GetWorkersRunner{names: []string{"worker-1"}, atespace: "space-a"}},
		{name: "selector", runner: GetWorkersRunner{names: []string{"worker-1"}, selector: "foo=bar"}},
		{name: "sandbox class", runner: GetWorkersRunner{names: []string{"worker-1"}, sandboxClass: "gvisor"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.runner.Run(context.Background()); err == nil {
				t.Errorf("expected error combining names with a filter flag, got nil")
			}
		})
	}
}

func TestTopWorkersRunner_Success(t *testing.T) {
	workers := []*ateapipb.Worker{
		{
			WorkerNamespace: "ate-demo-counter",
			WorkerPool:      "counter",
			WorkerPod:       "counter-worker-pool-7b9f8-x123",
			SandboxClass:    "gvisor",
			Labels:          map[string]string{"ate.dev/worker-pool": "counter"},
			Status:          &ateapipb.WorkerStatus{Capacity: &ateapipb.WorkerResources{Actors: 1}, Allocated: &ateapipb.WorkerResources{Actors: 1}},
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

	expected := `NAME                             POOL      CLASS     STATUS          CPU(CORES)   MEMORY(bytes)
counter-worker-pool-7b9f8-x123   counter   gvisor    ASSIGNED(1/1)   342m         412Mi
counter-worker-pool-7b9f8-y456   counter   microvm   FREE            2m           64Mi
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

	expected := `NAME    POOL     CLASS    STATUS   CPU(CORES)            MEMORY(bytes)
pod-1   pool-1   gvisor   FREE     metrics unavailable   metrics unavailable
`
	if diff := cmp.Diff(expected, buf.String()); diff != "" {
		t.Errorf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestTopWorkersRunner_FilterAtespace(t *testing.T) {
	workers := []*ateapipb.Worker{
		{
			Metadata:        &ateapipb.ResourceMetadata{Name: "worker-1"},
			WorkerNamespace: "ns-1",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-1",
			SandboxClass:    "microvm",
			Status:          &ateapipb.WorkerStatus{Capacity: &ateapipb.WorkerResources{Actors: 1}, Allocated: &ateapipb.WorkerResources{Actors: 1}},
		},
		{
			Metadata:        &ateapipb.ResourceMetadata{Name: "worker-2"},
			WorkerNamespace: "ns-1",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-2",
			Status:          &ateapipb.WorkerStatus{Capacity: &ateapipb.WorkerResources{Actors: 1}, Allocated: &ateapipb.WorkerResources{Actors: 1}},
		},
	}

	var buf bytes.Buffer
	runner := &TopWorkersRunner{
		workerLister: &mockWorkerLister{workers: workers},
		actorLister: &mockActorLister{byAtespace: map[string][]*ateapipb.Actor{
			"space-a": {actorOn("space-a", "actor-a", "worker-1")},
			"space-b": {actorOn("space-b", "actor-b", "worker-2")},
		}},
		podMetricsLister: &mockPodMetricsLister{metrics: nil},
		atespace:         "space-a",
		outputFmt:        "table",
		out:              &buf,
	}

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}

	expected := `NAME    POOL     CLASS     STATUS          CPU(CORES)            MEMORY(bytes)
pod-1   pool-1   microvm   ASSIGNED(1/1)   metrics unavailable   metrics unavailable
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

	expected := `NAME    POOL      CLASS    STATUS   CPU(CORES)            MEMORY(bytes)
pod-1   counter   gvisor   FREE     metrics unavailable   metrics unavailable
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

	expected := `NAME    POOL     CLASS   STATUS   CPU(CORES)            MEMORY(bytes)
pod-1   pool-1           FREE     metrics unavailable   metrics unavailable
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
