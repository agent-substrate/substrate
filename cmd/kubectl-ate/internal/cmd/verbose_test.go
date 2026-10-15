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
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
)

func setVerbosity(t *testing.T, v int) {
	t.Helper()
	prev := verbosity
	verbosity = v
	t.Cleanup(func() { verbosity = prev })
}

func TestVlogfLevels(t *testing.T) {
	tests := []struct {
		level     int
		wantBasic bool
		wantFlow  bool
		wantTrace bool
	}{
		{level: 0, wantBasic: false, wantFlow: false, wantTrace: false},
		{level: vBasic, wantBasic: true, wantFlow: false, wantTrace: false},
		{level: vFlow, wantBasic: true, wantFlow: true, wantTrace: false},
		{level: vTrace, wantBasic: true, wantFlow: true, wantTrace: true},
	}

	for _, tc := range tests {
		setVerbosity(t, tc.level)
		var buf bytes.Buffer
		vlogf(&buf, vBasic, "basic-msg")
		vlogf(&buf, vFlow, "flow-msg")
		vlogf(&buf, vTrace, "trace-msg")
		got := buf.String()

		if strings.Contains(got, "basic-msg") != tc.wantBasic {
			t.Errorf("verbosity=%d: basic-msg present=%t, want %t (got %q)", tc.level, !tc.wantBasic, tc.wantBasic, got)
		}
		if strings.Contains(got, "flow-msg") != tc.wantFlow {
			t.Errorf("verbosity=%d: flow-msg present=%t, want %t (got %q)", tc.level, !tc.wantFlow, tc.wantFlow, got)
		}
		if strings.Contains(got, "trace-msg") != tc.wantTrace {
			t.Errorf("verbosity=%d: trace-msg present=%t, want %t (got %q)", tc.level, !tc.wantTrace, tc.wantTrace, got)
		}
	}
}

func TestLoggingUnaryInterceptor(t *testing.T) {
	req := &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: "demo", Name: "act-1"}}
	wantActor := &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "demo", Name: "act-1"}}
	const method = ateapipb.Control_GetActor_FullMethodName

	okInvoker := func(_ context.Context, _ string, _, reply any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		proto.Merge(reply.(proto.Message), wantActor)
		return nil
	}
	errInvoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		return errors.New("boom")
	}

	t.Run("vBasic silent on fast success, logs failure", func(t *testing.T) {
		setVerbosity(t, vBasic)
		var errOut bytes.Buffer
		ic := newLoggingUnaryInterceptor(&errOut)

		reply := &ateapipb.Actor{}
		if err := ic(context.Background(), method, req, reply, nil, okInvoker); err != nil {
			t.Fatalf("interceptor unexpected error: %v", err)
		}
		if errOut.Len() != 0 {
			t.Errorf("vBasic fast success wrote %q, want empty", errOut.String())
		}

		_ = ic(context.Background(), method, req, reply, nil, errInvoker)
		if !strings.Contains(errOut.String(), "gRPC /ateapi.Control/GetActor failed") {
			t.Errorf("vBasic failure stderr = %q, want 'gRPC /ateapi.Control/GetActor failed'", errOut.String())
		}
	})

	t.Run("vFlow logs RPC call and completion", func(t *testing.T) {
		setVerbosity(t, vFlow)
		var errOut bytes.Buffer
		ic := newLoggingUnaryInterceptor(&errOut)

		reply := &ateapipb.Actor{}
		if err := ic(context.Background(), method, req, reply, nil, okInvoker); err != nil {
			t.Fatalf("interceptor unexpected error: %v", err)
		}
		got := errOut.String()
		if !strings.Contains(got, "gRPC /ateapi.Control/GetActor\n") || !strings.Contains(got, "gRPC /ateapi.Control/GetActor completed in ") {
			t.Errorf("vFlow stderr = %q, want call and completion lines", got)
		}
	})

	t.Run("vTrace logs request and response payloads", func(t *testing.T) {
		setVerbosity(t, vTrace)
		var errOut bytes.Buffer
		ic := newLoggingUnaryInterceptor(&errOut)

		reply := &ateapipb.Actor{}
		if err := ic(context.Background(), method, req, reply, nil, okInvoker); err != nil {
			t.Fatalf("interceptor unexpected error: %v", err)
		}
		got := errOut.String()
		if !strings.Contains(got, "gRPC /ateapi.Control/GetActor request:") || !strings.Contains(got, `"act-1"`) || !strings.Contains(got, "gRPC /ateapi.Control/GetActor response") {
			t.Errorf("vTrace stderr = %q, want request and response payloads", got)
		}
	})
}

func TestLogsActorRunner_FollowVerboseSurfacesDelaysAndErrors(t *testing.T) {
	setVerbosity(t, vBasic)

	var mu sync.Mutex
	var getCalls int
	mockAPI := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
			mu.Lock()
			defer mu.Unlock()
			getCalls++
			switch getCalls {
			case 1:
				return nil, errors.New("transient apiserver error")
			case 2:
				return &ateapipb.Actor{
					Metadata: &ateapipb.ResourceMetadata{Name: "act-1"},
					Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
				}, nil
			default:
				return &ateapipb.Actor{
					Metadata: &ateapipb.ResourceMetadata{Name: "act-1"},
					Status: &ateapipb.ActorStatus{
						State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
						WorkerAssignment: &ateapipb.WorkerAssignment{
							WorkerPod:       "pod-1",
							WorkerNamespace: "ns-1",
						},
					},
				}, nil
			}
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var streamCalls int
	mockStreamer := &mockPodLogsStreamer{
		StreamLogsFunc: func(streamCtx context.Context, ns, name string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
			mu.Lock()
			streamCalls++
			call := streamCalls
			mu.Unlock()
			if call == 1 {
				return nil, errors.New("kubelet stream unavailable")
			}
			cancel()
			return io.NopCloser(strings.NewReader("")), nil
		},
	}

	var stdout, stderr bytes.Buffer
	runner := &LogsActorRunner{
		apiClient:         mockAPI,
		actorRef:          resources.ActorRef{Atespace: "space-1", Name: "act-1"},
		filter:            logLineFilter{target: resources.ActorRef{Atespace: "space-1", Name: "act-1"}, source: logSourceAll},
		streamer:          mockStreamer,
		stdout:            &stdout,
		stderr:            &stderr,
		follow:            true,
		pollInterval:      1 * time.Millisecond,
		reconnectInterval: 1 * time.Millisecond,
		tickerInterval:    50 * time.Millisecond,
	}

	_ = runner.Run(ctx)
	gotStderr := stderr.String()

	for _, want := range []string{
		"Failed to get actor space-1/act-1: transient apiserver error",
		"Actor space-1/act-1 is not running (state=ACTOR_STATE_SUSPENDED); waiting for worker assignment...",
		"Failed to stream logs from pod ns-1/pod-1: kubelet stream unavailable",
	} {
		if !strings.Contains(gotStderr, want) {
			t.Errorf("stderr = %q, want substring %q", gotStderr, want)
		}
	}
}

func TestTopWorkersRunner_VerboseSurfacesMetricsIssues(t *testing.T) {
	workers := []*ateapipb.Worker{
		{
			Metadata:        &ateapipb.ResourceMetadata{Name: "w-1"},
			WorkerNamespace: "ns-1",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-1",
		},
		{
			Metadata:        &ateapipb.ResourceMetadata{Name: "w-2"},
			WorkerNamespace: "ns-1",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-2",
		},
	}

	t.Run("vBasic surfaces ListPodMetrics failure", func(t *testing.T) {
		setVerbosity(t, vBasic)
		var out, errOut bytes.Buffer
		runner := &TopWorkersRunner{
			workerLister:     &mockWorkerLister{workers: workers},
			podMetricsLister: &mockPodMetricsLister{err: errors.New("metrics API down")},
			outputFmt:        "table",
			out:              &out,
			stderr:           &errOut,
		}
		if err := runner.Run(context.Background()); err != nil {
			t.Fatalf("Run() error: %v", err)
		}
		if !strings.Contains(errOut.String(), "Failed to list pod metrics (CPU/memory will be unavailable): metrics API down") {
			t.Errorf("stderr = %q, want ListPodMetrics error", errOut.String())
		}
	})

	t.Run("vFlow surfaces missing pod metrics and container fallback", func(t *testing.T) {
		setVerbosity(t, vFlow)
		var out, errOut bytes.Buffer
		runner := &TopWorkersRunner{
			workerLister: &mockWorkerLister{workers: workers},
			podMetricsLister: &mockPodMetricsLister{metrics: []metricsv1beta1.PodMetrics{
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "ns-1", Name: "pod-1"},
					Containers: []metricsv1beta1.ContainerMetrics{
						{
							Name: "sidecar",
							Usage: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("100m"),
								corev1.ResourceMemory: resource.MustParse("32Mi"),
							},
						},
					},
				},
			}},
			outputFmt: "table",
			out:       &out,
			stderr:    &errOut,
		}
		if err := runner.Run(context.Background()); err != nil {
			t.Fatalf("Run() error: %v", err)
		}
		got := errOut.String()
		for _, want := range []string{
			`Container "worker" not found in pod metrics for ns-1/pod-1; summing all 1 container(s)`,
			"No pod metrics returned for worker pod ns-1/pod-2",
		} {
			if !strings.Contains(got, want) {
				t.Errorf("stderr = %q, want substring %q", got, want)
			}
		}
	})
}
