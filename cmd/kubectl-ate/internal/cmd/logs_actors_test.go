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
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
)

func TestFilterAndDisplayLogLine(t *testing.T) {
	tests := []struct {
		name          string
		line          string
		targetActorID string
		container     string
		supervisor    bool
		wantMatched   bool
		wantTime      string
		wantOutput    string
	}{
		{
			name:          "matching actor, JSON log with RFC3339Nano",
			line:          `{"time":"2026-05-16T01:03:38.602878302Z","level":"info","msg":"Count","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-1"}}`,
			targetActorID: "act-1",
			wantMatched:   true,
			wantTime:      "2026-05-16T01:03:38.602878302Z",
			wantOutput:    `{"time":"2026-05-16T01:03:38.602878302Z","level":"info","msg":"Count"}`,
		},
		{
			name:          "matching actor, plain text log",
			line:          `{"time":"2026-05-16T01:03:38Z","message":"Hello","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-1"}}`,
			targetActorID: "act-1",
			wantMatched:   true,
			wantTime:      "2026-05-16T01:03:38Z",
			wantOutput:    `{"time":"2026-05-16T01:03:38Z","message":"Hello"}`,
		},
		{
			name:          "matching actor, JSON log with no timestamp fallback",
			line:          `{"level":"error","msg":"Failed","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-1"}}`,
			targetActorID: "act-1",
			wantMatched:   true,
			wantTime:      "",
			wantOutput:    `{"level":"error","msg":"Failed"}`,
		},
		{
			name:          "matching actor, fallback to standard labels key",
			line:          `{"time":"2026-05-16T01:03:38.602878302Z","level":"info","msg":"Count","labels":{"ate.dev/actor_id":"act-1"}}`,
			targetActorID: "act-1",
			wantMatched:   true,
			wantTime:      "2026-05-16T01:03:38.602878302Z",
			wantOutput:    `{"time":"2026-05-16T01:03:38.602878302Z","level":"info","msg":"Count"}`,
		},
		{
			name:          "non-matching actor",
			line:          `{"time":"2026-05-16T01:03:38Z","message":"Hello world","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-2"}}`,
			targetActorID: "act-1",
			wantMatched:   false,
			wantTime:      "", // zero time: another actor's line must not advance follow's resume cursor
			wantOutput:    "",
		},
		{
			name:          "invalid json line",
			line:          "not a json line",
			targetActorID: "act-1",
			wantMatched:   false,
			wantTime:      "",
			wantOutput:    "",
		},
		{
			name:          "matching actor, flat JSON log",
			line:          `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"Hello","traceID":"abc-123","err":"timeout","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-1"}}`,
			targetActorID: "act-1",
			wantMatched:   true,
			wantTime:      "2026-05-16T01:03:38Z",
			wantOutput:    `{"time":"2026-05-16T01:03:38Z","err":"timeout","level":"info","msg":"Hello","traceID":"abc-123"}`,
		},
		{
			name:          "matching actor, severity and message keys",
			line:          `{"time":"2026-05-16T01:03:38Z","severity":"error","message":"Disk full","custom_tag":"alert","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-1"}}`,
			targetActorID: "act-1",
			wantMatched:   true,
			wantTime:      "2026-05-16T01:03:38Z",
			wantOutput:    `{"time":"2026-05-16T01:03:38Z","custom_tag":"alert","message":"Disk full","severity":"error"}`,
		},
		{
			name:          "matching actor, 2-field structured log without time",
			line:          `{"message":"login failed","code":401,"logging.googleapis.com/labels":{"ate.dev/actor_id":"act-1"}}`,
			targetActorID: "act-1",
			wantMatched:   true,
			wantTime:      "",
			wantOutput:    `{"code":401,"message":"login failed"}`,
		},
		{
			name:          "matching actor, JSON log with custom application labels",
			line:          `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"Hello","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-1","app":"my-app"}}`,
			targetActorID: "act-1",
			wantMatched:   true,
			wantTime:      "2026-05-16T01:03:38Z",
			wantOutput:    `{"time":"2026-05-16T01:03:38Z","level":"info","logging.googleapis.com/labels":{"app":"my-app"},"msg":"Hello"}`,
		},
		{
			name:          "container filter matches the named container",
			line:          `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-1","ate.dev/container_name":"counter"}}`,
			targetActorID: "act-1",
			container:     "counter",
			wantMatched:   true,
			wantTime:      "2026-05-16T01:03:38Z",
			wantOutput:    `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi"}`,
		},
		{
			name:          "container filter excludes a different container",
			line:          `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-1","ate.dev/container_name":"sidecar"}}`,
			targetActorID: "act-1",
			container:     "counter",
			wantMatched:   false,
			wantTime:      "", // filtered out: only displayed lines may advance follow's resume cursor
			wantOutput:    "",
		},
		{
			name:          "supervisor filter matches a lifecycle line without container_name",
			line:          `{"time":"2026-05-16T01:03:38Z","message":"Actor started","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-1"}}`,
			targetActorID: "act-1",
			supervisor:    true,
			wantMatched:   true,
			wantTime:      "2026-05-16T01:03:38Z",
			wantOutput:    `{"time":"2026-05-16T01:03:38Z","message":"Actor started"}`,
		},
		{
			name:          "supervisor filter excludes a container line",
			line:          `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-1","ate.dev/container_name":"counter"}}`,
			targetActorID: "act-1",
			supervisor:    true,
			wantMatched:   false,
			wantTime:      "", // filtered out: only displayed lines may advance follow's resume cursor
			wantOutput:    "",
		},
		{
			name:          "default filter shows a container line",
			line:          `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-1","ate.dev/container_name":"counter"}}`,
			targetActorID: "act-1",
			wantMatched:   true,
			wantTime:      "2026-05-16T01:03:38Z",
			wantOutput:    `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi"}`,
		},
		{
			// Non-GCE shape: actor metadata under "labels", app labels under the GCE key.
			name:          "metadata under labels key, app data under GCE key",
			line:          `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi","logging.googleapis.com/labels":{"app":"my-app"},"labels":{"ate.dev/actor_id":"act-1","ate.dev/container_name":"counter"}}`,
			targetActorID: "act-1",
			container:     "counter",
			wantMatched:   true,
			wantTime:      "2026-05-16T01:03:38Z",
			wantOutput:    `{"time":"2026-05-16T01:03:38Z","level":"info","logging.googleapis.com/labels":{"app":"my-app"},"msg":"hi"}`,
		},
		{
			// Both keys carry metadata: GCE key wins; container read from its map (counter).
			name:          "both keys carry metadata, GCE key wins atomically",
			line:          `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-1","ate.dev/container_name":"counter"},"labels":{"ate.dev/actor_id":"act-1","ate.dev/container_name":"sidecar"}}`,
			targetActorID: "act-1",
			container:     "counter",
			wantMatched:   true,
			wantTime:      "2026-05-16T01:03:38Z",
			wantOutput:    `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi"}`,
		},
		{
			// Same line: the non-authoritative key's container (sidecar) is ignored.
			name:          "both keys carry metadata, non-GCE container is ignored",
			line:          `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-1","ate.dev/container_name":"counter"},"labels":{"ate.dev/actor_id":"act-1","ate.dev/container_name":"sidecar"}}`,
			targetActorID: "act-1",
			container:     "sidecar",
			wantMatched:   false,
			wantTime:      "",
			wantOutput:    "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logTime, matched := filterAndDisplayLogLine(tc.line, logLineFilter{actorID: tc.targetActorID, container: tc.container, supervisor: tc.supervisor}, &buf)

			if matched != tc.wantMatched {
				t.Errorf("got matched = %v, want %v", matched, tc.wantMatched)
			}

			if tc.wantTime != "" {
				parsedTime, err := time.Parse(time.RFC3339Nano, tc.wantTime)
				if err != nil {
					parsedTime, _ = time.Parse(time.RFC3339, tc.wantTime)
				}
				if !logTime.Equal(parsedTime) {
					t.Errorf("got logTime = %v, want %v", logTime, parsedTime)
				}
			} else {
				if !logTime.IsZero() {
					t.Errorf("got non-zero logTime = %v, want zero", logTime)
				}
			}

			gotOutput := strings.TrimSpace(buf.String())
			if gotOutput != tc.wantOutput {
				t.Errorf("got output %q, want %q", gotOutput, tc.wantOutput)
			}
		})
	}
}

type mockAteAPIClient struct {
	GetActorFunc func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.GetActorResponse, error)
	CloseCalls   int
}

func (m *mockAteAPIClient) GetActor(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.GetActorResponse, error) {
	if m.GetActorFunc != nil {
		return m.GetActorFunc(ctx, in, opts...)
	}
	return nil, fmt.Errorf("GetActorFunc not implemented")
}

func (m *mockAteAPIClient) Close() {
	m.CloseCalls++
}

type mockPodLogsStreamer struct {
	StreamLogsFunc func(ctx context.Context, namespace, podName string, opts *corev1.PodLogOptions) (io.ReadCloser, error)
}

func (m *mockPodLogsStreamer) StreamLogs(ctx context.Context, namespace, podName string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
	if m.StreamLogsFunc != nil {
		return m.StreamLogsFunc(ctx, namespace, podName, opts)
	}
	return nil, fmt.Errorf("StreamLogsFunc not implemented")
}

func TestLogsActorRunner_Run_OneShotSuccess(t *testing.T) {
	actorID := "act-123"
	podName := "pod-xyz"
	namespace := "ns-abc"

	mockAPI := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.GetActorResponse, error) {
			if in.GetActorRef().GetName() != actorID {
				return nil, fmt.Errorf("unexpected actor ID: %s", in.GetActorRef().GetName())
			}
			return &ateapipb.GetActorResponse{
				Actor: &ateapipb.Actor{
					ActorId:           actorID,
					AteomPodName:      podName,
					AteomPodNamespace: namespace,
					Status:            ateapipb.Actor_STATUS_RUNNING,
				},
			}, nil
		},
	}

	logLine := `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"Hello world","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-123"}}`
	mockStreamer := &mockPodLogsStreamer{
		StreamLogsFunc: func(ctx context.Context, ns, name string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
			if ns != namespace || name != podName {
				return nil, fmt.Errorf("unexpected pod %s/%s", ns, name)
			}
			if opts.Follow {
				return nil, fmt.Errorf("expected follow to be false in one-shot mode")
			}
			return io.NopCloser(strings.NewReader(logLine + "\n")), nil
		},
	}

	var stdout, stderr bytes.Buffer
	runner := &LogsActorRunner{
		apiClient: mockAPI,
		streamer:  mockStreamer,
		stdout:    &stdout,
		stderr:    &stderr,
		follow:    false,
	}

	err := runner.Run(context.Background(), actorID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mockAPI.CloseCalls != 1 {
		t.Errorf("expected Close to be called once, got %d", mockAPI.CloseCalls)
	}

	gotOutput := strings.TrimSpace(stdout.String())
	wantOutput := `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"Hello world"}`
	if gotOutput != wantOutput {
		t.Errorf("got stdout %q, want %q", gotOutput, wantOutput)
	}
}

func TestLogsActorRunner_Run_OneShot_ContainerFilter(t *testing.T) {
	actorID := "act-123"
	podName := "pod-xyz"
	namespace := "ns-abc"

	mockAPI := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.GetActorResponse, error) {
			return &ateapipb.GetActorResponse{
				Actor: &ateapipb.Actor{
					ActorId:           actorID,
					AteomPodName:      podName,
					AteomPodNamespace: namespace,
					Status:            ateapipb.Actor_STATUS_RUNNING,
				},
			}, nil
		},
	}

	counterLine := `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"from counter","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-123","ate.dev/container_name":"counter"}}`
	sidecarLine := `{"time":"2026-05-16T01:03:39Z","level":"info","msg":"from sidecar","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-123","ate.dev/container_name":"sidecar"}}`
	supervisorLine := `{"time":"2026-05-16T01:03:40Z","message":"Actor started","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-123"}}`

	mockStreamer := &mockPodLogsStreamer{
		StreamLogsFunc: func(ctx context.Context, ns, name string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(counterLine + "\n" + sidecarLine + "\n" + supervisorLine + "\n")), nil
		},
	}

	var stdout, stderr bytes.Buffer
	runner := &LogsActorRunner{
		apiClient: mockAPI,
		streamer:  mockStreamer,
		stdout:    &stdout,
		stderr:    &stderr,
		follow:    false,
		container: "counter",
	}

	if err := runner.Run(context.Background(), actorID); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	gotOutput := strings.TrimSpace(stdout.String())
	wantOutput := `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"from counter"}`
	if gotOutput != wantOutput {
		t.Errorf("got stdout %q, want only the counter line %q", gotOutput, wantOutput)
	}
}

func TestLogsActorRunner_Run_OneShot_SupervisorFilter(t *testing.T) {
	actorID := "act-123"
	podName := "pod-xyz"
	namespace := "ns-abc"

	mockAPI := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.GetActorResponse, error) {
			return &ateapipb.GetActorResponse{
				Actor: &ateapipb.Actor{
					ActorId:           actorID,
					AteomPodName:      podName,
					AteomPodNamespace: namespace,
					Status:            ateapipb.Actor_STATUS_RUNNING,
				},
			}, nil
		},
	}

	counterLine := `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"from counter","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-123","ate.dev/container_name":"counter"}}`
	supervisorLine := `{"time":"2026-05-16T01:03:40Z","message":"Actor started","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-123"}}`

	mockStreamer := &mockPodLogsStreamer{
		StreamLogsFunc: func(ctx context.Context, ns, name string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(counterLine + "\n" + supervisorLine + "\n")), nil
		},
	}

	var stdout, stderr bytes.Buffer
	runner := &LogsActorRunner{
		apiClient:  mockAPI,
		streamer:   mockStreamer,
		stdout:     &stdout,
		stderr:     &stderr,
		follow:     false,
		supervisor: true,
	}

	if err := runner.Run(context.Background(), actorID); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	gotOutput := strings.TrimSpace(stdout.String())
	wantOutput := `{"time":"2026-05-16T01:03:40Z","message":"Actor started"}`
	if gotOutput != wantOutput {
		t.Errorf("got stdout %q, want only the supervisor line %q", gotOutput, wantOutput)
	}
}

func TestValidateLogFilterFlags(t *testing.T) {
	tests := []struct {
		name       string
		container  string
		supervisor bool
		wantErr    bool
	}{
		{"neither", "", false, false},
		{"container only", "counter", false, false},
		{"supervisor only", "", true, false},
		{"both", "counter", true, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateLogFilterFlags(tc.container, tc.supervisor)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				if !strings.Contains(err.Error(), "mutually exclusive") {
					t.Errorf("error %q missing substring %q", err, "mutually exclusive")
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// TestLogsActorRunner_Run_RejectsContainerAndSupervisor verifies the runner
// enforces its own invariant when constructed directly (not only via the CLI),
// failing fast before any control-plane or pod calls.
func TestLogsActorRunner_Run_RejectsContainerAndSupervisor(t *testing.T) {
	mockAPI := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.GetActorResponse, error) {
			t.Error("GetActor should not be called when filters are mutually exclusive")
			return nil, fmt.Errorf("should not be called")
		},
	}
	mockStreamer := &mockPodLogsStreamer{
		StreamLogsFunc: func(ctx context.Context, ns, name string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
			t.Error("StreamLogs should not be called when filters are mutually exclusive")
			return nil, fmt.Errorf("should not be called")
		},
	}

	var stdout, stderr bytes.Buffer
	runner := &LogsActorRunner{
		apiClient:  mockAPI,
		streamer:   mockStreamer,
		stdout:     &stdout,
		stderr:     &stderr,
		container:  "counter",
		supervisor: true,
	}

	err := runner.Run(context.Background(), "act-1")
	if err == nil {
		t.Fatal("expected an error when both --container and --supervisor are set, got nil")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("unexpected error: %v (want substring %q)", err, "mutually exclusive")
	}
	if mockAPI.CloseCalls != 1 {
		t.Errorf("expected Close to be called once, got %d", mockAPI.CloseCalls)
	}
}

func TestLogsActorRunner_Run_OneShot_ActorNotRunning(t *testing.T) {
	actorID := "act-123"

	mockAPI := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.GetActorResponse, error) {
			return &ateapipb.GetActorResponse{
				Actor: &ateapipb.Actor{
					ActorId: actorID,
					Status:  ateapipb.Actor_STATUS_SUSPENDED, // not running
				},
			}, nil
		},
	}

	mockStreamer := &mockPodLogsStreamer{
		StreamLogsFunc: func(ctx context.Context, ns, name string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
			return nil, fmt.Errorf("StreamLogs should not be called")
		},
	}

	var stdout, stderr bytes.Buffer
	runner := &LogsActorRunner{
		apiClient: mockAPI,
		streamer:  mockStreamer,
		stdout:    &stdout,
		stderr:    &stderr,
		follow:    false,
	}

	err := runner.Run(context.Background(), actorID)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	wantErrMsg := "actor act-123 is not currently running on any worker pod"
	if !strings.Contains(err.Error(), wantErrMsg) {
		t.Errorf("unexpected error message: %v (expected substring %q)", err, wantErrMsg)
	}

	if mockAPI.CloseCalls != 1 {
		t.Errorf("expected Close to be called once, got %d", mockAPI.CloseCalls)
	}
}

func TestLogsActorRunner_Run_Follow_SuspendedToRunning(t *testing.T) {
	actorID := "act-123"
	podName := "pod-xyz"
	namespace := "ns-abc"

	var getActorCalls int
	var getActorMu sync.Mutex

	mockAPI := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.GetActorResponse, error) {
			getActorMu.Lock()
			defer getActorMu.Unlock()
			getActorCalls++

			if getActorCalls == 1 {
				// First call: suspended
				return &ateapipb.GetActorResponse{
					Actor: &ateapipb.Actor{
						ActorId: actorID,
						Status:  ateapipb.Actor_STATUS_SUSPENDED,
					},
				}, nil
			}

			// Subsequent calls: running
			return &ateapipb.GetActorResponse{
				Actor: &ateapipb.Actor{
					ActorId:           actorID,
					AteomPodName:      podName,
					AteomPodNamespace: namespace,
					Status:            ateapipb.Actor_STATUS_RUNNING,
				},
			}, nil
		},
	}

	logLine := `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"Follow hello","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-123"}}`

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockStreamer := &mockPodLogsStreamer{
		StreamLogsFunc: func(streamCtx context.Context, ns, name string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
			if ns != namespace || name != podName {
				return nil, fmt.Errorf("unexpected pod %s/%s", ns, name)
			}
			if !opts.Follow {
				return nil, fmt.Errorf("expected follow to be true in follow mode")
			}

			// Cancel main context soon to break the outer infinite loop
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()

			if opts.SinceTime != nil {
				return io.NopCloser(strings.NewReader("")), nil
			}

			return io.NopCloser(strings.NewReader(logLine + "\n")), nil
		},
	}

	var stdout, stderr bytes.Buffer
	runner := &LogsActorRunner{
		apiClient:         mockAPI,
		streamer:          mockStreamer,
		stdout:            &stdout,
		stderr:            &stderr,
		follow:            true,
		pollInterval:      1 * time.Millisecond,
		reconnectInterval: 1 * time.Millisecond,
		tickerInterval:    1 * time.Millisecond,
	}

	err := runner.Run(ctx, actorID)
	if err != nil && err != context.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}

	if mockAPI.CloseCalls != 1 {
		t.Errorf("expected Close to be called once, got %d", mockAPI.CloseCalls)
	}

	gotStderr := stderr.String()
	wantErrStderr := fmt.Sprintf("Actor is currently running on pod %s/%s\n", namespace, podName)
	if !strings.Contains(gotStderr, wantErrStderr) {
		t.Errorf("got stderr %q, want it to contain %q", gotStderr, wantErrStderr)
	}

	gotStdout := strings.TrimSpace(stdout.String())
	wantStdout := `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"Follow hello"}`
	if gotStdout != wantStdout {
		t.Errorf("got stdout %q, want %q", gotStdout, wantStdout)
	}
}

// TestLogsActorRunner_Run_Follow_ForeignActorLineDoesNotAdvanceCursor is a
// regression test: a worker pod's log stream can contain lines from a different
// actor that previously ran on it. Those foreign lines must never advance the
// follow resume cursor (SinceTime); otherwise a reconnect could skip the target
// actor's own logs.
func TestLogsActorRunner_Run_Follow_ForeignActorLineDoesNotAdvanceCursor(t *testing.T) {
	actorID := "act-123"
	podName := "pod-xyz"
	namespace := "ns-abc"

	mockAPI := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.GetActorResponse, error) {
			return &ateapipb.GetActorResponse{
				Actor: &ateapipb.Actor{
					ActorId:           actorID,
					AteomPodName:      podName,
					AteomPodNamespace: namespace,
					Status:            ateapipb.Actor_STATUS_RUNNING,
				},
			}, nil
		},
	}

	// A line belonging to a DIFFERENT actor that shares the worker pod's stream.
	// It carries a parseable timestamp but must not move the resume cursor.
	foreignLine := `{"time":"2026-05-16T01:03:38Z","message":"not mine","logging.googleapis.com/labels":{"ate.dev/actor_id":"other"}}`

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var streamCalls int
	var reconnectHadSinceTime bool
	secondCall := make(chan struct{})
	var once sync.Once

	mockStreamer := &mockPodLogsStreamer{
		StreamLogsFunc: func(streamCtx context.Context, ns, name string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
			mu.Lock()
			streamCalls++
			call := streamCalls
			mu.Unlock()

			if call == 1 {
				// First connection: emit one foreign line, then EOF to force a reconnect.
				return io.NopCloser(strings.NewReader(foreignLine + "\n")), nil
			}

			// Reconnection: record whether a resume cursor was carried over.
			mu.Lock()
			reconnectHadSinceTime = opts.SinceTime != nil
			mu.Unlock()
			once.Do(func() { close(secondCall) })
			<-streamCtx.Done()
			return nil, streamCtx.Err()
		},
	}

	runner := &LogsActorRunner{
		apiClient:         mockAPI,
		streamer:          mockStreamer,
		stdout:            &bytes.Buffer{},
		stderr:            &bytes.Buffer{},
		follow:            true,
		pollInterval:      1 * time.Millisecond,
		reconnectInterval: 1 * time.Millisecond,
		tickerInterval:    time.Hour, // keep the migration monitor out of the way
	}

	done := make(chan struct{})
	go func() {
		_ = runner.Run(ctx, actorID)
		close(done)
	}()

	select {
	case <-secondCall:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("reconnect (second StreamLogs call) never happened")
	}

	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if reconnectHadSinceTime {
		t.Error("reconnect carried a SinceTime cursor; a foreign actor's line must not advance the follow cursor")
	}
}

func TestLogsActorRunner_Run_Follow_NotFoundActor(t *testing.T) {
	actorID := "act-notfound"

	mockAPI := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.GetActorResponse, error) {
			return nil, status.Error(codes.NotFound, "actor not found")
		},
	}

	mockStreamer := &mockPodLogsStreamer{
		StreamLogsFunc: func(ctx context.Context, ns, name string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
			return nil, fmt.Errorf("StreamLogs should not be called")
		},
	}

	var stdout, stderr bytes.Buffer
	runner := &LogsActorRunner{
		apiClient:         mockAPI,
		streamer:          mockStreamer,
		stdout:            &stdout,
		stderr:            &stderr,
		follow:            true,
		pollInterval:      1 * time.Millisecond,
		reconnectInterval: 1 * time.Millisecond,
		tickerInterval:    1 * time.Millisecond,
	}

	err := runner.Run(context.Background(), actorID)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	wantErrMsg := "actor act-notfound not found"
	if !strings.Contains(err.Error(), wantErrMsg) {
		t.Errorf("unexpected error: %v (expected %q)", err, wantErrMsg)
	}
}

func TestLogsActorRunner_Run_Follow_ActorMigration(t *testing.T) {
	actorID := "act-migrate"

	var getActorCalls int
	var getActorMu sync.Mutex

	lineRead := make(chan struct{})

	mockAPI := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.GetActorResponse, error) {
			getActorMu.Lock()
			defer getActorMu.Unlock()
			getActorCalls++

			if getActorCalls == 1 {
				// 1. Initial call for stream 1: pod-1
				return &ateapipb.GetActorResponse{
					Actor: &ateapipb.Actor{
						ActorId:           actorID,
						AteomPodName:      "pod-1",
						AteomPodNamespace: "ns",
						Status:            ateapipb.Actor_STATUS_RUNNING,
					},
				}, nil
			}

			// 2. Poll call or reconnect call: pod-2
			// Wait until the first log line is actually read by the scanner to prevent premature cancellation
			select {
			case <-lineRead:
			case <-ctx.Done():
				return nil, ctx.Err()
			}

			return &ateapipb.GetActorResponse{
				Actor: &ateapipb.Actor{
					ActorId:           actorID,
					AteomPodName:      "pod-2",
					AteomPodNamespace: "ns",
					Status:            ateapipb.Actor_STATUS_RUNNING,
				},
			}, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var streamCalls int
	var streamMu sync.Mutex

	mockStreamer := &mockPodLogsStreamer{
		StreamLogsFunc: func(streamCtx context.Context, ns, name string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
			streamMu.Lock()
			defer streamMu.Unlock()
			streamCalls++

			if streamCalls == 1 {
				if name != "pod-1" {
					return nil, fmt.Errorf("expected pod-1, got %s", name)
				}
				// Return a read closer that blocks or keeps stream open
				// So the migration checking ticker gets triggered.
				pr, pw := io.Pipe()
				go func() {
					// write one line and then keep it open
					fmt.Fprintln(pw, `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"line 1 from pod-1","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-migrate"}}`)
					close(lineRead) // guaranteed to have been read because io.Pipe is unbuffered!
					// wait until context is cancelled
					<-streamCtx.Done()
					pw.Close()
				}()
				return pr, nil
			}

			// Reconnection to pod-2!
			if name != "pod-2" {
				return nil, fmt.Errorf("expected pod-2, got %s", name)
			}

			// Now we can cancel the main context to exit the follow loop
			cancel()

			return io.NopCloser(strings.NewReader(`{"time":"2026-05-16T01:03:39Z","level":"info","msg":"line 1 from pod-2","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-migrate"}}` + "\n")), nil
		},
	}

	var stdout, stderr bytes.Buffer
	runner := &LogsActorRunner{
		apiClient:         mockAPI,
		streamer:          mockStreamer,
		stdout:            &stdout,
		stderr:            &stderr,
		follow:            true,
		pollInterval:      1 * time.Millisecond,
		reconnectInterval: 1 * time.Millisecond,
		tickerInterval:    1 * time.Millisecond,
	}

	err := runner.Run(ctx, actorID)
	if err != nil && err != context.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}

	stdoutStr := stdout.String()
	if !strings.Contains(stdoutStr, "line 1 from pod-1") {
		t.Errorf("expected output to contain log from pod-1, got %q", stdoutStr)
	}
	if !strings.Contains(stdoutStr, "line 1 from pod-2") {
		t.Errorf("expected output to contain log from pod-2, got %q", stdoutStr)
	}
}

func TestLogsActorRunner_Run_Follow_ActorSuspendedMidStream(t *testing.T) {
	actorID := "act-suspended-mid"

	var getActorCalls int
	var getActorMu sync.Mutex

	lineRead := make(chan struct{})

	mockAPI := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.GetActorResponse, error) {
			getActorMu.Lock()
			defer getActorMu.Unlock()
			getActorCalls++

			// 1. Initial call: running on pod-1
			if getActorCalls == 1 {
				return &ateapipb.GetActorResponse{
					Actor: &ateapipb.Actor{
						ActorId:           actorID,
						AteomPodName:      "pod-1",
						AteomPodNamespace: "ns",
						Status:            ateapipb.Actor_STATUS_RUNNING,
					},
				}, nil
			}

			// 2. Poll call from background ticker: suspended
			if getActorCalls == 2 {
				// Wait until the scanner has actually read the initial log line
				select {
				case <-lineRead:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				return &ateapipb.GetActorResponse{
					Actor: &ateapipb.Actor{
						ActorId: actorID,
						Status:  ateapipb.Actor_STATUS_SUSPENDED,
					},
				}, nil
			}

			// 3. Loop reconnection call: suspended (still suspended, so it will wait)
			if getActorCalls == 3 {
				return &ateapipb.GetActorResponse{
					Actor: &ateapipb.Actor{
						ActorId: actorID,
						Status:  ateapipb.Actor_STATUS_SUSPENDED,
					},
				}, nil
			}

			// 4. Subsequent loop reconnection call: running again on pod-1
			return &ateapipb.GetActorResponse{
				Actor: &ateapipb.Actor{
					ActorId:           actorID,
					AteomPodName:      "pod-1",
					AteomPodNamespace: "ns",
					Status:            ateapipb.Actor_STATUS_RUNNING,
				},
			}, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var streamCalls int
	var streamMu sync.Mutex

	mockStreamer := &mockPodLogsStreamer{
		StreamLogsFunc: func(streamCtx context.Context, ns, name string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
			streamMu.Lock()
			defer streamMu.Unlock()
			streamCalls++

			if streamCalls == 1 {
				pr, pw := io.Pipe()
				go func() {
					fmt.Fprintln(pw, `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"before suspend","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-suspended-mid"}}`)
					close(lineRead) // guaranteed to have been read!
					<-streamCtx.Done()
					pw.Close()
				}()
				return pr, nil
			}

			// Second stream (after resuming): cancel context to stop test
			cancel()

			return io.NopCloser(strings.NewReader(`{"time":"2026-05-16T01:03:40Z","level":"info","msg":"after resume","logging.googleapis.com/labels":{"ate.dev/actor_id":"act-suspended-mid"}}` + "\n")), nil
		},
	}

	var stdout, stderr bytes.Buffer
	runner := &LogsActorRunner{
		apiClient:         mockAPI,
		streamer:          mockStreamer,
		stdout:            &stdout,
		stderr:            &stderr,
		follow:            true,
		pollInterval:      1 * time.Millisecond,
		reconnectInterval: 1 * time.Millisecond,
		tickerInterval:    1 * time.Millisecond,
	}

	err := runner.Run(ctx, actorID)
	if err != nil && err != context.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}

	stdoutStr := stdout.String()
	if !strings.Contains(stdoutStr, "before suspend") {
		t.Errorf("expected output to contain 'before suspend', got %q", stdoutStr)
	}
	if !strings.Contains(stdoutStr, "after resume") {
		t.Errorf("expected output to contain 'after resume', got %q", stdoutStr)
	}
}
