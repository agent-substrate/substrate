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
		name            string
		line            string
		targetAtespace  string
		targetActorName string
		wantMatched     bool
		wantTime        string
		wantOutput      string
	}{
		{
			name:            "matching actor, JSON log with RFC3339Nano",
			line:            `{"time":"2026-05-16T01:03:38.602878302Z","level":"info","msg":"Count","logging.googleapis.com/labels":{"ate.dev/actor_atespace":"space-1","ate.dev/actor_name":"act-1"}}`,
			targetAtespace:  "space-1",
			targetActorName: "act-1",
			wantMatched:     true,
			wantTime:        "2026-05-16T01:03:38.602878302Z",
			wantOutput:      `{"time":"2026-05-16T01:03:38.602878302Z","level":"info","msg":"Count"}`,
		},
		{
			name:            "matching actor, plain text log",
			line:            `{"time":"2026-05-16T01:03:38Z","message":"Hello","logging.googleapis.com/labels":{"ate.dev/actor_atespace":"space-1","ate.dev/actor_name":"act-1"}}`,
			targetAtespace:  "space-1",
			targetActorName: "act-1",
			wantMatched:     true,
			wantTime:        "2026-05-16T01:03:38Z",
			wantOutput:      `{"time":"2026-05-16T01:03:38Z","message":"Hello"}`,
		},
		{
			name:            "matching actor, JSON log with no timestamp fallback",
			line:            `{"level":"error","msg":"Failed","logging.googleapis.com/labels":{"ate.dev/actor_atespace":"space-1","ate.dev/actor_name":"act-1"}}`,
			targetAtespace:  "space-1",
			targetActorName: "act-1",
			wantMatched:     true,
			wantTime:        "",
			wantOutput:      `{"level":"error","msg":"Failed"}`,
		},
		{
			name:            "matching actor, fallback to standard labels key",
			line:            `{"time":"2026-05-16T01:03:38.602878302Z","level":"info","msg":"Count","labels":{"ate.dev/actor_atespace":"space-1","ate.dev/actor_name":"act-1"}}`,
			targetAtespace:  "space-1",
			targetActorName: "act-1",
			wantMatched:     true,
			wantTime:        "2026-05-16T01:03:38.602878302Z",
			wantOutput:      `{"time":"2026-05-16T01:03:38.602878302Z","level":"info","msg":"Count"}`,
		},
		{
			name:            "non-matching actor",
			line:            `{"time":"2026-05-16T01:03:38Z","message":"Hello world","logging.googleapis.com/labels":{"ate.dev/actor_atespace":"space-1","ate.dev/actor_name":"act-2"}}`,
			targetAtespace:  "space-1",
			targetActorName: "act-1",
			wantMatched:     false,
			wantTime:        "2026-05-16T01:03:38Z",
			wantOutput:      "",
		},
		{
			name:            "same actor name in a different atespace",
			line:            `{"time":"2026-05-16T01:03:38Z","message":"Hello world","logging.googleapis.com/labels":{"ate.dev/actor_atespace":"space-2","ate.dev/actor_name":"act-1"}}`,
			targetAtespace:  "space-1",
			targetActorName: "act-1",
			wantMatched:     false,
			wantTime:        "2026-05-16T01:03:38Z",
			wantOutput:      "",
		},
		{
			name:            "matching actor name without atespace label",
			line:            `{"time":"2026-05-16T01:03:38Z","message":"Hello world","logging.googleapis.com/labels":{"ate.dev/actor_name":"act-1"}}`,
			targetAtespace:  "space-1",
			targetActorName: "act-1",
			wantMatched:     false,
			wantTime:        "2026-05-16T01:03:38Z",
			wantOutput:      "",
		},
		{
			name:            "empty target atespace does not match empty atespace label",
			line:            `{"time":"2026-05-16T01:03:38Z","message":"Hello world","logging.googleapis.com/labels":{"ate.dev/actor_atespace":"","ate.dev/actor_name":"act-1"}}`,
			targetAtespace:  "",
			targetActorName: "act-1",
			wantMatched:     false,
			wantTime:        "2026-05-16T01:03:38Z",
			wantOutput:      "",
		},
		{
			name:            "empty target actor name does not match empty name label",
			line:            `{"time":"2026-05-16T01:03:38Z","message":"Hello world","logging.googleapis.com/labels":{"ate.dev/actor_atespace":"space-1","ate.dev/actor_name":""}}`,
			targetAtespace:  "space-1",
			targetActorName: "",
			wantMatched:     false,
			wantTime:        "2026-05-16T01:03:38Z",
			wantOutput:      "",
		},
		{
			name:            "invalid json line",
			line:            "not a json line",
			targetAtespace:  "space-1",
			targetActorName: "act-1",
			wantMatched:     false,
			wantTime:        "",
			wantOutput:      "",
		},
		{
			name:            "matching actor, flat JSON log",
			line:            `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"Hello","traceID":"abc-123","err":"timeout","logging.googleapis.com/labels":{"ate.dev/actor_atespace":"space-1","ate.dev/actor_name":"act-1"}}`,
			targetAtespace:  "space-1",
			targetActorName: "act-1",
			wantMatched:     true,
			wantTime:        "2026-05-16T01:03:38Z",
			wantOutput:      `{"time":"2026-05-16T01:03:38Z","err":"timeout","level":"info","msg":"Hello","traceID":"abc-123"}`,
		},
		{
			name:            "matching actor, severity and message keys",
			line:            `{"time":"2026-05-16T01:03:38Z","severity":"error","message":"Disk full","custom_tag":"alert","logging.googleapis.com/labels":{"ate.dev/actor_atespace":"space-1","ate.dev/actor_name":"act-1"}}`,
			targetAtespace:  "space-1",
			targetActorName: "act-1",
			wantMatched:     true,
			wantTime:        "2026-05-16T01:03:38Z",
			wantOutput:      `{"time":"2026-05-16T01:03:38Z","custom_tag":"alert","message":"Disk full","severity":"error"}`,
		},
		{
			name:            "matching actor, 2-field structured log without time",
			line:            `{"message":"login failed","code":401,"logging.googleapis.com/labels":{"ate.dev/actor_atespace":"space-1","ate.dev/actor_name":"act-1"}}`,
			targetAtespace:  "space-1",
			targetActorName: "act-1",
			wantMatched:     true,
			wantTime:        "",
			wantOutput:      `{"code":401,"message":"login failed"}`,
		},
		{
			name:            "matching actor, JSON log with custom application labels",
			line:            `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"Hello","logging.googleapis.com/labels":{"ate.dev/actor_atespace":"space-1","ate.dev/actor_name":"act-1","app":"my-app"}}`,
			targetAtespace:  "space-1",
			targetActorName: "act-1",
			wantMatched:     true,
			wantTime:        "2026-05-16T01:03:38Z",
			wantOutput:      `{"time":"2026-05-16T01:03:38Z","level":"info","logging.googleapis.com/labels":{"app":"my-app"},"msg":"Hello"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logTime, matched := filterAndDisplayLogLine(tc.line, tc.targetAtespace, tc.targetActorName, &buf)

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
	GetActorFunc func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error)
	CloseCalls   int
}

func (m *mockAteAPIClient) GetActor(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
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
	actorName := "act-123"
	podName := "pod-xyz"
	namespace := "ns-abc"

	mockAPI := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
			if in.GetActor().GetName() != actorName {
				return nil, fmt.Errorf("unexpected actor name: %s", in.GetActor().GetName())
			}
			return &ateapipb.Actor{
				Metadata:          &ateapipb.ResourceMetadata{Name: actorName},
				AteomPodName:      podName,
				AteomPodNamespace: namespace,
				Status:            ateapipb.Actor_STATUS_RUNNING,
			}, nil
		},
	}

	logLine := `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"Hello world","logging.googleapis.com/labels":{"ate.dev/actor_atespace":"space-1","ate.dev/actor_name":"act-123"}}`
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
		atespace:  "space-1",
		streamer:  mockStreamer,
		stdout:    &stdout,
		stderr:    &stderr,
		follow:    false,
	}

	err := runner.Run(context.Background(), actorName)
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

func TestLogsActorRunner_Run_OneShot_ActorNotRunning(t *testing.T) {
	actorName := "act-123"

	mockAPI := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
			return &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Name: actorName},
				Status:   ateapipb.Actor_STATUS_SUSPENDED, // not running
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
		atespace:  "space-1",
		streamer:  mockStreamer,
		stdout:    &stdout,
		stderr:    &stderr,
		follow:    false,
	}

	err := runner.Run(context.Background(), actorName)
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
	actorName := "act-123"
	podName := "pod-xyz"
	namespace := "ns-abc"

	var getActorCalls int
	var getActorMu sync.Mutex

	mockAPI := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
			getActorMu.Lock()
			defer getActorMu.Unlock()
			getActorCalls++

			if getActorCalls == 1 {
				// First call: suspended
				return &ateapipb.Actor{
					Metadata: &ateapipb.ResourceMetadata{Name: actorName},
					Status:   ateapipb.Actor_STATUS_SUSPENDED,
				}, nil
			}

			// Subsequent calls: running
			return &ateapipb.Actor{
				Metadata:          &ateapipb.ResourceMetadata{Name: actorName},
				AteomPodName:      podName,
				AteomPodNamespace: namespace,
				Status:            ateapipb.Actor_STATUS_RUNNING,
			}, nil
		},
	}

	logLine := `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"Follow hello","logging.googleapis.com/labels":{"ate.dev/actor_atespace":"space-1","ate.dev/actor_name":"act-123"}}`

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
		atespace:          "space-1",
		streamer:          mockStreamer,
		stdout:            &stdout,
		stderr:            &stderr,
		follow:            true,
		pollInterval:      1 * time.Millisecond,
		reconnectInterval: 1 * time.Millisecond,
		tickerInterval:    1 * time.Millisecond,
	}

	err := runner.Run(ctx, actorName)
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

func TestLogsActorRunner_Run_Follow_NotFoundActor(t *testing.T) {
	actorName := "act-notfound"

	mockAPI := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
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
		atespace:          "space-1",
		streamer:          mockStreamer,
		stdout:            &stdout,
		stderr:            &stderr,
		follow:            true,
		pollInterval:      1 * time.Millisecond,
		reconnectInterval: 1 * time.Millisecond,
		tickerInterval:    1 * time.Millisecond,
	}

	err := runner.Run(context.Background(), actorName)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	wantErrMsg := "actor act-notfound not found"
	if !strings.Contains(err.Error(), wantErrMsg) {
		t.Errorf("unexpected error: %v (expected %q)", err, wantErrMsg)
	}
}

func TestLogsActorRunner_Run_Follow_ActorMigration(t *testing.T) {
	actorName := "act-migrate"

	var getActorCalls int
	var getActorMu sync.Mutex

	lineRead := make(chan struct{})

	mockAPI := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
			getActorMu.Lock()
			defer getActorMu.Unlock()
			getActorCalls++

			if getActorCalls == 1 {
				// 1. Initial call for stream 1: pod-1
				return &ateapipb.Actor{
					Metadata:          &ateapipb.ResourceMetadata{Name: actorName},
					AteomPodName:      "pod-1",
					AteomPodNamespace: "ns",
					Status:            ateapipb.Actor_STATUS_RUNNING,
				}, nil
			}

			// 2. Poll call or reconnect call: pod-2
			// Wait until the first log line is actually read by the scanner to prevent premature cancellation
			select {
			case <-lineRead:
			case <-ctx.Done():
				return nil, ctx.Err()
			}

			return &ateapipb.Actor{
				Metadata:          &ateapipb.ResourceMetadata{Name: actorName},
				AteomPodName:      "pod-2",
				AteomPodNamespace: "ns",
				Status:            ateapipb.Actor_STATUS_RUNNING,
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
					fmt.Fprintln(pw, `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"line 1 from pod-1","logging.googleapis.com/labels":{"ate.dev/actor_atespace":"space-1","ate.dev/actor_name":"act-migrate"}}`)
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

			return io.NopCloser(strings.NewReader(`{"time":"2026-05-16T01:03:39Z","level":"info","msg":"line 1 from pod-2","logging.googleapis.com/labels":{"ate.dev/actor_atespace":"space-1","ate.dev/actor_name":"act-migrate"}}` + "\n")), nil
		},
	}

	var stdout, stderr bytes.Buffer
	runner := &LogsActorRunner{
		apiClient:         mockAPI,
		atespace:          "space-1",
		streamer:          mockStreamer,
		stdout:            &stdout,
		stderr:            &stderr,
		follow:            true,
		pollInterval:      1 * time.Millisecond,
		reconnectInterval: 1 * time.Millisecond,
		tickerInterval:    1 * time.Millisecond,
	}

	err := runner.Run(ctx, actorName)
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
	actorName := "act-suspended-mid"

	var getActorCalls int
	var getActorMu sync.Mutex

	lineRead := make(chan struct{})

	mockAPI := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
			getActorMu.Lock()
			defer getActorMu.Unlock()
			getActorCalls++

			// 1. Initial call: running on pod-1
			if getActorCalls == 1 {
				return &ateapipb.Actor{
					Metadata:          &ateapipb.ResourceMetadata{Name: actorName},
					AteomPodName:      "pod-1",
					AteomPodNamespace: "ns",
					Status:            ateapipb.Actor_STATUS_RUNNING,
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
				return &ateapipb.Actor{
					Metadata: &ateapipb.ResourceMetadata{Name: actorName},
					Status:   ateapipb.Actor_STATUS_SUSPENDED,
				}, nil
			}

			// 3. Loop reconnection call: suspended (still suspended, so it will wait)
			if getActorCalls == 3 {
				return &ateapipb.Actor{
					Metadata: &ateapipb.ResourceMetadata{Name: actorName},
					Status:   ateapipb.Actor_STATUS_SUSPENDED,
				}, nil
			}

			// 4. Subsequent loop reconnection call: running again on pod-1
			return &ateapipb.Actor{
				Metadata:          &ateapipb.ResourceMetadata{Name: actorName},
				AteomPodName:      "pod-1",
				AteomPodNamespace: "ns",
				Status:            ateapipb.Actor_STATUS_RUNNING,
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
					fmt.Fprintln(pw, `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"before suspend","logging.googleapis.com/labels":{"ate.dev/actor_atespace":"space-1","ate.dev/actor_name":"act-suspended-mid"}}`)
					close(lineRead) // guaranteed to have been read!
					<-streamCtx.Done()
					pw.Close()
				}()
				return pr, nil
			}

			// Second stream (after resuming): cancel context to stop test
			cancel()

			return io.NopCloser(strings.NewReader(`{"time":"2026-05-16T01:03:40Z","level":"info","msg":"after resume","logging.googleapis.com/labels":{"ate.dev/actor_atespace":"space-1","ate.dev/actor_name":"act-suspended-mid"}}` + "\n")), nil
		},
	}

	var stdout, stderr bytes.Buffer
	runner := &LogsActorRunner{
		apiClient:         mockAPI,
		atespace:          "space-1",
		streamer:          mockStreamer,
		stdout:            &stdout,
		stderr:            &stderr,
		follow:            true,
		pollInterval:      1 * time.Millisecond,
		reconnectInterval: 1 * time.Millisecond,
		tickerInterval:    1 * time.Millisecond,
	}

	err := runner.Run(ctx, actorName)
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
