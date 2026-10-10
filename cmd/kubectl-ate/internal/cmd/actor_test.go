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

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestActorCommandArgs(t *testing.T) {
	runCommandArgsTests(t, []commandArgsTest{
		{name: "list", command: getActorsCmd},
		{name: "get", command: getActorsCmd, args: []string{"actor-1"}},
		{name: "get multiple", command: getActorsCmd, args: []string{"actor-1", "actor-2"}},
	})
}

func TestBuildCreateActorRequest(t *testing.T) {
	tests := []struct {
		name        string
		templateRef string
		tag         string
		want        *ateapipb.Actor
		wantErr     bool
	}{
		{
			name:        "bare template name defaults to the actor's atespace",
			templateRef: "counter",
			want: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: "demo", Name: "my-counter"},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "demo", Name: "counter"},
			},
		},
		{
			name:        "bare tag name defaults to the actor's atespace",
			templateRef: "counter",
			tag:         "before-upgrade",
			want: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: "demo", Name: "my-counter"},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "demo", Name: "counter"},
				SourceTag:     &ateapipb.ObjectRef{Atespace: "demo", Name: "before-upgrade"},
			},
		},
		{
			name:        "qualified tag in a different atespace",
			templateRef: "counter",
			tag:         "other-atespace/before-upgrade",
			want: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: "demo", Name: "my-counter"},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "demo", Name: "counter"},
				SourceTag:     &ateapipb.ObjectRef{Atespace: "other-atespace", Name: "before-upgrade"},
			},
		},
		{
			name:        "qualified template in a different atespace",
			templateRef: "shared-templates/counter",
			want: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: "demo", Name: "my-counter"},
				ActorTemplate: &ateapipb.ObjectRef{Atespace: "shared-templates", Name: "counter"},
			},
		},
		{name: "malformed template ref", templateRef: "a/b/c", wantErr: true},
		{name: "malformed tag", templateRef: "counter", tag: "a/b/c", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := buildCreateActorRequest("my-counter", "demo", test.templateRef, test.tag)
			if (err != nil) != test.wantErr {
				t.Fatalf("buildCreateActorRequest error = %v, wantErr %t", err, test.wantErr)
			}
			if test.wantErr {
				return
			}
			want := &ateapipb.CreateActorRequest{Actor: test.want}
			if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
				t.Errorf("request mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestFilterAndDisplayLogLine(t *testing.T) {
	tests := []struct {
		name        string
		line        string
		target      resources.ActorRef
		source      string
		container   string
		wantMatched bool
		wantOutput  string
	}{
		{
			name:        "matching actor, JSON log with RFC3339Nano",
			line:        `{"time":"2026-05-16T01:03:38.602878302Z","level":"info","msg":"Count","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-1"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			wantMatched: true,
			wantOutput:  `{"time":"2026-05-16T01:03:38.602878302Z","level":"info","msg":"Count"}`,
		},
		{
			name:        "matching actor, plain text log",
			line:        `{"time":"2026-05-16T01:03:38Z","message":"Hello","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-1"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			wantMatched: true,
			wantOutput:  `{"time":"2026-05-16T01:03:38Z","message":"Hello"}`,
		},
		{
			name:        "matching actor, JSON log with no timestamp fallback",
			line:        `{"level":"error","msg":"Failed","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-1"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			wantMatched: true,
			wantOutput:  `{"level":"error","msg":"Failed"}`,
		},
		{
			name:        "matching actor, fallback to standard labels key",
			line:        `{"time":"2026-05-16T01:03:38.602878302Z","level":"info","msg":"Count","labels":{"ate.atespace":"space-1","ate.actor.name":"act-1"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			wantMatched: true,
			wantOutput:  `{"time":"2026-05-16T01:03:38.602878302Z","level":"info","msg":"Count"}`,
		},
		{
			name:        "non-matching actor",
			line:        `{"time":"2026-05-16T01:03:38Z","message":"Hello world","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-2"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			wantMatched: false,
			wantOutput:  "",
		},
		{
			name:        "same actor name in a different atespace",
			line:        `{"time":"2026-05-16T01:03:38Z","message":"Hello world","logging.googleapis.com/labels":{"ate.atespace":"space-2","ate.actor.name":"act-1"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			wantMatched: false,
			wantOutput:  "",
		},
		{
			name:        "matching actor name without atespace label",
			line:        `{"time":"2026-05-16T01:03:38Z","message":"Hello world","logging.googleapis.com/labels":{"ate.actor.name":"act-1"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			wantMatched: false,
			wantOutput:  "",
		},
		{
			name:        "empty target atespace does not match empty atespace label",
			line:        `{"time":"2026-05-16T01:03:38Z","message":"Hello world","logging.googleapis.com/labels":{"ate.atespace":"","ate.actor.name":"act-1"}}`,
			target:      resources.ActorRef{Atespace: "", Name: "act-1"},
			wantMatched: false,
			wantOutput:  "",
		},
		{
			name:        "empty target actor name does not match empty name label",
			line:        `{"time":"2026-05-16T01:03:38Z","message":"Hello world","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":""}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: ""},
			wantMatched: false,
			wantOutput:  "",
		},
		{
			name:        "invalid json line",
			line:        "not a json line",
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			wantMatched: false,
			wantOutput:  "",
		},
		{
			name:        "matching actor, flat JSON log",
			line:        `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"Hello","traceID":"abc-123","err":"timeout","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-1"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			wantMatched: true,
			wantOutput:  `{"time":"2026-05-16T01:03:38Z","err":"timeout","level":"info","msg":"Hello","traceID":"abc-123"}`,
		},
		{
			name:        "matching actor, severity and message keys",
			line:        `{"time":"2026-05-16T01:03:38Z","severity":"error","message":"Disk full","custom_tag":"alert","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-1"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			wantMatched: true,
			wantOutput:  `{"time":"2026-05-16T01:03:38Z","custom_tag":"alert","message":"Disk full","severity":"error"}`,
		},
		{
			name:        "matching actor, 2-field structured log without time",
			line:        `{"message":"login failed","code":401,"logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-1"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			wantMatched: true,
			wantOutput:  `{"code":401,"message":"login failed"}`,
		},
		{
			name:        "matching actor, JSON log with custom application labels",
			line:        `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"Hello","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-1","app":"my-app"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			wantMatched: true,
			wantOutput:  `{"time":"2026-05-16T01:03:38Z","level":"info","logging.googleapis.com/labels":{"app":"my-app"},"msg":"Hello"}`,
		},
		{
			// ateom drops these at the producer; the CLI strips the whole reserved
			// namespace too, so a label that reached the stream some other way is
			// never printed as platform attribution.
			name:        "matching actor, label in substrate's reserved namespace is stripped",
			line:        `{"time":"2026-05-16T01:03:38Z","msg":"Hello","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-1","ate.tenant":"forged","app":"my-app"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			wantMatched: true,
			wantOutput:  `{"time":"2026-05-16T01:03:38Z","logging.googleapis.com/labels":{"app":"my-app"},"msg":"Hello"}`,
		},
		{
			name:        "no filter shows a container line",
			line:        `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-1","ate.actor.container.name":"counter"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			wantMatched: true,
			wantOutput:  `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi"}`,
		},
		{
			name:        "container filter matches the named container",
			line:        `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-1","ate.actor.container.name":"counter"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			container:   "counter",
			wantMatched: true,
			wantOutput:  `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi"}`,
		},
		{
			name:        "no filter excludes pause container diagnostics",
			line:        `{"time":"2026-05-16T01:03:38Z","message":"Args: [runsc create _pause]","labels":{"ate.atespace":"space-1","ate.actor.name":"act-1","ate.actor.container.name":"_pause"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			wantMatched: false,
		},
		{
			name:        "explicit pause filter shows runsc diagnostics",
			line:        `{"time":"2026-05-16T01:03:38Z","message":"Args: [runsc create _pause]","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-1","ate.actor.container.name":"_pause"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			container:   "_pause",
			wantMatched: true,
			wantOutput:  `{"time":"2026-05-16T01:03:38Z","message":"Args: [runsc create _pause]"}`,
		},
		{
			name:        "application filter excludes pause container diagnostics",
			line:        `{"time":"2026-05-16T01:03:38Z","message":"Args: [runsc create _pause]","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-1","ate.actor.container.name":"_pause"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			container:   "counter",
			wantMatched: false,
		},
		{
			name:        "no filter shows lifecycle events",
			line:        `{"time":"2026-05-16T01:03:38Z","message":"Actor started","labels":{"ate.atespace":"space-1","ate.actor.name":"act-1"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			wantMatched: true,
			wantOutput:  `{"time":"2026-05-16T01:03:38Z","message":"Actor started"}`,
		},
		{
			name:        "container filter excludes a different container",
			line:        `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-1","ate.actor.container.name":"sidecar"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			container:   "counter",
			wantMatched: false,
			wantOutput:  "",
		},
		{
			name:        "container filter excludes a lifecycle event",
			line:        `{"time":"2026-05-16T01:03:38Z","message":"Actor started","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-1"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			container:   "counter",
			wantMatched: false,
			wantOutput:  "",
		},
		{
			name:        "source=all shows a lifecycle event",
			line:        `{"time":"2026-05-16T01:03:38Z","message":"Actor started","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-1"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			source:      "all",
			wantMatched: true,
			wantOutput:  `{"time":"2026-05-16T01:03:38Z","message":"Actor started"}`,
		},
		{
			name:        "source=lifecycle shows a lifecycle event",
			line:        `{"time":"2026-05-16T01:03:38Z","message":"Actor checkpointing","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-1"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			source:      "lifecycle",
			wantMatched: true,
			wantOutput:  `{"time":"2026-05-16T01:03:38Z","message":"Actor checkpointing"}`,
		},
		{
			name:        "source=lifecycle excludes a container line",
			line:        `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-1","ate.actor.container.name":"counter"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			source:      "lifecycle",
			wantMatched: false,
			wantOutput:  "",
		},
		{
			name:        "source=lifecycle excludes another actor's lifecycle event",
			line:        `{"time":"2026-05-16T01:03:38Z","message":"Actor started","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-2"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			source:      "lifecycle",
			wantMatched: false,
			wantOutput:  "",
		},
		{
			name:        "source=containers shows any container's line",
			line:        `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-1","ate.actor.container.name":"sidecar"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			source:      "containers",
			wantMatched: true,
			wantOutput:  `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi"}`,
		},
		{
			name:        "source=containers excludes a lifecycle event",
			line:        `{"time":"2026-05-16T01:03:38Z","message":"Actor restored","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-1"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			source:      "containers",
			wantMatched: false,
			wantOutput:  "",
		},
		{
			name:        "source=containers with container filter keeps only the named container",
			line:        `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"hi","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-1","ate.actor.container.name":"sidecar"}}`,
			target:      resources.ActorRef{Atespace: "space-1", Name: "act-1"},
			source:      "containers",
			container:   "counter",
			wantMatched: false,
			wantOutput:  "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			matched := filterAndDisplayLogLine(tc.line, logLineFilter{target: tc.target, source: logSource(tc.source), container: tc.container}, &buf)

			if matched != tc.wantMatched {
				t.Errorf("got matched = %v, want %v", matched, tc.wantMatched)
			}

			gotOutput := strings.TrimSpace(buf.String())
			if gotOutput != tc.wantOutput {
				t.Errorf("got output %q, want %q", gotOutput, tc.wantOutput)
			}
		})
	}
}

func TestNewLogLineFilter(t *testing.T) {
	target := resources.ActorRef{Atespace: "space-1", Name: "act-1"}

	tests := []struct {
		name      string
		source    string
		container string
		wantErr   string
	}{
		{name: "explicit empty source", source: "", wantErr: `invalid --source ""`},
		{name: "all", source: "all"},
		{name: "containers", source: "containers"},
		{name: "lifecycle", source: "lifecycle"},
		{name: "all with container", source: "all", container: "counter"},
		{name: "containers with container", source: "containers", container: "counter"},
		{name: "lifecycle with container", source: "lifecycle", container: "counter", wantErr: "--container cannot be combined with --source=lifecycle"},
		{name: "unknown source", source: "supervisor", wantErr: `invalid --source "supervisor"`},
		{name: "source is case-sensitive", source: "Lifecycle", wantErr: `invalid --source "Lifecycle"`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := newLogLineFilter(target, tc.source, tc.container)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("got err %v, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			want := logLineFilter{target: target, source: logSource(tc.source), container: tc.container}
			if got != want {
				t.Errorf("got %+v, want %+v", got, want)
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
				Metadata: &ateapipb.ResourceMetadata{Name: actorName},
				Status: &ateapipb.ActorStatus{
					State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
					WorkerAssignment: &ateapipb.WorkerAssignment{
						WorkerPod:       podName,
						WorkerNamespace: namespace,
					},
				},
			}, nil
		},
	}

	logLine := `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"Hello world","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-123"}}`
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
		actorRef:  resources.ActorRef{Atespace: "space-1", Name: actorName},
		filter:    logLineFilter{target: resources.ActorRef{Atespace: "space-1", Name: actorName}},
		streamer:  mockStreamer,
		stdout:    &stdout,
		stderr:    &stderr,
		follow:    false,
	}

	err := runner.Run(context.Background())
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

// TestLogsActorRunner_Run_OneShot_SourceFilter exercises the --source and
// --container selection against one stream that interleaves two containers
// and lifecycle events.
func TestLogsActorRunner_Run_OneShot_SourceFilter(t *testing.T) {
	actorName := "act-123"

	startedLine := `{"time":"2026-05-16T01:03:37Z","message":"Actor started","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-123"}}`
	counterLine := `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"from counter","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-123","ate.actor.container.name":"counter"}}`
	sidecarLine := `{"time":"2026-05-16T01:03:39Z","level":"info","msg":"from sidecar","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-123","ate.actor.container.name":"sidecar"}}`
	pauseLine := `{"time":"2026-05-16T01:03:39Z","message":"Args: [runsc create _pause]","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-123","ate.actor.container.name":"_pause"}}`
	checkpointingLine := `{"time":"2026-05-16T01:03:40Z","message":"Actor checkpointing","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-123"}}`
	stream := strings.Join([]string{startedLine, counterLine, sidecarLine, pauseLine, checkpointingLine}, "\n") + "\n"

	tests := []struct {
		name       string
		source     string
		container  string
		wantOutput []string
	}{
		{
			name:       "no filter shows application and lifecycle logs",
			wantOutput: []string{"Actor started", "from counter", "from sidecar", "Actor checkpointing"},
		},
		{
			name:       "source=all excludes pause diagnostics",
			source:     "all",
			wantOutput: []string{"Actor started", "from counter", "from sidecar", "Actor checkpointing"},
		},
		{
			name:       "container filter",
			container:  "counter",
			wantOutput: []string{"from counter"},
		},
		{
			name:       "explicit pause container filter",
			container:  "_pause",
			wantOutput: []string{"Args: [runsc create _pause]"},
		},
		{
			name:       "source=containers with explicit pause filter",
			source:     "containers",
			container:  "_pause",
			wantOutput: []string{"Args: [runsc create _pause]"},
		},
		{
			name:       "source=containers drops lifecycle events",
			source:     "containers",
			wantOutput: []string{"from counter", "from sidecar"},
		},
		{
			name:       "source=containers with container filter",
			source:     "containers",
			container:  "sidecar",
			wantOutput: []string{"from sidecar"},
		},
		{
			name:       "source=lifecycle keeps only lifecycle events",
			source:     "lifecycle",
			wantOutput: []string{"Actor started", "Actor checkpointing"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockAPI := &mockAteAPIClient{
				GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
					return &ateapipb.Actor{
						Metadata: &ateapipb.ResourceMetadata{Name: actorName},
						Status: &ateapipb.ActorStatus{
							State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
							WorkerAssignment: &ateapipb.WorkerAssignment{
								WorkerPod:       "pod-xyz",
								WorkerNamespace: "ns-abc",
							},
						},
					}, nil
				},
			}
			streamCalls := 0
			mockStreamer := &mockPodLogsStreamer{
				StreamLogsFunc: func(ctx context.Context, ns, name string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
					streamCalls++
					return io.NopCloser(strings.NewReader(stream)), nil
				},
			}

			var stdout, stderr bytes.Buffer
			runner := &LogsActorRunner{
				apiClient: mockAPI,
				actorRef:  resources.ActorRef{Atespace: "space-1", Name: actorName},
				streamer:  mockStreamer,
				stdout:    &stdout,
				stderr:    &stderr,
				filter:    logLineFilter{target: resources.ActorRef{Atespace: "space-1", Name: actorName}, source: logSource(tc.source), container: tc.container},
			}

			err := runner.Run(context.Background())
			if mockAPI.CloseCalls != 1 {
				t.Errorf("api client Close called %d times, want 1", mockAPI.CloseCalls)
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if streamCalls != 1 {
				t.Errorf("streamed %d times, want 1", streamCalls)
			}

			var gotLines []string
			if got := strings.TrimSpace(stdout.String()); got != "" {
				gotLines = strings.Split(got, "\n")
			}
			if len(gotLines) != len(tc.wantOutput) {
				t.Fatalf("got %d lines %q, want %d", len(gotLines), gotLines, len(tc.wantOutput))
			}
			for i, want := range tc.wantOutput {
				if !strings.Contains(gotLines[i], want) {
					t.Errorf("line %d = %q, want it to contain %q", i, gotLines[i], want)
				}
			}
		})
	}
}

func TestLogsActorRunner_Run_OneShot_ActorNotRunning(t *testing.T) {
	actorName := "act-123"

	mockAPI := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
			return &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Name: actorName},
				Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED}, // not running
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
		actorRef:  resources.ActorRef{Atespace: "space-1", Name: actorName},
		filter:    logLineFilter{target: resources.ActorRef{Atespace: "space-1", Name: actorName}},
		streamer:  mockStreamer,
		stdout:    &stdout,
		stderr:    &stderr,
		follow:    false,
	}

	err := runner.Run(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	wantErrMsg := "actor space-1/act-123 is not currently running on any worker pod"
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
					Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
				}, nil
			}

			// Subsequent calls: running
			return &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Name: actorName},
				Status: &ateapipb.ActorStatus{
					State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
					WorkerAssignment: &ateapipb.WorkerAssignment{
						WorkerPod:       podName,
						WorkerNamespace: namespace,
					},
				},
			}, nil
		},
	}

	// As the kubelet streams it with Timestamps set.
	logLine := `2026-05-16T01:03:38.500000000Z {"time":"2026-05-16T01:03:38Z","level":"info","msg":"Follow hello","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-123"}}`

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockStreamer := &mockPodLogsStreamer{
		StreamLogsFunc: func(streamCtx context.Context, ns, name string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
			if ns != namespace || name != podName {
				return nil, fmt.Errorf("unexpected pod %s/%s", ns, name)
			}
			if !opts.Follow || !opts.Timestamps {
				return nil, fmt.Errorf("expected follow and timestamps to be set in follow mode")
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
		actorRef:          resources.ActorRef{Atespace: "space-1", Name: actorName},
		filter:            logLineFilter{target: resources.ActorRef{Atespace: "space-1", Name: actorName}},
		streamer:          mockStreamer,
		stdout:            &stdout,
		stderr:            &stderr,
		follow:            true,
		pollInterval:      1 * time.Millisecond,
		reconnectInterval: 1 * time.Millisecond,
		tickerInterval:    1 * time.Millisecond,
	}

	err := runner.Run(ctx)
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
		actorRef:          resources.ActorRef{Atespace: "space-1", Name: actorName},
		filter:            logLineFilter{target: resources.ActorRef{Atespace: "space-1", Name: actorName}},
		streamer:          mockStreamer,
		stdout:            &stdout,
		stderr:            &stderr,
		follow:            true,
		pollInterval:      1 * time.Millisecond,
		reconnectInterval: 1 * time.Millisecond,
		tickerInterval:    1 * time.Millisecond,
	}

	err := runner.Run(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	wantErrMsg := "actor space-1/act-notfound not found"
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
					Metadata: &ateapipb.ResourceMetadata{Name: actorName},
					Status: &ateapipb.ActorStatus{
						State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
						WorkerAssignment: &ateapipb.WorkerAssignment{
							WorkerPod:       "pod-1",
							WorkerNamespace: "ns",
						},
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

			return &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Name: actorName},
				Status: &ateapipb.ActorStatus{
					State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
					WorkerAssignment: &ateapipb.WorkerAssignment{
						WorkerPod:       "pod-2",
						WorkerNamespace: "ns",
					},
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
					fmt.Fprintln(pw, `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"line 1 from pod-1","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-migrate"}}`)
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

			return io.NopCloser(strings.NewReader(`{"time":"2026-05-16T01:03:39Z","level":"info","msg":"line 1 from pod-2","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-migrate"}}` + "\n")), nil
		},
	}

	var stdout, stderr bytes.Buffer
	runner := &LogsActorRunner{
		apiClient:         mockAPI,
		actorRef:          resources.ActorRef{Atespace: "space-1", Name: actorName},
		filter:            logLineFilter{target: resources.ActorRef{Atespace: "space-1", Name: actorName}},
		streamer:          mockStreamer,
		stdout:            &stdout,
		stderr:            &stderr,
		follow:            true,
		pollInterval:      1 * time.Millisecond,
		reconnectInterval: 1 * time.Millisecond,
		tickerInterval:    1 * time.Millisecond,
	}

	err := runner.Run(ctx)
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
					Metadata: &ateapipb.ResourceMetadata{Name: actorName},
					Status: &ateapipb.ActorStatus{
						State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
						WorkerAssignment: &ateapipb.WorkerAssignment{
							WorkerPod:       "pod-1",
							WorkerNamespace: "ns",
						},
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
				return &ateapipb.Actor{
					Metadata: &ateapipb.ResourceMetadata{Name: actorName},
					Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
				}, nil
			}

			// 3. Loop reconnection call: suspended (still suspended, so it will wait)
			if getActorCalls == 3 {
				return &ateapipb.Actor{
					Metadata: &ateapipb.ResourceMetadata{Name: actorName},
					Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
				}, nil
			}

			// 4. Subsequent loop reconnection call: running again on pod-1
			return &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Name: actorName},
				Status: &ateapipb.ActorStatus{
					State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
					WorkerAssignment: &ateapipb.WorkerAssignment{
						WorkerPod:       "pod-1",
						WorkerNamespace: "ns",
					},
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
					fmt.Fprintln(pw, `{"time":"2026-05-16T01:03:38Z","level":"info","msg":"before suspend","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-suspended-mid"}}`)
					close(lineRead) // guaranteed to have been read!
					<-streamCtx.Done()
					pw.Close()
				}()
				return pr, nil
			}

			// Second stream (after resuming): cancel context to stop test
			cancel()

			return io.NopCloser(strings.NewReader(`{"time":"2026-05-16T01:03:40Z","level":"info","msg":"after resume","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-suspended-mid"}}` + "\n")), nil
		},
	}

	var stdout, stderr bytes.Buffer
	runner := &LogsActorRunner{
		apiClient:         mockAPI,
		actorRef:          resources.ActorRef{Atespace: "space-1", Name: actorName},
		filter:            logLineFilter{target: resources.ActorRef{Atespace: "space-1", Name: actorName}},
		streamer:          mockStreamer,
		stdout:            &stdout,
		stderr:            &stderr,
		follow:            true,
		pollInterval:      1 * time.Millisecond,
		reconnectInterval: 1 * time.Millisecond,
		tickerInterval:    1 * time.Millisecond,
	}

	err := runner.Run(ctx)
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

// The follow resume cursor is the kubelet's timestamp on the last line read,
// displayed or not. Not the last displayed line: under a sparse filter such
// as --source=lifecycle a reconnect would replay everything since the last
// match. Not the line's own time either: an actor's record keeps a time the
// actor wrote, which can run ahead of the node's clock and would make a
// reconnect skip lines. On reconnect, lines up to the cursor are dropped,
// since SinceTime only has second precision.
func TestLogsActorRunner_Run_Follow_CursorIsKubeletTimestampOfLastLineRead(t *testing.T) {
	actorName := "act-123"

	mockAPI := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
			return &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Name: actorName},
				Status: &ateapipb.ActorStatus{
					State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
					WorkerAssignment: &ateapipb.WorkerAssignment{
						WorkerPod:       "pod-xyz",
						WorkerNamespace: "ns-abc",
					},
				},
			}, nil
		},
	}

	// Lines as the kubelet streams them with Timestamps set. Under
	// --source=lifecycle only the first is displayed. The container line's
	// own time is far ahead of the node's clock.
	lifecycleLine := `2026-05-16T01:03:38.100000000Z {"time":"2026-05-16T01:03:38Z","message":"Actor started","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-123"}}`
	foreignLine := `2026-05-16T01:03:39.200000000Z {"time":"2026-05-16T01:03:39Z","message":"not mine","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"other"}}`
	counterLine := `2026-05-16T01:03:40.300000000Z {"time":"2099-01-01T00:00:00Z","level":"info","msg":"from counter","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-123","ate.actor.container.name":"counter"}}`
	// After the reconnect the kubelet re-sends the cursor's second: the
	// counter line again, then a new lifecycle event in the same second.
	laterLine := `2026-05-16T01:03:40.900000000Z {"time":"2026-05-16T01:03:40Z","message":"Actor checkpointing","logging.googleapis.com/labels":{"ate.atespace":"space-1","ate.actor.name":"act-123"}}`
	wantSince := time.Date(2026, 5, 16, 1, 3, 40, 300000000, time.UTC)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var streamCalls int
	var reconnectSince *metav1.Time
	var reconnectTimestamps bool
	thirdCall := make(chan struct{})

	mockStreamer := &mockPodLogsStreamer{
		StreamLogsFunc: func(streamCtx context.Context, ns, name string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
			mu.Lock()
			defer mu.Unlock()
			streamCalls++
			switch streamCalls {
			case 1:
				return io.NopCloser(strings.NewReader(lifecycleLine + "\n" + foreignLine + "\n" + counterLine + "\n")), nil
			case 2:
				reconnectSince = opts.SinceTime
				reconnectTimestamps = opts.Timestamps
				return io.NopCloser(strings.NewReader(counterLine + "\n" + laterLine + "\n")), nil
			case 3:
				close(thirdCall)
			}
			return io.NopCloser(strings.NewReader("")), nil
		},
	}

	var stdout bytes.Buffer
	runner := &LogsActorRunner{
		apiClient:         mockAPI,
		actorRef:          resources.ActorRef{Atespace: "space-1", Name: actorName},
		streamer:          mockStreamer,
		stdout:            &stdout,
		stderr:            &bytes.Buffer{},
		follow:            true,
		filter:            logLineFilter{target: resources.ActorRef{Atespace: "space-1", Name: actorName}, source: logSourceLifecycle},
		pollInterval:      1 * time.Millisecond,
		reconnectInterval: 1 * time.Millisecond,
		tickerInterval:    time.Hour, // keep the migration monitor out of the way
	}

	done := make(chan struct{})
	go func() {
		_ = runner.Run(ctx)
		close(done)
	}()

	select {
	case <-thirdCall:
	case <-time.After(2 * time.Second):
		cancel()
		<-done
		t.Fatal("the third StreamLogs call never happened")
	}

	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if !reconnectTimestamps {
		t.Error("follow did not ask the kubelet for timestamps")
	}
	if reconnectSince == nil {
		t.Fatal("reconnect carried no SinceTime cursor")
	}
	if !reconnectSince.Time.Equal(wantSince) {
		t.Errorf("reconnect SinceTime = %v, want %v: the kubelet's timestamp on the last line read, not the last line displayed and not the line's own time", reconnectSince.Time, wantSince)
	}
	want := "{\"time\":\"2026-05-16T01:03:38Z\",\"message\":\"Actor started\"}\n" +
		"{\"time\":\"2026-05-16T01:03:40Z\",\"message\":\"Actor checkpointing\"}\n"
	if got := stdout.String(); got != want {
		t.Errorf("output =\n%s\nwant\n%s", got, want)
	}
}

func TestSplitKubeletTimestamp(t *testing.T) {
	tests := []struct {
		name     string
		line     string
		wantOK   bool
		wantTime time.Time
		wantRest string
	}{
		{
			name:     "kubelet timestamp prefix",
			line:     `2026-05-16T01:03:40.300000000Z {"msg":"hi"}`,
			wantOK:   true,
			wantTime: time.Date(2026, 5, 16, 1, 3, 40, 300000000, time.UTC),
			wantRest: `{"msg":"hi"}`,
		},
		{
			name:     "no prefix",
			line:     `{"msg":"hi there"}`,
			wantOK:   false,
			wantRest: `{"msg":"hi there"}`,
		},
		{
			name:     "no space at all",
			line:     `plain`,
			wantOK:   false,
			wantRest: `plain`,
		},
		{
			name:     "a space but not a timestamp",
			line:     `not-a-time {"msg":"hi"}`,
			wantOK:   false,
			wantRest: `not-a-time {"msg":"hi"}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stamp, rest, ok := splitKubeletTimestamp(tc.line)
			if ok != tc.wantOK || rest != tc.wantRest || !stamp.Equal(tc.wantTime) {
				t.Errorf("splitKubeletTimestamp(%q) = %v, %q, %v; want %v, %q, %v", tc.line, stamp, rest, ok, tc.wantTime, tc.wantRest, tc.wantOK)
			}
		})
	}
}
