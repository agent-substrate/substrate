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
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
)

func TestWaitActorCommandContract(t *testing.T) {
	for _, test := range []struct {
		name    string
		args    []string
		wantErr bool
	}{
		{name: "one actor", args: []string{"actor-1"}},
		{name: "missing actor", wantErr: true},
		{name: "multiple actors", args: []string{"actor-1", "actor-2"}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := waitActorCmd.Args(waitActorCmd, test.args)
			if (err != nil) != test.wantErr {
				t.Fatalf("Args(%q) error = %v, wantErr %t", test.args, err, test.wantErr)
			}
		})
	}

	for _, name := range []string{"atespace", "for", "timeout"} {
		if flag := waitActorCmd.Flags().Lookup(name); flag == nil {
			t.Errorf("--%s flag is not registered", name)
		}
	}
	if flag := waitActorCmd.Flags().ShorthandLookup("a"); flag == nil || flag.Name != "atespace" {
		t.Errorf("-a flag = %v, want --atespace", flag)
	}
}

func TestParseWaitActorStatus(t *testing.T) {
	tests := []struct {
		condition string
		want      ateapipb.Actor_Status
		wantErr   bool
	}{
		{condition: "status=running", want: ateapipb.Actor_STATUS_RUNNING},
		{condition: "STATUS=STATUS_SUSPENDED", want: ateapipb.Actor_STATUS_SUSPENDED},
		{condition: "condition=running", wantErr: true},
		{condition: "status=unknown", wantErr: true},
		{condition: "status=unspecified", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.condition, func(t *testing.T) {
			got, err := parseWaitActorStatus(test.condition)
			if (err != nil) != test.wantErr {
				t.Fatalf("parseWaitActorStatus(%q) error = %v, wantErr %t", test.condition, err, test.wantErr)
			}
			if got != test.want {
				t.Errorf("parseWaitActorStatus(%q) = %v, want %v", test.condition, got, test.want)
			}
		})
	}
}

func TestWaitActorRunnerTransition(t *testing.T) {
	statuses := []ateapipb.Actor_Status{
		ateapipb.Actor_STATUS_RESUMING,
		ateapipb.Actor_STATUS_RUNNING,
	}
	var calls int
	client := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
			if got := in.GetActor().GetAtespace(); got != "team-a" {
				t.Errorf("atespace = %q, want team-a", got)
			}
			status := statuses[calls]
			calls++
			return &ateapipb.Actor{Status: status}, nil
		},
	}
	var out bytes.Buffer
	runner := &WaitActorRunner{apiClient: client, out: &out, pollInterval: time.Millisecond}

	if err := runner.Run(context.Background(), "team-a", "agent-1", ateapipb.Actor_STATUS_RUNNING, time.Second); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if calls != 2 {
		t.Errorf("GetActor calls = %d, want 2", calls)
	}
	if got, want := out.String(), "actor/agent-1 condition met\n"; got != want {
		t.Errorf("output = %q, want %q", got, want)
	}
}

func TestWaitActorRunnerAlreadyAtTarget(t *testing.T) {
	var calls int
	client := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
			calls++
			return &ateapipb.Actor{Status: ateapipb.Actor_STATUS_RUNNING}, nil
		},
	}
	var out bytes.Buffer
	runner := &WaitActorRunner{apiClient: client, out: &out}

	if err := runner.Run(context.Background(), "team-a", "agent-1", ateapipb.Actor_STATUS_RUNNING, time.Second); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if calls != 1 {
		t.Errorf("GetActor calls = %d, want 1", calls)
	}
	if got, want := out.String(), "actor/agent-1 condition met\n"; got != want {
		t.Errorf("output = %q, want %q", got, want)
	}
}

func TestWaitActorRunnerReturnsGetError(t *testing.T) {
	wantErr := errors.New("get failed")
	client := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
			return nil, wantErr
		},
	}
	runner := &WaitActorRunner{apiClient: client, out: io.Discard}

	err := runner.Run(context.Background(), "team-a", "agent-1", ateapipb.Actor_STATUS_RUNNING, time.Second)
	if !errors.Is(err, wantErr) {
		t.Fatalf("Run() error = %v, want wrapped %v", err, wantErr)
	}
}

func TestWaitActorRunnerZeroTimeout(t *testing.T) {
	client := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
			return &ateapipb.Actor{Status: ateapipb.Actor_STATUS_SUSPENDED}, nil
		},
	}
	runner := &WaitActorRunner{apiClient: client, out: &bytes.Buffer{}}

	err := runner.Run(context.Background(), "team-a", "agent-1", ateapipb.Actor_STATUS_RUNNING, 0)
	if err == nil || !strings.Contains(err.Error(), "timed out after 0s") {
		t.Fatalf("Run() error = %v, want zero-timeout error", err)
	}
}

func TestWaitActorRunnerTimeoutCancelsGet(t *testing.T) {
	client := &mockAteAPIClient{
		GetActorFunc: func(ctx context.Context, in *ateapipb.GetActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	runner := &WaitActorRunner{apiClient: client, out: &bytes.Buffer{}}

	err := runner.Run(context.Background(), "team-a", "agent-1", ateapipb.Actor_STATUS_RUNNING, time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "timed out after 1ms") {
		t.Fatalf("Run() error = %v, want timeout error", err)
	}
}
