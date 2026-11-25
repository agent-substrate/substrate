// Copyright 2026 The Agent Substrate Authors
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
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	ateenvpb "go.ate.dev/substrate/internal/proto/ateenv/v1alpha"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPct(t *testing.T) {
	tests := []struct {
		name   string
		sorted []float64
		p      float64
		want   float64
	}{
		{"empty", nil, 50, 0},
		{"one value", []float64{2}, 90, 2},
		{"p50 of ten", []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 50, 5},
		{"p90 of ten", []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 90, 9},
		{"p100 is max", []float64{1, 2, 3}, 100, 3},
		{"p0 is min", []float64{1, 2, 3}, 0, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := pct(tc.sorted, tc.p); got != tc.want {
				t.Errorf("pct(%v, %v) = %v, want %v", tc.sorted, tc.p, got, tc.want)
			}
		})
	}
}

func TestSummarize(t *testing.T) {
	recs := []callRecord{
		{Op: "ResumeActor", EndS: 1.0, Latency: 1.0, OK: true},
		{Op: "ResumeActor", EndS: 2.0, Latency: 2.0, OK: true, Retries: 3},
		{Op: "ResumeActor", EndS: 3.0, Latency: 3.0, OK: true},
		{Op: "ResumeActor", EndS: 5.0, Latency: 5.0, OK: false, Code: "Internal"},
	}
	st := summarize("resume", 1, "ResumeActor", 4, time.Unix(0, 0), recs)
	if st.Count != 4 || st.OK != 3 || st.Failed != 1 {
		t.Errorf("count/ok/failed = %d/%d/%d, want 4/3/1", st.Count, st.OK, st.Failed)
	}
	if st.TotalS != 5.0 {
		t.Errorf("TotalS = %v, want 5 (the failed call ended last)", st.TotalS)
	}
	if st.P50 != 2.0 || st.Max != 3.0 {
		t.Errorf("P50/Max = %v/%v, want 2/3 (successes only)", st.P50, st.Max)
	}
	if st.Retries != 3 {
		t.Errorf("Retries = %d, want 3", st.Retries)
	}
	if !reflect.DeepEqual(st.Codes, map[string]int{"Internal": 1}) {
		t.Errorf("Codes = %v, want Internal:1", st.Codes)
	}
	// 3 of 4 calls succeeded, so 75% was reached at the third success and
	// 90% and 100% were never reached.
	want := map[string]float64{"10": 1, "25": 1, "50": 2, "75": 3}
	if !reflect.DeepEqual(st.TimeToPct, want) {
		t.Errorf("TimeToPct = %v, want %v", st.TimeToPct, want)
	}
}

func TestRetryPolicyNext(t *testing.T) {
	aborted := status.Error(codes.Aborted, "concurrent update")
	noRoom := status.Error(codes.ResourceExhausted, "no worker has room for the actor")
	contention := status.Error(codes.Internal, "bind volume: 409 concurrency contention, please retry")
	all := retryPolicy{maxConflict: 2, contention: true, noRoom: true}
	tests := []struct {
		name     string
		policy   retryPolicy
		err      error
		attempt  int
		resume   bool
		wantWait time.Duration
		wantOK   bool
	}{
		{"success", all, nil, 1, true, 0, false},
		{"aborted first retry", all, aborted, 1, false, 200 * time.Millisecond, true},
		{"aborted last retry", all, aborted, 2, false, 400 * time.Millisecond, true},
		{"aborted out of retries", all, aborted, 3, false, 0, false},
		{"aborted backoff caps at 3s", retryPolicy{maxConflict: 50}, aborted, 40, false, 3 * time.Second, true},
		{"no room on resume", all, noRoom, 7, true, time.Second, true},
		{"no room off", retryPolicy{}, noRoom, 1, true, 0, false},
		{"no room on suspend", all, noRoom, 1, false, 0, false},
		{"contention first retry", all, contention, 1, true, 50 * time.Millisecond, true},
		{"contention doubles", all, contention, 3, true, 200 * time.Millisecond, true},
		{"contention caps at 1s", all, contention, 9, true, time.Second, true},
		{"contention off", retryPolicy{}, contention, 1, true, 0, false},
		{"other error", all, errors.New("boom"), 1, true, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wait, ok := tc.policy.next(tc.err, tc.attempt, tc.resume)
			if wait != tc.wantWait || ok != tc.wantOK {
				t.Errorf("next() = (%v, %v), want (%v, %v)", wait, ok, tc.wantWait, tc.wantOK)
			}
		})
	}
}

func TestLifecycleRows(t *testing.T) {
	calls := []callRecord{
		{Op: "CreateActor", Latency: 0.01, OK: true},
		{Op: "SuspendActor", Latency: 0.4, OK: true},
		{Op: "SuspendActor", Latency: 0.6, OK: true},
		{Op: "SuspendActor", OK: false},
		{Op: "CreateActor", Latency: 0.02, OK: true},
	}
	want := []opRow{
		{op: "CreateActor", samples: 2, p50: 0.01, p90: 0.02, max: 0.02},
		{op: "SuspendActor", samples: 2, failed: 1, p50: 0.4, p90: 0.6, max: 0.6},
	}
	if got := lifecycleRows(calls); !reflect.DeepEqual(got, want) {
		t.Errorf("lifecycleRows() = %+v, want %+v", got, want)
	}
}

func TestActorNames(t *testing.T) {
	got := actorNames("s6", 40, 3)
	want := []string{"s6-0040", "s6-0041", "s6-0042"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("actorNames() = %v, want %v", got, want)
	}
}

func TestFailedCalls(t *testing.T) {
	tests := []struct {
		name  string
		calls []callRecord
		want  int
	}{
		{"none", nil, 0},
		{"all ok", []callRecord{{OK: true}, {OK: true}}, 0},
		{"some failed", []callRecord{{OK: true}, {OK: false}, {OK: false}}, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := failedCalls(tt.calls); got != tt.want {
				t.Errorf("failedCalls() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestInBatch(t *testing.T) {
	actor := func(name string) *ateapipb.Actor {
		return &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: name}}
	}
	listed := []*ateapipb.Actor{actor("d-0000"), actor("d-0001"), actor("d-0040")}
	tests := []struct {
		name  string
		names []string
		want  []string
	}{
		{"earlier step's actors are left out", []string{"d-0040"}, []string{"d-0040"}},
		{"all in batch", []string{"d-0000", "d-0001", "d-0040"}, []string{"d-0000", "d-0001", "d-0040"}},
		{"batch not listed yet", []string{"d-0041"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []string
			for _, a := range inBatch(listed, tt.names) {
				got = append(got, a.GetMetadata().GetName())
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("inBatch() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExitCode(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"context canceled", context.Canceled, 130},
		{"wrapped context canceled", fmt.Errorf("ListActors: %w", context.Canceled), 130},
		{"grpc canceled", fmt.Errorf("ListActors: %w", status.Error(codes.Canceled, "context canceled")), 130},
		{"other error", errors.New("boom"), 1},
		{"grpc unavailable", status.Error(codes.Unavailable, "down"), 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := exitCode(tt.err); got != tt.want {
				t.Errorf("exitCode() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestParseScript(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    []scriptStep
		wantErr bool
	}{
		{
			name: "runs and cycles",
			in:   "# build\nrun clone git clone http://m/linux-head.git /data/linux\n\ncycle\nrun status git -C /data/linux status --porcelain\n",
			want: []scriptStep{
				{name: "clone", command: "git clone http://m/linux-head.git /data/linux"},
				{},
				{name: "status", command: "git -C /data/linux status --porcelain"},
			},
		},
		{name: "run without command", in: "run clone\n", wantErr: true},
		{name: "cycle with argument", in: "cycle 5\n", wantErr: true},
		{name: "unknown step", in: "sleep 5\n", wantErr: true},
		{name: "API phase name", in: "run suspend true\n", wantErr: true},
		{name: "empty", in: "# nothing\n", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseScript(strings.NewReader(tt.in))
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseScript() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseScript() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestParseEnv(t *testing.T) {
	got, err := parseEnv([]string{"MIRROR=http://m", "GIT_CONFIG_VALUE_0=a=b"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"MIRROR": "http://m", "GIT_CONFIG_VALUE_0": "a=b"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("parseEnv() = %v, want %v", got, want)
	}
	for _, bad := range []string{"NOVALUE", "=x"} {
		if _, err := parseEnv([]string{bad}); err == nil {
			t.Errorf("parseEnv(%q) succeeded, want an error", bad)
		}
	}
}

func TestSplitHostPort(t *testing.T) {
	host, port, err := splitHostPort("git-mirror.benchmarking.svc.cluster.local:80")
	if err != nil || host != "git-mirror.benchmarking.svc.cluster.local" || port != 80 {
		t.Errorf("splitHostPort() = %q, %d, %v", host, port, err)
	}
	for _, bad := range []string{"no-port", "host:0", "host:70000", "host:x"} {
		if _, _, err := splitHostPort(bad); err == nil {
			t.Errorf("splitHostPort(%q) succeeded, want an error", bad)
		}
	}
}

// fakeGuest is an ate-env-guest whose one process runs for polls GetProcess
// calls and then ends as exited.
type fakeGuest struct {
	ateenvpb.ProcessServiceClient
	polls  int
	exited *ateenvpb.Process
	stderr string
	starts []*ateenvpb.StartProcessRequest
	output []*ateenvpb.StreamProcessOutputRequest
}

func (f *fakeGuest) StartProcess(_ context.Context, in *ateenvpb.StartProcessRequest, _ ...grpc.CallOption) (*ateenvpb.Process, error) {
	f.starts = append(f.starts, in)
	return &ateenvpb.Process{ProcessId: "p1", State: ateenvpb.ProcessState_PROCESS_STATE_RUNNING}, nil
}

func (f *fakeGuest) GetProcess(_ context.Context, in *ateenvpb.GetProcessRequest, _ ...grpc.CallOption) (*ateenvpb.Process, error) {
	if in.GetProcessId() != "p1" {
		return nil, status.Error(codes.NotFound, "no such process")
	}
	if f.polls > 0 {
		f.polls--
		return &ateenvpb.Process{ProcessId: "p1", State: ateenvpb.ProcessState_PROCESS_STATE_RUNNING}, nil
	}
	return f.exited, nil
}

func (f *fakeGuest) StreamProcessOutput(_ context.Context, in *ateenvpb.StreamProcessOutputRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[ateenvpb.ProcessOutput], error) {
	f.output = append(f.output, in)
	return &fakeOutput{msgs: []*ateenvpb.ProcessOutput{{Output: &ateenvpb.ProcessOutput_Stderr{Stderr: []byte(f.stderr)}}}}, nil
}

type fakeOutput struct {
	grpc.ClientStream
	msgs []*ateenvpb.ProcessOutput
}

func (o *fakeOutput) Recv() (*ateenvpb.ProcessOutput, error) {
	if len(o.msgs) == 0 {
		return nil, io.EOF
	}
	m := o.msgs[0]
	o.msgs = o.msgs[1:]
	return m, nil
}

func TestRunCommand(t *testing.T) {
	started := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	exited := func(code int32, finished time.Time) *ateenvpb.Process {
		return &ateenvpb.Process{
			ProcessId: "p1", State: ateenvpb.ProcessState_PROCESS_STATE_EXITED, ExitCode: code,
			StartedAt: timestamppb.New(started), FinishedAt: timestamppb.New(finished),
		}
	}
	tests := []struct {
		name        string
		exited      *ateenvpb.Process
		wantOK      bool
		wantLatency float64
		wantErr     string
	}{
		{name: "success takes the guest's run time", exited: exited(0, started.Add(7*time.Second)), wantOK: true, wantLatency: 7},
		{name: "nonzero exit records stderr", exited: exited(2, started.Add(time.Second)), wantLatency: 1, wantErr: `exit 2, stderr: "boom"`},
		{name: "no finish time", exited: &ateenvpb.Process{ProcessId: "p1", State: ateenvpb.ProcessState_PROCESS_STATE_EXITED}, wantErr: "without start and finish times"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &runner{poll: time.Millisecond}
			guest := &fakeGuest{polls: 2, exited: tt.exited, stderr: "boom\n"}
			ok := r.runCommand(t.Context(), time.Now(), guest, "a-0000", scriptStep{name: "build", command: "make"}, nil)
			if ok != tt.wantOK || len(guest.starts) != 1 || len(r.calls) != 1 {
				t.Fatalf("runCommand() = %v with %d starts and %d records, want %v, 1 and 1", ok, len(guest.starts), len(r.calls), tt.wantOK)
			}
			start := guest.starts[0]
			if !reflect.DeepEqual(start.GetCommand(), []string{"sh", "-c", "make"}) || start.GetTimeout().AsDuration() != stepTimeout {
				t.Errorf("StartProcess(%v), want sh -c make with timeout %v", start, stepTimeout)
			}
			for _, o := range guest.output {
				if o.GetStdoutOffset() != math.MaxInt64 {
					t.Errorf("StreamProcessOutput(%v), want stdout skipped", o)
				}
			}
			rec := r.calls[0]
			if rec.Phase != "build" || rec.OK != tt.wantOK || !strings.Contains(rec.Err, tt.wantErr) {
				t.Errorf("record = %+v, want phase build, OK %v and error containing %q", rec, tt.wantOK, tt.wantErr)
			}
			if tt.wantLatency > 0 && rec.Latency != tt.wantLatency {
				t.Errorf("latency = %v, want %v", rec.Latency, tt.wantLatency)
			}
		})
	}
}
