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
	"reflect"
	"testing"
	"time"

	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
