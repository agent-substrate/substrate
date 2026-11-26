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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	clocktesting "k8s.io/utils/clock/testing"

	"go.ate.dev/substrate/api/ateapi/v1"
)

// fakeDrainWorkers answers RequestActorSuspend per actor name. It returns
// the code in fail, after waiting for the channel in hold to close.
type fakeDrainWorkers struct {
	ateapi.WorkerServiceClient
	fail map[string]codes.Code
	hold map[string]chan struct{}

	mu          sync.Mutex
	requests    []*ateapi.RequestActorSuspendRequest
	calls       map[string]int
	inFlight    int
	maxInFlight int
}

func newFakeDrainWorkers() *fakeDrainWorkers {
	return &fakeDrainWorkers{
		fail:  map[string]codes.Code{},
		hold:  map[string]chan struct{}{},
		calls: map[string]int{},
	}
}

func (f *fakeDrainWorkers) RequestActorSuspend(ctx context.Context, req *ateapi.RequestActorSuspendRequest, _ ...grpc.CallOption) (*ateapi.RequestActorSuspendResponse, error) {
	name := req.GetActor().GetName()
	f.mu.Lock()
	f.requests = append(f.requests, req)
	f.calls[name]++
	f.inFlight++
	f.maxInFlight = max(f.maxInFlight, f.inFlight)
	hold := f.hold[name]
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.inFlight--
		f.mu.Unlock()
	}()
	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if c, ok := f.fail[name]; ok {
		return nil, status.Error(c, "refused")
	}
	return &ateapi.RequestActorSuspendResponse{}, nil
}

func drainCandidates(names ...string) (map[string]bool, []drainCandidate) {
	holders := map[string]bool{}
	var candidates []drainCandidate
	for _, n := range names {
		holders["uid-"+n] = true
		candidates = append(candidates, drainCandidate{podUID: "pod", atespace: "ns", name: n, uid: "uid-" + n})
	}
	return holders, candidates
}

// Each step advances the clock, optionally forgets as for a new plugin
// socket, and starts the suspends of the actors that hold leases.
func TestDrainSuspenderRetries(t *testing.T) {
	type step struct {
		advance time.Duration
		forget  bool
		holders []string
		want    int
	}
	for _, tc := range []struct {
		name      string
		fail      map[string]codes.Code
		steps     []step
		wantCalls map[string]int
	}{
		{
			name: "asks each holder once",
			steps: []step{
				{holders: []string{"a", "b"}, want: 2},
				{holders: []string{"a", "b"}},
				{advance: time.Hour, holders: []string{"a", "b"}},
			},
			wantCalls: map[string]int{"a": 1, "b": 1},
		},
		{
			name: "does not retry an actor not hosted here",
			fail: map[string]codes.Code{"a": codes.NotFound},
			steps: []step{
				{holders: []string{"a"}, want: 1},
				{advance: time.Hour, holders: []string{"a"}},
			},
			wantCalls: map[string]int{"a": 1},
		},
		{
			name: "backs off after a refusal",
			fail: map[string]codes.Code{"a": codes.FailedPrecondition},
			steps: []step{
				{holders: []string{"a"}, want: 1},
				{holders: []string{"a"}},
				{advance: drainRetryMin, holders: []string{"a"}, want: 1},
				{advance: drainRetryMin, holders: []string{"a"}},
				{advance: drainRetryMin, holders: []string{"a"}, want: 1},
			},
			wantCalls: map[string]int{"a": 3},
		},
		{
			name: "asks again after a new plugin socket",
			steps: []step{
				{holders: []string{"a"}, want: 1},
				{holders: []string{"a"}},
				{forget: true, holders: []string{"a"}, want: 1},
			},
			wantCalls: map[string]int{"a": 2},
		},
		{
			name: "asks again after the actor dropped its lease and took another",
			steps: []step{
				{holders: []string{"a"}, want: 1},
				{},
				{holders: []string{"a"}, want: 1},
			},
			wantCalls: map[string]int{"a": 2},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workers := newFakeDrainWorkers()
			for n, c := range tc.fail {
				workers.fail[n] = c
			}
			clk := clocktesting.NewFakePassiveClock(time.Unix(0, 0))
			d := newDrainSuspender(workers, clk)
			for i, s := range tc.steps {
				clk.SetTime(clk.Now().Add(s.advance))
				if s.forget {
					d.forget()
				}
				holders, candidates := drainCandidates(s.holders...)
				got, wait := d.start(t.Context(), holders, candidates)
				wait()
				if got != s.want {
					t.Errorf("step %d started %d suspends, want %d", i, got, s.want)
				}
			}
			if diff := cmp.Diff(tc.wantCalls, workers.calls); diff != "" {
				t.Errorf("RequestActorSuspend calls per actor mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// At most drainSuspendConcurrency suspends run at once. A later call starts
// the rest once slots free up.
func TestDrainSuspenderConcurrency(t *testing.T) {
	workers := newFakeDrainWorkers()
	release := make(chan struct{})
	var names []string
	for i := range drainSuspendConcurrency + 2 {
		n := fmt.Sprintf("a%d", i)
		names = append(names, n)
		workers.hold[n] = release
	}
	d := newDrainSuspender(workers, clocktesting.NewFakePassiveClock(time.Unix(0, 0)))
	holders, candidates := drainCandidates(names...)

	got, wait := d.start(t.Context(), holders, candidates)
	if got != drainSuspendConcurrency {
		t.Errorf("first call started %d suspends, want %d", got, drainSuspendConcurrency)
	}
	if got, _ := d.start(t.Context(), holders, candidates); got != 0 {
		t.Errorf("call with every slot busy started %d suspends, want 0", got)
	}
	close(release)
	wait()
	got, wait = d.start(t.Context(), holders, candidates)
	wait()
	if got != 2 {
		t.Errorf("call after the slots freed started %d suspends, want 2", got)
	}
	if workers.maxInFlight > drainSuspendConcurrency {
		t.Errorf("%d suspends ran at once, want at most %d", workers.maxInFlight, drainSuspendConcurrency)
	}
}

// A slow suspend holds only its own slot: another actor's retry starts while
// it runs.
func TestDrainSuspenderSlowSuspendDoesNotDelayRetries(t *testing.T) {
	workers := newFakeDrainWorkers()
	release := make(chan struct{})
	workers.hold["slow"] = release
	workers.fail["fast"] = codes.FailedPrecondition
	clk := clocktesting.NewFakePassiveClock(time.Unix(0, 0))
	d := newDrainSuspender(workers, clk)
	holders, both := drainCandidates("slow", "fast")
	slow, fast := both[:1], both[1:]

	got, waitSlow := d.start(t.Context(), holders, slow)
	if got != 1 {
		t.Fatalf("starting the slow suspend started %d, want 1", got)
	}
	defer waitSlow()
	defer close(release)
	got, waitFast := d.start(t.Context(), holders, fast)
	waitFast()
	if got != 1 {
		t.Fatalf("starting the fast suspend started %d, want 1", got)
	}
	clk.SetTime(clk.Now().Add(drainRetryMin))
	got, waitRetry := d.start(t.Context(), holders, both)
	waitRetry()
	if got != 1 {
		t.Errorf("retry while the slow suspend runs started %d suspends, want 1", got)
	}
	if diff := cmp.Diff(map[string]int{"slow": 1, "fast": 2}, workers.calls); diff != "" {
		t.Errorf("RequestActorSuspend calls per actor mismatch (-want +got):\n%s", diff)
	}
}

func TestDrainRetryDelay(t *testing.T) {
	for failures, want := range map[int]time.Duration{
		1:  2 * time.Second,
		2:  4 * time.Second,
		5:  32 * time.Second,
		6:  time.Minute,
		60: time.Minute,
	} {
		if got := drainRetryDelay(failures); got != want {
			t.Errorf("drainRetryDelay(%d) = %v, want %v", failures, got, want)
		}
	}
}
