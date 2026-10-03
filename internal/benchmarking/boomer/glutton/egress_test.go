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

package glutton

import (
	"context"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	"github.com/agent-substrate/substrate/internal/benchmarking/glutton/fake"
	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
)

// egressConfig is a dynconfig with the egress loop on.
func egressConfig() dynconfig.Config {
	return dynconfig.Config{
		EgressURL:        "http://example.com/",
		EgressInterval:   time.Second,
		EgressConnection: dynconfig.EgressConnectionReuse,
	}
}

// statKey identifies one locust_request_duration_milliseconds series.
type statKey struct {
	name   string
	status string
}

// readStatCounts snapshots how many requests each stats row has recorded.
// The registry is global to the test binary, so a test reads it twice and
// compares.
func readStatCounts(t *testing.T) map[statKey]uint64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	out := map[statKey]uint64{}
	for _, mf := range families {
		if mf.GetName() != "locust_request_duration_milliseconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var key statKey
			for _, lp := range m.GetLabel() {
				switch lp.GetName() {
				case "name":
					key.name = lp.GetValue()
				case "status":
					key.status = lp.GetValue()
				}
			}
			out[key] += m.GetHistogram().GetSampleCount()
		}
	}
	return out
}

func TestEnsureEgressPolicyRequest(t *testing.T) {
	srv := &fake.Server{}
	dyn := egressConfig()
	dyn.EgressURL = "http://example.com:8080/path"
	u := newTestGluttonActor(t, srv, dyn)
	u.cfg.Atespace = "bench-test"
	ctrl := u.cfg.APIStub.(*fakeControlClient)

	if !u.ensureEgressPolicy(context.Background()) {
		t.Fatal("ensureEgressPolicy = false after a successful create")
	}
	reqs := ctrl.recordedEgressPolicyRequests()
	if len(reqs) != 1 {
		t.Fatalf("CreateActorEgressPolicy calls = %d, want 1", len(reqs))
	}
	req := reqs[0]
	if req.GetActor().GetAtespace() != "bench-test" || req.GetActor().GetName() != "memactor" {
		t.Errorf("policy actor = %v, want bench-test/memactor", req.GetActor())
	}
	if md := req.GetEgressPolicy().GetMetadata(); md.GetAtespace() != "bench-test" || md.GetName() != "default" {
		t.Errorf("policy metadata = %v, want atespace bench-test, name default", md)
	}
	rules := req.GetEgressPolicy().GetRules()
	if len(rules) != 1 {
		t.Fatalf("policy rules = %v, want one http rule", rules)
	}
	httpRule := rules[0].GetHttp()
	if !slices.Equal(httpRule.GetHostnames(), []string{"example.com"}) || !slices.Equal(httpRule.GetPorts().GetNumbers(), []int32{8080}) {
		t.Errorf("http rule = %v, want example.com on port 8080", httpRule)
	}

	// The policy outlives every wake, so it is created once.
	u.ensureEgressPolicy(context.Background())
	if got := len(ctrl.recordedEgressPolicyRequests()); got != 1 {
		t.Errorf("CreateActorEgressPolicy calls after repeat = %d, want 1", got)
	}
}

func TestEnsureEgressPolicyAlreadyExistsIsSuccess(t *testing.T) {
	srv := &fake.Server{}
	u := newTestGluttonActor(t, srv, egressConfig())
	u.cfg.APIStub.(*fakeControlClient).egressPolicyErrs = []error{status.Error(codes.AlreadyExists, "exists")}

	if !u.ensureEgressPolicy(context.Background()) || !u.egressPolicyCreated {
		t.Fatal("ensureEgressPolicy failed on AlreadyExists; want success")
	}
}

func TestEnsureEgressPolicyDisabledByDefault(t *testing.T) {
	srv := &fake.Server{}
	u := newTestGluttonActor(t, srv, dynconfig.Config{})

	if !u.ensureEgressPolicy(context.Background()) {
		t.Fatal("ensureEgressPolicy = false with no egress_url; want true")
	}
	if got := len(u.cfg.APIStub.(*fakeControlClient).recordedEgressPolicyRequests()); got != 0 {
		t.Errorf("CreateActorEgressPolicy calls with no egress_url = %d, want 0", got)
	}
}

// TestIterateWaitsForEgressPolicy checks a wake is skipped, without resuming
// the actor, until its policy exists, and that a wake then starts and drains
// the egress loop around the live window.
func TestIterateWaitsForEgressPolicy(t *testing.T) {
	srv := &fake.Server{}
	ctrl := &fakeControlClient{egressPolicyErrs: []error{status.Error(codes.Unavailable, "ateapi down")}}
	cfg := newTestConfig(t, srv, &userclass.Config{
		APIStub:  ctrl,
		Atespace: "bench-test",
		Dyn:      dynconfig.NewHolder(egressConfig()),
	})
	rt := &taskRuntime{cfg: cfg}

	rt.iterate()
	if calls := ctrl.recordedCalls(); slices.Contains(calls, "ResumeActor") {
		t.Fatalf("calls after a failed policy create = %v, want no ResumeActor", calls)
	}
	if paths := srv.RecordedPaths(); len(paths) != 0 {
		t.Fatalf("actor requests after a failed policy create = %v, want none", paths)
	}

	rt.iterate()
	calls := ctrl.recordedCalls()
	if want := []string{"CreateActorEgressPolicy", "ResumeActor", "SuspendActor"}; !isSubsequence(want, calls) {
		t.Errorf("calls = %v, want %v in order", calls, want)
	}
	if paths := srv.RecordedPaths(); !isSubsequence([]string{useEgressPath, pingPath, drainEgressPath}, paths) {
		t.Errorf("actor requests = %v, want UseEgress, then the ping, then DrainEgress", paths)
	}
	egressReqs := srv.RecordedEgressRequests()
	if len(egressReqs) != 1 || egressReqs[0].GetUrl() != "http://example.com/" || egressReqs[0].GetIntervalMs() != 1000 {
		t.Errorf("UseEgress requests = %v, want one for http://example.com/ every 1000ms", egressReqs)
	}
	if drains := srv.RecordedDrainRequests(); len(drains) != 1 || !drains[0].GetStop() {
		t.Errorf("DrainEgress requests = %v, want one with stop", drains)
	}
}

// TestIterateEgressFailuresDoNotReplaceActor checks failed egress calls are
// booked as stats only and never cost the actor.
func TestIterateEgressFailuresDoNotReplaceActor(t *testing.T) {
	srv := &fake.Server{EgressDrain: &gluttonpb.DrainEgressResponse{Samples: []*gluttonpb.EgressSample{
		{First: true, Error: "connection reset by peer"},
		{StatusCode: http.StatusForbidden},
	}}}
	ctrl := &fakeControlClient{}
	cfg := newTestConfig(t, srv, &userclass.Config{
		APIStub:  ctrl,
		Atespace: "bench-test",
		Dyn:      dynconfig.NewHolder(egressConfig()),
	})
	rt := &taskRuntime{cfg: cfg}

	for range maxConsecutiveFailures + 1 {
		rt.iterate()
	}
	if calls := ctrl.recordedCalls(); slices.Contains(calls, "DeleteActor") {
		t.Errorf("calls = %v, want no DeleteActor for egress failures", calls)
	}
}

func TestStartEgressConnectionModes(t *testing.T) {
	for _, tc := range []struct {
		connection        string
		disableKeepAlives bool
	}{
		{connection: dynconfig.EgressConnectionReuse},
		{connection: dynconfig.EgressConnectionNew, disableKeepAlives: true},
	} {
		t.Run(tc.connection, func(t *testing.T) {
			srv := &fake.Server{}
			dyn := egressConfig()
			dyn.EgressInterval = 250 * time.Millisecond
			dyn.EgressConnection = tc.connection
			u := newTestGluttonActor(t, srv, dyn)
			u.egressPolicyCreated = true

			u.startEgress(context.Background())

			if !u.egressStarted {
				t.Fatal("egressStarted = false after a successful UseEgress")
			}
			reqs := srv.RecordedEgressRequests()
			if len(reqs) != 1 {
				t.Fatalf("UseEgress calls = %d, want 1", len(reqs))
			}
			if reqs[0].GetIntervalMs() != 250 || reqs[0].GetDisableKeepAlives() != tc.disableKeepAlives {
				t.Errorf("UseEgress request = %v, want interval 250ms and disable_keep_alives %v", reqs[0], tc.disableKeepAlives)
			}
		})
	}
}

func TestStartEgressDisabledByDefault(t *testing.T) {
	srv := &fake.Server{}
	u := newTestGluttonActor(t, srv, dynconfig.Config{})

	u.startEgress(context.Background())
	u.drainEgress(context.Background())

	if paths := srv.RecordedPaths(); len(paths) != 0 {
		t.Errorf("actor requests with no egress_url = %v, want none", paths)
	}
}

func TestDrainEgressRecordsRows(t *testing.T) {
	srv := &fake.Server{EgressDrain: &gluttonpb.DrainEgressResponse{Samples: []*gluttonpb.EgressSample{
		{First: true, StatusCode: http.StatusOK, LatencyUs: 40_000},
		{ReusedConn: true, StatusCode: http.StatusOK, LatencyUs: 10_000},
		{ReusedConn: true, StatusCode: http.StatusServiceUnavailable, LatencyUs: 10_000},
		{StatusCode: http.StatusOK, LatencyUs: 30_000},
		{Error: "read tcp 10.0.0.5:43210->93.184.215.14:80: read: connection reset by peer", LatencyUs: 5_000},
	}}}
	u := newTestGluttonActor(t, srv, egressConfig())
	u.egressStarted = true

	before := readStatCounts(t)
	u.drainEgress(context.Background())
	after := readStatCounts(t)

	if u.egressStarted {
		t.Error("egressStarted = true after the drain")
	}
	for _, want := range []struct {
		key   statKey
		count uint64
	}{
		{statKey{"GluttonDrainEgress", "success"}, 1},
		{statKey{"GluttonEgressFirst", "success"}, 1},
		{statKey{"GluttonEgress", "success"}, 1},
		{statKey{"GluttonEgress", "failure"}, 1},
		{statKey{"GluttonEgressNewConn", "success"}, 1},
		{statKey{"GluttonEgressNewConn", "failure"}, 1},
	} {
		if got := after[want.key] - before[want.key]; got != want.count {
			t.Errorf("row %v recorded %d, want %d", want.key, got, want.count)
		}
	}
}

// TestDrainEgressFailureStopsLoop checks a failed drain still sends the stop,
// so the hibernate does not snapshot a running loop.
func TestDrainEgressFailureStopsLoop(t *testing.T) {
	srv := &fake.Server{Status: http.StatusServiceUnavailable}
	u := newTestGluttonActor(t, srv, egressConfig())
	u.egressStarted = true

	u.drainEgress(context.Background())

	if paths := srv.RecordedPaths(); !slices.Equal(paths, []string{drainEgressPath, useEgressPath}) {
		t.Errorf("actor requests = %v, want the drain then a stopping UseEgress", paths)
	}
	if u.egressStarted {
		t.Error("egressStarted = true after the failed drain")
	}
}

func TestStripLocalAddr(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{
			in:   "read tcp 10.0.0.5:43210->93.184.215.14:80: read: connection reset by peer",
			want: "read tcp 93.184.215.14:80: read: connection reset by peer",
		},
		{
			in:   "write tcp [fd00::5]:43210->[2606:2800::1]:80: write: broken pipe",
			want: "write tcp [2606:2800::1]:80: write: broken pipe",
		},
		{
			in:   `Get "http://example.com/": context deadline exceeded (Client.Timeout exceeded while awaiting headers)`,
			want: `Get "http://example.com/": context deadline exceeded (Client.Timeout exceeded while awaiting headers)`,
		},
	} {
		if got := stripLocalAddr(tc.in); got != tc.want {
			t.Errorf("stripLocalAddr(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// isSubsequence reports whether want appears in got in order, not
// necessarily adjacent.
func isSubsequence(want, got []string) bool {
	i := 0
	for _, g := range got {
		if i < len(want) && g == want[i] {
			i++
		}
	}
	return i == len(want)
}
