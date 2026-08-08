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

// Tests for the sender's addressing, outcome classification and connection reuse.

package routercap

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
	"google.golang.org/protobuf/proto"
)

// fakeActor stands in for the router plus a glutton actor: it echoes the ping
// message back, and records the Host header it was addressed with.
type fakeActor struct {
	mu     sync.Mutex
	hosts  []string
	status int
	// corrupt makes the echo come back with a different message, standing in
	// for a misroute that still returns 200.
	corrupt bool
}

func (f *fakeActor) handler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.hosts = append(f.hosts, r.Host)
	st, corrupt := f.status, f.corrupt
	f.mu.Unlock()

	if st != 0 && st != http.StatusOK {
		http.Error(w, "upstream connect error or disconnect/reset before headers", st)
		return
	}
	ping := &gluttonpb.PingRequest{}
	if err := proto.Unmarshal(body, ping); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	msg := ping.Message
	if corrupt {
		msg = "not-the-message-you-sent"
	}
	out, _ := proto.Marshal(&gluttonpb.PingResponse{Message: msg})
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Write(out)
}

func (f *fakeActor) seenHosts() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.hosts...)
}

func testActors(n int) []Actor {
	out := make([]Actor, n)
	for i := range out {
		name := string(rune('a' + i))
		out[i] = Actor{Atespace: "routercap", Name: name, Host: name + ".routercap." + actorDomain}
	}
	return out
}

func newTestSender(t *testing.T, url string, actors []Actor) *Sender {
	t.Helper()
	s, err := NewSender(SenderConfig{RouterURLs: []string{url}, Actors: actors, MaxConnections: 64, RequestTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}
	t.Cleanup(s.CloseIdleConnections)
	return s
}

func TestSenderAddressesActorsByHostHeader(t *testing.T) {
	// The router routes on Host; the URL only picks the socket. If this ever
	// stops holding, every request lands on one actor and the run measures a
	// single worker pod.
	fa := &fakeActor{}
	srv := httptest.NewServer(http.HandlerFunc(fa.handler))
	defer srv.Close()

	actors := testActors(3)
	s := newTestSender(t, srv.URL, actors)
	for i := 0; i < 6; i++ {
		if out, st := s.Send(context.Background()); out != OutcomeOK {
			t.Fatalf("send %d: outcome %s status %d", i, out, st)
		}
	}

	hosts := fa.seenHosts()
	if len(hosts) != 6 {
		t.Fatalf("server saw %d requests, want 6", len(hosts))
	}
	counts := map[string]int{}
	for _, h := range hosts {
		counts[h]++
	}
	if len(counts) != 3 {
		t.Fatalf("load hit %d distinct actors, want 3: %v", len(counts), counts)
	}
	for _, a := range actors {
		if counts[a.Host] != 2 {
			t.Errorf("actor %s got %d of 6 requests, want an even 2", a.Host, counts[a.Host])
		}
	}
}

func TestSenderClassifiesOutcomes(t *testing.T) {
	cases := []struct {
		name    string
		setup   func(*fakeActor)
		want    Outcome
		wantSt  int
		checkSt bool
	}{
		{"OK", func(f *fakeActor) {}, OutcomeOK, 200, true},
		{
			// Envoy's shed response when a circuit breaker trips. It must land
			// in the latency distribution, not vanish from it.
			"CircuitBreakerShed",
			func(f *fakeActor) { f.status = http.StatusServiceUnavailable },
			OutcomeHTTPError, 503, true,
		},
		{
			// A 200 carrying someone else's payload is the one failure a
			// status-code-only harness scores as a success.
			"MisrouteWithA200",
			func(f *fakeActor) { f.corrupt = true },
			OutcomeBadBody, 200, true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fa := &fakeActor{}
			tc.setup(fa)
			srv := httptest.NewServer(http.HandlerFunc(fa.handler))
			defer srv.Close()

			s := newTestSender(t, srv.URL, testActors(1))
			got, st := s.Send(context.Background())
			if got != tc.want {
				t.Errorf("outcome = %s, want %s", got, tc.want)
			}
			if tc.checkSt && st != tc.wantSt {
				t.Errorf("status = %d, want %d", st, tc.wantSt)
			}
		})
	}
}

func TestSenderReportsATransportFailureWithNoStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	s := newTestSender(t, srv.URL, testActors(1))
	srv.Close() // nothing is listening now

	got, st := s.Send(context.Background())
	if got != OutcomeTransportError {
		t.Errorf("outcome = %s, want %s", got, OutcomeTransportError)
	}
	if st != 0 {
		t.Errorf("status = %d, want 0: there was no response to take one from", st)
	}
}

func TestSenderReusesConnections(t *testing.T) {
	// The property the whole rig depends on. Without keep-alive the generator
	// burns one source port per request and hits its own ceiling long before
	// the router hits anything.
	fa := &fakeActor{}
	srv := httptest.NewServer(http.HandlerFunc(fa.handler))
	defer srv.Close()

	s := newTestSender(t, srv.URL, testActors(4))
	for i := 0; i < 50; i++ {
		if out, _ := s.Send(context.Background()); out != OutcomeOK {
			t.Fatalf("send %d failed with %s", i, out)
		}
	}

	// Serial sends: one connection carries all fifty.
	cs := s.Stats()
	if cs.NewConnections != 1 {
		t.Errorf("NewConnections = %v, want 1 for 50 serial requests", cs.NewConnections)
	}
	if cs.RequestsPerConnection != 50 {
		t.Errorf("RequestsPerConnection = %v, want 50", cs.RequestsPerConnection)
	}
	if cs.ConnectionsInUse != 1 {
		t.Errorf("ConnectionsInUse = %d, want 1", cs.ConnectionsInUse)
	}
}

func TestSenderStatsPartitionTheRun(t *testing.T) {
	// Consecutive Stats calls must not double-count, or the keep-alive guard
	// would read connection churn that already happened in an earlier window.
	fa := &fakeActor{}
	srv := httptest.NewServer(http.HandlerFunc(fa.handler))
	defer srv.Close()

	s := newTestSender(t, srv.URL, testActors(1))
	for i := 0; i < 10; i++ {
		s.Send(context.Background())
	}
	first := s.Stats()
	for i := 0; i < 10; i++ {
		s.Send(context.Background())
	}
	second := s.Stats()

	if first.NewConnections != 1 {
		t.Errorf("first window NewConnections = %v, want 1", first.NewConnections)
	}
	if second.NewConnections != 0 {
		t.Errorf("second window NewConnections = %v, want 0: the connection was opened in the first window", second.NewConnections)
	}
	// Zero new connections is perfect reuse, not a ratio of zero; the
	// keep-alive guard skips this case rather than reading it as a failure.
	if second.RequestsPerConnection != 0 {
		t.Errorf("RequestsPerConnection = %v, want 0 when nothing was dialed", second.RequestsPerConnection)
	}
	if AnyFatal(DefaultGuardConfig().Check(&Sample{Client: second, Containers: map[string]ContainerUsage{}})) {
		t.Error("a window with perfect connection reuse tripped a fatal guard")
	}
}

func TestNewSenderRejectsAnEmptyPool(t *testing.T) {
	if _, err := NewSender(SenderConfig{RouterURLs: []string{"http://10.0.0.1:8080"}}); err == nil {
		t.Fatal("NewSender accepted a sender with no actors to address")
	}
	if _, err := NewSender(SenderConfig{Actors: testActors(1)}); err == nil {
		t.Fatal("NewSender accepted a sender with no router URL")
	}
}
