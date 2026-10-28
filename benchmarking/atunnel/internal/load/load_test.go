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

package load

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"go.ate.dev/substrate/benchmarking/atunnel/internal/report"
)

func TestPacerOffersTheRateWhateverTheWork(t *testing.T) {
	p := &pacer{rate: 200, duration: time.Second, warmup: 200 * time.Millisecond, workers: 4, maxPending: 1000}
	var measured atomic.Int64
	p.run(context.Background(), func(_ context.Context, u unit) {
		if u.measured {
			measured.Add(1)
		}
		// Slower than the interval for one worker; four keep up.
		time.Sleep(10 * time.Millisecond)
	})
	if got := p.scheduled.Load(); got != 200 {
		t.Errorf("scheduled %d units, want 200", got)
	}
	if got := measured.Load(); got != 160 {
		t.Errorf("measured %d units, want 160 after a 20%% warmup", got)
	}
	if got := p.skipped.Load(); got != 0 {
		t.Errorf("skipped %d units with room to queue", got)
	}
}

func TestPacerSkipsWhatCannotQueue(t *testing.T) {
	p := &pacer{rate: 1000, duration: 200 * time.Millisecond, workers: 1, maxPending: 1}
	p.run(context.Background(), func(context.Context, unit) { time.Sleep(50 * time.Millisecond) })
	if p.skipped.Load() == 0 {
		t.Error("no units skipped though one worker cannot keep up and the queue holds one")
	}
}

func TestPercentiles(t *testing.T) {
	var s samples
	for i := 1; i <= 100; i++ {
		s.record(time.Duration(i) * time.Millisecond)
	}
	got := s.percentiles()
	want := report.Percentiles{P50: 51, P90: 91, P99: 100, Max: 100, Mean: 50.5}
	if got != want {
		t.Errorf("percentiles = %+v, want %+v", got, want)
	}
	if empty := s.percentiles(); empty != (report.Percentiles{}) {
		t.Errorf("percentiles after draining = %+v, want zero", empty)
	}
}

func TestSelect(t *testing.T) {
	all, err := Select("all")
	if err != nil || len(all) != 7 {
		t.Fatalf("Select(all) = %d scenarios, %v; want 7", len(all), err)
	}
	some, err := Select("ingressh1, EgressTCP")
	if err != nil || len(some) != 2 || some[0].Name != "IngressH1" || some[1].Name != "EgressTCP" {
		t.Fatalf("Select = %v, %v", some, err)
	}
	if _, err := Select("Nope"); err == nil {
		t.Fatal("Select accepted an unknown scenario")
	}
}

// TestEgressActor runs egress modes through the egress actor, against a plain
// HTTP server standing in for the far end of the sandbox's redirect.
func TestEgressActor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	target := listen(t)
	go func() {
		_ = http.Serve(target, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }))
	}()

	socket := filepath.Join(t.TempDir(), "egress-actor.sock")
	lis, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	egressTargets := Targets{Egress: target.Addr().String(), EgressDiscard: target.Addr().String()}
	go func() { _ = serveEgressActor(ctx, lis, egressTargets) }()

	l, err := executeOnEgressActor(ctx, socket, options{mode: EgressHTTP, rate: 100, duration: 300 * time.Millisecond, workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	if l.Completed == 0 || l.Errors > 0 {
		t.Fatalf("EgressHTTP through the egress actor: completed %d, errors %d: %v", l.Completed, l.Errors, l.ErrorSamples)
	}

	held := false
	l, err = executeOnEgressActor(ctx, socket, options{
		mode: EgressIdle, rate: 100, duration: 500 * time.Millisecond, workers: 4, connections: 5,
		whileHolding: func(context.Context) { held = true },
	})
	if err != nil {
		t.Fatal(err)
	}
	if !held || l.Completed != 5 {
		t.Fatalf("EgressIdle through the egress actor: held %v, completed %d, want true and 5", held, l.Completed)
	}
}

// TestEgressActors splits an EgressIdle run across several egress actors and
// adds up what they report.
func TestEgressActors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	target := listen(t)
	go func() {
		_ = http.Serve(target, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }))
	}()

	var sockets []string
	for i := range 3 {
		socket := filepath.Join(t.TempDir(), fmt.Sprintf("egress-actor-%d.sock", i))
		lis, err := net.Listen("unix", socket)
		if err != nil {
			t.Fatal(err)
		}
		go func() { _ = serveEgressActor(ctx, lis, Targets{Egress: target.Addr().String()}) }()
		sockets = append(sockets, socket)
	}

	var held atomic.Int32
	l, err := executeOnEgressActors(ctx, sockets, options{
		mode: EgressIdle, rate: 300, duration: 500 * time.Millisecond, workers: 4, connections: 10,
		whileHolding: func(context.Context) { held.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if held.Load() != 1 || l.Completed != 10 || l.Errors != 0 {
		t.Fatalf("EgressIdle across 3 egress actors: whileHolding ran %d times, completed %d, errors %d; want 1, 10, 0", held.Load(), l.Completed, l.Errors)
	}
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	return lis
}
