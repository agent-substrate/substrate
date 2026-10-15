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

package ateomstats

import (
	"context"
	"errors"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
)

// reading is one read of counters by name; a single counter reads as "".
type reading map[string]uint64

// measure folds r into a as one reading and returns the activation's CPU.
func measure(t *testing.T, a *Activation, r reading) uint64 {
	t.Helper()
	s, err := a.Measure(context.Background(), func() (*ateworkerpb.WorkloadStatsSample, map[string]uint64, error) {
		s := &ateworkerpb.WorkloadStatsSample{Source: ateworkerpb.StatsSource_STATS_SOURCE_GUEST_AGENT}
		if len(r) == 1 {
			if v, ok := r[""]; ok {
				s.CpuUsageUsec = v
				return s, nil, nil
			}
		}
		return s, r, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return s.GetCpuUsageUsec()
}

func TestActivationEpoch(t *testing.T) {
	t.Parallel()
	start := time.Unix(1700, 5)
	a := NewActivation(start, false)
	if got := a.WithEpoch(&ateworkerpb.WorkloadStatsSample{}).GetEpochUnixNano(); got != start.UnixNano() {
		t.Errorf("pending epoch = %d, want %d", got, start.UnixNano())
	}
	s, _ := a.Measure(context.Background(), func() (*ateworkerpb.WorkloadStatsSample, map[string]uint64, error) {
		return &ateworkerpb.WorkloadStatsSample{Source: ateworkerpb.StatsSource_STATS_SOURCE_CGROUP}, nil, nil
	})
	if s.GetEpochUnixNano() != start.UnixNano() {
		t.Errorf("measured epoch = %d, want %d", s.GetEpochUnixNano(), start.UnixNano())
	}
}

func TestActivationCPU(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		resumesCounters bool
		readings        []reading
		want            []uint64
	}{
		{"counter from zero counts in full", false,
			[]reading{{"": 400}, {"": 900}}, []uint64{400, 900}},
		{"resumed counter counts from the first reading", true,
			[]reading{{"": 5000}, {"": 5400}, {"": 6000}}, []uint64{0, 400, 1000}},
		// b's read fails once: it keeps its last value, so its return adds
		// only its own increase, not its whole counter again.
		{"a failed read neither lowers nor re-adds", true,
			[]reading{{"a": 600, "b": 400}, {"a": 605}, {"a": 606, "b": 404}}, []uint64{0, 5, 10}},
		// An exited container stops being read; the survivors keep counting.
		{"an exited container stops contributing", false,
			[]reading{{"a": 600, "b": 400}, {"a": 700}, {"a": 750}}, []uint64{1000, 1100, 1150}},
		{"a restarted container counts its new value", false,
			[]reading{{"a": 600}, {"a": 50}, {"a": 80}}, []uint64{600, 650, 680}},
		{"a counter first seen later in a resumed guest only sets its baseline", true,
			[]reading{{"a": 600}, {"a": 610, "b": 9000}, {"a": 620, "b": 9010}}, []uint64{0, 10, 30}},
		// A guest can report any counter; a wrapped total would decrease.
		{"a corrupt maximum then a restart stays at the maximum", false,
			[]reading{{"a": math.MaxUint64}, {"a": 5}}, []uint64{math.MaxUint64, math.MaxUint64}},
		{"a counter first seen later in a cold activation counts in full", false,
			[]reading{{"a": 600}, {"a": 610, "b": 90}}, []uint64{600, 700}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			a := NewActivation(time.Now(), tt.resumesCounters)
			for i, r := range tt.readings {
				if got := measure(t, a, r); got != tt.want[i] {
					t.Errorf("reading %d %v: cpu = %d, want %d", i, r, got, tt.want[i])
				}
			}
		})
	}
}

func TestActivationMeasureError(t *testing.T) {
	t.Parallel()
	a := NewActivation(time.Now(), true)
	want := errors.New("guest gone")
	if _, err := a.Measure(context.Background(), func() (*ateworkerpb.WorkloadStatsSample, map[string]uint64, error) { return nil, nil, want }); !errors.Is(err, want) {
		t.Fatalf("Measure() error = %v, want %v", err, want)
	}
	if got := measure(t, a, reading{"": 700}); got != 0 {
		t.Errorf("first reading after a failed one = %d, want 0 (it is the baseline)", got)
	}
}

func TestActivationStore(t *testing.T) {
	t.Parallel()
	a := NewActivation(time.Now(), false)
	if a.Latest() != nil {
		t.Fatal("Latest() before any Store is not nil")
	}
	measured := func(at int64) *ateworkerpb.WorkloadStatsSample {
		return &ateworkerpb.WorkloadStatsSample{Source: ateworkerpb.StatsSource_STATS_SOURCE_CGROUP, ObservedAtUnixNano: at}
	}
	newer, older := measured(20), measured(10)
	a.Store(newer)
	a.Store(older)
	if a.Latest() != newer {
		t.Error("an older sample replaced a newer one")
	}
	p := &ateworkerpb.WorkloadStatsSample{ObservedAtUnixNano: 30}
	a.Store(p)
	if a.Latest() != p {
		t.Error("Latest() is not the newest sample")
	}
	a.Final(func(m *ateworkerpb.WorkloadStatsSample) {
		if m != newer {
			t.Errorf("final sample = %v, want the newest measured one", m)
		}
	})
}

func TestActivationInitialAfterFinal(t *testing.T) {
	t.Parallel()
	a := NewActivation(time.Now(), false)
	a.Final(func(m *ateworkerpb.WorkloadStatsSample) {
		if m != nil {
			t.Errorf("final sample with nothing measured = %v, want nil", m)
		}
	})
	wrote := false
	a.Initial(&ateworkerpb.WorkloadStatsSample{Source: ateworkerpb.StatsSource_STATS_SOURCE_CGROUP}, func() { wrote = true })
	if wrote || a.Latest() != nil {
		t.Error("initial record written after the final one")
	}
}

// TestActivationConcurrentMeasure starts readings together and holds each one
// open: any two that overlap fail the test.
func TestActivationConcurrentMeasure(t *testing.T) {
	t.Parallel()
	a := NewActivation(time.Now(), false)
	var inRead atomic.Int32
	var raw atomic.Uint64
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := a.Measure(context.Background(), func() (*ateworkerpb.WorkloadStatsSample, map[string]uint64, error) {
				if inRead.Add(1) > 1 {
					t.Error("readings overlapped")
				}
				defer inRead.Add(-1)
				time.Sleep(time.Millisecond)
				return &ateworkerpb.WorkloadStatsSample{Source: ateworkerpb.StatsSource_STATS_SOURCE_CGROUP, CpuUsageUsec: raw.Add(10)}, nil, nil
			})
			if err != nil {
				t.Error(err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := measure(t, a, reading{"": raw.Load()}); got != 160 {
		t.Errorf("cpu after 16 readings of +10 = %d, want 160", got)
	}
}

// TestActivationMeasureGivesUp pins that a reading waiting behind another
// ends with its context.
func TestActivationMeasureGivesUp(t *testing.T) {
	t.Parallel()
	a := NewActivation(time.Now(), false)
	held, release := make(chan struct{}), make(chan struct{})
	go a.Measure(context.Background(), func() (*ateworkerpb.WorkloadStatsSample, map[string]uint64, error) {
		close(held)
		<-release
		return &ateworkerpb.WorkloadStatsSample{}, nil, nil
	})
	<-held
	defer close(release)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.Measure(ctx, func() (*ateworkerpb.WorkloadStatsSample, map[string]uint64, error) {
		t.Error("read ran while another held the lock")
		return nil, nil, nil
	}); !errors.Is(err, context.Canceled) {
		t.Errorf("Measure() error = %v, want context.Canceled", err)
	}
}

// TestActivationPeriodicWindow pins the order of the records: periodic ones
// only between the initial reading and the final record.
func TestActivationPeriodicWindow(t *testing.T) {
	t.Parallel()
	sample := func(at int64) *ateworkerpb.WorkloadStatsSample {
		return &ateworkerpb.WorkloadStatsSample{Source: ateworkerpb.StatsSource_STATS_SOURCE_CGROUP, ObservedAtUnixNano: at}
	}
	a := NewActivation(time.Now(), false)
	var wrote []string
	write := func(kind string) func() { return func() { wrote = append(wrote, kind) } }

	if a.Sampling() {
		t.Error("Sampling() before the initial reading = true")
	}
	a.Periodic(sample(1), write("periodic-before"))
	a.Initial(sample(2), write("initial"))
	if !a.Sampling() {
		t.Error("Sampling() after the initial reading = false")
	}
	a.Periodic(sample(3), write("periodic"))
	a.Initial(sample(4), write("initial-again"))
	a.Final(func(*ateworkerpb.WorkloadStatsSample) { wrote = append(wrote, "final") })
	if a.Sampling() {
		t.Error("Sampling() after the final record = true")
	}
	a.Periodic(sample(5), write("periodic-after"))

	if want := []string{"initial", "periodic", "final"}; !slices.Equal(wrote, want) {
		t.Errorf("records = %v, want %v", wrote, want)
	}
}

// TestActivationFailedInitialOpensWindow pins that a failed initial reading
// still lets periodic records start, so an unreachable guest is reported.
func TestActivationFailedInitialOpensWindow(t *testing.T) {
	t.Parallel()
	a := NewActivation(time.Now(), true)
	a.Initial(nil, func() { t.Error("wrote an initial record for a failed reading") })
	if !a.Sampling() {
		t.Fatal("Sampling() after a failed initial reading = false")
	}
	wrote := false
	a.Periodic(&ateworkerpb.WorkloadStatsSample{ObservedAtUnixNano: 1}, func() { wrote = true })
	if !wrote {
		t.Error("no periodic record after a failed initial reading")
	}
}

// TestActivationFinalOnce pins that a second Final writes nothing.
func TestActivationFinalOnce(t *testing.T) {
	t.Parallel()
	a := NewActivation(time.Now(), false)
	writes := 0
	a.Final(func(*ateworkerpb.WorkloadStatsSample) { writes++ })
	a.Final(func(*ateworkerpb.WorkloadStatsSample) { writes++ })
	if writes != 1 {
		t.Errorf("Final wrote %d records, want 1", writes)
	}
}

// TestActivationStoreMeasuredAfterNewerPending stores a pending sample, then a
// measured one read earlier: the discovery read keeps the newer pending sample,
// and the final record still gets the measured one.
func TestActivationStoreMeasuredAfterNewerPending(t *testing.T) {
	t.Parallel()
	a := NewActivation(time.Now(), false)
	older := &ateworkerpb.WorkloadStatsSample{Source: ateworkerpb.StatsSource_STATS_SOURCE_CGROUP, ObservedAtUnixNano: 10}
	a.Store(&ateworkerpb.WorkloadStatsSample{Source: ateworkerpb.StatsSource_STATS_SOURCE_CGROUP, ObservedAtUnixNano: 5})
	pending := &ateworkerpb.WorkloadStatsSample{ObservedAtUnixNano: 20}
	a.Store(pending)
	a.Store(older)
	if a.Latest() != pending {
		t.Errorf("Latest() = %v, want the newer pending sample", a.Latest())
	}
	a.Final(func(m *ateworkerpb.WorkloadStatsSample) {
		if m != older {
			t.Errorf("final sample = %v, want the measured sample read at 10", m)
		}
	})
}

// TestActivationMeasureNilSample pins that a read returning no sample and no
// error is a failed reading, not a panic, and leaves the counters unstarted.
func TestActivationMeasureNilSample(t *testing.T) {
	t.Parallel()
	a := NewActivation(time.Now(), true)
	if _, err := a.Measure(context.Background(), func() (*ateworkerpb.WorkloadStatsSample, map[string]uint64, error) {
		return nil, nil, nil
	}); !errors.Is(err, errNoSample) {
		t.Fatalf("Measure() error = %v, want errNoSample", err)
	}
	if got := measure(t, a, reading{"": 700}); got != 0 {
		t.Errorf("first reading after the empty one = %d, want 0 (it is the baseline)", got)
	}
}
