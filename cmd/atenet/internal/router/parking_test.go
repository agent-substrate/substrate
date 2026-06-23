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

package router

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestParkingLot_CapacityAndRelease(t *testing.T) {
	lot := newParkingLot(parkingConfig{enabled: true, maxWait: time.Second, maxParked: 2}, nil)
	ctx := context.Background()

	r1, ok := lot.enter(ctx)
	if !ok {
		t.Fatal("first enter should be admitted")
	}
	r2, ok := lot.enter(ctx)
	if !ok {
		t.Fatal("second enter should be admitted")
	}
	if got := lot.activeCount(); got != 2 {
		t.Fatalf("active = %d, want 2", got)
	}

	// Lot is full; the third request must be shed.
	if _, ok := lot.enter(ctx); ok {
		t.Fatal("third enter should be rejected when lot is full")
	}

	// Releasing a slot frees room for a new request.
	r1(parkOutcomeServed)
	if got := lot.activeCount(); got != 1 {
		t.Fatalf("active after release = %d, want 1", got)
	}
	r3, ok := lot.enter(ctx)
	if !ok {
		t.Fatal("enter should be admitted after a slot was released")
	}

	r2(parkOutcomeServed)
	r3(parkOutcomeServed)
	if got := lot.activeCount(); got != 0 {
		t.Fatalf("active after all released = %d, want 0", got)
	}
}

func TestParkingLot_ReleaseIsIdempotent(t *testing.T) {
	lot := newParkingLot(parkingConfig{enabled: true, maxWait: time.Second, maxParked: 1}, nil)

	release, ok := lot.enter(context.Background())
	if !ok {
		t.Fatal("enter should be admitted")
	}
	release(parkOutcomeServed)
	release(parkOutcomeServed) // double release must not double-count
	if got := lot.activeCount(); got != 0 {
		t.Fatalf("active = %d, want 0 after idempotent release", got)
	}
}

func TestParkingLot_DisabledAlwaysAdmits(t *testing.T) {
	// maxParked is 0, but a disabled lot ignores capacity entirely.
	lot := newParkingLot(parkingConfig{enabled: false, maxParked: 0}, nil)

	for i := 0; i < 5; i++ {
		release, ok := lot.enter(context.Background())
		if !ok {
			t.Fatalf("disabled lot rejected request %d", i)
		}
		release(parkOutcomeServed)
	}
	if got := lot.activeCount(); got != 0 {
		t.Fatalf("disabled lot should not account slots, active = %d", got)
	}
}

func TestParkingLot_NilSafe(t *testing.T) {
	var lot *parkingLot
	release, ok := lot.enter(context.Background())
	if !ok {
		t.Fatal("nil lot should admit")
	}
	release(parkOutcomeServed) // must not panic
	if got := lot.activeCount(); got != 0 {
		t.Fatalf("nil lot active = %d, want 0", got)
	}
	if s := lot.status(); s.Enabled || s.Active != 0 {
		t.Fatalf("nil lot status = %+v, want zero value", s)
	}
}

func TestParkingLot_ConcurrentEntryRespectsCapacity(t *testing.T) {
	const capacity = 8
	const goroutines = 100
	lot := newParkingLot(parkingConfig{enabled: true, maxWait: time.Second, maxParked: capacity}, nil)

	var admitted int64
	var mu sync.Mutex
	releases := make([]func(parkOutcome), 0, capacity)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			if release, ok := lot.enter(context.Background()); ok {
				mu.Lock()
				admitted++
				releases = append(releases, release)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if admitted != capacity {
		t.Fatalf("admitted = %d, want exactly %d", admitted, capacity)
	}
	if got := lot.activeCount(); got != capacity {
		t.Fatalf("active = %d, want %d", got, capacity)
	}
	for _, r := range releases {
		r(parkOutcomeServed)
	}
	if got := lot.activeCount(); got != 0 {
		t.Fatalf("active after releasing all = %d, want 0", got)
	}
}

func TestParkOutcomeFor(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want parkOutcome
	}{
		{"nil is served", nil, parkOutcomeServed},
		{"canceled", context.Canceled, parkOutcomeCanceled},
		{"deadline is timeout", context.DeadlineExceeded, parkOutcomeTimeout},
		{"other is error", errOther, parkOutcomeError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := parkOutcomeFor(tc.err); got != tc.want {
				t.Errorf("parkOutcomeFor(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

type simpleErr string

func (e simpleErr) Error() string { return string(e) }

const errOther simpleErr = "boom"
