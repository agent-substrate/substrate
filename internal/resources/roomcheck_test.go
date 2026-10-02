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

package resources

import (
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/api/resource"
)

// RoomCheck reads the wire form directly, so it is held to the same answers as
// parsing everything into Quantities and calling Sub then Covers.
func TestRoomCheckAgreesWithQuantities(t *testing.T) {
	tests := []struct {
		name      string
		want      *ateapipb.Resources
		capacity  *ateapipb.Resources
		allocated *ateapipb.Resources
		ok        bool
	}{
		{
			name:     "enough of every dimension",
			want:     limits("cpu", "500m", "memory", "1Gi"),
			capacity: limits("cpu", "4", "memory", "8Gi"),
			ok:       true,
		},
		{
			name:      "exactly what is left",
			want:      limits("cpu", "500m", "memory", "1Gi"),
			capacity:  limits("cpu", "2", "memory", "4Gi"),
			allocated: limits("cpu", "1500m", "memory", "3Gi"),
			ok:        true,
		},
		{
			name:      "short in one dimension once allocation is taken off",
			want:      limits("cpu", "500m", "memory", "1Gi"),
			capacity:  limits("cpu", "2", "memory", "4Gi"),
			allocated: limits("cpu", "1", "memory", "3584Mi"),
			ok:        false,
		},
		{
			name:     "a dimension the worker never reported is none of it",
			want:     limits("cpu", "1", "nvidia.com/gpu", "1"),
			capacity: limits("cpu", "4"),
			ok:       false,
		},
		{
			name:     "asking for none of an absent dimension fits",
			want:     limits("nvidia.com/gpu", "0"),
			capacity: limits("cpu", "4"),
			ok:       true,
		},
		{
			name:      "an overcommitted dimension covers nothing",
			want:      limits("cpu", "1"),
			capacity:  limits("cpu", "2"),
			allocated: limits("cpu", "3"),
			ok:        false,
		},
		{
			name:     "a repeated name sums rather than shadowing",
			want:     limits("cpu", "3"),
			capacity: limits("cpu", "2", "cpu", "1"),
			ok:       true,
		},
		{
			name:      "a repeated name in the allocation sums too",
			want:      limits("cpu", "1"),
			capacity:  limits("cpu", "2"),
			allocated: limits("cpu", "1", "cpu", "500m"),
			ok:        false,
		},
		{
			name:     "an actor asking for nothing fits anywhere",
			want:     nil,
			capacity: limits("cpu", "0"),
			ok:       true,
		},
		{
			name:     "an actor asking for nothing fits a worker reporting nothing",
			want:     nil,
			capacity: nil,
			ok:       true,
		},
		{
			name:     "a worker reporting nothing has none of anything",
			want:     limits("cpu", "1"),
			capacity: nil,
			ok:       false,
		},
		{
			// Fractional binary quantities leave ParseQuantity's int64 fast path.
			name:      "quantities off the int64 fast path compare exactly",
			want:      limits("memory", "1.5Gi"),
			capacity:  limits("memory", "3.5Gi"),
			allocated: limits("memory", "2Gi"),
			ok:        true,
		},
		{
			name:      "quantities off the int64 fast path still come up short",
			want:      limits("memory", "1.5Gi"),
			capacity:  limits("memory", "3.5Gi"),
			allocated: limits("memory", "2049Mi"),
			ok:        false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			check, err := NewRoomCheck(tc.want)
			if err != nil {
				t.Fatalf("NewRoomCheck: %v", err)
			}
			got := check.Admits(tc.capacity, tc.allocated)
			if got != tc.ok {
				t.Errorf("Admits() = %v, want %v", got, tc.ok)
			}
			if ref := referenceAnswer(t, tc.want, tc.capacity, tc.allocated); got != ref {
				t.Errorf("Admits() = %v but Quantities.Sub then Covers says %v", got, ref)
			}
		})
	}
}

// referenceAnswer is the question asked the long way, through Quantities.
func referenceAnswer(t *testing.T, want, capacity, allocated *ateapipb.Resources) bool {
	t.Helper()
	need, err := ParseQuantities(want)
	if err != nil {
		t.Fatal(err)
	}
	free, err := ParseQuantities(capacity)
	if err != nil {
		t.Fatal(err)
	}
	if free == nil {
		free = Quantities{}
	}
	used, err := ParseQuantities(allocated)
	if err != nil {
		t.Fatal(err)
	}
	free.Sub(used)
	return free.Covers(need)
}

func TestRoomCheckUnreadableRecords(t *testing.T) {
	t.Run("an unparseable size is an error", func(t *testing.T) {
		if _, err := NewRoomCheck(limits("cpu", "banana")); err == nil {
			t.Error("NewRoomCheck() = nil error, want one")
		}
	})

	check, err := NewRoomCheck(limits("cpu", "1"))
	if err != nil {
		t.Fatalf("NewRoomCheck: %v", err)
	}
	t.Run("an unparseable capacity has no room, even in a dimension not asked for", func(t *testing.T) {
		if check.Admits(limits("cpu", "4", "memory", "lots"), nil) {
			t.Error("Admits() = true, want false")
		}
	})
	t.Run("an unparseable allocation has no room", func(t *testing.T) {
		if check.Admits(limits("cpu", "4"), limits("cpu", "some")) {
			t.Error("Admits() = true, want false")
		}
	})
	t.Run("a memoized failure still has no room", func(t *testing.T) {
		if check.Admits(limits("cpu", "some"), nil) {
			t.Error("Admits() = true, want false")
		}
	})
	t.Run("an actor asking for nothing is not blocked by an unreadable worker", func(t *testing.T) {
		check, err := NewRoomCheck(nil)
		if err != nil {
			t.Fatalf("NewRoomCheck: %v", err)
		}
		if !check.Admits(limits("cpu", "banana"), nil) {
			t.Error("Admits() = false, want true")
		}
	})
}

// The check is asked of every worker in the fleet on every placement, so it
// must not allocate per worker once it has seen the fleet's quantity strings,
// whether or not they take ParseQuantity's int64 fast path.
func TestRoomCheckAllocatesNothingPerWorker(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		want, capacity, allocated *ateapipb.Resources
	}{
		{
			name:      "canonical quantities",
			want:      limits("cpu", "500m", "memory", "256Mi"),
			capacity:  limits("cpu", "16", "memory", "64Gi"),
			allocated: limits("cpu", "8500m", "memory", "9472Mi"),
		},
		{
			name:      "fractional binary quantities",
			want:      limits("memory", "1.5Gi"),
			capacity:  limits("memory", "63.5Gi"),
			allocated: limits("memory", "10.5Gi"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			check, err := NewRoomCheck(tc.want)
			if err != nil {
				t.Fatalf("NewRoomCheck: %v", err)
			}
			if !check.Admits(tc.capacity, tc.allocated) {
				t.Fatal("Admits() = false, want true")
			}
			allocs := testing.AllocsPerRun(100, func() {
				if !check.Admits(tc.capacity, tc.allocated) {
					t.Fatal("Admits() = false, want true")
				}
			})
			if allocs != 0 {
				t.Errorf("Admits() allocates %v times per worker, want 0", allocs)
			}
		})
	}
}

// A memoized quantity is read for every worker checked. Arithmetic on the
// worker's scratch must never write back into the memo, or one worker's
// subtraction would change what the next worker is compared against.
func TestRoomCheckMemoIsNotMutated(t *testing.T) {
	for _, tc := range []struct {
		name      string
		capacity  string
		allocated string
	}{
		{name: "int64 form", capacity: "3.5Gi", allocated: "1.5Gi"},
		{
			// Nine decimal places under a ten-digit byte count overflow an
			// int64 at nano scale, so both stay inf.Dec in the memo, and the
			// scratch arithmetic reads them through a shared pointer.
			name:      "inf.Dec form",
			capacity:  "12.000000001Gi",
			allocated: "9.000000001Gi",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			check, err := NewRoomCheck(limits("memory", "1.5Gi"))
			if err != nil {
				t.Fatalf("NewRoomCheck: %v", err)
			}
			capacity := limits("memory", tc.capacity)
			allocated := limits("memory", tc.allocated)
			for i := range 3 {
				if !check.Admits(capacity, allocated) {
					t.Fatalf("round %d: Admits() = false, want true", i)
				}
			}
			for s, p := range check.parsed {
				want := resource.MustParse(s)
				if p.quantity.Cmp(want) != 0 {
					t.Errorf("memo for %q drifted to %v", s, p.quantity.String())
				}
			}
		})
	}
}
