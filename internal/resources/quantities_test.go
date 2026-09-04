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
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/resource"
)

func limits(pairs ...string) *ateapipb.Resources {
	out := &ateapipb.Resources{}
	for i := 0; i < len(pairs); i += 2 {
		out.Limits = append(out.Limits, &ateapipb.Limits{Name: pairs[i], Quantity: pairs[i+1]})
	}
	return out
}

// The whole reason Proto sorts. Capacity is compared with proto.Equal to decide
// whether a report changed anything, and a repeated field compares positionally:
// unsorted, two equal sets would look different and write on every report.
func TestProtoIsOrderIndependent(t *testing.T) {
	a, err := ParseQuantities(limits("cpu", "2", "memory", "8Gi"))
	if err != nil {
		t.Fatalf("ParseQuantities: %v", err)
	}
	b, err := ParseQuantities(limits("memory", "8Gi", "cpu", "2"))
	if err != nil {
		t.Fatalf("ParseQuantities: %v", err)
	}
	if !proto.Equal(a.Proto(), b.Proto()) {
		t.Errorf("the same set in two orders round-trips unequal:\n%v\n%v", a.Proto(), b.Proto())
	}
}

// A dimension that reaches zero is dropped, so a Worker whose Actors all left
// compares equal to one that never held any.
func TestProtoDropsEmptyDimensions(t *testing.T) {
	q := Quantities{"cpu": resource.MustParse("0")}
	if got := q.Proto(); got != nil {
		t.Errorf("Proto() = %v for an all-zero set, want nil", got)
	}
	if got := (Quantities{}).Proto(); got != nil {
		t.Errorf("Proto() = %v for an empty set, want nil", got)
	}
}

func TestParseQuantities(t *testing.T) {
	t.Run("an unparseable quantity is an error, not a skip", func(t *testing.T) {
		if _, err := ParseQuantities(limits("cpu", "banana")); err == nil {
			t.Error("ParseQuantities() = nil error, want one")
		}
	})

	t.Run("nothing declared parses to nothing", func(t *testing.T) {
		got, err := ParseQuantities(nil)
		if err != nil || got != nil {
			t.Errorf("ParseQuantities(nil) = %v, %v, want nil, nil", got, err)
		}
	})

	t.Run("a repeated name sums rather than shadowing", func(t *testing.T) {
		got, err := ParseQuantities(limits("cpu", "1", "cpu", "500m"))
		if err != nil {
			t.Fatalf("ParseQuantities: %v", err)
		}
		cpu := got["cpu"]
		if want := resource.MustParse("1500m"); cpu.Cmp(want) != 0 {
			t.Errorf("cpu = %v, want %v", cpu, want)
		}
	})
}

func TestQuantitiesAddAndSub(t *testing.T) {
	q := Quantities{}
	q.Add(Quantities{"cpu": resource.MustParse("2"), "memory": resource.MustParse("4Gi")})
	q.Add(Quantities{"cpu": resource.MustParse("1")})
	cpu := q["cpu"]
	if want := resource.MustParse("3"); cpu.Cmp(want) != 0 {
		t.Errorf("cpu after adds = %v, want %v", cpu, want)
	}

	q.Sub(Quantities{"cpu": resource.MustParse("3"), "memory": resource.MustParse("4Gi")})
	if got := q.Proto(); got != nil {
		t.Errorf("Proto() = %v after subtracting everything back out, want nil", got)
	}
}

func TestQuantitiesCovers(t *testing.T) {
	tests := []struct {
		name string
		have Quantities
		want Quantities
		ok   bool
	}{
		{
			name: "enough of every dimension",
			have: Quantities{"cpu": resource.MustParse("4"), "memory": resource.MustParse("8Gi")},
			want: Quantities{"cpu": resource.MustParse("2"), "memory": resource.MustParse("4Gi")},
			ok:   true,
		},
		{
			name: "exactly enough",
			have: Quantities{"cpu": resource.MustParse("2")},
			want: Quantities{"cpu": resource.MustParse("2")},
			ok:   true,
		},
		{
			name: "short in one dimension",
			have: Quantities{"cpu": resource.MustParse("4"), "memory": resource.MustParse("1Gi")},
			want: Quantities{"cpu": resource.MustParse("2"), "memory": resource.MustParse("4Gi")},
			ok:   false,
		},
		{
			// A Worker reports everything it has, so a dimension it never
			// reported is one it has none of. Asking for a GPU must not land on
			// a Worker that never said it had one.
			name: "a dimension the worker never reported is none of it",
			have: Quantities{"cpu": resource.MustParse("4")},
			want: Quantities{"cpu": resource.MustParse("2"), "nvidia.com/gpu": resource.MustParse("1")},
			ok:   false,
		},
		{
			// Asking for zero of something absent is still satisfiable.
			name: "asking for none of an absent dimension fits",
			have: Quantities{"cpu": resource.MustParse("4")},
			want: Quantities{"nvidia.com/gpu": resource.MustParse("0")},
			ok:   true,
		},
		{
			name: "an actor asking for nothing fits anywhere",
			have: Quantities{"cpu": resource.MustParse("0")},
			want: nil,
			ok:   true,
		},
		{
			// What is left goes negative once a Worker is overcommitted, and
			// nothing more may be placed on it.
			name: "an overcommitted dimension covers nothing",
			have: Quantities{"cpu": *resource.NewMilliQuantity(-500, resource.DecimalSI)},
			want: Quantities{"cpu": resource.MustParse("1")},
			ok:   false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.have.Covers(tc.want); got != tc.ok {
				t.Errorf("Covers() = %v, want %v", got, tc.ok)
			}
		})
	}
}

func TestCPUMemory(t *testing.T) {
	got := CPUMemory(2500, 8<<30)
	want := limits("cpu", "2500m", "memory", "8Gi")
	if !proto.Equal(got, want) {
		t.Errorf("CPUMemory() = %v, want %v", got, want)
	}
	if got := CPUMemory(0, 0); got != nil {
		t.Errorf("CPUMemory(0, 0) = %v, want nil", got)
	}
}
