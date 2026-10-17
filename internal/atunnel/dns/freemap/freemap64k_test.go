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

package freemap

import (
	"math/rand/v2"
	"testing"
)

// asInt flattens a (index, ok) result into an int, with -1 for !ok, so tests
// can compare against a single expected value.
func asInt(idx uint16, ok bool) int {
	if !ok {
		return -1
	}
	return int(idx)
}

// fill sets every index in [start, end).
func fill(m *Map64k, start, end int) {
	for i := start; i < end; i++ {
		m.Set(uint16(i))
	}
}

// refMap pairs a Map64k with a naive boolean-slice model of it.
type refMap struct {
	m         Map64k
	occupied  []bool
	wantCount int
}

func newRefMap() *refMap {
	return &refMap{occupied: make([]bool, map64kBits)}
}

func (r *refMap) set(idx uint16) {
	if !r.occupied[idx] {
		r.occupied[idx] = true
		r.wantCount++
	}
	r.m.Set(idx)
}

func (r *refMap) clear(idx uint16) {
	if r.occupied[idx] {
		r.occupied[idx] = false
		r.wantCount--
	}
	r.m.Clear(idx)
}

func (r *refMap) naiveFindFirstUnsetRange(start uint16, end int) int {
	for idx := int(start); idx < end; idx++ {
		if !r.occupied[idx] {
			return idx
		}
	}
	return -1
}

func (r *refMap) naiveFindFirstUnsetFrom(start uint16) int {
	if idx := r.naiveFindFirstUnsetRange(start, map64kBits); idx != -1 {
		return idx
	}
	return r.naiveFindFirstUnsetRange(0, int(start))
}

// checkRange compares findFirstUnsetRange against the model.
func (r *refMap) checkRange(t *testing.T, step int, start uint16, end int) {
	t.Helper()
	if got, want := r.m.findFirstUnsetRange(start, end), r.naiveFindFirstUnsetRange(start, end); got != want {
		t.Fatalf("step %d: findFirstUnsetRange(%d, %d) = %d, want %d", step, start, end, got, want)
	}
}

// checkFrom compares FindFirstUnsetFrom and FindFirstUnset against the model.
func (r *refMap) checkFrom(t *testing.T, step int, start uint16) {
	t.Helper()
	if got, want := asInt(r.m.FindFirstUnsetFrom(start)), r.naiveFindFirstUnsetFrom(start); got != want {
		t.Fatalf("step %d: FindFirstUnsetFrom(%d) = %d, want %d", step, start, got, want)
	}
	if got, want := asInt(r.m.FindFirstUnset()), r.naiveFindFirstUnsetRange(0, map64kBits); got != want {
		t.Fatalf("step %d: FindFirstUnset() = %d, want %d", step, got, want)
	}
}

// checkInvariants compares Count and IsFull against the model.
func (r *refMap) checkInvariants(t *testing.T, step int) {
	t.Helper()
	if got := r.m.Count(); got != r.wantCount {
		t.Fatalf("step %d: Count() = %d, want %d", step, got, r.wantCount)
	}
	if got, want := r.m.IsFull(), r.wantCount == map64kBits; got != want {
		t.Fatalf("step %d: IsFull() = %v, want %v", step, got, want)
	}
}

func TestGetIndex(t *testing.T) {
	tests := []struct {
		index uint16
		want  bitsetIndex
	}{
		{index: 0, want: bitsetIndex{l0: 0, l0Bit: 0, l1: 0, l1Bit: 0}},
		{index: 63, want: bitsetIndex{l0: 0, l0Bit: 63, l1: 0, l1Bit: 0}},
		{index: 64, want: bitsetIndex{l0: 1, l0Bit: 0, l1: 0, l1Bit: 1}},
		{index: 127, want: bitsetIndex{l0: 1, l0Bit: 63, l1: 0, l1Bit: 1}},
		{index: 4095, want: bitsetIndex{l0: 63, l0Bit: 63, l1: 0, l1Bit: 63}},
		{index: 4096, want: bitsetIndex{l0: 64, l0Bit: 0, l1: 1, l1Bit: 0}},
		{index: 65535, want: bitsetIndex{l0: 1023, l0Bit: 63, l1: 15, l1Bit: 63}},
	}

	for _, tc := range tests {
		got := getIndex(tc.index)
		if got != tc.want {
			t.Errorf("getIndex(%d) = %+v, want %+v", tc.index, got, tc.want)
		}
	}
}

func TestMap64k_Basic(t *testing.T) {
	var m Map64k
	if got := m.Count(); got != 0 {
		t.Errorf("expected count 0, got %d", got)
	}
	if m.IsFull() {
		t.Errorf("expected IsFull() = false on empty map")
	}
	if got := asInt(m.FindFirstUnset()); got != 0 {
		t.Errorf("expected first unset 0, got %d", got)
	}
	if m.Get(0) {
		t.Errorf("expected Get(0) = false initially")
	}

	m.Set(0)
	if got := m.Count(); got != 1 {
		t.Errorf("expected count 1, got %d", got)
	}
	if m.IsFull() {
		t.Errorf("expected IsFull() = false with 1 bit set")
	}
	if !m.Get(0) {
		t.Errorf("expected Get(0) = true after Set(0)")
	}
	if got := asInt(m.FindFirstUnset()); got != 1 {
		t.Errorf("expected first unset 1, got %d", got)
	}

	// Idempotent Set
	m.Set(0)
	if got := m.Count(); got != 1 {
		t.Errorf("expected count 1 after redundant set, got %d", got)
	}

	m.Clear(0)
	if got := m.Count(); got != 0 {
		t.Errorf("expected count 0, got %d", got)
	}
	if m.Get(0) {
		t.Errorf("expected Get(0) = false after Clear(0)")
	}
	if got := asInt(m.FindFirstUnset()); got != 0 {
		t.Errorf("expected first unset 0, got %d", got)
	}

	// Idempotent Clear
	m.Clear(0)
	if got := m.Count(); got != 0 {
		t.Errorf("expected count 0 after redundant clear, got %d", got)
	}
}

func TestMap64k_Get(t *testing.T) {
	var m Map64k
	testIndices := []uint16{0, 1, 63, 64, 127, 1024, 4095, 4096, 65534, 65535}

	for _, idx := range testIndices {
		if m.Get(idx) {
			t.Errorf("Get(%d) = true on empty map, want false", idx)
		}
		m.Set(idx)
		if !m.Get(idx) {
			t.Errorf("Get(%d) = false after Set, want true", idx)
		}
		m.Clear(idx)
		if m.Get(idx) {
			t.Errorf("Get(%d) = true after Clear, want false", idx)
		}
	}
}

func TestMap64k_HierarchyPropagation(t *testing.T) {
	var m Map64k

	// 1. Fill word 0 (bits 0..63). L1 bit 0 should be set, L2 should still be 0.
	fill(&m, 0, 64)
	if (m.l1[0] & 1) == 0 {
		t.Errorf("expected l1[0] bit 0 to be set")
	}
	if m.l2 != 0 {
		t.Errorf("expected l2 to be 0, got %x", m.l2)
	}

	// Clearing one bit in word 0 should clear L1 bit 0.
	m.Clear(10)
	if (m.l1[0] & 1) != 0 {
		t.Errorf("expected l1[0] bit 0 to be cleared after clearing bit 10")
	}

	// Re-setting bit 10 should restore L1 bit 0.
	m.Set(10)
	if (m.l1[0] & 1) == 0 {
		t.Errorf("expected l1[0] bit 0 to be set after re-setting bit 10")
	}

	// 2. Fill all 4096 bits in L1 word 0 (words 0..63). L2 bit 0 should be set.
	fill(&m, 64, 4096)
	if (m.l2 & 1) == 0 {
		t.Errorf("expected l2 bit 0 to be set after filling 4096 bits")
	}

	// Clearing one bit should clear L1 bit and propagate to L2 bit 0.
	m.Clear(2000)
	if (m.l2 & 1) != 0 {
		t.Errorf("expected l2 bit 0 to be cleared after clearing bit 2000")
	}
}

func TestMap64k_FindFirstUnsetFrom_Empty(t *testing.T) {
	var m Map64k
	testIndices := []uint16{0, 1, 63, 64, 100, 1023, 1024, 4095, 4096, 65535}
	for _, idx := range testIndices {
		if got := asInt(m.FindFirstUnsetFrom(idx)); got != int(idx) {
			t.Errorf("FindFirstUnsetFrom(%d) on empty map = %d, want %d", idx, got, idx)
		}
	}
}

func TestMap64k_FindFirstUnsetFrom_Full(t *testing.T) {
	var m Map64k
	fill(&m, 0, map64kBits)
	if got := m.Count(); got != map64kBits {
		t.Fatalf("expected count %d, got %d", map64kBits, got)
	}
	if !m.IsFull() {
		t.Errorf("expected IsFull() = true on full map")
	}
	if _, ok := m.FindFirstUnset(); ok {
		t.Errorf("FindFirstUnset on full map returned ok = true")
	}
	for _, idx := range []uint16{0, 1, 100, 4096, 65535} {
		if _, ok := m.FindFirstUnsetFrom(idx); ok {
			t.Errorf("FindFirstUnsetFrom(%d) on full map returned ok = true", idx)
		}
	}
}

func TestMap64k_FindFirstUnsetFrom_SingleHole(t *testing.T) {
	var m Map64k
	fill(&m, 0, map64kBits)

	testHoles := []uint16{0, 1, 63, 64, 127, 128, 4095, 4096, 32768, 65534, 65535}
	for _, hole := range testHoles {
		m.Clear(hole)
		if m.IsFull() {
			t.Errorf("hole %d: expected IsFull() = false", hole)
		}
		if got := asInt(m.FindFirstUnset()); got != int(hole) {
			t.Errorf("hole %d: FindFirstUnset = %d, want %d", hole, got, hole)
		}

		// Starting from any index should find the only hole
		for _, start := range []uint16{0, hole, hole + 1, 65535} {
			if got := asInt(m.FindFirstUnsetFrom(start)); got != int(hole) {
				t.Errorf("hole %d, start %d: FindFirstUnsetFrom = %d, want %d", hole, start, got, hole)
			}
		}

		m.Set(hole)
		if !m.IsFull() {
			t.Errorf("hole %d: expected IsFull() = true after re-setting hole", hole)
		}
	}
}

func TestMap64k_FindFirstUnsetFrom_Levels(t *testing.T) {
	check := func(t *testing.T, m *Map64k, start uint16, want int) {
		t.Helper()
		if got := asInt(m.FindFirstUnsetFrom(start)); got != want {
			t.Errorf("FindFirstUnsetFrom(%d) = %d, want %d", start, got, want)
		}
	}

	t.Run("same L0 word", func(t *testing.T) {
		var m Map64k
		fill(&m, 0, 10)
		for _, start := range []uint16{0, 5, 10} {
			check(t, &m, start, 10)
		}
	})

	t.Run("next L0 word in same L1 word", func(t *testing.T) {
		var m Map64k
		// Fill L0 word 0 (bits 0..63) and bits 0..4 of word 1 (bits 64..68)
		fill(&m, 0, 69)
		for _, start := range []uint16{0, 63, 64} {
			check(t, &m, start, 69)
		}
	})

	t.Run("next L1 word from L2", func(t *testing.T) {
		var m Map64k
		// Fill entire L1 word 0 (bits 0..4095) and bits 4096..4099 in L1 word 1.
		fill(&m, 0, 4100)
		for _, start := range []uint16{0, 2000, 4095, 4096} {
			check(t, &m, start, 4100)
		}

		// Fill the rest of L1 word 1 and bits 8192..8199 in L1 word 2.
		fill(&m, 4100, 8200)
		for _, start := range []uint16{0, 4095, 4096, 8191, 8192} {
			check(t, &m, start, 8200)
		}
	})

	t.Run("wrap around", func(t *testing.T) {
		var m Map64k
		fill(&m, 100, map64kBits)
		fill(&m, 0, 10)
		// Search from 100 should wrap around and find 10
		for _, start := range []uint16{100, 65535} {
			check(t, &m, start, 10)
		}
	})

	t.Run("wrap around with last bit set", func(t *testing.T) {
		var m Map64k
		m.Set(65535)
		check(t, &m, 65535, 0)
	})
}

func TestMap64k_FindFirstUnsetRange(t *testing.T) {
	var m Map64k

	// Empty ranges
	if got := m.findFirstUnsetRange(5, 5); got != -1 {
		t.Errorf("range [5, 5) = %d, want -1", got)
	}
	if got := m.findFirstUnsetRange(10, 5); got != -1 {
		t.Errorf("range [10, 5) = %d, want -1", got)
	}

	// Range within single word
	fill(&m, 10, 20)
	if got := m.findFirstUnsetRange(10, 20); got != -1 {
		t.Errorf("range [10, 20) fully set, got %d, want -1", got)
	}
	if got := m.findFirstUnsetRange(10, 25); got != 20 {
		t.Errorf("range [10, 25), got %d, want 20", got)
	}
	if got := m.findFirstUnsetRange(5, 20); got != 5 {
		t.Errorf("range [5, 20), got %d, want 5", got)
	}

	// Range spanning word boundary
	fill(&m, 60, 64)
	// Bits 60..63 are set, 64 is unset
	if got := m.findFirstUnsetRange(60, 64); got != -1 {
		t.Errorf("range [60, 64) fully set, got %d, want -1", got)
	}
	if got := m.findFirstUnsetRange(60, 70); got != 64 {
		t.Errorf("range [60, 70), got %d, want 64", got)
	}

	// Range spanning L1 boundary (word 63 to word 64, bit 4095 to 4096)
	fill(&m, 4090, 4096)
	if got := m.findFirstUnsetRange(4090, 4096); got != -1 {
		t.Errorf("range [4090, 4096) fully set, got %d, want -1", got)
	}
	if got := m.findFirstUnsetRange(4090, 4100); got != 4096 {
		t.Errorf("range [4090, 4100), got %d, want 4096", got)
	}

	// Full span
	if got := m.findFirstUnsetRange(0, map64kBits); got != 0 {
		t.Errorf("range [0, map64kBits), got %d, want 0", got)
	}

	// Full map
	fill(&m, 0, map64kBits)
	if got := m.findFirstUnsetRange(0, map64kBits); got != -1 {
		t.Errorf("range [0, map64kBits) on full map, got %d, want -1", got)
	}
	if got := m.findFirstUnsetRange(100, 200); got != -1 {
		t.Errorf("range [100, 200) on full map, got %d, want -1", got)
	}
}

// TestMap64k_FindFirstUnsetRange_Cutoffs checks that a free bit found by the
// L1 (step 2) and L2 (step 3) scans is rejected when it lies past `end`.
func TestMap64k_FindFirstUnsetRange_Cutoffs(t *testing.T) {
	t.Run("L1 scan", func(t *testing.T) {
		var m Map64k
		fill(&m, 0, 128) // L0 words 0 and 1 full; first free is 128.
		if got := m.findFirstUnsetRange(0, 100); got != -1 {
			t.Errorf("range [0, 100), got %d, want -1", got)
		}
		if got := m.findFirstUnsetRange(0, 128); got != -1 {
			t.Errorf("range [0, 128), got %d, want -1", got)
		}
		if got := m.findFirstUnsetRange(0, 129); got != 128 {
			t.Errorf("range [0, 129), got %d, want 128", got)
		}
	})

	t.Run("L2 scan", func(t *testing.T) {
		var m Map64k
		fill(&m, 0, 8192) // L1 words 0 and 1 full; first free is 8192.
		if got := m.findFirstUnsetRange(100, 5000); got != -1 {
			t.Errorf("range [100, 5000), got %d, want -1", got)
		}
		if got := m.findFirstUnsetRange(100, 8192); got != -1 {
			t.Errorf("range [100, 8192), got %d, want -1", got)
		}
		if got := m.findFirstUnsetRange(100, 9000); got != 8192 {
			t.Errorf("range [100, 9000), got %d, want 8192", got)
		}
	})
}

// TestMap64k_Differential against a naive linear search on a boolean slice.
func TestMap64k_Differential(t *testing.T) {
	ref := newRefMap()
	r := rand.New(rand.NewPCG(42, 1337))

	for step := range 5000 {
		op := r.IntN(12)
		switch {
		case op < 3: // Set a random bit
			idx := uint16(r.IntN(map64kBits))
			ref.set(idx)
			if !ref.m.Get(idx) {
				t.Fatalf("step %d: Get(%d) = false after Set", step, idx)
			}
		case op < 5: // Clear a random bit
			idx := uint16(r.IntN(map64kBits))
			ref.clear(idx)
			if ref.m.Get(idx) {
				t.Fatalf("step %d: Get(%d) = true after Clear", step, idx)
			}
		case op == 5: // Set an entire 64-bit L0 word
			base := uint16(r.IntN(l0words) * wordSize)
			for i := range uint16(wordSize) {
				ref.set(base + i)
			}
		case op == 6: // Clear an entire 64-bit L0 word
			base := uint16(r.IntN(l0words) * wordSize)
			for i := range uint16(wordSize) {
				ref.clear(base + i)
			}
		case op == 7: // Set or clear an entire 4096-bit L1 block
			base := uint16(r.IntN(l1words) * wordSize * wordSize)
			set := r.IntN(2) == 0
			for i := range uint16(wordSize * wordSize) {
				if set {
					ref.set(base + i)
				} else {
					ref.clear(base + i)
				}
			}
		case op < 10: // Query range
			start := uint16(r.IntN(map64kBits))
			end := int(start) + r.IntN(map64kBits-int(start)+1)
			ref.checkRange(t, step, start, end)
		default: // Query from
			ref.checkFrom(t, step, uint16(r.IntN(map64kBits)))
		}

		ref.checkInvariants(t, step)
	}
}

func TestMap64k_Differential_Dense(t *testing.T) {
	ref := newRefMap()
	r := rand.New(rand.NewPCG(99, 12345))

	// Start completely full so all L1 and L2 summary bits are set, then punch a few holes.
	for i := range map64kBits {
		ref.set(uint16(i))
	}
	// holes records every cleared index. It may contain duplicates (clearing an
	// already-free bit appends it again); that is harmless and keeps the
	// property that an empty holes list means the map is completely full, so
	// the test regularly drives the map back to full.
	var holes []uint16
	for range 16 {
		idx := uint16(r.IntN(map64kBits))
		ref.clear(idx)
		holes = append(holes, idx)
	}

	for step := range 5000 {
		op := r.IntN(10)
		switch {
		case op < 3: // Plug an existing hole (or random bit) so L0/L1/L2 words become full again
			if len(holes) > 0 {
				hIdx := r.IntN(len(holes))
				ref.set(holes[hIdx])
				holes[hIdx] = holes[len(holes)-1]
				holes = holes[:len(holes)-1]
			} else {
				ref.set(uint16(r.IntN(map64kBits)))
			}
		case op < 5: // Punch a new hole
			idx := uint16(r.IntN(map64kBits))
			ref.clear(idx)
			holes = append(holes, idx)
		case op < 7: // Query range
			start := uint16(r.IntN(map64kBits))
			end := int(start) + r.IntN(map64kBits-int(start)+1)
			ref.checkRange(t, step, start, end)
		default: // Query from and FindFirstUnset
			ref.checkFrom(t, step, uint16(r.IntN(map64kBits)))
		}

		ref.checkInvariants(t, step)
	}
}

// singleHoleMap returns a full map with only index 50000 free.
func singleHoleMap() *Map64k {
	var m Map64k
	fill(&m, 0, map64kBits)
	m.Clear(50000)
	return &m
}

// sparseMap returns a map with ~37% of bits set at random.
func sparseMap() *Map64k {
	var m Map64k
	r := rand.New(rand.NewPCG(1, 2))
	for range 30000 {
		m.Set(uint16(r.IntN(map64kBits)))
	}
	return &m
}

func BenchmarkFindFirstUnsetFrom(b *testing.B) {
	b.Run("sparse", func(b *testing.B) {
		m := sparseMap()
		var i uint16
		for b.Loop() {
			m.FindFirstUnsetFrom(i)
			i++
		}
	})

	b.Run("dense_single_hole", func(b *testing.B) {
		m := singleHoleMap()
		var i uint16
		for b.Loop() {
			m.FindFirstUnsetFrom(i)
			i++
		}
	})
}

func BenchmarkFindFirstUnsetRange(b *testing.B) {
	b.Run("sparse", func(b *testing.B) {
		m := sparseMap()
		var i int
		for b.Loop() {
			start := uint16(i % (map64kBits - 100))
			m.findFirstUnsetRange(start, int(start)+100)
			i++
		}
	})

	b.Run("dense_single_hole", func(b *testing.B) {
		m := singleHoleMap()
		var i int
		for b.Loop() {
			start := uint16(i % (map64kBits - 100))
			m.findFirstUnsetRange(start, map64kBits)
			i++
		}
	})
}

// BenchmarkSetClear measures an allocate/free cycle, including the L1/L2
// summary updates when a word transitions between full and not full.
func BenchmarkSetClear(b *testing.B) {
	b.Run("sparse", func(b *testing.B) {
		var m Map64k
		var i uint16
		for b.Loop() {
			m.Set(i)
			m.Clear(i)
			i++
		}
	})

	b.Run("dense_single_hole", func(b *testing.B) {
		m := singleHoleMap()
		for b.Loop() {
			m.Set(50000) // Fills L0, L1 and L2.
			m.Clear(50000)
		}
	})
}
