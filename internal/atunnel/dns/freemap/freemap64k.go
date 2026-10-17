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
	"math/bits"
)

const (
	map64kBits = 65536
	wordSize   = 64
	l0words    = map64kBits / wordSize // 1024 words
	l1words    = l0words / wordSize    // 16 words
	l2Mask     = (1 << l1words) - 1    // 0xFFFF
)

// Map64k is a free map that supports 64k entries. This data structure
// uses ~8 KiB of memory.
//
// The zero value is an empty map ready to use.
//
// This data structure is not thread-safe.
//
// This is represented as a hierarchical bit-set.
type Map64k struct {
	// Layer 0: Leaf-level storage (1 = occupied, 0 = empty)
	l0 [l0words]uint64
	// Layer 1: Intermediate summary (1 = corresponding L0 word is fully occupied)
	l1 [l1words]uint64
	// Layer 2: Root summary (1 = corresponding L1 word is fully occupied)
	l2 uint64
	// count is the number of bits currently set in the map.
	count int
}

// Count returns the number of bits set in the map.
func (m *Map64k) Count() int { return m.count }

// IsFull returns true if there is no more room in the map.
func (m *Map64k) IsFull() bool { return m.count == map64kBits }

// bitsetIndex into the hierarchical bitset.
type bitsetIndex struct {
	l0, l0Bit int
	l1, l1Bit int
}

// getIndex computes the index across all of the hierarchical bitsets.
func getIndex(index uint16) bitsetIndex {
	return bitsetIndex{
		l0:    int(index / wordSize),
		l0Bit: int(index % wordSize),
		l1:    int(index / wordSize / wordSize),
		l1Bit: int((index / wordSize) % wordSize),
	}
}

// Get returns true if the index is set.
func (m *Map64k) Get(index uint16) bool {
	sidx := getIndex(index)
	return m.l0[sidx.l0]&(1<<sidx.l0Bit) != 0
}

// Set marks index as occupied. Setting an occupied index is a no-op.
func (m *Map64k) Set(index uint16) {
	sidx := getIndex(index)

	// Bail out early if the slot was already set.
	if m.l0[sidx.l0]&(1<<sidx.l0Bit) != 0 {
		return
	}

	m.count++
	m.l0[sidx.l0] |= (1 << sidx.l0Bit)

	// If the L0 is full, mark l1.
	if m.l0[sidx.l0] == ^uint64(0) {
		m.l1[sidx.l1] |= (1 << sidx.l1Bit)
		// If the L1 is full, mark l2.
		if m.l1[sidx.l1] == ^uint64(0) {
			m.l2 |= (1 << sidx.l1)
		}
	}
}

// Clear marks index as free. Clearing a free index is a no-op.
func (m *Map64k) Clear(index uint16) {
	sidx := getIndex(index)

	if m.l0[sidx.l0]&(1<<sidx.l0Bit) == 0 {
		// Bit was already clear, return.
		return
	}

	m.count--
	wasFull := m.l0[sidx.l0] == ^uint64(0)
	m.l0[sidx.l0] &^= 1 << sidx.l0Bit

	// If this word was full but is no longer full, propagate the update.
	if wasFull {
		l1WasFull := m.l1[sidx.l1] == ^uint64(0)
		m.l1[sidx.l1] &^= 1 << sidx.l1Bit
		if l1WasFull {
			m.l2 &^= 1 << sidx.l1
		}
	}
}

// FindFirstUnset returns the lowest free index.
//
// Returns false if the map is full.
func (m *Map64k) FindFirstUnset() (uint16, bool) {
	if m.IsFull() {
		return 0, false
	}
	// A non-full map has at least one non-full L0 word, hence a non-full L1
	// word, hence a zero bit within the low l1words bits of l2.
	targetL1 := bits.TrailingZeros64((^m.l2) & l2Mask)
	targetL0 := (targetL1 * wordSize) + bits.TrailingZeros64(^m.l1[targetL1])
	return uint16((targetL0 * wordSize) + bits.TrailingZeros64(^m.l0[targetL0])), true
}

// FindFirstUnsetFrom returns the first free index at or after `index`. Search
// will wrap around the end of the ID space.
//
// Returns false if the map is full.
func (m *Map64k) FindFirstUnsetFrom(index uint16) (uint16, bool) {
	// Search from index to the end.
	if idx := m.findFirstUnsetRange(index, map64kBits); idx != -1 {
		return uint16(idx), true
	}
	// Wrap around: since [index, map64kBits) is completely occupied, any free
	// bit in the map must be in [0, index).
	return m.FindFirstUnset()
}

// findFirstUnsetRange searches for the first empty (0) bit in the range [start, end).
//
// Returns -1 if the entire range is completely occupied.
func (m *Map64k) findFirstUnsetRange(start uint16, end int) int {
	if int(start) >= end {
		return -1
	}

	// Fast path: nothing to find in a full map.
	if m.IsFull() {
		return -1
	}

	startIdx := getIndex(start)

	// 1. Scan remaining bits in the current Layer 0 word.
	w0 := m.l0[startIdx.l0] | ((uint64(1) << startIdx.l0Bit) - 1)
	if inv0 := ^w0; inv0 != 0 {
		idx := (startIdx.l0 * wordSize) + bits.TrailingZeros64(inv0)
		if idx < end {
			return idx
		}
		return -1
	}

	// 2. Scan remaining Layer 0 words in the current Layer 1 word.
	if nextL1Bit := startIdx.l1Bit + 1; nextL1Bit < wordSize && (startIdx.l0+1)*wordSize < end {
		w1 := m.l1[startIdx.l1] | ((uint64(1) << nextL1Bit) - 1)
		if inv1 := ^w1; inv1 != 0 {
			l0WordInL1 := bits.TrailingZeros64(inv1)
			targetL0 := (startIdx.l1 * wordSize) + l0WordInL1
			idx := (targetL0 * wordSize) + bits.TrailingZeros64(^m.l0[targetL0])
			if idx < end {
				return idx
			}
			return -1
		}
	}

	// 3. Scan remaining Layer 1 words in Layer 2 (Root).
	if nextL1 := startIdx.l1 + 1; nextL1 < l1words && nextL1*wordSize*wordSize < end {
		w2 := m.l2 | ((uint64(1) << nextL1) - 1)
		if inv2 := (^w2) & l2Mask; inv2 != 0 {
			targetL1 := bits.TrailingZeros64(inv2)
			l0WordInL1 := bits.TrailingZeros64(^m.l1[targetL1])
			targetL0 := (targetL1 * wordSize) + l0WordInL1
			idx := (targetL0 * wordSize) + bits.TrailingZeros64(^m.l0[targetL0])
			if idx < end {
				return idx
			}
			return -1
		}
	}

	return -1
}
