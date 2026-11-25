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

package fakeip

import (
	"container/list"
	"log/slog"
	"math/rand/v2"
	"net/netip"
	"sync"
)

// capacity is how many names a table holds. It bounds the worker memory one
// actor's lookups can take.
const capacity = 4096

// Table holds one actor's names and the fake address of each. It is safe for
// concurrent use.
//
// An address is given out once in the actor's life. A name that loses its
// address gets a new one the next time it is looked up, and the old address
// never comes back, so a client that kept it cannot reach a different name.
type Table struct {
	mu      sync.Mutex
	byName  map[string]*list.Element
	byIndex map[uint32]*list.Element
	// recent holds every entry, most recently used first.
	recent *list.List
	// next is the pool position the next new name gets, and left is how many
	// positions are still to be given out.
	next     uint32
	left     uint32
	capacity int
	// log carries the actor's attributes, so the warning below says whose
	// addresses ran out.
	log *slog.Logger
	// ranOut is set once the table has warned that it has no address left.
	ranOut bool
}

// entry is one name and the pool position of its address.
type entry struct {
	name  string
	index uint32
}

// New returns an empty table that logs to log.
func New(log *slog.Logger) *Table {
	// The start is random because an actor restored from a snapshot that
	// carries no table may still hold addresses from an older one, and those
	// should not match the names this table gives addresses to.
	return newTable(log, capacity, rand.Uint32N(poolSize), poolSize)
}

func newTable(log *slog.Logger, capacity int, next, left uint32) *Table {
	return &Table{
		log:      log,
		byName:   make(map[string]*list.Element),
		byIndex:  make(map[uint32]*list.Element),
		recent:   list.New(),
		next:     next,
		left:     left,
		capacity: capacity,
	}
}

// Address returns the fake address of name, giving it one if it has none.
// When the table is full, the name used longest ago loses its address. It
// reports false once every address in the pool has been given out.
func (t *Table) Address(name string) (netip.Addr, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if element, ok := t.byName[name]; ok {
		t.recent.MoveToFront(element)
		return addressAt(element.Value.(entry).index), true
	}
	index, ok := t.unusedIndex()
	if !ok {
		// TODO(liorlieberman): once the gateway decides plain TCP on the name
		// (#3873), should a used-up table reuse addresses that have gone unused
		// for a long time, say a day, instead of answering with real addresses?

		// Once, since every new name the actor looks up lands here from now on.
		if !t.ranOut {
			t.ranOut = true
			t.log.Warn("The actor has used up its fake DNS addresses; names it has not looked up before now resolve to their real addresses")
		}
		return netip.Addr{}, false
	}
	if t.recent.Len() >= t.capacity {
		oldest := t.recent.Remove(t.recent.Back()).(entry)
		delete(t.byName, oldest.name)
		delete(t.byIndex, oldest.index)
	}
	t.add(name, index)
	return addressAt(index), true
}

// Name returns the name addr was given to. It reports false for an address
// that belongs to no name, which includes one whose name lost it.
func (t *Table) Name(addr netip.Addr) (string, bool) {
	if !Contains(addr) {
		return "", false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	element, ok := t.byIndex[indexOf(addr)]
	if !ok {
		return "", false
	}
	t.recent.MoveToFront(element)
	return element.Value.(entry).name, true
}

// add records name at index as the most recently used entry.
func (t *Table) add(name string, index uint32) {
	element := t.recent.PushFront(entry{name: name, index: index})
	t.byName[name] = element
	t.byIndex[index] = element
}

// unusedIndex returns the next pool position that was never given out.
func (t *Table) unusedIndex() (uint32, bool) {
	for t.left > 0 {
		index := t.next
		t.next = (t.next + 1) % poolSize
		t.left--
		if usable(index) {
			return index, true
		}
	}
	return 0, false
}
