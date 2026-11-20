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
	"bytes"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"testing"
)

// usableAddresses is how many pool addresses a table can hand out.
const usableAddresses = poolSize - 2*(poolSize/256)

// mustAddress returns the address table gives name, failing the test if the
// table has none left.
func mustAddress(t *testing.T, table *Table, name string) netip.Addr {
	t.Helper()
	addr, ok := table.Address(name)
	if !ok {
		t.Fatalf("Address(%q) found no address left", name)
	}
	return addr
}

func TestAddressIsStablePerName(t *testing.T) {
	table := newTable(slog.Default(), 8, 0, poolSize)
	first := mustAddress(t, table, "api.example.com")
	other := mustAddress(t, table, "db.example.com")
	if first == other {
		t.Fatalf("two names share the address %s", first)
	}
	if again := mustAddress(t, table, "api.example.com"); again != first {
		t.Fatalf("the address of api.example.com changed from %s to %s", first, again)
	}
	for _, addr := range []netip.Addr{first, other} {
		if !Contains(addr) || !usable(indexOf(addr)) {
			t.Errorf("%s is not a usable fake address", addr)
		}
	}
}

func TestName(t *testing.T) {
	table := newTable(slog.Default(), 8, 0, poolSize)
	addr := mustAddress(t, table, "api.example.com")

	tests := []struct {
		name     string
		addr     netip.Addr
		wantName string
		wantOK   bool
	}{
		{name: "known address", addr: addr, wantName: "api.example.com", wantOK: true},
		{name: "known address, IPv4-mapped", addr: netip.AddrFrom16(addr.As16()), wantName: "api.example.com", wantOK: true},
		{name: "fake address no name holds", addr: addr.Next()},
		{name: "address outside the pool", addr: netip.MustParseAddr("93.184.216.34")},
		{name: "IPv6 address", addr: netip.MustParseAddr("2001:db8::1")},
		{name: "zero address", addr: netip.Addr{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := table.Name(tc.addr)
			if got != tc.wantName || ok != tc.wantOK {
				t.Errorf("Name(%s) = %q, %v, want %q, %v", tc.addr, got, ok, tc.wantName, tc.wantOK)
			}
		})
	}
}

func TestFullTableForgetsNameUsedLongestAgo(t *testing.T) {
	tests := []struct {
		name string
		// touch uses an entry after a, b and c were added in that order.
		touch      func(table *Table, a netip.Addr)
		wantForgot string
	}{
		{name: "nothing used since", touch: func(*Table, netip.Addr) {}, wantForgot: "a.example.com"},
		{name: "lookup of the name", touch: func(table *Table, _ netip.Addr) { table.Address("a.example.com") }, wantForgot: "b.example.com"},
		{name: "connection to the address", touch: func(table *Table, a netip.Addr) { table.Name(a) }, wantForgot: "b.example.com"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			table := newTable(slog.Default(), 3, 0, poolSize)
			addresses := map[string]netip.Addr{}
			for _, name := range []string{"a.example.com", "b.example.com", "c.example.com"} {
				addresses[name] = mustAddress(t, table, name)
			}
			tc.touch(table, addresses["a.example.com"])
			mustAddress(t, table, "d.example.com")

			for name, addr := range addresses {
				got, ok := table.Name(addr)
				if name == tc.wantForgot {
					if ok {
						t.Errorf("Name(%s) = %q, want it forgotten", addr, got)
					}
					continue
				}
				if !ok || got != name {
					t.Errorf("Name(%s) = %q, %v, want %q", addr, got, ok, name)
				}
			}
		})
	}
}

// A client may still hold the address of a forgotten name, so the name gets a
// new one when it is looked up again and the old one stays unused.
func TestForgottenNameGetsNewAddress(t *testing.T) {
	table := newTable(slog.Default(), 1, 0, poolSize)
	first := mustAddress(t, table, "a.example.com")
	mustAddress(t, table, "b.example.com")
	if again := mustAddress(t, table, "a.example.com"); again == first {
		t.Fatalf("a.example.com got %s back after losing it", first)
	}
	if name, ok := table.Name(first); ok {
		t.Fatalf("Name(%s) = %q, want the address to stay unused", first, name)
	}
}

func TestAddressIsGivenOutOnce(t *testing.T) {
	table := newTable(slog.Default(), 4, rand.Uint32N(poolSize), poolSize)
	seen := make(map[netip.Addr]bool, usableAddresses)
	for i := range usableAddresses {
		addr := mustAddress(t, table, fmt.Sprintf("host-%d.example.com", i))
		if seen[addr] {
			t.Fatalf("name %d got %s, which an earlier name had", i, addr)
		}
		if !usable(indexOf(addr)) {
			t.Fatalf("name %d got the unusable address %s", i, addr)
		}
		seen[addr] = true
	}

	// Every usable address has been given out, so a new name gets none.
	if addr, ok := table.Address("one-more.example.com"); ok {
		t.Fatalf("Address() = %s, want none left", addr)
	}
	// The names the table still holds keep theirs.
	last := fmt.Sprintf("host-%d.example.com", usableAddresses-1)
	addr, ok := table.Address(last)
	if !ok {
		t.Fatalf("Address(%q) found none, want the address it already had", last)
	}
	if name, ok := table.Name(addr); !ok || name != last {
		t.Fatalf("Name(%s) = %q, %v, want %q", addr, name, ok, last)
	}
}

func TestRunningOutWarnsOnce(t *testing.T) {
	var logs bytes.Buffer
	// Positions 1 and 2 are the only ones left, and both are usable.
	table := newTable(slog.New(slog.NewTextHandler(&logs, nil)), capacity, 1, 2)
	mustAddress(t, table, "a.example.com")
	mustAddress(t, table, "b.example.com")
	if logs.Len() != 0 {
		t.Fatalf("the table logged before running out:\n%s", logs.String())
	}

	for i := range 3 {
		if addr, ok := table.Address(fmt.Sprintf("new-%d.example.com", i)); ok {
			t.Fatalf("Address() = %s, want none left", addr)
		}
	}
	if got := strings.Count(logs.String(), "level=WARN"); got != 1 {
		t.Fatalf("the table logged %d warnings after running out, want 1:\n%s", got, logs.String())
	}
}

// tableModel is a simple table to check Table against: its names, most
// recently used first, and every address it ever saw given out.
type tableModel struct {
	capacity  int
	names     []string
	addresses map[string]netip.Addr
	givenOut  map[netip.Addr]bool
}

func (m *tableModel) use(name string) {
	if i := slices.Index(m.names, name); i >= 0 {
		m.names = slices.Delete(m.names, i, i+1)
	}
	m.names = slices.Insert(m.names, 0, name)
}

// address records that Table returned got for name and reports what is wrong
// with it, if anything.
func (m *tableModel) address(name string, got netip.Addr) error {
	if want, ok := m.addresses[name]; ok {
		m.use(name)
		if got != want {
			return fmt.Errorf("address of %s changed from %s to %s", name, want, got)
		}
		return nil
	}
	if m.givenOut[got] {
		return fmt.Errorf("%s got %s, which was given out before", name, got)
	}
	if len(m.names) == m.capacity {
		oldest := m.names[len(m.names)-1]
		m.names = m.names[:len(m.names)-1]
		delete(m.addresses, oldest)
	}
	m.givenOut[got] = true
	m.addresses[name] = got
	m.use(name)
	return nil
}

// name returns what Table.Name should return for addr.
func (m *tableModel) name(addr netip.Addr) (string, bool) {
	for name, held := range m.addresses {
		if held == addr {
			m.use(name)
			return name, true
		}
	}
	return "", false
}

func TestTableMatchesModel(t *testing.T) {
	const (
		tableCapacity = 8
		steps         = 20000
	)
	table := newTable(slog.Default(), tableCapacity, rand.Uint32N(poolSize), poolSize)
	model := &tableModel{capacity: tableCapacity, addresses: map[string]netip.Addr{}, givenOut: map[netip.Addr]bool{}}
	// More names than the table holds, so names are forgotten and come back.
	names := make([]string, 3*tableCapacity)
	for i := range names {
		names[i] = fmt.Sprintf("host-%d.example.com", i)
	}
	var issued []netip.Addr

	for step := range steps {
		if len(issued) == 0 || rand.IntN(2) == 0 {
			name := names[rand.IntN(len(names))]
			got := mustAddress(t, table, name)
			if err := model.address(name, got); err != nil {
				t.Fatalf("step %d: %v", step, err)
			}
			issued = append(issued, got)
			continue
		}
		addr := issued[rand.IntN(len(issued))]
		gotName, gotOK := table.Name(addr)
		wantName, wantOK := model.name(addr)
		if gotName != wantName || gotOK != wantOK {
			t.Fatalf("step %d: Name(%s) = %q, %v, want %q, %v", step, addr, gotName, gotOK, wantName, wantOK)
		}
	}
}

func TestTableConcurrentUse(t *testing.T) {
	table := newTable(slog.Default(), 16, 0, poolSize)
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 2000 {
				name := fmt.Sprintf("host-%d.example.com", (worker+i)%32)
				addr, ok := table.Address(name)
				if !ok {
					t.Errorf("Address(%q) found no address left", name)
					return
				}
				// Another goroutine may have pushed name out since.
				if got, ok := table.Name(addr); ok && got != name {
					t.Errorf("Name(%s) = %q, want %q", addr, got, name)
					return
				}
			}
		}()
	}
	wg.Wait()
}
