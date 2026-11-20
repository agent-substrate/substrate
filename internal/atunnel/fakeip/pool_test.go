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
	"net/netip"
	"testing"
)

func TestContains(t *testing.T) {
	tests := []struct {
		addr string
		want bool
	}{
		{"198.18.0.0", true},
		{"198.18.0.1", true},
		{"198.19.255.255", true},
		{"::ffff:198.18.0.1", true},
		{"198.17.255.255", false},
		{"198.20.0.0", false},
		{"169.254.17.1", false},
		{"10.0.0.1", false},
		{"2001:db8::1", false},
	}
	for _, tc := range tests {
		t.Run(tc.addr, func(t *testing.T) {
			if got := Contains(netip.MustParseAddr(tc.addr)); got != tc.want {
				t.Errorf("Contains(%s) = %v, want %v", tc.addr, got, tc.want)
			}
		})
	}
	if Contains(netip.Addr{}) {
		t.Error("Contains reports the zero Addr as a fake address")
	}
}

func TestAddressAtAndIndexOf(t *testing.T) {
	tests := []struct {
		index uint32
		addr  string
	}{
		{0, "198.18.0.0"},
		{1, "198.18.0.1"},
		{256, "198.18.1.0"},
		{1 << 16, "198.19.0.0"},
		{poolSize - 1, "198.19.255.255"},
	}
	for _, tc := range tests {
		t.Run(tc.addr, func(t *testing.T) {
			addr := netip.MustParseAddr(tc.addr)
			if got := addressAt(tc.index); got != addr {
				t.Errorf("addressAt(%d) = %s, want %s", tc.index, got, addr)
			}
			if got := indexOf(addr); got != tc.index {
				t.Errorf("indexOf(%s) = %d, want %d", addr, got, tc.index)
			}
		})
	}
}

func TestPoolSizeMatchesPrefix(t *testing.T) {
	if want := uint32(1) << (32 - pool.Bits()); poolSize != want {
		t.Fatalf("poolSize = %d, want %d for %s", poolSize, want, pool)
	}
	if last := addressAt(poolSize - 1); !pool.Contains(last) || pool.Contains(last.Next()) {
		t.Fatalf("the pool's last address is %s, which is not the end of %s", last, pool)
	}
}

func TestUsable(t *testing.T) {
	for index := range uint32(poolSize) {
		last := addressAt(index).As4()[3]
		if got, want := usable(index), last != 0 && last != 255; got != want {
			t.Fatalf("usable(%d) = %v for %s, want %v", index, got, addressAt(index), want)
		}
	}
}
