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
	"encoding/binary"
	"net/netip"
)

// pool is the range fake addresses come from. RFC 2544 reserves it for
// benchmarking, so no real destination should be in it.
var pool = netip.MustParsePrefix("198.18.0.0/15")

// poolSize is the number of addresses in pool.
const poolSize = 1 << 17

// Contains reports whether addr is in the fake address range.
func Contains(addr netip.Addr) bool {
	return pool.Contains(addr.Unmap())
}

// addressAt returns the address at position index in the pool.
func addressAt(index uint32) netip.Addr {
	first := pool.Addr().As4()
	var address [4]byte
	binary.BigEndian.PutUint32(address[:], binary.BigEndian.Uint32(first[:])+index)
	return netip.AddrFrom4(address)
}

// indexOf returns the position of a pool address in the pool.
func indexOf(addr netip.Addr) uint32 {
	first, address := pool.Addr().As4(), addr.Unmap().As4()
	return binary.BigEndian.Uint32(address[:]) - binary.BigEndian.Uint32(first[:])
}

// usable reports whether the address at index can be handed out. Addresses
// ending in .0 and .255 are left out because some network stacks treat them as
// network and broadcast addresses.
func usable(index uint32) bool {
	last := byte(index)
	return last != 0 && last != 255
}
