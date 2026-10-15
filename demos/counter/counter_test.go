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

package main

import (
	"slices"
	"testing"
)

func TestAssignedAddrs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ifaces []ifaceInfo
		wantV4 []string
		wantV6 []string
	}{{
		name:   "no interfaces",
		ifaces: nil,
		wantV4: []string{},
		wantV6: []string{},
	}, {
		// The sandbox as it looks today: the actor veth plus loopback.
		name: "actor veth IPv4 only",
		ifaces: []ifaceInfo{
			{Name: "lo", CIDRs: []string{"127.0.0.1/8", "::1/128"}},
			{Name: "eth0", CIDRs: []string{"169.254.17.2/30", "fe80::a8:1eff:fe00:2/64"}},
		},
		wantV4: []string{"169.254.17.2"},
		wantV6: []string{},
	}, {
		// The whole point of the endpoint: fe80:: alone must not read as IPv6,
		// or the dual-stack assertion passes on an IPv4-only actor.
		name: "link-local IPv6 alone is not IPv6",
		ifaces: []ifaceInfo{
			{Name: "eth0", CIDRs: []string{"fe80::a8:1eff:fe00:2/64"}},
		},
		wantV4: []string{},
		wantV6: []string{},
	}, {
		name: "actor veth dual stack",
		ifaces: []ifaceInfo{
			{Name: "lo", CIDRs: []string{"127.0.0.1/8", "::1/128"}},
			{Name: "eth0", CIDRs: []string{"169.254.17.2/30", "fd00:169:254::2/126", "fe80::a8:1eff:fe00:2/64"}},
		},
		wantV4: []string{"169.254.17.2"},
		wantV6: []string{"fd00:169:254::2"},
	}, {
		// A ULA is the only kind of IPv6 the actor veth ever carries.
		name: "ULA counts as IPv6",
		ifaces: []ifaceInfo{
			{Name: "eth0", CIDRs: []string{"fd00:10:244::22/64"}},
		},
		wantV4: []string{},
		wantV6: []string{"fd00:10:244::22"},
	}, {
		name: "loopback alone is neither family",
		ifaces: []ifaceInfo{
			{Name: "lo", CIDRs: []string{"127.0.0.1/8", "::1/128"}},
		},
		wantV4: []string{},
		wantV6: []string{},
	}, {
		// net.Addr.String() never spells an IPv4 address this way, but a
		// v4-mapped form must classify as IPv4 rather than inflate the v6 list.
		name: "v4-mapped IPv6 is IPv4",
		ifaces: []ifaceInfo{
			{Name: "eth0", CIDRs: []string{"::ffff:169.254.17.2/126"}},
		},
		wantV4: []string{"169.254.17.2"},
		wantV6: []string{},
	}, {
		name: "unparseable entries are skipped",
		ifaces: []ifaceInfo{
			{Name: "eth0", CIDRs: []string{"not-an-address", "169.254.17.2/30"}},
		},
		wantV4: []string{"169.254.17.2"},
		wantV6: []string{},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			gotV4, gotV6 := assignedAddrs(tc.ifaces)
			if !slices.Equal(gotV4, tc.wantV4) {
				t.Errorf("assignedAddrs() v4 = %v, want %v", gotV4, tc.wantV4)
			}
			if !slices.Equal(gotV6, tc.wantV6) {
				t.Errorf("assignedAddrs() v6 = %v, want %v", gotV6, tc.wantV6)
			}
		})
	}
}

// TestCurrentNetInfo runs the collector against the machine's real interfaces.
// It cannot assert which families are present, only that every interface is
// reported and that the classification is a subset of what was collected.
func TestCurrentNetInfo(t *testing.T) {
	info, err := currentNetInfo()
	if err != nil {
		t.Fatalf("currentNetInfo() error = %v", err)
	}
	if len(info.Interfaces) == 0 {
		t.Fatal("currentNetInfo() reported no interfaces")
	}
	wantV4, wantV6 := assignedAddrs(info.Interfaces)
	if !slices.Equal(info.IPv4, wantV4) {
		t.Errorf("IPv4 = %v, want %v", info.IPv4, wantV4)
	}
	if !slices.Equal(info.IPv6, wantV6) {
		t.Errorf("IPv6 = %v, want %v", info.IPv6, wantV6)
	}
}
