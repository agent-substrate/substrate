//go:build linux

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

// Package ateomnet provides shared networking configuration logic for Substrate runtime agents.
package ateomnet

import (
	"fmt"
	"net"

	"github.com/google/nftables/expr"
	"github.com/vishvananda/netlink"
)

const (
	ActorVethName    = "eth0"
	ActorVethGateway = "169.254.17.1"
	ActorVethIP      = "169.254.17.2"

	// hostVethLocalAddress is the gateway interface's IP address and prefix length.
	hostVethLocalAddress = "169.254.17.1/30"
	// actorVethLocalAddress is the actor interface's IP address and prefix length.
	actorVethLocalAddress = "169.254.17.2/30"

	// ActorVethSubnet is the point-to-point /30 the actor veth lives on.
	ActorVethSubnet = "169.254.17.0/30"
)

var (
	HostVethAddr  = MustParseAddr(hostVethLocalAddress)
	ActorVethAddr = MustParseAddr(actorVethLocalAddress)
	ActorVethGwIP = MustParseIP(ActorVethGateway)
)

// MustParseAddr parses a CIDR string into a netlink.Addr, panicking on error.
func MustParseAddr(cidr string) *netlink.Addr {
	a, err := netlink.ParseAddr(cidr)
	if err != nil {
		panic(fmt.Sprintf("parsing constant CIDR %q: %v", cidr, err))
	}
	return a
}

// MustParseIP parses an IPv4 string into a net.IP, panicking on error.
func MustParseIP(s string) net.IP {
	ip := net.ParseIP(s).To4()
	if ip == nil {
		panic(fmt.Sprintf("parsing constant IPv4 %q", s))
	}
	return ip
}

// MustParseMAC parses a MAC address string into a net.HardwareAddr, panicking on error.
func MustParseMAC(s string) net.HardwareAddr {
	m, err := net.ParseMAC(s)
	if err != nil {
		panic(fmt.Sprintf("parsing constant MAC %q: %v", s, err))
	}
	return m
}

func l4ProtocolEqual(proto byte) []expr.Any {
	return []expr.Any{
		&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1},
		&expr.Cmp{
			Op:       expr.CmpOpEq,
			Register: 1,
			Data:     []byte{proto},
		},
	}
}
