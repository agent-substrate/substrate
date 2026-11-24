//go:build linux

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

// Package actornet provides shared networking configuration logic for Substrate runtime agents.
package actornet

import (
	"fmt"
	"net"
	"net/netip"

	"github.com/google/nftables/expr"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"
)

const (
	ActorVethName        = "eth0"
	ActorVethGateway     = "169.254.17.1"
	ActorVethIP          = "169.254.17.2"
	ActorVethIPv6Gateway = "fd00:169:254::1"
	ActorVethIPv6        = "fd00:169:254::2"

	// ActorHTTPUpstream is the in-sandbox HTTP endpoint atunnel proxies actor ingress to.
	ActorHTTPUpstream = "http://" + ActorVethIP + ":80"

	// hostVethLocalAddress is the gateway interface's IP address and prefix length.
	hostVethLocalAddress = "169.254.17.1/30"
	// actorVethLocalAddress is the actor interface's IP address and prefix length.
	actorVethLocalAddress = "169.254.17.2/30"

	// ActorVethSubnet is the point-to-point /30 the actor veth lives on.
	ActorVethSubnet = "169.254.17.0/30"
	// ActorVethIPv6Subnet is the point-to-point /126 the actor veth lives on.
	ActorVethIPv6Subnet = "fd00:169:254::/126"
)

var (
	HostVethAddr      = MustParseAddr(hostVethLocalAddress)
	ActorVethAddr     = MustParseAddr(actorVethLocalAddress)
	ActorVethGwIP     = MustParseIP(ActorVethGateway)
	HostVethIPv6Addr  = mustParseIPv6VethAddr(ActorVethIPv6Gateway + "/126")
	ActorVethIPv6Addr = mustParseIPv6VethAddr(ActorVethIPv6 + "/126")
	ActorVethIPv6GwIP = MustParseIP(ActorVethIPv6Gateway)
)

func mustParseIPv6VethAddr(cidr string) *netlink.Addr {
	addr := MustParseAddr(cidr)
	// NODAD keeps each new veth address usable immediately instead of tentative for about one second.
	addr.Flags = unix.IFA_F_NODAD
	return addr
}

// IPFamilies is IPv4 or IPv4 and IPv6. Actor networks are never IPv6-only.
type IPFamilies uint8

const (
	IPv4 IPFamilies = 1 << iota
	IPv6
)

// IPFamiliesForPodIPs returns IPv4 and adds IPv6 when the gate and worker Pod allow it.
func IPFamiliesForPodIPs(podIPs []string, enableIPv6 bool) (IPFamilies, error) {
	if len(podIPs) == 0 {
		return 0, fmt.Errorf("worker Pod has no IP addresses")
	}
	families := IPv4
	for _, value := range podIPs {
		ip, err := netip.ParseAddr(value)
		if err != nil {
			return 0, fmt.Errorf("invalid worker Pod IP %q: %w", value, err)
		}
		if enableIPv6 && ip.Is6() {
			families |= IPv6
		}
	}
	return families, nil
}

// Has reports whether families contains family.
func (f IPFamilies) Has(family IPFamilies) bool { return f&family != 0 }

// String returns the canonical runtime compatibility value for families.
func (f IPFamilies) String() string {
	switch f {
	case IPv4:
		return "ipv4"
	case IPv4 | IPv6:
		return "ipv4,ipv6"
	default:
		return fmt.Sprintf("invalid(%d)", f)
	}
}

// MustParseAddr parses a CIDR string into a netlink.Addr, panicking on error.
func MustParseAddr(cidr string) *netlink.Addr {
	a, err := netlink.ParseAddr(cidr)
	if err != nil {
		panic(fmt.Sprintf("parsing constant CIDR %q: %v", cidr, err))
	}
	return a
}

// MustParseIP parses an IP address, panicking on error.
func MustParseIP(s string) net.IP {
	ip := net.ParseIP(s)
	if ip == nil {
		panic(fmt.Sprintf("parsing constant IP %q", s))
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

func l4ProtocolNotEqual(proto byte) []expr.Any {
	return []expr.Any{
		&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1},
		&expr.Cmp{
			Op:       expr.CmpOpNeq,
			Register: 1,
			Data:     []byte{proto},
		},
	}
}
