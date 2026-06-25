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

package main

// Actor networking mirrors cmd/ateom-gvisor's veth model: a fresh
// point-to-point veth pair per activation, with the worker side (ateom0,
// 169.254.17.1/30) staying in the pod netns next to the pod's real eth0, and
// the peer moved into the interior netns, renamed eth0, and given the stable
// actor address 169.254.17.2/30. nftables rules in the pod netns masquerade
// actor egress behind the pod IP and DNAT inbound pod-IP:80 to the actor.
//
// kata consumes the interior netns exactly like a CNI-provisioned container
// netns: its tcfilter network model builds a tap cross-connected to eth0 (the
// veth peer) and gives the guest eth0's address. Because that address is a
// CONSTANT, a restored guest's frozen network config stays valid on any pod —
// no in-guest reconfiguration needed.
//
// (Copied with light adaptation from cmd/ateom-gvisor; expected to be
// de-duplicated into a shared package later.)

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"runtime"

	"github.com/google/nftables"
	"github.com/google/nftables/binaryutil"
	"github.com/google/nftables/expr"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"

	"github.com/agent-substrate/substrate/internal/serverboot"
)

const (
	hostVethName      = "ateom0"
	actorVethName     = "eth0"
	actorVethTempName = "ateom1"
	hostVethCIDR      = "169.254.17.1/30"
	actorVethCIDR     = "169.254.17.2/30"
	actorVethGateway  = "169.254.17.1"
	actorVethIP       = "169.254.17.2"
	actorNftTableName = "ateom_actor"

	// hostVethMAC is deliberately FIXED (locally administered), unlike
	// ateom-gvisor where the kernel's random veth MAC is fine. A CH snapshot
	// freezes the guest kernel's ARP cache, including the entry for the
	// gateway 169.254.17.1; restoring against a new veth pair with a random
	// MAC would blackhole guest egress until that entry expires. A constant
	// gateway MAC keeps the frozen entry valid on every pod.
	hostVethMAC = "02:a8:1e:00:00:01"

	// actorGuestMAC is the FIXED MAC for the guest's eth0 (the CH virtio-net) on
	// the ateom-owned-boot path. Fixed for the same reason as hostVethMAC: a cold
	// boot freezes this MAC into the guest+snapshot, and restore re-adds the
	// virtio-net under the same MAC (SnapshotNetDevices reads it back), so the
	// guest's frozen interface config stays valid across pods. Distinct from the
	// gateway MAC (…:01).
	actorGuestMAC = "02:a8:1e:00:00:02"

	// actorVethSubnet is the point-to-point /30 the actor veth lives on; the guest
	// needs the connected (scope-link) route to it so the gateway is reachable.
	actorVethSubnet = "169.254.17.0/30"
)

// Parsed forms of the fixed network constants above, cooked once at package init
// (a malformed constant is a programmer error, so these panic). Callers use them
// directly instead of re-parsing on every activation.
var (
	hostVethAddr   = mustParseAddr(hostVethCIDR)
	hostVethHWAddr = mustParseMAC(hostVethMAC)
	actorVethAddr  = mustParseAddr(actorVethCIDR)
	actorVethGwIP  = mustParseIP(actorVethGateway)
)

func mustParseAddr(cidr string) *netlink.Addr {
	a, err := parseAddr(cidr)
	if err != nil {
		panic(fmt.Sprintf("parsing constant CIDR %q: %v", cidr, err))
	}
	return a
}

func mustParseMAC(s string) net.HardwareAddr {
	m, err := net.ParseMAC(s)
	if err != nil {
		panic(fmt.Sprintf("parsing constant MAC %q: %v", s, err))
	}
	return m
}

func mustParseIP(s string) net.IP {
	ip := net.ParseIP(s).To4()
	if ip == nil {
		panic(fmt.Sprintf("parsing constant IPv4 %q", s))
	}
	return ip
}

// setupActorNetwork builds a fresh point-to-point network between the worker
// pod netns and the kata interior netns (see the package comment). Idempotent
// via cleanup-before-setup; also sweeps stale kata taps out of the interior
// netns so the sandbox always builds on a clean slate.
func (s *AteomService) setupActorNetwork(ctx context.Context) (retErr error) {
	s.cleanupActorNetworkOrExit(ctx, "Failed to clean up stale actor network before setup")
	defer func() {
		if retErr != nil {
			s.cleanupActorNetworkOrExit(ctx, "Failed to clean up partially configured actor network")
		}
	}()

	podIP, err := podIPv4()
	if err != nil {
		return fmt.Errorf("while resolving pod IPv4 address: %w", err)
	}

	// Sweep leftover links (kata taps from torn-down runs, restore taps) from
	// the persistent interior netns before the new veth peer arrives.
	if err := netNSDo(ctx, s.interiorNetNS, func(ctx context.Context) error {
		links, err := netlink.LinkList()
		if err != nil {
			return fmt.Errorf("while listing interior netns links: %w", err)
		}
		for _, l := range links {
			if l.Attrs().Name == "lo" {
				continue
			}
			if err := netlink.LinkDel(l); err != nil {
				slog.WarnContext(ctx, "Failed to delete leftover interior link", slog.String("link", l.Attrs().Name), slog.Any("err", err))
			}
		}
		return nil
	}); err != nil {
		return err
	}

	veth := &netlink.Veth{
		LinkAttrs: netlink.LinkAttrs{
			Name:         hostVethName,
			HardwareAddr: hostVethHWAddr,
		},
		PeerName: actorVethTempName,
	}
	if err := netlink.LinkAdd(veth); err != nil {
		return fmt.Errorf("while creating actor veth pair: %w", err)
	}

	hostLink, err := netlink.LinkByName(hostVethName)
	if err != nil {
		return fmt.Errorf("while getting host veth: %w", err)
	}
	if err := netlink.AddrReplace(hostLink, hostVethAddr); err != nil {
		return fmt.Errorf("while assigning host veth address: %w", err)
	}
	if err := netlink.LinkSetUp(hostLink); err != nil {
		return fmt.Errorf("while bringing up host veth: %w", err)
	}

	actorLink, err := netlink.LinkByName(actorVethTempName)
	if err != nil {
		return fmt.Errorf("while getting actor veth peer: %w", err)
	}
	if err := netlink.LinkSetNsFd(actorLink, int(s.interiorNetNS)); err != nil {
		return fmt.Errorf("while moving actor veth peer into interior netns: %w", err)
	}

	if err := netNSDo(ctx, s.interiorNetNS, configureActorVeth); err != nil {
		return fmt.Errorf("while configuring actor veth in interior netns: %w", err)
	}

	if err := enableIPv4Forwarding(); err != nil {
		return err
	}
	if err := installActorNftablesRules(podIP); err != nil {
		return err
	}

	return nil
}

func configureActorVeth(ctx context.Context) error {
	// Run inside the interior netns after setupActorNetwork moves the veth peer
	// there. kata reads link names, addresses, and routes from this namespace
	// when the sandbox starts, so the peer is renamed to eth0 and configured
	// like a normal container interface; the guest is configured identically by
	// the kata agent.
	loLink, err := netlink.LinkByName("lo")
	if err != nil {
		return fmt.Errorf("while acquiring lo in interior netns: %w", err)
	}
	if err := netlink.LinkSetUp(loLink); err != nil {
		return fmt.Errorf("while bringing up lo in interior netns: %w", err)
	}

	actorLink, err := netlink.LinkByName(actorVethTempName)
	if err != nil {
		return fmt.Errorf("while acquiring actor veth in interior netns: %w", err)
	}
	if err := netlink.LinkSetName(actorLink, actorVethName); err != nil {
		return fmt.Errorf("while renaming actor veth to %q: %w", actorVethName, err)
	}
	actorLink, err = netlink.LinkByName(actorVethName)
	if err != nil {
		return fmt.Errorf("while reacquiring actor veth in interior netns: %w", err)
	}

	if err := netlink.AddrReplace(actorLink, actorVethAddr); err != nil {
		return fmt.Errorf("while assigning actor veth address: %w", err)
	}
	if err := netlink.LinkSetUp(actorLink); err != nil {
		return fmt.Errorf("while bringing up actor veth: %w", err)
	}

	if err := netlink.RouteReplace(&netlink.Route{
		LinkIndex: actorLink.Attrs().Index,
		Gw:        actorVethGwIP,
	}); err != nil {
		return fmt.Errorf("while installing actor default route: %w", err)
	}

	return nil
}

// cleanupActorNetwork removes all per-activation network state owned by ateom.
// Intentionally idempotent: runs before setup, after checkpoint, and from
// setup-failure cleanup.
func (s *AteomService) cleanupActorNetwork(ctx context.Context) error {
	var cleanupErr error
	if err := removeActorNftablesRules(); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("while removing actor nftables rules: %w", err))
		slog.WarnContext(ctx, "Failed to remove actor nftables rules; continuing actor netns cleanup", slog.Any("err", err))
	}

	if link, err := netlink.LinkByName(hostVethName); err == nil {
		if err := netlink.LinkDel(link); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("while deleting host veth: %w", err))
			slog.WarnContext(ctx, "Failed to delete host veth; continuing actor netns cleanup", slog.Any("err", err))
		}
	} else if _, ok := err.(netlink.LinkNotFoundError); !ok {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("while looking up host veth: %w", err))
		slog.WarnContext(ctx, "Failed to look up host veth; continuing actor netns cleanup", slog.Any("err", err))
	}

	if err := netNSDo(ctx, s.interiorNetNS, func(_ context.Context) error {
		for _, name := range []string{actorVethName, actorVethTempName} {
			link, err := netlink.LinkByName(name)
			if err == nil {
				if err := netlink.LinkDel(link); err != nil {
					return fmt.Errorf("while deleting interior veth %q: %w", name, err)
				}
				continue
			}
			if _, ok := err.(netlink.LinkNotFoundError); !ok {
				return fmt.Errorf("while looking up interior veth %q: %w", name, err)
			}
		}
		return nil
	}); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("while cleaning interior netns links: %w", err))
	}

	return cleanupErr
}

func (s *AteomService) cleanupActorNetworkOrExit(ctx context.Context, msg string) {
	if err := s.cleanupActorNetwork(ctx); err != nil {
		serverboot.Fatal(ctx, msg, err)
	}
}

// podIPv4 resolves the worker pod IPv4 address from the pod namespace's real
// eth0 (which stays in the pod namespace in the veth model).
func podIPv4() (net.IP, error) {
	eth0Link, err := netlink.LinkByName("eth0")
	if err != nil {
		return nil, fmt.Errorf("while getting pod eth0: %w", err)
	}
	addrs, err := netlink.AddrList(eth0Link, netlink.FAMILY_V4)
	if err != nil {
		return nil, fmt.Errorf("while listing pod eth0 addresses: %w", err)
	}
	for _, addr := range addrs {
		if addr.IP == nil {
			continue
		}
		if ip := addr.IP.To4(); ip != nil {
			return ip, nil
		}
	}
	return nil, fmt.Errorf("pod eth0 has no IPv4 address")
}

func parseAddr(cidr string) (*netlink.Addr, error) {
	addr, err := netlink.ParseAddr(cidr)
	if err != nil {
		return nil, fmt.Errorf("while parsing address %q: %w", cidr, err)
	}
	return addr, nil
}

func enableIPv4Forwarding() error {
	// Actor packets enter the worker pod via the host-side veth and leave
	// through the pod's eth0; the kernel will not route between them otherwise.
	// Open the existing sysctl O_WRONLY (not os.WriteFile, which would create a
	// regular file if the knob were missing) so an absent knob is a clear error.
	f, err := os.OpenFile("/proc/sys/net/ipv4/ip_forward", os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("while opening ip_forward sysctl: %w", err)
	}
	defer f.Close()
	if _, err := f.Write([]byte("1\n")); err != nil {
		return fmt.Errorf("while enabling IPv4 forwarding in worker pod netns: %w", err)
	}
	return nil
}

func installActorNftablesRules(podIP net.IP) error {
	// Dedicated ateom-owned IPv4 table (cheap cleanup, no CNI chain mutation):
	//   * postrouting: masquerade actor egress (169.254.17.2) behind the pod IP.
	//   * prerouting: DNAT pod-IP:80/tcp to the actor veth IP.
	//   * forward: accept forwarded packets between the actor veth and pod eth0.
	// Mirrors cmd/ateom-gvisor (same compatibility-bridge caveats and TODOs).
	if err := removeActorNftablesRules(); err != nil {
		return err
	}

	c := &nftables.Conn{}
	table := &nftables.Table{
		Family: nftables.TableFamilyIPv4,
		Name:   actorNftTableName,
	}
	c.AddTable(table)

	prerouting := c.AddChain(&nftables.Chain{
		Name:     "prerouting",
		Table:    table,
		Type:     nftables.ChainTypeNAT,
		Hooknum:  nftables.ChainHookPrerouting,
		Priority: nftables.ChainPriorityNATDest,
	})
	preroutingExprs := append(ipDestinationEqual(podIP.String()), tcpDestinationPortEqual(80)...)
	preroutingExprs = append(preroutingExprs,
		&expr.Immediate{
			Register: 1,
			Data:     net.ParseIP(actorVethIP).To4(),
		},
		&expr.Immediate{
			Register: 2,
			Data:     binaryutil.BigEndian.PutUint16(80),
		},
		&expr.NAT{
			Type:        expr.NATTypeDestNAT,
			Family:      unix.NFPROTO_IPV4,
			RegAddrMin:  1,
			RegProtoMin: 2,
		},
	)
	c.AddRule(&nftables.Rule{
		Table: table,
		Chain: prerouting,
		Exprs: preroutingExprs,
	})

	postrouting := c.AddChain(&nftables.Chain{
		Name:     "postrouting",
		Table:    table,
		Type:     nftables.ChainTypeNAT,
		Hooknum:  nftables.ChainHookPostrouting,
		Priority: nftables.ChainPriorityNATSource,
	})
	c.AddRule(&nftables.Rule{
		Table: table,
		Chain: postrouting,
		Exprs: append(ipSourceEqual(actorVethIP), &expr.Masq{}),
	})

	acceptPolicy := nftables.ChainPolicyAccept
	forward := c.AddChain(&nftables.Chain{
		Name:     "forward",
		Table:    table,
		Type:     nftables.ChainTypeFilter,
		Hooknum:  nftables.ChainHookForward,
		Priority: nftables.ChainPriorityFilter,
		Policy:   &acceptPolicy,
	})
	c.AddRule(&nftables.Rule{
		Table: table,
		Chain: forward,
		Exprs: []expr.Any{
			&expr.Verdict{Kind: expr.VerdictAccept},
		},
	})

	if err := c.Flush(); err != nil {
		return fmt.Errorf("while installing actor nftables rules: %w", err)
	}
	return nil
}

func removeActorNftablesRules() error {
	c := &nftables.Conn{}
	tables, err := c.ListTablesOfFamily(nftables.TableFamilyIPv4)
	if err != nil {
		return fmt.Errorf("while listing nftables tables: %w", err)
	}
	for _, table := range tables {
		if table.Name != actorNftTableName {
			continue
		}
		c.DelTable(table)
		if err := c.Flush(); err != nil {
			return fmt.Errorf("while deleting actor nftables table: %w", err)
		}
		return nil
	}
	return nil
}

func ipSourceEqual(ip string) []expr.Any {
	return ipPayloadEqual(12, ip)
}

func ipDestinationEqual(ip string) []expr.Any {
	return ipPayloadEqual(16, ip)
}

func ipPayloadEqual(offset uint32, ip string) []expr.Any {
	return []expr.Any{
		&expr.Payload{
			DestRegister: 1,
			Base:         expr.PayloadBaseNetworkHeader,
			Offset:       offset,
			Len:          4,
		},
		&expr.Cmp{
			Op:       expr.CmpOpEq,
			Register: 1,
			Data:     net.ParseIP(ip).To4(),
		},
	}
}

func tcpDestinationPortEqual(port uint16) []expr.Any {
	return []expr.Any{
		&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1},
		&expr.Cmp{
			Op:       expr.CmpOpEq,
			Register: 1,
			Data:     []byte{unix.IPPROTO_TCP},
		},
		&expr.Payload{
			DestRegister: 1,
			Base:         expr.PayloadBaseTransportHeader,
			Offset:       2,
			Len:          2,
		},
		&expr.Cmp{
			Op:       expr.CmpOpEq,
			Register: 1,
			Data:     binaryutil.BigEndian.PutUint16(port),
		},
	}
}

// createNetNSWithoutSwitching creates a named netns and returns its handle,
// restoring the caller's current netns before returning.
func createNetNSWithoutSwitching(name string) (netns.NsHandle, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	curNetNS, err := netns.Get()
	if err != nil {
		return -1, fmt.Errorf("while getting current netns: %w", err)
	}
	defer func() {
		if err := netns.Set(curNetNS); err != nil {
			panic(fmt.Sprintf("Failed to restore original netns: %v", err))
		}
	}()

	interiorNetNS, err := netns.NewNamed(name)
	if err != nil {
		return -1, fmt.Errorf("while creating interior network namespace: %w", err)
	}
	return interiorNetNS, nil
}

// netNSDo runs do() with the OS thread switched into targetNS, then restores it.
func netNSDo(ctx context.Context, targetNS netns.NsHandle, do func(context.Context) error) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	curNetNS, err := netns.Get()
	if err != nil {
		return fmt.Errorf("while getting current netns: %w", err)
	}
	defer func() {
		if err := netns.Set(curNetNS); err != nil {
			panic(fmt.Sprintf("Failed to restore original netns: %v", err))
		}
	}()

	if err := netns.Set(targetNS); err != nil {
		return fmt.Errorf("setting target netns: %w", err)
	}
	if err := do(ctx); err != nil {
		return fmt.Errorf("while executing function in target netns: %w", err)
	}
	return nil
}

// setupRestoreTap recreates, in the interior netns, the tap + TC-mirror wiring
// kata's tcfilter network model builds at boot: a tap device cross-connected to
// eth0 (the actor veth peer) with mirred-redirect ingress filters in both
// directions. Returns the open tap FDs (one per queue pair) for
// cloud-hypervisor to adopt via vm.restore net_fds (the snapshot's virtio-net
// device is fd-backed, so CH requires fresh FDs on restore). Call after
// setupActorNetwork.
func (s *AteomService) setupRestoreTap(ctx context.Context, name string, queuePairs int) ([]*os.File, error) {
	var fds []*os.File
	err := netNSDo(ctx, s.interiorNetNS, func(ctx context.Context) error {
		eth0, err := netlink.LinkByName(actorVethName)
		if err != nil {
			return fmt.Errorf("acquiring actor veth in interior netns: %w", err)
		}
		if old, lerr := netlink.LinkByName(name); lerr == nil {
			_ = netlink.LinkDel(old)
		}
		flags := netlink.TUNTAP_NO_PI | netlink.TUNTAP_VNET_HDR
		if queuePairs > 1 {
			flags |= netlink.TUNTAP_MULTI_QUEUE
		}
		tap := &netlink.Tuntap{
			LinkAttrs: netlink.LinkAttrs{Name: name, MTU: eth0.Attrs().MTU},
			Mode:      netlink.TUNTAP_MODE_TAP,
			Flags:     flags,
			Queues:    queuePairs,
		}
		if err := netlink.LinkAdd(tap); err != nil {
			return fmt.Errorf("creating tap %q: %w", name, err)
		}
		fds = tap.Fds
		if err := netlink.LinkSetUp(tap); err != nil {
			return fmt.Errorf("bringing up tap %q: %w", name, err)
		}
		// Cross-connect: everything arriving on the veth peer redirects out the
		// tap and vice versa (kata's TCFilterModel: ingress qdisc + match-all u32
		// with a mirred egress-redirect action, here via U32.RedirIndex).
		for _, pair := range [][2]netlink.Link{{eth0, tap}, {tap, eth0}} {
			qdisc := &netlink.Ingress{QdiscAttrs: netlink.QdiscAttrs{
				LinkIndex: pair[0].Attrs().Index,
				Parent:    netlink.HANDLE_INGRESS,
				Handle:    netlink.MakeHandle(0xffff, 0),
			}}
			if err := netlink.QdiscReplace(qdisc); err != nil {
				return fmt.Errorf("adding ingress qdisc to %q: %w", pair[0].Attrs().Name, err)
			}
			filter := &netlink.U32{
				FilterAttrs: netlink.FilterAttrs{
					LinkIndex: pair[0].Attrs().Index,
					Parent:    netlink.MakeHandle(0xffff, 0),
					Priority:  1,
					Protocol:  unix.ETH_P_ALL,
				},
				ClassId:    netlink.MakeHandle(1, 1),
				RedirIndex: pair[1].Attrs().Index,
			}
			if err := netlink.FilterAdd(filter); err != nil {
				return fmt.Errorf("adding mirred filter %s -> %s: %w", pair[0].Attrs().Name, pair[1].Attrs().Name, err)
			}
		}
		return nil
	})
	if err != nil {
		for _, f := range fds {
			_ = f.Close()
		}
		return nil, err
	}
	return fds, nil
}

// actorVethMTU reads the MTU of the actor veth (eth0 in the interior netns) so the
// owned-boot path can configure the guest eth0 with a matching MTU via the agent
// (UpdateInterface). Defaults to 1500 if the link can't be read.
func (s *AteomService) actorVethMTU(ctx context.Context) int {
	mtu := 1500
	_ = netNSDo(ctx, s.interiorNetNS, func(ctx context.Context) error {
		if l, err := netlink.LinkByName(actorVethName); err == nil {
			mtu = l.Attrs().MTU
		} else {
			slog.WarnContext(ctx, "Failed to read actor veth MTU; using default",
				slog.String("link", actorVethName), slog.Int("default_mtu", mtu), slog.Any("err", err))
		}
		return nil
	})
	return mtu
}
