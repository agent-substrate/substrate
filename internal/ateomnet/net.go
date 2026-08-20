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
)

const (
	HostVethName      = "ateom0"
	ActorVethName     = "eth0"
	HostVethCIDR      = "169.254.17.1/30"
	ActorVethCIDR     = "169.254.17.2/30"
	ActorVethGateway  = "169.254.17.1"
	ActorVethIP       = "169.254.17.2"
	ActorNftTableName = "ateom_actor"

	// ActorVethSubnet is the point-to-point /30 the actor veth lives on.
	ActorVethSubnet = "169.254.17.0/30"
)

var (
	HostVethAddr  = MustParseAddr(HostVethCIDR)
	ActorVethAddr = MustParseAddr(ActorVethCIDR)
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

// ConfigureActorVeth configures the actor veth inside the interior netns.
// It assumes it is already running inside the target network namespace.
func ConfigureActorVeth(ctx context.Context) error {
	// Run inside the gVisor interior netns. SetupActorNetwork has already created
	// the veth peer here, under its final name, so this only has to address it.
	// gVisor reads link names, addresses, and routes from this namespace when the
	// workload starts, so eth0 is configured like a normal container interface:
	//
	//   * lo is brought up for localhost behavior.
	//   * eth0 receives the actor-side /30 address.
	//   * the default route points to the worker-side veth gateway.
	loLink, err := netlink.LinkByName("lo")
	if err != nil {
		return fmt.Errorf("while acquiring lo in interior netns: %w", err)
	}
	if err := netlink.LinkSetUp(loLink); err != nil {
		return fmt.Errorf("while bringing up lo in interior netns: %w", err)
	}

	actorLink, err := netlink.LinkByName(ActorVethName)
	if err != nil {
		return fmt.Errorf("while acquiring actor veth in interior netns: %w", err)
	}

	if err := netlink.AddrReplace(actorLink, ActorVethAddr); err != nil {
		return fmt.Errorf("while assigning actor veth address: %w", err)
	}
	if err := netlink.LinkSetUp(actorLink); err != nil {
		return fmt.Errorf("while bringing up actor veth: %w", err)
	}

	if err := netlink.RouteReplace(&netlink.Route{
		LinkIndex: actorLink.Attrs().Index,
		Gw:        ActorVethGwIP,
	}); err != nil {
		return fmt.Errorf("while installing actor default route: %w", err)
	}

	return nil
}

// CleanupActorNetwork removes all per-activation network state owned by ateom.
// Intentionally idempotent.
func CleanupActorNetwork(ctx context.Context, interiorNetNS netns.NsHandle) error {
	// Remove all per-activation network state owned by ateom. Deleting the
	// worker-side veth also deletes its peer, but the pair is born with its peer
	// already in the actor netns, so a setup that failed before the worker side
	// was named can leave that peer behind on its own. For that reason cleanup
	// also enters the interior netns and deletes the actor interface if present.
	//
	// This function is intentionally idempotent so it can run before setup, after
	// checkpoint, and from setup failure cleanup without requiring the caller to
	// know how far network initialization progressed.
	var cleanupErr error
	if err := RemoveActorNftablesRules(); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("while removing actor nftables rules: %w", err))
		slog.WarnContext(ctx, "Failed to remove actor nftables rules; continuing actor netns cleanup", "err", err)
	}

	if link, err := netlink.LinkByName(HostVethName); err == nil {
		if err := netlink.LinkDel(link); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("while deleting host veth: %w", err))
			slog.WarnContext(ctx, "Failed to delete host veth; continuing actor netns cleanup", "err", err)
		}
	} else if _, notFound := errors.AsType[netlink.LinkNotFoundError](err); !notFound {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("while looking up host veth: %w", err))
		slog.WarnContext(ctx, "Failed to look up host veth; continuing actor netns cleanup", "err", err)
	}

	if err := NetNSDo(ctx, interiorNetNS, func(_ context.Context) error {
		link, err := netlink.LinkByName(ActorVethName)
		if err == nil {
			if err := netlink.LinkDel(link); err != nil {
				return fmt.Errorf("while deleting interior veth %q: %w", ActorVethName, err)
			}
			return nil
		}
		if _, notFound := errors.AsType[netlink.LinkNotFoundError](err); !notFound {
			return fmt.Errorf("while looking up interior veth %q: %w", ActorVethName, err)
		}
		return nil
	}); err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("while cleaning interior netns links: %w", err))
	}

	return cleanupErr
}

// PodIPv4 resolves the worker pod IPv4 address from the pod namespace's real eth0.
func PodIPv4() (net.IP, error) {
	// Resolve the worker pod IPv4 address from the pod namespace's real eth0.
	// Because eth0 now stays in the pod namespace, this IP remains available for
	// both normal worker connectivity and the temporary inbound DNAT rule.
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

// EnableIPv4Forwarding enables IPv4 forwarding in the current network namespace.
func EnableIPv4Forwarding() error {
	// Forwarding is required because actor packets now enter the worker pod via
	// the host-side veth and then leave through the pod's eth0. Without this, the
	// kernel would not route traffic between those interfaces even though both
	// live in the worker pod network namespace.
	//
	// Without privileged, the container runtime bind-mounts /proc/sys read-only.
	// The worker holds CAP_SYS_ADMIN and uses no user namespace, so the ro flag
	// is not locked: clear it, write the sysctl, restore ro.
	const path = "/proc/sys/net/ipv4/ip_forward"
	if b, err := os.ReadFile(path); err == nil && len(b) > 0 && b[0] == '1' {
		return nil
	}
	if err := os.WriteFile(path, []byte("1\n"), 0o644); err == nil {
		return nil
	}
	if err := unix.Mount("none", "/proc/sys", "", unix.MS_BIND|unix.MS_REMOUNT, ""); err != nil {
		return fmt.Errorf("while remounting /proc/sys read-write to enable IPv4 forwarding: %w", err)
	}
	defer func() {
		_ = unix.Mount("none", "/proc/sys", "", unix.MS_BIND|unix.MS_REMOUNT|unix.MS_RDONLY, "")
	}()
	if err := os.WriteFile(path, []byte("1\n"), 0o644); err != nil {
		return fmt.Errorf("while enabling IPv4 forwarding in worker pod netns: %w", err)
	}
	return nil
}

// InstallActorNftablesRules configures the NAT and filtering rules for the
// actor. egressPort, when non-zero, is the local atunnel egress listener actor
// TCP egress is redirected to; zero leaves the redirect uninstalled.
func InstallActorNftablesRules(egressPort uint16) error {
	// Install a dedicated nftables table for the active actor. Keeping all
	// rules in an ateom-owned table makes cleanup simple and avoids mutating
	// Kubernetes or CNI-managed chains directly.
	//
	// The table is in the inet family so one table can carry both address
	// families once the actor veth is dual-stack. NAT there needs Linux 5.2.
	// A bare payload match in that family is ambiguous, so every IPv4 match
	// opens with an NFPROTO comparison. An inet nat chain registers the nat
	// hooks for both families, so IPv6 traffic in this netns is conntracked
	// where an ip table left it untracked.
	//
	// TODO(#246): Add the IPv6 veth addressing and forwarding this table is
	// waiting on. The actor network itself is still IPv4-only.
	//
	// The rules do three things:
	//
	//   * prerouting: redirect new actor TCP connections to atunnel's local
	//     listener. REDIRECT preserves SO_ORIGINAL_DST for the CONNECT authority.
	//   * postrouting: masquerade traffic not handled by the TCP tunnel, notably
	//     DNS over UDP, so hostname resolution continues to work.
	//   * forward: accept forwarded packets between the actor veth and pod eth0.
	//
	// TODO: Restrict the compatibility masquerade to DNS traffic sent to the
	// configured cluster resolver and drop all other non-tunneled actor egress.
	if err := RemoveActorNftablesRules(); err != nil {
		return err
	}

	c := &nftables.Conn{}
	table := &nftables.Table{
		Family: nftables.TableFamilyINet,
		Name:   ActorNftTableName,
	}
	c.AddTable(table)

	prerouting := c.AddChain(&nftables.Chain{
		Name:     "prerouting",
		Table:    table,
		Type:     nftables.ChainTypeNAT,
		Hooknum:  nftables.ChainHookPrerouting,
		Priority: nftables.ChainPriorityNATDest,
	})
	if redirectRule := ActorEgressRedirectRule(table, prerouting, egressPort); redirectRule != nil {
		c.AddRule(redirectRule)
	}

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
		Exprs: append(ipv4SourceEqual(ActorVethIP), &expr.Masq{}),
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
	// Unqualified, so this accepts forwarded IPv6 too -- what the actor needs
	// once it is dual-stack. accept is per-table, so it cannot override a drop
	// from the CNI's own forward chains.
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

// RemoveActorNftablesRules removes the ateom nftables table.
func RemoveActorNftablesRules() error {
	// Delete the whole ateom nftables table if it exists. The table is
	// per-worker and currently per-active-actor because this worker path runs at
	// most one actor at a time. Missing tables are treated as already clean.
	//
	// Both families are swept, not just the inet one this installs into: a table
	// name is unique per family, so an ip table left by an earlier ateom would
	// survive every later cleanup and keep redirecting alongside the new one.
	// The pod netns outlives an in-place container restart, so an ateom that
	// predates the inet table can share a netns with one that follows it
	// wherever WorkerPool.spec.ateomImage is a mutable tag -- the dev loop.
	// TODO(ypgao): Drop the ip sweep once no live pod can predate this change.
	c := &nftables.Conn{}
	for _, family := range []struct {
		name string
		id   nftables.TableFamily
	}{{"inet", nftables.TableFamilyINet}, {"ip", nftables.TableFamilyIPv4}} {
		tables, err := c.ListTablesOfFamily(family.id)
		if err != nil {
			return fmt.Errorf("while listing %s nftables tables: %w", family.name, err)
		}
		for _, table := range tables {
			if table.Name != ActorNftTableName {
				continue
			}
			c.DelTable(table)
			if err := c.Flush(); err != nil {
				return fmt.Errorf("while deleting the %s actor nftables table: %w", family.name, err)
			}
		}
	}
	return nil
}

func ipv4SourceEqual(ip string) []expr.Any {
	return ipv4PayloadEqual(12, ip)
}

// ipv4PayloadEqual matches a 4-byte IPv4 network-header field. The leading
// nfproto comparison is what makes it safe in the inet table: without it the
// payload load would read the same offset out of an IPv6 header.
func ipv4PayloadEqual(offset uint32, ip string) []expr.Any {
	return []expr.Any{
		&expr.Meta{Key: expr.MetaKeyNFPROTO, Register: 1},
		&expr.Cmp{
			Op:       expr.CmpOpEq,
			Register: 1,
			Data:     []byte{unix.NFPROTO_IPV4},
		},
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

func TCPProtocol() []expr.Any {
	return []expr.Any{
		&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1},
		&expr.Cmp{
			Op:       expr.CmpOpEq,
			Register: 1,
			Data:     []byte{unix.IPPROTO_TCP},
		},
	}
}

// ActorEgressRedirectRule returns the prerouting rule that redirects actor TCP
// egress to the local atunnel egress listener on port, or nil when port is zero
// (tunneled egress disabled, so actor egress stays on the masquerade path).
func ActorEgressRedirectRule(table *nftables.Table, chain *nftables.Chain, port uint16) *nftables.Rule {
	if port == 0 {
		return nil
	}
	exprs := append(ipv4SourceEqual(ActorVethIP), TCPProtocol()...)
	exprs = append(exprs,
		&expr.Immediate{
			Register: 1,
			Data:     binaryutil.BigEndian.PutUint16(port),
		},
		&expr.Redir{RegisterProtoMin: 1},
	)
	return &nftables.Rule{Table: table, Chain: chain, Exprs: exprs}
}

// CreateNetNSWithoutSwitching creates a named netns and returns its handle,
// restoring the caller's current netns before returning.
func CreateNetNSWithoutSwitching(name string) (netns.NsHandle, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// We need to create the new NS, then switch back to the current netns.
	curNetNS, err := netns.Get()
	if err != nil {
		return -1, fmt.Errorf("while getting current netns: %w", err)
	}
	// Registered before the restoring defer below since deferred calls are LIFO.
	defer curNetNS.Close()
	defer func() {
		if err := netns.Set(curNetNS); err != nil {
			// Better to blow up the program than continue execution with
			// one OS thread randomly in a different netns.
			panic(fmt.Sprintf("Failed to restore original netns: %v", err))
		}
	}()

	interiorNetNS, err := netns.NewNamed(name)
	if err != nil {
		return -1, fmt.Errorf("while creating interior network namespace: %w", err)
	}
	return interiorNetNS, nil
}

// NetNSDo runs do() with the OS thread switched into targetNS, then restores it.
func NetNSDo(ctx context.Context, targetNS netns.NsHandle, do func(context.Context) error) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// We need to create the new NS, then switch back to the current netns.
	curNetNS, err := netns.Get()
	if err != nil {
		return fmt.Errorf("while getting current netns: %w", err)
	}
	// Registered before the restoring defer below since deferred calls are LIFO.
	defer curNetNS.Close()
	defer func() {
		if err := netns.Set(curNetNS); err != nil {
			// Better to blow up the program than continue execution with
			// one OS thread randomly in a different netns.
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

// DumpNetInfo dumps link and route information for debugging.
func DumpNetInfo(ctx context.Context, prefix string) error {
	links, err := netlink.LinkList()
	if err != nil {
		return fmt.Errorf("in netlink.LinkList(): %w", err)
	}

	for _, link := range links {
		slog.InfoContext(ctx, prefix+"Link", slog.String("name", link.Attrs().Name), slog.String("type", link.Type()), slog.Any("attrs", link.Attrs()))

		addrs, err := netlink.AddrList(link, netlink.FAMILY_V4)
		if err != nil {
			return fmt.Errorf("while getting pod eth0 addresses: %w", err)
		}
		slog.InfoContext(ctx, prefix+"Link Addresses", slog.String("link", link.Attrs().Name), slog.Any("addrs", addrs))

		rts, err := netlink.RouteList(link, netlink.FAMILY_V4)
		if err != nil {
			return fmt.Errorf("while getting routes off eth0: %w", err)
		}
		for _, rt := range rts {
			slog.InfoContext(ctx, prefix+"Link Routes", slog.Any("link", link.Attrs().Name), slog.Any("route", rt), slog.Any("route-string", rt.String()))
		}
	}

	return nil
}

type NetworkConfig struct {
	// InteriorNetNS is the target network namespace for the actor's veth pair peer.
	// Used by: Both gVisor and MicroVM.
	InteriorNetNS netns.NsHandle

	// HostVethHWAddr is the hardware address to assign to the host veth interface.
	// Used by: MicroVM (to ensure consistent MAC addresses across snapshot/restore).
	HostVethHWAddr net.HardwareAddr
	// SweepInteriorLinks indicates whether to delete existing links in the interior netns (excluding loopback).
	// Used by: MicroVM (to clean up stale tap devices).
	SweepInteriorLinks bool

	// DumpNetInfo indicates whether to dump network information to the logs for debugging purposes.
	// Used by: gVisor.
	DumpNetInfo bool

	// EgressRedirectPort is the local atunnel egress listener port actor TCP
	// egress is redirected to. Zero installs no redirect, leaving actor egress
	// on the masquerade path.
	// Used by: Both gVisor and MicroVM.
	EgressRedirectPort uint16
}

// SetupActorNetwork builds a fresh point-to-point network between the worker
// pod netns and the interior netns.
func SetupActorNetwork(ctx context.Context, cfg NetworkConfig) (retErr error) {
	// Build a fresh point-to-point network between the worker pod netns and the
	// gVisor interior netns. The worker side keeps the pod's real eth0 and creates
	// ateom0 as the gateway; the pair's peer is born inside the actor netns as
	// eth0, where it gets the actor-side address and a default route via the
	// worker-side veth address. This replaces the old behavior of moving the
	// Kubernetes-provided eth0 out of the worker pod.
	//
	// The nftables rules installed here redirect actor TCP egress to atunnel
	// when configured and masquerade traffic the TCP tunnel does not handle
	// (notably DNS over UDP).
	//
	// Clean up stale state from a failed prior activation before creating the
	// next actor-side network. The worker currently runs one actor at a time.
	if err := CleanupActorNetwork(ctx, cfg.InteriorNetNS); err != nil {
		return fmt.Errorf("failed to clean up stale actor network before setup: %w", err)
	}
	defer func() {
		if retErr != nil {
			if err := CleanupActorNetwork(ctx, cfg.InteriorNetNS); err != nil {
				slog.WarnContext(ctx, "Failed to clean up partially configured actor network", slog.Any("err", err))
			}
		}
	}()

	if cfg.SweepInteriorLinks {
		if err := NetNSDo(ctx, cfg.InteriorNetNS, func(ctx context.Context) error {
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
	}

	// The peer is born in the interior netns under its final name. Do not replace
	// this with the obvious "create locally, LinkSetNsFd across, rename to eth0":
	// moving and renaming a netdev each cost an RCU grace period under the global
	// RTNL lock, which is ~18ms vs ~3ms for all of SetupActorNetwork, on the
	// resume path. Naming the peer here is safe because its name is resolved in
	// its own netns, so it never collides with the pod's eth0.
	veth := &netlink.Veth{
		LinkAttrs: netlink.LinkAttrs{
			Name: HostVethName,
		},
		PeerName: ActorVethName,
		// netlink.NsFd, not netns.NsHandle: only the netlink type is recognized
		// as IFLA_NET_NS_FD on the peer, though both are file descriptors.
		PeerNamespace: netlink.NsFd(int(cfg.InteriorNetNS)),
	}
	if len(cfg.HostVethHWAddr) > 0 {
		veth.LinkAttrs.HardwareAddr = cfg.HostVethHWAddr
	}

	if err := netlink.LinkAdd(veth); err != nil {
		return fmt.Errorf("while creating actor veth pair: %w", err)
	}

	hostLink, err := netlink.LinkByName(HostVethName)
	if err != nil {
		return fmt.Errorf("while getting host veth: %w", err)
	}
	if err := netlink.AddrReplace(hostLink, HostVethAddr); err != nil {
		return fmt.Errorf("while assigning host veth address: %w", err)
	}
	if err := netlink.LinkSetUp(hostLink); err != nil {
		return fmt.Errorf("while bringing up host veth: %w", err)
	}

	if err := NetNSDo(ctx, cfg.InteriorNetNS, ConfigureActorVeth); err != nil {
		return fmt.Errorf("while configuring actor veth in interior netns: %w", err)
	}

	if err := EnableIPv4Forwarding(); err != nil {
		return err
	}
	if err := InstallActorNftablesRules(cfg.EgressRedirectPort); err != nil {
		return err
	}

	if cfg.DumpNetInfo {
		if err := DumpNetInfo(ctx, "Pod NetNS "); err != nil {
			return fmt.Errorf("while dumping pod netns links: %w", err)
		}
		if err := NetNSDo(ctx, cfg.InteriorNetNS, func(ctx context.Context) error {
			return DumpNetInfo(ctx, "Interior NetNS ")
		}); err != nil {
			return fmt.Errorf("while dumping interior netns links: %w", err)
		}
	}

	return nil
}
