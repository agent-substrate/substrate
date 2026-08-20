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

package ateomnet

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"runtime"
	"testing"

	"github.com/agent-substrate/substrate/internal/roottest"
	"github.com/google/nftables"
	"github.com/google/nftables/binaryutil"
	"github.com/google/nftables/expr"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

// withTestNetNS runs fn with the calling thread inside a throwaway netns
// standing in for the worker pod's, and hands it a second throwaway netns
// standing in for an actor's interior one.
//
// Both are anonymous (netns.New, not NewNamed) so the test leaves nothing behind
// in /run/netns, and every link, sysctl, and nftables table SetupActorNetwork
// touches is scoped to a namespace that disappears with the test rather than to
// the machine running it.
func withTestNetNS(t *testing.T, fn func(interior netns.NsHandle)) {
	t.Helper()

	// Locked for the whole body: netns is a per-thread property, so an unlocked
	// goroutine could be rescheduled onto a thread still in the original
	// namespace midway through. It also means a t.Fatal inside fn tears the
	// thread down instead of returning it to the pool mis-configured.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	orig, err := netns.Get()
	if err != nil {
		t.Fatalf("getting current netns: %v", err)
	}
	defer orig.Close()
	// Registered before the namespaces below so it runs after they are closed.
	defer func() {
		if err := netns.Set(orig); err != nil {
			t.Errorf("restoring original netns: %v", err)
		}
	}()

	pod, err := netns.New() // netns.New switches the thread into the new namespace
	if err != nil {
		t.Fatalf("creating pod netns: %v", err)
	}
	defer pod.Close()
	interior, err := netns.New()
	if err != nil {
		t.Fatalf("creating interior netns: %v", err)
	}
	defer interior.Close()
	if err := netns.Set(pod); err != nil {
		t.Fatalf("entering pod netns: %v", err)
	}

	fn(interior)
}

// requireNftables skips when this kernel cannot serve what SetupActorNetwork
// installs, which is a property of the machine rather than of the code under
// test. Listing the inet family is not a sufficient probe: inet filter is far
// older than inet nat, which needs Linux 5.2, so this builds and drops a nat
// chain in the family the actor table uses.
func requireNftables(t *testing.T) {
	t.Helper()
	c := &nftables.Conn{}
	probe := c.AddTable(&nftables.Table{Family: nftables.TableFamilyINet, Name: "ateom_nft_probe"})
	c.AddChain(&nftables.Chain{
		Name:     "prerouting",
		Table:    probe,
		Type:     nftables.ChainTypeNAT,
		Hooknum:  nftables.ChainHookPrerouting,
		Priority: nftables.ChainPriorityNATDest,
	})
	if err := c.Flush(); err != nil {
		t.Skipf("nftables inet nat unavailable in this environment: %v", err)
	}
	c.DelTable(probe)
	if err := c.Flush(); err != nil {
		t.Fatalf("deleting the nftables probe table: %v", err)
	}
}

// actorNftTableExists reports whether the actor table is present in the family
// InstallActorNftablesRules creates it in. The family is load-bearing:
// ListTablesOfFamily puts it in the netlink dump header, so the kernel filters
// the dump and a query for the wrong family comes back empty rather than
// erroring.
func actorNftTableExists(t *testing.T) bool {
	t.Helper()
	c := &nftables.Conn{}
	tables, err := c.ListTablesOfFamily(nftables.TableFamilyINet)
	if err != nil {
		t.Fatalf("listing inet nftables tables: %v", err)
	}
	for _, table := range tables {
		if table.Name == ActorNftTableName {
			return true
		}
	}
	return false
}

// linkByName returns the link, or nil when it does not exist.
func linkByName(t *testing.T, name string) netlink.Link {
	t.Helper()
	link, err := netlink.LinkByName(name)
	if err == nil {
		return link
	}
	if _, ok := errors.AsType[netlink.LinkNotFoundError](err); ok {
		return nil
	}
	t.Fatalf("looking up link %q: %v", name, err)
	return nil
}

func hasAddr(t *testing.T, link netlink.Link, cidr string) bool {
	t.Helper()
	addrs, err := netlink.AddrList(link, netlink.FAMILY_V4)
	if err != nil {
		t.Fatalf("listing addresses of %q: %v", link.Attrs().Name, err)
	}
	want := MustParseAddr(cidr)
	for _, addr := range addrs {
		if addr.IPNet != nil && addr.IPNet.String() == want.IPNet.String() {
			return true
		}
	}
	return false
}

// TestSetupActorNetworkFinalState pins the namespace state gVisor and the
// micro-VM guest read after an activation: what links exist, where, with which
// addresses and routes. It deliberately asserts the end state rather than the
// sequence of netlink calls that produced it, so the setup path stays free to
// get there differently (as it did when the veth peer stopped being created in
// the pod netns and moved across).
func TestSetupActorNetworkFinalState(t *testing.T) {
	roottest.Require(t, "creating network namespaces, veth pairs, and nftables rules")
	ctx := context.Background()

	withTestNetNS(t, func(interior netns.NsHandle) {
		requireNftables(t)

		if err := SetupActorNetwork(ctx, NetworkConfig{InteriorNetNS: interior}); err != nil {
			t.Fatalf("SetupActorNetwork: %v", err)
		}

		// Worker pod side: the gateway end of the point-to-point link.
		host := linkByName(t, HostVethName)
		if host == nil {
			t.Fatalf("host veth %q missing from the pod netns", HostVethName)
		}
		if !hasAddr(t, host, HostVethCIDR) {
			t.Errorf("host veth %q does not carry %s", HostVethName, HostVethCIDR)
		}
		if host.Attrs().Flags&1 == 0 { // net.FlagUp
			t.Errorf("host veth %q is not up", HostVethName)
		}

		// The actor interface must exist ONLY in the interior netns. A peer left
		// in the pod netns would mean the pair was built the old way, and worse,
		// would collide with the pod's own eth0 on a real worker.
		if stray := linkByName(t, ActorVethName); stray != nil {
			t.Errorf("actor interface %q must not exist in the pod netns", ActorVethName)
		}

		if err := NetNSDo(ctx, interior, func(context.Context) error {
			actor := linkByName(t, ActorVethName)
			if actor == nil {
				t.Fatalf("actor veth %q missing from the interior netns", ActorVethName)
			}
			if !hasAddr(t, actor, ActorVethCIDR) {
				t.Errorf("actor veth %q does not carry %s", ActorVethName, ActorVethCIDR)
			}
			if actor.Attrs().Flags&1 == 0 {
				t.Errorf("actor veth %q is not up", ActorVethName)
			}

			if lo := linkByName(t, "lo"); lo == nil {
				t.Error("interior netns has no loopback")
			} else if lo.Attrs().Flags&1 == 0 {
				t.Error("interior loopback is not up")
			}

			routes, err := netlink.RouteList(actor, netlink.FAMILY_V4)
			if err != nil {
				t.Fatalf("listing interior routes: %v", err)
			}
			// A default route reports its destination either as nil or as an
			// explicit 0.0.0.0/0, depending on how the kernel rendered it.
			isDefault := func(route netlink.Route) bool {
				if route.Dst == nil {
					return true
				}
				ones, _ := route.Dst.Mask.Size()
				return ones == 0
			}
			var haveDefault bool
			for _, route := range routes {
				if isDefault(route) && route.Gw.Equal(ActorVethGwIP) {
					haveDefault = true
				}
			}
			if !haveDefault {
				t.Errorf("interior netns has no default route via %s, got %v", ActorVethGateway, routes)
			}
			return nil
		}); err != nil {
			t.Fatalf("inspecting interior netns: %v", err)
		}
	})
}

// TestSetupActorNetworkIsRepeatable covers the activation cycle a reused worker
// runs: set up, tear down, set up again. The second setup has to succeed against
// whatever the first one left behind.
func TestSetupActorNetworkIsRepeatable(t *testing.T) {
	roottest.Require(t, "creating network namespaces, veth pairs, and nftables rules")
	ctx := context.Background()

	withTestNetNS(t, func(interior netns.NsHandle) {
		requireNftables(t)

		for i := range 3 {
			if err := SetupActorNetwork(ctx, NetworkConfig{InteriorNetNS: interior}); err != nil {
				t.Fatalf("SetupActorNetwork (activation %d): %v", i, err)
			}
			if linkByName(t, HostVethName) == nil {
				t.Fatalf("host veth %q missing after activation %d", HostVethName, i)
			}
			if !actorNftTableExists(t) {
				t.Fatalf("nftables table %q missing after activation %d", ActorNftTableName, i)
			}
			if err := CleanupActorNetwork(ctx, interior); err != nil {
				t.Fatalf("CleanupActorNetwork (activation %d): %v", i, err)
			}
			// Install and teardown have to name the same family. When they do not,
			// teardown's dump comes back empty, its "missing tables are already
			// clean" path reports success, and the table survives -- so the next
			// activation stacks another copy of every chain and rule onto it and
			// the leak is invisible to every other assertion here.
			if actorNftTableExists(t) {
				t.Fatalf("nftables table %q survived cleanup after activation %d", ActorNftTableName, i)
			}
		}

		// Cleanup is idempotent: the extra call after the loop's last one must
		// still succeed, and both ends must be gone.
		if err := CleanupActorNetwork(ctx, interior); err != nil {
			t.Fatalf("CleanupActorNetwork on an already-clean network: %v", err)
		}
		if stray := linkByName(t, HostVethName); stray != nil {
			t.Errorf("host veth %q survived cleanup", HostVethName)
		}
		if actorNftTableExists(t) {
			t.Errorf("nftables table %q survived a repeated cleanup", ActorNftTableName)
		}
		if err := NetNSDo(ctx, interior, func(context.Context) error {
			if stray := linkByName(t, ActorVethName); stray != nil {
				t.Errorf("actor veth %q survived cleanup", ActorVethName)
			}
			return nil
		}); err != nil {
			t.Fatalf("inspecting interior netns: %v", err)
		}
	})
}

// TestRemoveActorNftablesRulesSweepsIPv4Family covers the upgrade case: a
// worker whose previous ateom created the actor table in the ip family. Table
// names are unique per family, so an inet-only cleanup could never see that
// table, and it would have kept redirecting alongside the inet one installed
// next to it.
func TestRemoveActorNftablesRulesSweepsIPv4Family(t *testing.T) {
	roottest.Require(t, "creating network namespaces and nftables rules")

	withTestNetNS(t, func(netns.NsHandle) {
		requireNftables(t)

		c := &nftables.Conn{}
		c.AddTable(&nftables.Table{Family: nftables.TableFamilyIPv4, Name: ActorNftTableName})
		if err := c.Flush(); err != nil {
			t.Fatalf("creating the stand-in ip actor table: %v", err)
		}

		if err := RemoveActorNftablesRules(); err != nil {
			t.Fatalf("RemoveActorNftablesRules: %v", err)
		}

		tables, err := c.ListTablesOfFamily(nftables.TableFamilyIPv4)
		if err != nil {
			t.Fatalf("listing ip nftables tables: %v", err)
		}
		for _, table := range tables {
			if table.Name == ActorNftTableName {
				t.Fatal("the ip actor table survived cleanup")
			}
		}
	})
}

// TestSetupActorNetworkInstallsEgressRedirect covers the rule no other test in
// this package builds: they all leave EgressRedirectPort zero, so the kernel
// never sees either redirect. Their acceptance is not implied by the masquerade
// rule next to them -- redirect in the inet family is separate kernel support
// from the nat chain type -- and they are what the whole actor egress path
// rides on.
func TestSetupActorNetworkInstallsEgressRedirect(t *testing.T) {
	roottest.Require(t, "creating network namespaces, veth pairs, and nftables rules")
	ctx := context.Background()

	const egressPort = 15001

	withTestNetNS(t, func(interior netns.NsHandle) {
		requireNftables(t)

		if err := SetupActorNetwork(ctx, NetworkConfig{
			InteriorNetNS:      interior,
			EgressRedirectPort: egressPort,
		}); err != nil {
			t.Fatalf("SetupActorNetwork: %v", err)
		}

		c := &nftables.Conn{}
		rules, err := c.GetRules(
			&nftables.Table{Family: nftables.TableFamilyINet, Name: ActorNftTableName},
			&nftables.Chain{Name: "prerouting"},
		)
		if err != nil {
			t.Fatalf("listing prerouting rules of the actor table: %v", err)
		}
		if len(rules) != 2 {
			t.Fatalf("prerouting holds %d rules, want one redirect per family", len(rules))
		}

		// Read back what the kernel stored rather than what the builder emitted:
		// TestActorNftablesRuleExprs already pins the builder, and what is in
		// doubt here is whether an inet nat chain takes these expressions at all.
		// Both rules are installed whatever the pod's families are; the one whose
		// source address the actor never gets simply matches nothing.
		for i, rule := range rules {
			var nfproto []byte
			var havePort, haveRedir bool
			for _, e := range rule.Exprs {
				switch e := e.(type) {
				case *expr.Cmp:
					if nfproto == nil && len(e.Data) == 1 {
						nfproto = e.Data
					}
				case *expr.Immediate:
					havePort = havePort || bytes.Equal(e.Data, binaryutil.BigEndian.PutUint16(egressPort))
				case *expr.Redir:
					haveRedir = true
				}
			}
			wantNFProto := []byte{unix.NFPROTO_IPV4}
			if i == 1 {
				wantNFProto = []byte{unix.NFPROTO_IPV6}
			}
			if !bytes.Equal(nfproto, wantNFProto) || !havePort || !haveRedir {
				t.Errorf("prerouting rule %d has nfproto=%v port=%t redir=%t, want nfproto=%v and both, got %v",
					i, nfproto, havePort, haveRedir, wantNFProto, rule.Exprs)
			}
		}
	})
}

// addPodEth0 plants a dummy link carrying cidrs in the current netns, standing
// in for the worker pod's own primary interface. withTestNetNS hands out a bare
// namespace, and the families on that interface are what SetupActorNetwork reads
// to decide the families the actor gets.
//
// The name has to be exactly podPrimaryIfaceName: the probe is link-scoped, so
// under any other name it answers false and the test asserts the opposite of
// what it means to.
func addPodEth0(t *testing.T, cidrs ...string) {
	t.Helper()

	link := &netlink.Dummy{LinkAttrs: netlink.LinkAttrs{Name: podPrimaryIfaceName}}
	if err := netlink.LinkAdd(link); err != nil {
		t.Fatalf("creating the stand-in pod %s: %v", podPrimaryIfaceName, err)
	}
	if err := netlink.LinkSetUp(link); err != nil {
		t.Fatalf("bringing up the stand-in pod %s: %v", podPrimaryIfaceName, err)
	}
	for _, cidr := range cidrs {
		addr := MustParseAddr(cidr)
		addr.Flags |= unix.IFA_F_NODAD // else an IPv6 address stays tentative
		if err := netlink.AddrAdd(link, addr); err != nil {
			t.Fatalf("assigning %s to the stand-in pod %s: %v", cidr, podPrimaryIfaceName, err)
		}
	}
}

// writeSysctl turns an IPv6 knob off in the current netns. "all" flushes the
// addresses already assigned; "default" only reaches links created afterwards.
func writeSysctl(t *testing.T, knob string) {
	t.Helper()
	path := "/proc/sys/net/ipv6/conf/" + knob + "/disable_ipv6"
	if err := os.WriteFile(path, []byte("1\n"), 0o644); err != nil {
		t.Fatalf("disabling IPv6 via %s: %v", path, err)
	}
}

// assertIPv6AddrNoDAD requires cidr to be present on link and to carry
// IFA_F_NODAD.
//
// The flag is the whole point: the ateom container is unprivileged, so the
// accept_dad sysctl this replaced could not be written and setup failed outright
// on a real worker. It passes as root, where /proc/sys is writable either way,
// so nothing else here would catch a regression back to the sysctl.
func assertIPv6AddrNoDAD(t *testing.T, link netlink.Link, cidr string) {
	t.Helper()
	addrs, err := netlink.AddrList(link, netlink.FAMILY_V6)
	if err != nil {
		t.Fatalf("listing IPv6 addresses of %q: %v", link.Attrs().Name, err)
	}
	want := MustParseAddr(cidr)
	for _, addr := range addrs {
		if addr.IPNet == nil || addr.IPNet.String() != want.IPNet.String() {
			continue
		}
		if addr.Flags&unix.IFA_F_NODAD == 0 {
			t.Errorf("%s on %q has flags %#x, want IFA_F_NODAD (%#x) set", cidr, link.Attrs().Name, addr.Flags, unix.IFA_F_NODAD)
		}
		return
	}
	t.Errorf("%q does not carry %s, got %v", link.Attrs().Name, cidr, addrs)
}

// assertNoGlobalIPv6Addr requires link to carry no IPv6 address beyond the
// fe80::/64 the kernel gives every up link wherever IPv6 is enabled at all.
// That link-local is not what strands an actor -- the routable address is.
func assertNoGlobalIPv6Addr(t *testing.T, link netlink.Link) {
	t.Helper()
	addrs, err := netlink.AddrList(link, netlink.FAMILY_V6)
	if err != nil {
		t.Fatalf("listing IPv6 addresses of %q: %v", link.Attrs().Name, err)
	}
	for _, addr := range addrs {
		if addr.IP.IsGlobalUnicast() {
			t.Errorf("%q carries global IPv6 address %s, want none", link.Attrs().Name, addr)
		}
	}
}

// assertDefaultRoute requires link to carry -- or, when want is false, to not
// carry -- a default route via gw in the given family.
func assertDefaultRoute(t *testing.T, link netlink.Link, family int, gw net.IP, want bool) {
	t.Helper()

	dst := "0.0.0.0/0"
	if family == netlink.FAMILY_V6 {
		dst = "::/0"
	}
	routes, err := netlink.RouteList(link, family)
	if err != nil {
		t.Fatalf("listing %s routes of %q: %v", dst, link.Attrs().Name, err)
	}
	var got bool
	for _, route := range routes {
		// A default route reports its destination either as nil or as an
		// explicit zero-length mask, depending on how the kernel rendered it.
		ones := 0
		if route.Dst != nil {
			ones, _ = route.Dst.Mask.Size()
		}
		if ones == 0 && route.Gw.Equal(gw) {
			got = true
		}
	}
	switch {
	case want && !got:
		t.Errorf("%q has no %s route via %s, got %v", link.Attrs().Name, dst, gw, routes)
	case !want && got:
		t.Errorf("%q has a %s route via %s, want none, got %v", link.Attrs().Name, dst, gw, routes)
	}
}

// TestSetupActorNetworkIPv6Gate is the truth table for who gets actor IPv6.
// Both halves have to hold: the worker pod needs a global IPv6 address of its
// own, or the actor prefers the AAAA of a dual-stack destination and the
// connection dies with nowhere to go; and the veth has to accept an IPv6
// address at all, or the assignment fails with EPERM on the path of every
// SetupActorNetwork call and the actor never starts.
//
// The IPv4 half must come out identical in every case.
func TestSetupActorNetworkIPv6Gate(t *testing.T) {
	roottest.Require(t, "creating network namespaces, veth pairs, and nftables rules")
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		// podAddrs go on the stand-in pod interface before setup runs.
		podAddrs []string
		// disable, when set, runs in the pod netns after podAddrs are assigned.
		disable  func(*testing.T)
		wantIPv6 bool
	}{{
		name:     "dual-stack pod",
		podAddrs: []string{"10.244.0.7/24", "fd00:10:244::7/64"},
		wantIPv6: true,
	}, {
		// A probe that reads the wrong link or the wrong scope fails closed,
		// which every IPv4 case here would happily accept. This one notices.
		name:     "IPv6-only pod",
		podAddrs: []string{"fd00:10:244::7/64"},
		wantIPv6: true,
	}, {
		// An IPv4-only cluster whose kernel still has IPv6 compiled in, so every
		// capability probe says yes. This is the case that turned the IPv4 e2e
		// job red.
		name:     "pod without IPv6",
		podAddrs: []string{"10.244.0.7/24"},
	}, {
		// The default on IPv4-only GKE. Writing "all" also flushes podAddrs, so
		// both halves of the gate are false here.
		name:     "IPv6 disabled for the whole netns",
		podAddrs: []string{"10.244.0.7/24", "fd00:10:244::7/64"},
		disable: func(t *testing.T) {
			writeSysctl(t, "all")
			writeSysctl(t, "default")
		},
	}, {
		// The one case the capability half is there for: the pod keeps its
		// address, but the veth created next inherits disable_ipv6=1.
		name:     "IPv6 disabled per link",
		podAddrs: []string{"10.244.0.7/24", "fd00:10:244::7/64"},
		disable:  func(t *testing.T) { writeSysctl(t, "default") },
	}} {
		t.Run(tc.name, func(t *testing.T) {
			withTestNetNS(t, func(interior netns.NsHandle) {
				requireNftables(t)

				addPodEth0(t, tc.podAddrs...)
				if tc.disable != nil {
					tc.disable(t)
				}

				if err := SetupActorNetwork(ctx, NetworkConfig{InteriorNetNS: interior}); err != nil {
					t.Fatalf("SetupActorNetwork: %v", err)
				}

				host := linkByName(t, HostVethName)
				if host == nil {
					t.Fatalf("host veth %q missing from the pod netns", HostVethName)
				}
				if !hasAddr(t, host, HostVethCIDR) {
					t.Errorf("host veth %q does not carry %s", HostVethName, HostVethCIDR)
				}
				if tc.wantIPv6 {
					assertIPv6AddrNoDAD(t, host, HostVethIPv6CIDR)
				} else {
					assertNoGlobalIPv6Addr(t, host)
				}

				if err := NetNSDo(ctx, interior, func(context.Context) error {
					actor := linkByName(t, ActorVethName)
					if actor == nil {
						t.Fatalf("actor veth %q missing from the interior netns", ActorVethName)
					}
					if !hasAddr(t, actor, ActorVethCIDR) {
						t.Errorf("actor veth %q does not carry %s", ActorVethName, ActorVethCIDR)
					}
					assertDefaultRoute(t, actor, netlink.FAMILY_V4, ActorVethGwIP, true)

					// The interior netns is created fresh, so its own sysctls always
					// say IPv6 is available whatever the pod's families are. Only a
					// decision carried across from the pod netns gets this right.
					if tc.wantIPv6 {
						assertIPv6AddrNoDAD(t, actor, ActorVethIPv6CIDR)
					} else {
						assertNoGlobalIPv6Addr(t, actor)
					}
					assertDefaultRoute(t, actor, netlink.FAMILY_V6, ActorVethIPv6GwIP, tc.wantIPv6)
					return nil
				}); err != nil {
					t.Fatalf("inspecting interior netns: %v", err)
				}
			})
		})
	}
}

// TestSetupActorNetworkHostVethHWAddr covers the micro-VM requirement: a CH
// snapshot freezes the guest's ARP entry for the gateway, so the worker-side
// veth MAC has to be exactly the one the caller asked for, on every pod.
func TestSetupActorNetworkHostVethHWAddr(t *testing.T) {
	roottest.Require(t, "creating network namespaces, veth pairs, and nftables rules")
	ctx := context.Background()

	withTestNetNS(t, func(interior netns.NsHandle) {
		requireNftables(t)

		want := MustParseMAC("02:a8:1e:00:00:01")
		if err := SetupActorNetwork(ctx, NetworkConfig{
			InteriorNetNS:      interior,
			HostVethHWAddr:     want,
			SweepInteriorLinks: true,
		}); err != nil {
			t.Fatalf("SetupActorNetwork: %v", err)
		}

		host := linkByName(t, HostVethName)
		if host == nil {
			t.Fatalf("host veth %q missing from the pod netns", HostVethName)
		}
		if got := host.Attrs().HardwareAddr.String(); got != want.String() {
			t.Errorf("host veth MAC = %s, want %s", got, want)
		}
	})
}

// TestSetupActorNetworkSweepsInteriorLinks covers the other half of the micro-VM
// path: SweepInteriorLinks clears a previous activation's leftovers (kata's tap
// device) before the new pair is created, and must not take the loopback or the
// freshly created actor veth with it.
func TestSetupActorNetworkSweepsInteriorLinks(t *testing.T) {
	roottest.Require(t, "creating network namespaces, veth pairs, and nftables rules")
	ctx := context.Background()

	withTestNetNS(t, func(interior netns.NsHandle) {
		requireNftables(t)

		const leftover = "stale-tap0"
		if err := NetNSDo(ctx, interior, func(context.Context) error {
			return netlink.LinkAdd(&netlink.Dummy{LinkAttrs: netlink.LinkAttrs{Name: leftover}})
		}); err != nil {
			t.Fatalf("planting a leftover interior link: %v", err)
		}

		if err := SetupActorNetwork(ctx, NetworkConfig{
			InteriorNetNS:      interior,
			SweepInteriorLinks: true,
		}); err != nil {
			t.Fatalf("SetupActorNetwork: %v", err)
		}

		if err := NetNSDo(ctx, interior, func(context.Context) error {
			if stray := linkByName(t, leftover); stray != nil {
				t.Errorf("leftover interior link %q was not swept", leftover)
			}
			if linkByName(t, ActorVethName) == nil {
				t.Errorf("actor veth %q missing after a sweeping setup", ActorVethName)
			}
			if linkByName(t, "lo") == nil {
				t.Error("sweep removed the interior loopback")
			}
			return nil
		}); err != nil {
			t.Fatalf("inspecting interior netns: %v", err)
		}
	})
}
