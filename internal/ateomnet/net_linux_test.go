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
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/agent-substrate/substrate/internal/roottest"
	"github.com/google/nftables"
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

// requireNftables skips when the kernel in this environment cannot serve the
// nftables netlink API at all, which SetupActorNetwork needs and which is a
// property of the machine rather than of the code under test.
func requireNftables(t *testing.T) {
	t.Helper()
	c := &nftables.Conn{}
	if _, err := c.ListTablesOfFamily(nftables.TableFamilyIPv4); err != nil {
		t.Skipf("nftables unavailable in this environment: %v", err)
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
		assertIPv6AddrNoDAD(t, host, HostVethIPv6CIDR)

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
			assertIPv6AddrNoDAD(t, actor, ActorVethIPv6CIDR)

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
