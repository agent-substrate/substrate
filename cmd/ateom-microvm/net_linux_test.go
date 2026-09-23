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

import (
	"context"
	"testing"

	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"

	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/roottest"
)

func TestPrepareSandboxNetworkReplacesSameActor(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	ctx := context.Background()
	egress, err := atunnel.NewEgress(atunnel.TCPOriginalDestination)
	if err != nil {
		t.Fatal(err)
	}
	dns, err := atunnel.NewDNSRelay([]string{"127.0.0.1:53"})
	if err != nil {
		t.Fatal(err)
	}
	service := &AteomService{atunnelEgress: egress, atunnelEgressPort: 15001, dnsRelay: dns}
	t.Cleanup(func() {
		if err := service.releaseSandboxNetwork(ctx); err != nil {
			t.Error(err)
		}
	})
	const actorUID = "microvm-network-replace"
	for range 2 {
		if err := service.prepareSandboxNetwork(ctx, actorUID); err != nil {
			t.Fatal(err)
		}
	}
	named, err := netns.GetFromName(ateompath.ActorNetNSName(actorUID))
	if err != nil {
		t.Fatalf("opening replacement namespace by name: %v", err)
	}
	defer named.Close()
	if !named.Equal(service.sandboxNetNS()) {
		t.Fatal("namespace name does not refer to the replacement")
	}
}

// tapNetNS gives a test its own namespace to build a tap in.
func tapNetNS(t *testing.T, name string) netns.NsHandle {
	t.Helper()
	ns, err := ateomnet.CreateNetNSWithoutSwitching(name)
	if err != nil {
		t.Fatalf("creating namespace: %v", err)
	}
	t.Cleanup(func() {
		ns.Close()
		_ = netns.DeleteNamed(name)
	})
	return ns
}

// The tap is the micro-VM's whole boundary: cloud-hypervisor adopts the
// descriptors, and the kernel keeps the interface side, carrying the gateway
// address and the MAC the guest's snapshot froze into its ARP cache.
func TestSetupActorTap(t *testing.T) {
	roottest.Require(t, "creates network namespaces and tap devices")
	ctx := context.Background()
	ns := tapNetNS(t, "microvm-tap-test")

	fds, err := setupActorTap(ctx, ns, "tap0_kata", 1)
	if err != nil {
		t.Fatalf("setupActorTap: %v", err)
	}
	t.Cleanup(func() {
		for _, f := range fds {
			_ = f.Close()
		}
	})
	if len(fds) != 1 {
		t.Errorf("got %d descriptors, want one per queue pair", len(fds))
	}

	if err := ateomnet.NetNSDo(ctx, ns, func(context.Context) error {
		link, err := netlink.LinkByName("tap0_kata")
		if err != nil {
			return err
		}
		if got := link.Attrs().HardwareAddr.String(); got != gatewayMAC {
			t.Errorf("tap MAC = %s, want the fixed %s the guest's frozen ARP entry names", got, gatewayMAC)
		}
		if link.Attrs().Flags&1 == 0 { // net.FlagUp
			t.Error("tap is down")
		}
		if got := link.Attrs().MTU; got != actorTapMTU {
			t.Errorf("tap MTU = %d, want %d, the value the guest was snapshotted against", got, actorTapMTU)
		}
		addrs, err := netlink.AddrList(link, netlink.FAMILY_V4)
		if err != nil {
			return err
		}
		var found bool
		for _, a := range addrs {
			if a.IPNet.String() == ateomnet.HostVethAddr.IPNet.String() {
				found = true
			}
		}
		if !found {
			t.Errorf("tap addresses = %v, want the gateway %s the guest routes to", addrs, ateomnet.HostVethAddr)
		}
		return nil
	}); err != nil {
		t.Fatalf("inspecting the tap: %v", err)
	}

	if got := actorTapMTUOf(ctx, ns, "tap0_kata"); got != actorTapMTU {
		t.Errorf("actorTapMTUOf = %d, want %d", got, actorTapMTU)
	}
}

// A restore rebuilds the tap in a namespace that may still hold the previous
// one, and cloud-hypervisor needs fresh descriptors for its fd-backed
// virtio-net either way.
func TestSetupActorTapReplacesALeftover(t *testing.T) {
	roottest.Require(t, "creates network namespaces and tap devices")
	ctx := context.Background()
	ns := tapNetNS(t, "microvm-tap-replace")

	first, err := setupActorTap(ctx, ns, "tap0_kata", 1)
	if err != nil {
		t.Fatalf("first setupActorTap: %v", err)
	}
	for _, f := range first {
		defer f.Close()
	}

	second, err := setupActorTap(ctx, ns, "tap0_kata", 2)
	if err != nil {
		t.Fatalf("second setupActorTap over a leftover: %v", err)
	}
	for _, f := range second {
		defer f.Close()
	}
	if len(second) != 2 {
		t.Errorf("got %d descriptors, want one per queue pair", len(second))
	}
	if first[0].Fd() == second[0].Fd() {
		t.Error("the replacement reused the first tap's descriptor")
	}
}
